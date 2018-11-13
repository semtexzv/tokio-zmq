/*
 * This file is part of Futures ZMQ.
 *
 * Copyright © 2018 Riley Trautman
 *
 * Futures ZMQ is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Futures ZMQ is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Futures ZMQ.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
};

use async_zmq_types::Multipart;
use futures::{
    executor::{self, Notify},
    sync::oneshot,
    Async, Future, Poll,
};
use zmq::{poll, PollEvents, PollItem, Socket, DONTWAIT, POLLIN, POLLOUT, SNDMORE};

use crate::error::Error;

enum Request {
    Init(Socket, oneshot::Sender<usize>),
    SendMessage(usize, Multipart, oneshot::Sender<Response>),
    ReceiveMessage(usize, oneshot::Sender<Response>),
    Done,
}

enum Response {
    Sent,
    Received(Multipart),
    Error(Error),
}

enum PollKind {
    SendMsg,
    RecvMsg,
    SendRecv,
    UNUSED,
}

impl PollKind {
    fn as_events(&self) -> PollEvents {
        match *self {
            PollKind::SendMsg => POLLOUT,
            PollKind::RecvMsg => POLLIN,
            PollKind::SendRecv => POLLIN | POLLOUT,
            _ => 0,
        }
    }

    fn is_read(&self) -> bool {
        self.as_events() & POLLIN == POLLIN
    }

    fn is_write(&self) -> bool {
        self.as_events() & POLLOUT == POLLOUT
    }

    fn read(&mut self) {
        match *self {
            PollKind::SendMsg => *self = PollKind::SendRecv,
            _ => *self = PollKind::RecvMsg,
        }
    }

    fn clear_read(&mut self) {
        match *self {
            PollKind::SendRecv | PollKind::SendMsg => *self = PollKind::SendMsg,
            _ => *self = PollKind::UNUSED,
        }
    }

    fn write(&mut self) {
        match *self {
            PollKind::RecvMsg => *self = PollKind::SendRecv,
            _ => *self = PollKind::SendMsg,
        }
    }

    fn clear_write(&mut self) {
        match *self {
            PollKind::SendRecv | PollKind::RecvMsg => *self = PollKind::RecvMsg,
            _ => *self = PollKind::UNUSED,
        }
    }
}

struct Channel {
    ready: AtomicBool,
    tx: TcpStream,
    rx: TcpStream,
}

impl Channel {
    fn swap_false(&self) -> bool {
        self.ready.swap(false, Ordering::SeqCst)
    }

    fn swap_true(&self) -> bool {
        self.ready.swap(true, Ordering::SeqCst)
    }

    fn notify(&self) {
        if !self.swap_true() {
            let write_res = (&self.tx).write(&[1]);

            drop(write_res);
        }
    }
}

#[derive(Clone)]
struct Sender {
    tx: mpsc::Sender<Request>,
    channel: Arc<Channel>,
}

impl Sender {
    fn send(&self, request: Request) {
        let _ = self.tx.send(request);
        self.channel.notify();
    }
}

struct Receiver {
    rx: mpsc::Receiver<Request>,
    channel: Arc<Channel>,
}

impl Receiver {
    fn try_recv(&self) -> Option<Request> {
        self.rx.try_recv().ok()
    }

    /// Returns whether there are messages to look at
    fn drain(&self) -> bool {
        if !self.channel.swap_false() {
            return false;
        }

        loop {
            match (&self.channel.rx).read(&mut [0; 32]) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("I/O error: {}", e),
            }
        }

        return true;
    }
}

#[derive(Clone)]
pub struct Session {
    inner: Arc<InnerSession>,
}

impl Session {
    pub fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let conn1 = TcpStream::connect(&addr).unwrap();
        let conn2 = listener.accept().unwrap().0;

        drop(listener);

        conn1.set_nonblocking(true).unwrap();
        conn2.set_nonblocking(true).unwrap();

        let channel = Arc::new(Channel {
            ready: AtomicBool::new(false),
            tx: conn1,
            rx: conn2,
        });

        let (tx, rx) = mpsc::channel();

        let tx = Sender {
            tx: tx,
            channel: channel.clone(),
        };
        let rx = Receiver {
            rx: rx,
            channel: channel,
        };

        thread::spawn(move || {
            PollThread::new(rx).run();
        });

        Session {
            inner: InnerSession::init(tx),
        }
    }

    pub fn send(&self, id: usize, msg: Multipart) -> SendFuture {
        let (tx, rx) = oneshot::channel();

        self.inner.send(Request::SendMessage(id, msg, tx));

        SendFuture { rx }
    }

    pub fn recv(&self, id: usize) -> RecvFuture {
        let (tx, rx) = oneshot::channel();

        self.inner.send(Request::ReceiveMessage(id, tx));

        RecvFuture { rx }
    }

    pub fn init(&self, sock: Socket) -> InitFuture {
        let (tx, rx) = oneshot::channel();

        self.inner.send(Request::Init(sock, tx));

        InitFuture { rx }
    }
}

pub struct SendFuture {
    rx: oneshot::Receiver<Response>,
}

impl Future for SendFuture {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll()? {
            Async::Ready(res) => match res {
                Response::Sent => Ok(Async::Ready(())),
                Response::Error(e) => Err(e),
                _ => panic!("Response kind was not sent"),
            },
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub struct RecvFuture {
    rx: oneshot::Receiver<Response>,
}

impl Future for RecvFuture {
    type Item = Multipart;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll()? {
            Async::Ready(res) => match res {
                Response::Received(msg) => Ok(Async::Ready(msg)),
                Response::Error(e) => Err(e),
                _ => panic!("Response kind was not received"),
            },
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub struct InitFuture {
    rx: oneshot::Receiver<usize>,
}

impl Future for InitFuture {
    type Item = usize;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.rx.poll()?)
    }
}

struct InnerSession {
    tx: Mutex<Sender>,
}

impl InnerSession {
    fn init(tx: Sender) -> Arc<Self> {
        Arc::new(InnerSession { tx: Mutex::new(tx) })
    }

    fn send(&self, request: Request) {
        self.tx.lock().unwrap().clone().send(request);
    }
}

impl Drop for InnerSession {
    fn drop(&mut self) {
        self.tx.lock().unwrap().clone().send(Request::Done);
    }
}

#[derive(Clone)]
struct NotifyCanceled {
    channel: Arc<Channel>,
}

impl NotifyCanceled {
    fn new(channel: Arc<Channel>) -> Self {
        NotifyCanceled { channel }
    }
}

impl Notify for NotifyCanceled {
    fn notify(&self, _id: usize) {
        self.channel.notify();
    }
}

struct CheckCanceled<'a> {
    sender: &'a mut oneshot::Sender<Response>,
}

impl<'a> Future for CheckCanceled<'a> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        self.sender.poll_cancel()
    }
}

struct Pollable {
    sock: Socket,
    kind: PollKind,
    msg: Option<Multipart>,
    pending_recv_msg: Option<Multipart>,
    send_responder: Option<oneshot::Sender<Response>>,
    recv_responder: Option<oneshot::Sender<Response>>,
}

impl Pollable {
    fn new(sock: Socket) -> Self {
        Pollable {
            sock,
            kind: PollKind::UNUSED,
            msg: None,
            pending_recv_msg: None,
            send_responder: None,
            recv_responder: None,
        }
    }

    fn as_poll_item(&self) -> PollItem {
        self.sock.as_poll_item(self.kind.as_events())
    }

    fn is_readable(&self, poll_item: &PollItem) -> bool {
        self.kind.is_read() && poll_item.is_readable()
    }

    fn is_writable(&self, poll_item: &PollItem) -> bool {
        self.kind.is_write() && poll_item.is_writable()
    }

    fn read(&mut self) {
        self.kind.read();
    }

    fn clear_read(&mut self) {
        self.kind.clear_read();
    }

    fn write(&mut self) {
        self.kind.write();
    }

    fn clear_write(&mut self) {
        self.kind.clear_write();
    }

    fn message(&mut self, msg: Multipart) {
        self.msg = Some(msg);
    }

    fn send_responder(&mut self, r: oneshot::Sender<Response>) {
        self.send_responder = Some(r);
    }

    fn recv_responder(&mut self, r: oneshot::Sender<Response>) {
        self.recv_responder = Some(r);
    }

    fn recv_msg(&mut self) {
        let mut multipart = self.pending_recv_msg.take().unwrap_or(Multipart::new());

        loop {
            match self.sock.recv_msg(DONTWAIT) {
                Ok(msg) => {
                    let get_more = msg.get_more();
                    multipart.push_back(msg);

                    if get_more {
                        continue;
                    }
                    trace!("Received msg");
                    self.clear_read();
                    if let Err(_) = self
                        .recv_responder
                        .take()
                        .unwrap()
                        .send(Response::Received(multipart))
                    {
                        error!("Error responding with received message");
                    }
                    break;
                }
                Err(e) => match e {
                    zmq::Error::EAGAIN => {
                        warn!("EAGAIN while receiving");
                        self.pending_recv_msg = Some(multipart);
                        break;
                    }
                    e => {
                        error!("Error receiving message");
                        self.clear_read();
                        if let Err(_) = self
                            .recv_responder
                            .take()
                            .unwrap()
                            .send(Response::Error(e.into()))
                        {
                            error!("Error responding with error");
                        }
                        break;
                    }
                },
            }
        }
    }

    fn send_msg(&mut self) {
        if let Some(mut multipart) = self.msg.take() {
            loop {
                if let Some(msg) = multipart.pop_front() {
                    let flags = DONTWAIT | if multipart.is_empty() { 0 } else { SNDMORE };

                    match self.sock.send_msg(msg, flags) {
                        Ok(_) => {
                            trace!("Sent message");
                            if !multipart.is_empty() {
                                continue;
                            }

                            self.clear_write();
                            if let Err(_) = self.send_responder.take().unwrap().send(Response::Sent)
                            {
                                error!("Error responding with sent");
                            }
                            break;
                        }
                        Err(e) => match e {
                            zmq::Error::EAGAIN => {
                                warn!("EAGAIN while sendign");
                                self.msg = Some(multipart);
                                break;
                            }
                            e => {
                                self.clear_write();
                                error!("Error sending message");
                                if let Err(_) = self
                                    .send_responder
                                    .take()
                                    .unwrap()
                                    .send(Response::Error(e.into()))
                                {
                                    error!("Error responding with error");
                                }
                                break;
                            }
                        },
                    }
                }
            }
        }
    }
}

enum Action {
    Snd(usize),
    Rcv(usize),
}

struct PollThread {
    next_sock_id: usize,
    rx: Receiver,
    should_stop: bool,
    to_action: Vec<Action>,
    notify: Arc<NotifyCanceled>,
    sockets: BTreeMap<usize, Pollable>,
}

impl PollThread {
    fn new(rx: Receiver) -> Self {
        let channel = rx.channel.clone();

        PollThread {
            next_sock_id: 0,
            rx,
            should_stop: false,
            to_action: Vec::new(),
            notify: Arc::new(NotifyCanceled::new(channel)),
            sockets: BTreeMap::new(),
        }
    }

    fn run(&mut self) {
        loop {
            self.turn();
        }
    }

    fn try_recv(&mut self) {
        if self.rx.drain() {
            trace!("new messages to handle");
            while let Some(msg) = self.rx.try_recv() {
                self.handle_request(msg);

                if self.should_stop {
                    break;
                }
            }
        }
    }

    fn handle_request(&mut self, request: Request) {
        match request {
            Request::Init(sock, responder) => {
                let id = self.next_sock_id;

                self.sockets.insert(id, Pollable::new(sock));
                responder.send(id).unwrap();

                self.next_sock_id += 1;
            }
            Request::SendMessage(id, message, responder) => {
                trace!("Handling send");
                self.sockets.get_mut(&id).map(|pollable| {
                    pollable.write();
                    pollable.message(message);
                    pollable.send_responder(responder);
                });
            }
            Request::ReceiveMessage(id, responder) => {
                trace!("Handling recv");
                self.sockets.get_mut(&id).map(|pollable| {
                    pollable.read();
                    pollable.recv_responder(responder);
                });
            }
            Request::Done => {
                trace!("Handling done");
                self.should_stop = true;
            }
        }
    }

    fn check_responder(
        notify: &Arc<NotifyCanceled>,
        sender: &mut oneshot::Sender<Response>,
    ) -> bool {
        let mut cancel_check = executor::spawn(CheckCanceled { sender });

        if let Ok(Async::Ready(())) = cancel_check.poll_future_notify(notify, 0) {
            true
        } else {
            false
        }
    }

    fn drop_inactive(&mut self) {
        for ref mut pollable in self.sockets.values_mut() {
            if let Some(mut responder) = pollable.recv_responder.take() {
                let to_clear = Self::check_responder(&self.notify, &mut responder);

                if !to_clear {
                    pollable.recv_responder(responder);
                }
            }

            if let Some(mut responder) = pollable.send_responder.take() {
                let to_clear = Self::check_responder(&self.notify, &mut responder);

                if !to_clear {
                    pollable.send_responder(responder);
                }
            }
        }
    }

    fn poll(&mut self) {
        self.to_action.truncate(0);

        let (ids, mut poll_items): (Vec<_>, Vec<_>) = self
            .sockets
            .iter()
            .map(|(id, pollable)| (id, pollable.as_poll_item()))
            .unzip();

        let num_signalled = poll(&mut poll_items, 10).unwrap();

        let mut count = 0;
        for (id, item) in ids.into_iter().zip(poll_items) {
            // Prioritize outbound messages over inbound messages
            if self
                .sockets
                .get(&id)
                .map(|p| p.is_writable(&item))
                .unwrap_or(false)
            {
                trace!("{} is writable", id);
                self.to_action.push(Action::Snd(id));

                count += 1;
            } else if self
                .sockets
                .get(&id)
                .map(|p| p.is_readable(&item))
                .unwrap_or(false)
            {
                trace!("{} is readable", id);
                self.to_action.push(Action::Rcv(id));

                count += 1;
            }

            if count >= num_signalled {
                break;
            }
        }

        for action in self.to_action.drain(..).rev() {
            match action {
                Action::Rcv(id) => {
                    self.sockets
                        .get_mut(&id)
                        .map(|pollable| pollable.recv_msg());
                }
                Action::Snd(id) => {
                    self.sockets
                        .get_mut(&id)
                        .map(|pollable| pollable.send_msg());
                }
            }
        }
    }

    fn turn(&mut self) {
        self.drop_inactive();
        self.try_recv();
        self.poll();
    }
}
