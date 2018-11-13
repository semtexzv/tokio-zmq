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
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
};

use futures::{
    executor::{self, Notify},
    sync::oneshot,
    Async, Future, Poll,
};
use zmq::{poll, Message, PollEvents, PollItem, Socket, DONTWAIT, POLLIN, POLLOUT};

use crate::error::Error;

enum Request {
    SendMessage(Socket, Message, i32, oneshot::Sender<Response>),
    ReceiveMessage(Socket, oneshot::Sender<Response>),
    SendReceiveMessage(Socket, Message, i32, oneshot::Sender<Response>),
    Done,
}

impl Request {
    fn poll_kind(&self) -> PollKind {
        match *self {
            Request::SendMessage(_, _, _, _) => PollKind::SendMsg,
            Request::ReceiveMessage(_, _) => PollKind::RecvMsg,
            Request::SendReceiveMessage(_, _, _, _) => PollKind::SendRecv,
            _ => PollKind::UNUSED,
        }
    }
}

enum Response {
    Sent(Socket),
    Received(Socket, Message),
    ReceivedSndRcv(Socket, Message, Message, i32),
    Error(Socket, Error),
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

    fn send_receive(&self) -> bool {
        match *self {
            PollKind::SendRecv => true,
            _ => false,
        }
    }

    fn read(&self) -> bool {
        self.as_events() & POLLIN == POLLIN
    }

    fn write(&self) -> bool {
        self.as_events() & POLLOUT == POLLOUT
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

    pub fn send(&self, sock: Socket, msg: Message, flags: i32) -> SendFuture {
        let (tx, rx) = oneshot::channel();

        self.inner.send(Request::SendMessage(sock, msg, flags, tx));

        SendFuture { rx }
    }

    pub fn recv(&self, sock: Socket) -> RecvFuture {
        let (tx, rx) = oneshot::channel();

        self.inner.send(Request::ReceiveMessage(sock, tx));

        RecvFuture { rx }
    }

    pub fn send_recv(&self, sock: Socket, msg: Message, flags: i32) -> SendRecvFuture {
        let (tx, rx) = oneshot::channel();

        self.inner
            .send(Request::SendReceiveMessage(sock, msg, flags, tx));

        SendRecvFuture { rx }
    }
}

pub struct SendFuture {
    rx: oneshot::Receiver<Response>,
}

impl Future for SendFuture {
    type Item = Socket;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll()? {
            Async::Ready(res) => match res {
                Response::Sent(sock) => Ok(Async::Ready(sock)),
                Response::Error(_, e) => Err(e),
                _ => panic!("Response kind was not received"),
            },
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub struct RecvFuture {
    rx: oneshot::Receiver<Response>,
}

impl Future for RecvFuture {
    type Item = (Socket, Message);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll()? {
            Async::Ready(res) => match res {
                Response::Received(sock, msg) => Ok(Async::Ready((sock, msg))),
                Response::Error(_, e) => Err(e),
                _ => panic!("Response kind was not received"),
            },
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub struct SendRecvFuture {
    rx: oneshot::Receiver<Response>,
}

impl Future for SendRecvFuture {
    type Item = (Socket, Option<(Message, Message, i32)>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll()? {
            Async::Ready(res) => match res {
                Response::ReceivedSndRcv(sock, msg, snd_msg, flags) => {
                    Ok(Async::Ready((sock, Some((msg, snd_msg, flags)))))
                }
                Response::Sent(sock) => Ok(Async::Ready((sock, None))),
                Response::Error(_, e) => Err(e),
                _ => panic!("Response kind was not received"),
            },
            Async::NotReady => Ok(Async::NotReady),
        }
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
    msg: Option<(Message, i32)>,
    responder: oneshot::Sender<Response>,
}

impl Pollable {
    fn as_poll_item(&self) -> PollItem {
        self.sock.as_poll_item(self.kind.as_events())
    }

    fn is_readable(&self, poll_item: &PollItem) -> bool {
        self.kind.read() && poll_item.is_readable()
    }

    fn is_writable(&self, poll_item: &PollItem) -> bool {
        self.kind.write() && poll_item.is_writable()
    }
}

enum Action {
    Snd(usize),
    Rcv(usize),
}

struct PollThread {
    rx: Receiver,
    should_stop: bool,
    to_remove: Vec<usize>,
    to_action: Vec<Action>,
    notify: Arc<NotifyCanceled>,
    sockets: Vec<Pollable>,
}

impl PollThread {
    fn new(rx: Receiver) -> Self {
        let channel = rx.channel.clone();

        PollThread {
            rx,
            should_stop: false,
            to_remove: Vec::new(),
            to_action: Vec::new(),
            notify: Arc::new(NotifyCanceled::new(channel)),
            sockets: Vec::new(),
        }
    }

    fn run(&mut self) {
        loop {
            self.turn();
        }
    }

    fn try_recv(&mut self) {
        if self.rx.drain() {
            while let Some(msg) = self.rx.try_recv() {
                self.handle_request(msg);

                if self.should_stop {
                    break;
                }
            }
        }
    }

    fn handle_request(&mut self, request: Request) {
        let kind = request.poll_kind();

        match request {
            Request::SendMessage(sock, message, flags, responder) => {
                self.sockets.push(Pollable {
                    sock,
                    kind,
                    msg: Some((message, flags)),
                    responder,
                });
            }
            Request::ReceiveMessage(sock, responder) => {
                self.sockets.push(Pollable {
                    sock,
                    kind,
                    msg: None,
                    responder,
                });
            }
            Request::SendReceiveMessage(sock, message, flags, responder) => {
                self.sockets.push(Pollable {
                    sock,
                    kind,
                    msg: Some((message, flags)),
                    responder,
                });
            }
            Request::Done => {
                self.should_stop = true;
            }
        }
    }

    fn drop_inactive(&mut self) {
        self.to_remove.truncate(0);

        for (i, ref mut pollable) in self.sockets.iter_mut().enumerate() {
            let mut cancel_check = executor::spawn(CheckCanceled {
                sender: &mut pollable.responder,
            });

            if let Ok(Async::Ready(())) = cancel_check.poll_future_notify(&self.notify, 0) {
                self.to_remove.push(i);
            }
        }

        for index in self.to_remove.drain(..).rev() {
            let _pollable = self.sockets.remove(index);
        }
    }

    fn poll(&mut self) {
        self.to_action.truncate(0);

        let mut poll_items: Vec<PollItem> = self
            .sockets
            .iter()
            .map(|pollable| pollable.as_poll_item())
            .collect();

        let num_signalled = poll(&mut poll_items, 30).unwrap();

        let mut count = 0;
        for (index, item) in poll_items.into_iter().enumerate() {
            // Prioritize outbound messages over inbound messages
            if self.sockets[index].is_writable(&item) {
                self.to_action.push(Action::Snd(index));
                count += 1;
            } else if self.sockets[index].is_readable(&item) {
                self.to_action.push(Action::Rcv(index));
                count += 1;
            }

            if count >= num_signalled {
                break;
            }
        }

        let flags = DONTWAIT;

        for action in self.to_action.drain(..).rev() {
            match action {
                Action::Rcv(index) => {
                    let Pollable {
                        sock,
                        kind,
                        msg: snd_msg,
                        responder,
                    } = self.sockets.remove(index);

                    match sock.recv_msg(flags) {
                        Ok(msg) => {
                            if kind.send_receive() {
                                let (snd_msg, flags) = snd_msg.unwrap();
                                if let Err(_) = responder
                                    .send(Response::ReceivedSndRcv(sock, msg, snd_msg, flags))
                                {
                                    error!("Error responding with received message");
                                }
                            } else {
                                if let Err(_) = responder.send(Response::Received(sock, msg)) {
                                    error!("Error responding with received message");
                                }
                            }
                        }
                        Err(e) => {
                            if let Err(_) = responder.send(Response::Error(sock, e.into())) {
                                error!("Error responding with received message");
                            }
                        }
                    }

                    count += 1;
                }
                Action::Snd(index) => {
                    let Pollable {
                        sock,
                        kind: _,
                        msg,
                        responder,
                    } = self.sockets.remove(index);

                    if let Some((msg, sendflags)) = msg {
                        match sock.send_msg(msg, sendflags | flags) {
                            Ok(_) => {
                                if let Err(_) = responder.send(Response::Sent(sock)) {
                                    error!("Error responding with received message");
                                }
                            }
                            Err(e) => {
                                if let Err(_) = responder.send(Response::Error(sock, e.into())) {
                                    error!("Error responding with received message");
                                }
                            }
                        }
                    }
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
