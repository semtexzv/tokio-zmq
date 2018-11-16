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

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    io::{self, Read, Write},
    marker::PhantomData,
    mem::{replace, transmute},
    net::{TcpListener, TcpStream},
    os::raw::c_void,
    ptr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
};

use async_zmq_types::Multipart;
use futures::{
    executor::{self, Notify},
    sync::oneshot,
    Async, Future, Poll,
};
use libc::c_short;
use zmq::{poll, Message, PollEvents, PollItem, Socket, DONTWAIT, POLLIN, POLLOUT, SNDMORE};

use crate::error::Error;

enum Request {
    Init(Socket, oneshot::Sender<SockId>),
    SendMessage(usize, Multipart, oneshot::Sender<Response>),
    ReceiveMessage(usize, oneshot::Sender<Response>),
    DropSocket(usize),
    Done,
}

pub struct SockId(usize, Arc<Mutex<SockIdInner>>);

impl SockId {
    fn new(id: usize, tx: Sender) -> Self {
        SockId(id, Arc::new(Mutex::new(SockIdInner(id, tx))))
    }
}

impl fmt::Debug for SockId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SockId({}, _)", self.0)
    }
}

impl fmt::Display for SockId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

struct SockIdInner(usize, Sender);

impl Drop for SockIdInner {
    fn drop(&mut self) {
        trace!("Dropping {}", self.0);
        let _ = self.1.send(Request::DropSocket(self.0));
    }
}

enum Response {
    Sent,
    Received(Multipart),
    Full(Multipart),
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
        self.swap_true();
        let _ = (&self.tx).write(&[1]);
    }

    #[cfg(unix)]
    fn read_fd(&self) -> RawFd {
        self.rx.as_raw_fd()
    }

    #[cfg(windows)]
    fn read_fd(&self) -> RawSocket {
        self.rx.as_raw_socket()
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
        loop {
            match (&self.channel.rx).read(&mut [0; 32]) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("I/O error: {}", e),
            }
        }

        self.channel.swap_false()
    }
}

#[derive(Clone)]
pub struct Session {
    inner: Arc<Mutex<Option<InnerSession>>>,
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
            tx: tx.clone(),
            channel: channel.clone(),
        };
        let rx = Receiver {
            rx: rx,
            channel: channel,
        };

        let tx2 = tx.clone();
        let tx3 = tx.clone();

        let (wake_tx, wake_rx) = mpsc::channel();

        thread::spawn(move || {
            PollThread::new(tx2, rx).run();
        });

        thread::spawn(move || {
            WakeThread::new(tx3.channel, wake_rx).run();
        });

        Session {
            inner: InnerSession::init(tx, wake_tx),
        }
    }

    pub fn shutdown(&self) {
        *self.inner.lock().unwrap() = None;
    }

    pub fn send(&self, id: &SockId, msg: Multipart) -> SendFuture {
        let (tx, rx) = oneshot::channel();

        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
            .send(Request::SendMessage(id.0, msg, tx));

        SendFuture { rx }
    }

    pub fn recv(&self, id: &SockId) -> RecvFuture {
        let (tx, rx) = oneshot::channel();

        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
            .send(Request::ReceiveMessage(id.0, tx));

        RecvFuture { rx }
    }

    pub fn init(&self, sock: Socket) -> InitFuture {
        let (tx, rx) = oneshot::channel();

        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
            .send(Request::Init(sock, tx));

        InitFuture { rx }
    }
}

pub struct SendFuture {
    rx: oneshot::Receiver<Response>,
}

impl Future for SendFuture {
    type Item = Option<Multipart>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll()? {
            Async::Ready(res) => match res {
                Response::Sent => Ok(Async::Ready(None)),
                Response::Full(msg) => Ok(Async::Ready(Some(msg))),
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
    rx: oneshot::Receiver<SockId>,
}

impl Future for InitFuture {
    type Item = SockId;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(self.rx.poll()?)
    }
}

struct InnerSession {
    tx: Sender,
    wake_tx: mpsc::Sender<usize>,
}

impl InnerSession {
    fn init(tx: Sender, wake_tx: mpsc::Sender<usize>) -> Arc<Mutex<Option<Self>>> {
        Arc::new(Mutex::new(Some(InnerSession { tx, wake_tx })))
    }

    fn send(&self, request: Request) {
        self.tx.send(request);
    }
}

impl Drop for InnerSession {
    fn drop(&mut self) {
        info!("Dropping session");
        self.tx.send(Request::Done);
        let _ = self.wake_tx.send(0);
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
    id: usize,
    kind: PollKind,
    outbound_message_buffer: VecDeque<Multipart>,
    inbound_message_cache: Multipart,
    send_responder: Option<oneshot::Sender<Response>>,
    recv_responder: Option<oneshot::Sender<Response>>,
}

impl Pollable {
    fn new(sock: Socket, id: usize) -> Self {
        Pollable {
            sock,
            id,
            kind: PollKind::UNUSED,
            outbound_message_buffer: VecDeque::new(),
            inbound_message_cache: Multipart::new(),
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
        trace!("Setting read, {}", self.id);
        self.kind.read();
    }

    fn clear_read(&mut self) {
        trace!("Clearing read, {}", self.id);
        self.kind.clear_read();
    }

    fn write(&mut self) {
        trace!("Setting write, {}", self.id);
        self.kind.write();
    }

    fn clear_write(&mut self) {
        trace!("Clearing write, {}", self.id);
        self.kind.clear_write();
    }

    fn queue_message(&mut self, multipart: Multipart) -> Option<Multipart> {
        if self.outbound_message_buffer.len() < 30 {
            self.outbound_message_buffer.push_back(multipart);
            None
        } else {
            Some(multipart)
        }
    }

    fn send_responder(&mut self, r: oneshot::Sender<Response>) {
        if self.send_responder.is_some() {
            panic!("Overwriting an existing responder, {}", self.id);
        }
        self.send_responder = Some(r);
    }

    fn recv_responder(&mut self, r: oneshot::Sender<Response>) {
        if self.recv_responder.is_some() {
            panic!("Overwriting an existing responder, {}", self.id);
        }
        self.recv_responder = Some(r);
    }

    fn try_recieve_message(&mut self) -> Result<Option<Message>, zmq::Error> {
        match self.sock.recv_msg(DONTWAIT) {
            Ok(msg) => Ok(Some(msg)),
            Err(zmq::Error::EAGAIN) => {
                warn!("EAGAIN while receiving, {}", self.id);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn try_receive_multipart(&mut self) -> Result<Option<Multipart>, zmq::Error> {
        while let Some(msg) = self.try_recieve_message()? {
            if msg.get_more() {
                self.inbound_message_cache.push_back(msg);
                continue;
            }

            self.inbound_message_cache.push_back(msg);
            let multipart = replace(&mut self.inbound_message_cache, Multipart::new());

            return Ok(Some(multipart));
        }

        if !self.inbound_message_cache.is_empty() {
            warn!("EAGAIN in the middle of a multipart, {}", self.id);
        }

        Ok(None)
    }

    fn fetch_multiparts(&mut self) {
        if let Some(responder) = self.recv_responder.take() {
            match self.try_receive_multipart() {
                Ok(Some(multipart)) => {
                    self.clear_read();

                    if let Err(_) = responder.send(Response::Received(multipart)) {
                        error!("Error responding with Received, {}", self.id);
                    }
                }
                Ok(None) => {
                    self.recv_responder = Some(responder);
                }
                Err(zmq::Error::EFSM) => {
                    warn!("EFSM while receiving, {}", self.id);
                    self.recv_responder = Some(responder);
                }
                Err(e) => {
                    self.clear_read();

                    error!("Error fetching, {}, {}", self.id, e);
                    if let Err(_) = responder.send(Response::Error(e.into())) {
                        error!("Error responding with Error, {}", self.id);
                    }
                }
            }
        }
    }

    fn try_send_message(
        &self,
        message: Message,
        flags: i32,
    ) -> Result<Option<Message>, zmq::Error> {
        let msg_clone_res = Message::from_slice(&message);

        match self.sock.send_msg(message, flags) {
            Ok(_) => {
                trace!("SENT msg, {}", self.id);
                Ok(None)
            }
            Err(zmq::Error::EAGAIN) => {
                warn!("EAGAIN while sending, {}", self.id);
                msg_clone_res.map(Some)
            }
            Err(e) => Err(e),
        }
    }

    fn try_send_multipart(
        &self,
        mut multipart: Multipart,
    ) -> Result<Option<Multipart>, zmq::Error> {
        while let Some(msg) = multipart.pop_front() {
            let flags = DONTWAIT | if multipart.is_empty() { 0 } else { SNDMORE };

            if let Some(msg) = self.try_send_message(msg, flags)? {
                multipart.push_front(msg);
                return Ok(Some(multipart));
            }
        }

        Ok(None)
    }

    fn flush_multiparts(&mut self) {
        while let Some(multipart) = self.outbound_message_buffer.pop_front() {
            match self.try_send_multipart(multipart) {
                Ok(Some(multipart)) => {
                    self.outbound_message_buffer.push_front(multipart);
                    return;
                }
                Ok(None) => {
                    if let Some(responder) = self.send_responder.take() {
                        if let Err(_) = responder.send(Response::Sent) {
                            error!("Error responding with Sent, {}", self.id);
                        }
                    }
                    if self.outbound_message_buffer.is_empty() {
                        self.clear_write();
                        return;
                    }
                    continue;
                }
                Err(zmq::Error::EFSM) => {
                    warn!("EFSM while sending, {}", self.id);
                    return;
                }
                Err(e) => {
                    self.clear_write();

                    error!("Error flushing, {}, {}", self.id, e);
                    if let Some(responder) = self.send_responder.take() {
                        if let Err(_) = responder.send(Response::Error(e.into())) {
                            error!("Error responding with Error, {}", self.id);
                        }
                    }
                    return;
                }
            }
        }
    }
}

#[repr(C)]
pub struct MyPollItem<'a> {
    socket: *mut c_void,
    fd: zmq_sys::RawFd,
    events: c_short,
    revents: c_short,
    marker: PhantomData<&'a Socket>,
}

impl<'a> MyPollItem<'a> {
    fn from_fd(fd: zmq_sys::RawFd, events: PollEvents) -> Self {
        MyPollItem {
            socket: ptr::null_mut(),
            fd,
            events,
            revents: 0,
            marker: PhantomData,
        }
    }
}

struct WakeThread {
    channel: Arc<Channel>,
    wake_rx: mpsc::Receiver<usize>,
}

impl WakeThread {
    fn new(channel: Arc<Channel>, wake_rx: mpsc::Receiver<usize>) -> Self {
        WakeThread { channel, wake_rx }
    }

    fn run(&mut self) {
        loop {
            thread::sleep(Duration::from_millis(30));
            if let Some(_) = self.wake_rx.try_recv().ok() {
                return;
            }
            self.channel.notify();
        }
    }
}

enum Action {
    Snd(usize),
    Rcv(usize),
}

struct PollThread {
    flush_count: usize,
    next_sock_id: usize,
    tx: Sender,
    rx: Receiver,
    should_stop: bool,
    to_action: Vec<Action>,
    notify: Arc<NotifyCanceled>,
    sockets: BTreeMap<usize, Pollable>,
    channel: Arc<Channel>,
}

impl PollThread {
    fn new(tx: Sender, rx: Receiver) -> Self {
        let channel = rx.channel.clone();

        PollThread {
            flush_count: 0,
            next_sock_id: 0,
            tx,
            rx,
            should_stop: false,
            to_action: Vec::new(),
            notify: Arc::new(NotifyCanceled::new(channel.clone())),
            sockets: BTreeMap::new(),
            channel,
        }
    }

    fn run(&mut self) {
        loop {
            self.turn();
        }
    }

    fn try_recv(&mut self) {
        while let Some(msg) = self.rx.try_recv() {
            if !self.should_stop {
                self.handle_request(msg);
            } else {
                self.respond_stopping(msg);
            }
        }
        self.rx.drain();
    }

    fn respond_stopping(&mut self, request: Request) {
        match request {
            Request::Init(_, responder) => {
                let id = self.next_sock_id;

                if let Err(_) = responder.send(SockId::new(id, self.tx.clone())) {
                    error!("Error responding with init socket, {}", id);
                }

                self.next_sock_id += 1;
            }
            Request::SendMessage(id, _, responder) => {
                if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                    error!("Error responding with dropped, {}", id);
                }
            }
            Request::ReceiveMessage(id, responder) => {
                if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                    error!("Error responding with dropped, {}", id);
                }
            }
            Request::DropSocket(id) => {
                if let Some(mut pollable) = self.sockets.remove(&id) {
                    if let Some(responder) = pollable.send_responder.take() {
                        if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                            error!("Error notifying dropped socket, {}", id);
                        }
                    }

                    if let Some(responder) = pollable.recv_responder.take() {
                        if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                            error!("Error notifying dropped socket, {}", id);
                        }
                    }
                }
            }
            Request::Done => {
                info!("Handling done");
                self.should_stop = true;
            }
        }
    }

    fn handle_request(&mut self, request: Request) {
        match request {
            Request::Init(sock, responder) => {
                let id = self.next_sock_id;

                self.sockets.insert(id, Pollable::new(sock, id));
                if let Err(_) = responder.send(SockId::new(id, self.tx.clone())) {
                    error!("Error responding with init socket, {}", id);
                }

                self.next_sock_id += 1;
            }
            Request::SendMessage(id, message, responder) => {
                if let Some(pollable) = self.sockets.get_mut(&id) {
                    if let Some(message) = pollable.queue_message(message) {
                        trace!("Buffer full, flushing, {}", pollable.id);
                        pollable.flush_multiparts();

                        if let Some(msg) = pollable.queue_message(message) {
                            trace!("Buffer full, {}", id);
                            if let Err(_) = responder.send(Response::Full(msg)) {
                                error!("Error notifying of full buffer, {}", id);
                            }
                            return;
                        }
                    }
                    pollable.write();
                    pollable.send_responder(responder);
                    pollable.flush_multiparts();
                } else {
                    error!("Tried to send to dropped socket, {}", id);
                    if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                        error!("Error responding with dropped, {}", id);
                    }
                }
            }
            Request::ReceiveMessage(id, responder) => {
                if let Some(pollable) = self.sockets.get_mut(&id) {
                    pollable.recv_responder(responder);
                    pollable.read();
                    pollable.fetch_multiparts();
                } else {
                    error!("Tried to receive from dropped socket, {}", id);
                    if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                        error!("Error responding with dropped, {}", id);
                    }
                }
            }
            Request::DropSocket(id) => {
                if let Some(mut pollable) = self.sockets.remove(&id) {
                    if let Some(responder) = pollable.send_responder.take() {
                        if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                            error!("Error notifying dropped socket, {}", id);
                        }
                    }

                    if let Some(responder) = pollable.recv_responder.take() {
                        if let Err(_) = responder.send(Response::Error(Error::Dropped)) {
                            error!("Error notifying dropped socket, {}", id);
                        }
                    }
                }
            }
            Request::Done => {
                info!("Handling done");
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

        let io_item = MyPollItem::from_fd(self.channel.read_fd(), POLLIN);

        let io_item: PollItem = unsafe { transmute(io_item) };

        poll_items.push(io_item);

        let _num_signalled = match poll(&mut poll_items, -1) {
            Ok(num) => num,
            Err(e) => {
                error!("Error in poll, {}", e);
                return;
            }
        };

        for (id, item) in ids.into_iter().zip(poll_items) {
            // Prioritize outbound messages over inbound messages
            if self
                .sockets
                .get(&id)
                .map(|p| p.is_writable(&item))
                .unwrap_or(false)
            {
                trace!("{} write ready", id);
                self.to_action.push(Action::Snd(id));
            } else if self
                .sockets
                .get(&id)
                .map(|p| p.is_readable(&item))
                .unwrap_or(false)
            {
                trace!("{} read ready", id);
                self.to_action.push(Action::Rcv(id));
            }
        }

        for action in self.to_action.drain(..).rev() {
            match action {
                Action::Rcv(id) => {
                    self.sockets
                        .get_mut(&id)
                        .map(|pollable| pollable.fetch_multiparts());
                }
                Action::Snd(id) => {
                    self.sockets
                        .get_mut(&id)
                        .map(|pollable| pollable.flush_multiparts());
                }
            }
        }
    }

    fn check_flush_count(&mut self) {
        if self.flush_count == 10 {
            self.flush_count = 0;
            /*
            for pollable in self.sockets.values_mut() {
                pollable.flush_multiparts();
                pollable.fetch_multiparts();
            }
            */
        }

        self.flush_count += 1;
    }

    fn turn(&mut self) {
        self.drop_inactive();
        self.try_recv();
        self.poll();
        self.check_flush_count();
    }
}
