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

use std::{marker::PhantomData, mem};

use async_zmq_types::Multipart;
use futures::{Async, Future};
use zmq::SNDMORE;

use crate::{error::Error, socket::Socket, RecvFuture, SendFuture, SendRecvFuture, SESSION};

enum SendState {
    Pending(zmq::Socket),
    Running(SendFuture),
    Polling,
}

impl SendState {
    fn polling(&mut self) -> SendState {
        mem::replace(self, SendState::Polling)
    }

    fn poll_fut<T>(
        &mut self,
        mut fut: SendFuture,
        multipart: &mut Multipart,
    ) -> Result<Async<T>, Error>
    where
        T: From<Socket>,
    {
        if let Async::Ready(sock) = fut.poll()? {
            if !multipart.is_empty() {
                *self = SendState::Pending(sock);

                self.poll_flush(multipart)
            } else {
                Ok(Async::Ready(Socket::from_sock(sock).into()))
            }
        } else {
            *self = SendState::Running(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_flush<T>(&mut self, multipart: &mut Multipart) -> Result<Async<T>, Error>
    where
        T: From<Socket>,
    {
        match self.polling() {
            SendState::Pending(sock) => {
                if let Some(msg) = multipart.pop_front() {
                    let fut = if !multipart.is_empty() {
                        SESSION.send(sock, msg, SNDMORE)
                    } else {
                        SESSION.send(sock, msg, 0)
                    };

                    self.poll_fut(fut, multipart)
                } else {
                    Ok(Async::Ready(Socket::from_sock(sock).into()))
                }
            }
            SendState::Running(fut) => self.poll_fut(fut, multipart),
            SendState::Polling => {
                error!("Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }
}

pub struct MultipartRequest<T>
where
    T: From<Socket>,
{
    state: SendState,
    multipart: Multipart,
    phantom: PhantomData<T>,
}

impl<T> MultipartRequest<T>
where
    T: From<Socket>,
{
    pub fn new(sock: zmq::Socket, multipart: Multipart) -> Self {
        MultipartRequest {
            state: SendState::Pending(sock),
            multipart,
            phantom: PhantomData,
        }
    }
}

impl<T> Future for MultipartRequest<T>
where
    T: From<Socket>,
{
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        self.state.poll_flush(&mut self.multipart)
    }
}

enum RecvState {
    Pending(zmq::Socket),
    Running(RecvFuture),
    Polling,
}

impl RecvState {
    fn polling(&mut self) -> RecvState {
        mem::replace(self, RecvState::Polling)
    }

    fn poll_fut<T>(
        &mut self,
        mut fut: RecvFuture,
        multipart: &mut Multipart,
    ) -> Result<Async<(Multipart, T)>, Error>
    where
        T: From<Socket>,
    {
        if let Async::Ready((sock, msg)) = fut.poll()? {
            if msg.get_more() {
                *self = RecvState::Pending(sock);

                multipart.push_back(msg);
                self.poll_fetch(multipart)
            } else {
                multipart.push_back(msg);

                Ok(Async::Ready((
                    multipart.drain(..).collect::<Vec<_>>().into(),
                    Socket::from_sock(sock).into(),
                )))
            }
        } else {
            *self = RecvState::Running(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_fetch<T>(&mut self, multipart: &mut Multipart) -> Result<Async<(Multipart, T)>, Error>
    where
        T: From<Socket>,
    {
        match self.polling() {
            RecvState::Pending(sock) => {
                let fut = SESSION.recv(sock);

                self.poll_fut(fut, multipart)
            }
            RecvState::Running(fut) => self.poll_fut(fut, multipart),
            RecvState::Polling => {
                error!("Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }
}

pub struct MultipartResponse<T>
where
    T: From<Socket>,
{
    state: RecvState,
    multipart: Multipart,
    phantom: PhantomData<T>,
}

impl<T> MultipartResponse<T>
where
    T: From<Socket>,
{
    pub fn new(sock: zmq::Socket) -> Self {
        MultipartResponse {
            state: RecvState::Pending(sock),
            multipart: Multipart::new(),
            phantom: PhantomData,
        }
    }
}

impl<T> Future for MultipartResponse<T>
where
    T: From<Socket>,
{
    type Item = (Multipart, T);
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        self.state.poll_fetch(&mut self.multipart)
    }
}

pub enum RequestResponse<T>
where
    T: From<Socket>,
{
    Request(MultipartRequest<T>),
    Response(MultipartResponse<T>),
}

pub(crate) enum SendRecvState {
    Pending(zmq::Socket, Multipart),
    Running(SendRecvFuture, Multipart),
    Polling,
}

impl SendRecvState {
    fn polling(&mut self) -> SendRecvState {
        mem::replace(self, SendRecvState::Polling)
    }

    fn poll_fut<T>(
        &mut self,
        mut fut: SendRecvFuture,
        mut send_multipart: Multipart,
    ) -> Result<Async<RequestResponse<T>>, Error>
    where
        T: From<Socket>,
    {
        if let Async::Ready((sock, opt)) = fut.poll()? {
            match opt {
                Some((rcv_msg, snd_msg, _)) => {
                    let mut recv_multipart = Multipart::new();
                    recv_multipart.push_back(rcv_msg);
                    send_multipart.push_front(snd_msg);

                    Ok(Async::Ready(RequestResponse::Response(MultipartResponse {
                        state: RecvState::Pending(sock),
                        multipart: recv_multipart,
                        phantom: PhantomData,
                    })))
                }
                None => Ok(Async::Ready(RequestResponse::Request(MultipartRequest {
                    state: SendState::Pending(sock),
                    multipart: send_multipart,
                    phantom: PhantomData,
                }))),
            }
        } else {
            *self = SendRecvState::Running(fut, send_multipart);

            Ok(Async::NotReady)
        }
    }

    pub(crate) fn poll<T>(&mut self) -> Result<Async<RequestResponse<T>>, Error>
    where
        T: From<Socket>,
    {
        match self.polling() {
            SendRecvState::Pending(sock, mut send_multipart) => {
                if let Some(msg) = send_multipart.pop_front() {
                    let fut = if send_multipart.len() > 1 {
                        SESSION.send_recv(sock, msg, SNDMORE)
                    } else {
                        SESSION.send_recv(sock, msg, 0)
                    };

                    self.poll_fut(fut, send_multipart)
                } else {
                    Ok(Async::Ready(RequestResponse::Response(
                        MultipartResponse::new(sock),
                    )))
                }
            }
            SendRecvState::Running(fut, send_multipart) => self.poll_fut(fut, send_multipart),
            SendRecvState::Polling => {
                error!("Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }
}
