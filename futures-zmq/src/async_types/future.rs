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

use crate::{error::Error, socket::Socket, RecvFuture, SendFuture, SESSION};

enum SendState {
    Pending(Multipart),
    Running(SendFuture),
    Polling,
}

impl SendState {
    fn polling(&mut self) -> SendState {
        mem::replace(self, SendState::Polling)
    }

    fn poll_fut<T>(&mut self, sock: usize, mut fut: SendFuture) -> Result<Async<T>, Error>
    where
        T: From<Socket>,
    {
        if let Async::Ready(opt) = fut.poll()? {
            match opt {
                None => Ok(Async::Ready(Socket::from_sock(sock).into())),
                Some(multipart) => {
                    *self = SendState::Pending(multipart);

                    Ok(Async::NotReady)
                }
            }
        } else {
            *self = SendState::Running(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_flush<T>(&mut self, sock: usize, buffer_size: usize) -> Result<Async<T>, Error>
    where
        T: From<Socket>,
    {
        match self.polling() {
            SendState::Pending(multipart) => {
                trace!("Sending {:?}", multipart);
                let fut = SESSION.send(sock, multipart, buffer_size);

                self.poll_fut(sock, fut)
            }
            SendState::Running(fut) => self.poll_fut(sock, fut),
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
    sock: usize,
    buffer_size: usize,
    phantom: PhantomData<T>,
}

impl<T> MultipartRequest<T>
where
    T: From<Socket>,
{
    pub fn new(sock: usize, multipart: Multipart) -> Self {
        Self::new_with_buffer_size(sock, multipart, 1)
    }

    pub fn new_with_buffer_size(sock: usize, multipart: Multipart, buffer_size: usize) -> Self {
        MultipartRequest {
            state: SendState::Pending(multipart),
            sock,
            buffer_size,
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
        self.state.poll_flush(self.sock, self.buffer_size)
    }
}

enum RecvState {
    Pending,
    Running(RecvFuture),
    Polling,
}

impl RecvState {
    fn polling(&mut self) -> RecvState {
        mem::replace(self, RecvState::Polling)
    }

    fn poll_fut<T>(
        &mut self,
        sock: usize,
        mut fut: RecvFuture,
    ) -> Result<Async<(Multipart, T)>, Error>
    where
        T: From<Socket>,
    {
        if let Async::Ready(multipart) = fut.poll()? {
            trace!("Received {:?}", multipart);
            Ok(Async::Ready((multipart, Socket::from_sock(sock).into())))
        } else {
            *self = RecvState::Running(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_fetch<T>(&mut self, sock: usize) -> Result<Async<(Multipart, T)>, Error>
    where
        T: From<Socket>,
    {
        match self.polling() {
            RecvState::Pending => {
                let fut = SESSION.recv(sock);

                self.poll_fut(sock, fut)
            }
            RecvState::Running(fut) => self.poll_fut(sock, fut),
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
    sock: usize,
    phantom: PhantomData<T>,
}

impl<T> MultipartResponse<T>
where
    T: From<Socket>,
{
    pub fn new(sock: usize) -> Self {
        MultipartResponse {
            state: RecvState::Pending,
            sock,
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
        self.state.poll_fetch(self.sock)
    }
}
