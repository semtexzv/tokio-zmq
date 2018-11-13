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
use futures::{Async, Future, Stream};

use crate::{async_types::MultipartResponse, error::Error, socket::Socket};

enum StreamState {
    Pending(zmq::Socket),
    Running(MultipartResponse<Socket>),
    Polling,
}

impl StreamState {
    fn polling(&mut self) -> StreamState {
        mem::replace(self, StreamState::Polling)
    }

    fn poll_fut(
        &mut self,
        mut fut: MultipartResponse<Socket>,
    ) -> Result<Async<Option<Multipart>>, Error> {
        if let Async::Ready((msg, sock)) = fut.poll()? {
            *self = StreamState::Pending(sock.inner());

            Ok(Async::Ready(Some(msg)))
        } else {
            *self = StreamState::Running(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_fetch(&mut self) -> Result<Async<Option<Multipart>>, Error> {
        match self.polling() {
            StreamState::Pending(sock) => {
                let fut = MultipartResponse::new(sock);

                self.poll_fut(fut)
            }
            StreamState::Running(fut) => self.poll_fut(fut),
            StreamState::Polling => {
                error!("Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }
}

pub struct MultipartStream<T>
where
    T: From<Socket>,
{
    state: StreamState,
    phantom: PhantomData<T>,
}

impl<T> MultipartStream<T>
where
    T: From<Socket>,
{
    pub fn new(sock: zmq::Socket) -> Self {
        MultipartStream {
            state: StreamState::Pending(sock),
            phantom: PhantomData,
        }
    }
}

impl<T> Stream for MultipartStream<T>
where
    T: From<Socket>,
{
    type Item = Multipart;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.state.poll_fetch()
    }
}
