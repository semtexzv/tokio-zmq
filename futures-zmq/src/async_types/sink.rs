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

use std::{collections::VecDeque, marker::PhantomData, mem};

use async_zmq_types::Multipart;
use futures::{Async, AsyncSink, Future, Sink};

use crate::{async_types::MultipartRequest, error::Error, socket::Socket};

enum SinkState {
    Pending,
    Running(MultipartRequest<Socket>),
    Polling,
}

impl SinkState {
    fn polling(&mut self) -> SinkState {
        mem::replace(self, SinkState::Polling)
    }

    fn poll_fut(
        &mut self,
        sock: usize,
        mut fut: MultipartRequest<Socket>,
        multiparts: &mut VecDeque<Multipart>,
    ) -> Result<Async<()>, Error> {
        if let Async::Ready(_) = fut.poll()? {
            *self = SinkState::Pending;

            if !multiparts.is_empty() {
                self.poll_flush(sock, multiparts)
            } else {
                Ok(Async::Ready(()))
            }
        } else {
            *self = SinkState::Running(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_flush(
        &mut self,
        sock: usize,
        multiparts: &mut VecDeque<Multipart>,
    ) -> Result<Async<()>, Error> {
        match self.polling() {
            SinkState::Pending => {
                if let Some(multipart) = multiparts.pop_front() {
                    let fut = MultipartRequest::new(sock, multipart);

                    self.poll_fut(sock, fut, multiparts)
                } else {
                    Ok(Async::Ready(()))
                }
            }
            SinkState::Running(fut) => self.poll_fut(sock, fut, multiparts),
            SinkState::Polling => {
                error!("Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }

    fn start_send(
        &mut self,
        sock: usize,
        multiparts: &mut VecDeque<Multipart>,
        buffer_size: usize,
        multipart: Multipart,
    ) -> Result<AsyncSink<Multipart>, Error> {
        if buffer_size < multiparts.len() {
            if let Async::NotReady = self.poll_flush(sock, multiparts)? {
                if buffer_size < multiparts.len() {
                    return Ok(AsyncSink::NotReady(multipart));
                }
            }
        }

        multiparts.push_back(multipart);
        Ok(AsyncSink::Ready)
    }
}

pub struct MultipartSink<T>
where
    T: From<Socket>,
{
    state: SinkState,
    sock: usize,
    multiparts: VecDeque<Multipart>,
    buffer_size: usize,
    phantom: PhantomData<T>,
}

impl<T> MultipartSink<T>
where
    T: From<Socket>,
{
    pub fn new(sock: usize, buffer_size: usize) -> Self {
        MultipartSink {
            state: SinkState::Pending,
            sock,
            multiparts: VecDeque::new(),
            buffer_size,
            phantom: PhantomData,
        }
    }
}

impl<T> Sink for MultipartSink<T>
where
    T: From<Socket>,
{
    type SinkItem = Multipart;
    type SinkError = Error;

    fn start_send(
        &mut self,
        multipart: Self::SinkItem,
    ) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        self.state
            .start_send(self.sock, &mut self.multiparts, self.buffer_size, multipart)
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.state.poll_flush(self.sock, &mut self.multiparts)
    }
}
