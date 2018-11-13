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
use futures::{Async, AsyncSink, Future, Sink, Stream};

use crate::{
    async_types::{MultipartRequest, MultipartResponse},
    error::Error,
    socket::Socket,
};

enum SinkStreamState {
    Pending,
    Streaming(MultipartResponse<Socket>),
    Sinking(MultipartRequest<Socket>),
    Both(MultipartResponse<Socket>, MultipartRequest<Socket>),
    Polling,
}

impl SinkStreamState {
    fn polling(&mut self) -> SinkStreamState {
        mem::replace(self, SinkStreamState::Polling)
    }

    fn poll_sink(
        &mut self,
        sock: usize,
        mut sinking: MultipartRequest<Socket>,
        multiparts: &mut VecDeque<Multipart>,
    ) -> Result<Async<()>, Error> {
        if let Async::Ready(_) = sinking.poll()? {
            match self.polling() {
                SinkStreamState::Streaming(streaming) => {
                    *self = SinkStreamState::Streaming(streaming);
                }
                _ => *self = SinkStreamState::Pending,
            }

            if !multiparts.is_empty() {
                self.poll_flush(sock, multiparts)
            } else {
                Ok(Async::Ready(()))
            }
        } else {
            match self.polling() {
                SinkStreamState::Streaming(streaming) => {
                    *self = SinkStreamState::Both(streaming, sinking);
                }
                _ => {
                    *self = SinkStreamState::Sinking(sinking);
                }
            }

            Ok(Async::NotReady)
        }
    }

    fn poll_stream(
        &mut self,
        mut streaming: MultipartResponse<Socket>,
    ) -> Result<Async<Option<Multipart>>, Error> {
        if let Async::Ready((msg, _)) = streaming.poll()? {
            match self.polling() {
                SinkStreamState::Sinking(sinking) => *self = SinkStreamState::Sinking(sinking),
                _ => *self = SinkStreamState::Pending,
            }

            Ok(Async::Ready(Some(msg)))
        } else {
            match self.polling() {
                SinkStreamState::Sinking(sinking) => {
                    *self = SinkStreamState::Both(streaming, sinking)
                }
                _ => *self = SinkStreamState::Streaming(streaming),
            }

            Ok(Async::NotReady)
        }
    }

    fn poll_flush(
        &mut self,
        sock: usize,
        multiparts: &mut VecDeque<Multipart>,
    ) -> Result<Async<()>, Error> {
        match self.polling() {
            SinkStreamState::Pending => {
                if let Some(multipart) = multiparts.pop_front() {
                    let sinking = MultipartRequest::new(sock, multipart);

                    self.poll_sink(sock, sinking, multiparts)
                } else {
                    *self = SinkStreamState::Pending;

                    Ok(Async::Ready(()))
                }
            }
            SinkStreamState::Both(streaming, sinking) => {
                *self = SinkStreamState::Streaming(streaming);

                self.poll_sink(sock, sinking, multiparts)
            }
            SinkStreamState::Sinking(sinking) => self.poll_sink(sock, sinking, multiparts),
            SinkStreamState::Streaming(streaming) => {
                *self = SinkStreamState::Streaming(streaming);

                if let Some(multipart) = multiparts.pop_front() {
                    let sinking = MultipartRequest::new(sock, multipart);

                    self.poll_sink(sock, sinking, multiparts)
                } else {
                    Ok(Async::Ready(()))
                }
            }
            SinkStreamState::Polling => {
                error!("poll_flush, Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }

    fn poll_fetch(&mut self, sock: usize) -> Result<Async<Option<Multipart>>, Error> {
        match self.polling() {
            SinkStreamState::Pending => {
                let streaming = MultipartResponse::new(sock);

                self.poll_stream(streaming)
            }
            SinkStreamState::Sinking(sinking) => {
                *self = SinkStreamState::Sinking(sinking);

                let streaming = MultipartResponse::new(sock);

                self.poll_stream(streaming)
            }
            SinkStreamState::Streaming(streaming) => self.poll_stream(streaming),
            SinkStreamState::Both(streaming, sinking) => {
                *self = SinkStreamState::Sinking(sinking);

                self.poll_stream(streaming)
            }
            SinkStreamState::Polling => {
                error!("poll_fetch, Called polling while polling");
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

pub struct MultipartSinkStream<T>
where
    T: From<Socket>,
{
    state: SinkStreamState,
    sock: usize,
    multiparts: VecDeque<Multipart>,
    buffer_size: usize,
    phantom: PhantomData<T>,
}

impl<T> MultipartSinkStream<T>
where
    T: From<Socket>,
{
    pub fn new(sock: usize, buffer_size: usize) -> Self {
        MultipartSinkStream {
            state: SinkStreamState::Pending,
            sock,
            multiparts: VecDeque::new(),
            buffer_size,
            phantom: PhantomData,
        }
    }
}

impl<T> Sink for MultipartSinkStream<T>
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

impl<T> Stream for MultipartSinkStream<T>
where
    T: From<Socket>,
{
    type Item = Multipart;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.state.poll_fetch(self.sock)
    }
}
