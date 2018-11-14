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
    poll_thread::SockId,
    socket::Socket,
};

enum SinkStreamState {
    Pending(SockId, SockId),
    Streaming(MultipartResponse<Socket>, SockId),
    Sinking(MultipartRequest<Socket>, SockId),
    Both(MultipartResponse<Socket>, MultipartRequest<Socket>),
    Polling,
}

impl SinkStreamState {
    fn polling(&mut self) -> SinkStreamState {
        mem::replace(self, SinkStreamState::Polling)
    }

    fn poll_flush(&mut self, multiparts: &mut VecDeque<Multipart>) -> Result<Async<()>, Error> {
        match self.polling() {
            SinkStreamState::Pending(sock1, sock2) => {
                if let Some(multipart) = multiparts.pop_front() {
                    let mut sinking: MultipartRequest<Socket> =
                        MultipartRequest::new(sock1, multipart);

                    match sinking.poll()? {
                        Async::Ready(sock) => {
                            *self = SinkStreamState::Pending(sock.inner(), sock2);
                            Ok(Async::Ready(()))
                        }
                        Async::NotReady => {
                            *self = SinkStreamState::Sinking(sinking, sock2);
                            Ok(Async::NotReady)
                        }
                    }
                } else {
                    *self = SinkStreamState::Pending(sock1, sock2);
                    Ok(Async::Ready(()))
                }
            }
            SinkStreamState::Both(streaming, mut sinking) => match sinking.poll()? {
                Async::Ready(sock) => {
                    *self = SinkStreamState::Streaming(streaming, sock.inner());
                    Ok(Async::Ready(()))
                }
                Async::NotReady => {
                    *self = SinkStreamState::Both(streaming, sinking);
                    Ok(Async::NotReady)
                }
            },
            SinkStreamState::Sinking(mut sinking, sock2) => match sinking.poll()? {
                Async::Ready(sock) => {
                    *self = SinkStreamState::Pending(sock.inner(), sock2);
                    Ok(Async::Ready(()))
                }
                Async::NotReady => {
                    *self = SinkStreamState::Sinking(sinking, sock2);
                    Ok(Async::NotReady)
                }
            },
            SinkStreamState::Streaming(streaming, sock2) => {
                if let Some(multipart) = multiparts.pop_front() {
                    let mut sinking: MultipartRequest<Socket> =
                        MultipartRequest::new(sock2, multipart);

                    match sinking.poll()? {
                        Async::Ready(sock) => {
                            *self = SinkStreamState::Streaming(streaming, sock.inner());
                            Ok(Async::Ready(()))
                        }
                        Async::NotReady => {
                            *self = SinkStreamState::Both(streaming, sinking);
                            Ok(Async::NotReady)
                        }
                    }
                } else {
                    *self = SinkStreamState::Streaming(streaming, sock2);
                    Ok(Async::Ready(()))
                }
            }
            SinkStreamState::Polling => {
                error!("poll_flush, Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }

    fn poll_fetch(&mut self) -> Result<Async<Option<Multipart>>, Error> {
        match self.polling() {
            SinkStreamState::Pending(sock1, sock2) => {
                let mut streaming: MultipartResponse<Socket> = MultipartResponse::new(sock1);

                match streaming.poll()? {
                    Async::Ready((msg, sock)) => {
                        *self = SinkStreamState::Pending(sock.inner(), sock2);
                        Ok(Async::Ready(Some(msg)))
                    }
                    Async::NotReady => {
                        *self = SinkStreamState::Streaming(streaming, sock2);
                        Ok(Async::NotReady)
                    }
                }
            }
            SinkStreamState::Sinking(sinking, sock2) => {
                let mut streaming: MultipartResponse<Socket> = MultipartResponse::new(sock2);

                match streaming.poll()? {
                    Async::Ready((msg, sock)) => {
                        *self = SinkStreamState::Sinking(sinking, sock.inner());
                        Ok(Async::Ready(Some(msg)))
                    }
                    Async::NotReady => {
                        *self = SinkStreamState::Both(streaming, sinking);
                        Ok(Async::NotReady)
                    }
                }
            }
            SinkStreamState::Streaming(mut streaming, sock2) => match streaming.poll()? {
                Async::Ready((msg, sock)) => {
                    *self = SinkStreamState::Pending(sock.inner(), sock2);
                    Ok(Async::Ready(Some(msg)))
                }
                Async::NotReady => {
                    *self = SinkStreamState::Streaming(streaming, sock2);
                    Ok(Async::NotReady)
                }
            },
            SinkStreamState::Both(mut streaming, sinking) => match streaming.poll()? {
                Async::Ready((msg, sock)) => {
                    *self = SinkStreamState::Sinking(sinking, sock.inner());
                    Ok(Async::Ready(Some(msg)))
                }
                Async::NotReady => {
                    *self = SinkStreamState::Both(streaming, sinking);
                    Ok(Async::NotReady)
                }
            },
            SinkStreamState::Polling => {
                error!("poll_fetch, Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }

    fn start_send(
        &mut self,
        multiparts: &mut VecDeque<Multipart>,
        buffer_size: usize,
        multipart: Multipart,
    ) -> Result<AsyncSink<Multipart>, Error> {
        if buffer_size < multiparts.len() {
            if let Async::NotReady = self.poll_flush(multiparts)? {
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
    multiparts: VecDeque<Multipart>,
    buffer_size: usize,
    phantom: PhantomData<T>,
}

impl<T> MultipartSinkStream<T>
where
    T: From<Socket>,
{
    pub fn new(sock: SockId, buffer_size: usize) -> Self {
        use crate::poll_thread::DuplicateSock;

        MultipartSinkStream {
            state: SinkStreamState::Pending(sock.dup(), sock),
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
            .start_send(&mut self.multiparts, self.buffer_size, multipart)
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.state.poll_flush(&mut self.multiparts)
    }
}

impl<T> Stream for MultipartSinkStream<T>
where
    T: From<Socket>,
{
    type Item = Multipart;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.state.poll_fetch()
    }
}
