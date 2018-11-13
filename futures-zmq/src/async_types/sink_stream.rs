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
    async_types::{MultipartRequest, MultipartResponse, RequestResponse, SendRecvState},
    error::Error,
    socket::Socket,
};

enum SinkStreamState {
    Pending(zmq::Socket),
    Streaming(MultipartResponse<Socket>),
    Sinking(MultipartRequest<Socket>),
    Either(SendRecvState),
    Polling,
}

impl SinkStreamState {
    fn polling(&mut self) -> SinkStreamState {
        mem::replace(self, SinkStreamState::Polling)
    }

    fn poll_sink(
        &mut self,
        mut fut: MultipartRequest<Socket>,
        multiparts: &mut VecDeque<Multipart>,
    ) -> Result<Async<()>, Error> {
        if let Async::Ready(sock) = fut.poll()? {
            *self = SinkStreamState::Pending(sock.inner());

            if !multiparts.is_empty() {
                self.poll_flush(multiparts)
            } else {
                Ok(Async::Ready(()))
            }
        } else {
            *self = SinkStreamState::Sinking(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_stream(
        &mut self,
        mut fut: MultipartResponse<Socket>,
    ) -> Result<Async<Option<Multipart>>, Error> {
        if let Async::Ready((msg, sock)) = fut.poll()? {
            *self = SinkStreamState::Pending(sock.inner());

            Ok(Async::Ready(Some(msg)))
        } else {
            *self = SinkStreamState::Streaming(fut);

            Ok(Async::NotReady)
        }
    }

    fn poll_flush(&mut self, multiparts: &mut VecDeque<Multipart>) -> Result<Async<()>, Error> {
        match self.polling() {
            SinkStreamState::Pending(sock) => {
                if let Some(multipart) = multiparts.pop_front() {
                    let fut = MultipartRequest::new(sock, multipart);

                    self.poll_sink(fut, multiparts)
                } else {
                    Ok(Async::Ready(()))
                }
            }
            SinkStreamState::Either(mut snd_rcv_state) => match snd_rcv_state.poll()? {
                Async::NotReady => {
                    *self = SinkStreamState::Either(snd_rcv_state);

                    Ok(Async::NotReady)
                }
                Async::Ready(req_res) => match req_res {
                    RequestResponse::Request(req) => {
                        *self = SinkStreamState::Sinking(req);

                        self.poll_flush(multiparts)
                    }
                    RequestResponse::Response(res) => {
                        *self = SinkStreamState::Streaming(res);

                        Ok(Async::NotReady)
                    }
                },
            },
            SinkStreamState::Sinking(fut) => self.poll_sink(fut, multiparts),
            SinkStreamState::Streaming(fut) => {
                *self = SinkStreamState::Streaming(fut);

                Ok(Async::NotReady)
            }
            SinkStreamState::Polling => {
                error!("Called polling while polling");
                return Err(Error::Polling);
            }
        }
    }

    fn poll_fetch(
        &mut self,
        multiparts: &mut VecDeque<Multipart>,
    ) -> Result<Async<Option<Multipart>>, Error> {
        match self.polling() {
            SinkStreamState::Pending(sock) => match multiparts.pop_front() {
                Some(multipart) => {
                    *self = SinkStreamState::Either(SendRecvState::Pending(sock, multipart));

                    self.poll_fetch(multiparts)
                }
                None => {
                    let fut = MultipartResponse::new(sock);

                    self.poll_stream(fut)
                }
            },
            SinkStreamState::Sinking(fut) => {
                if let Async::NotReady = self.poll_sink(fut, multiparts)? {
                    return Ok(Async::NotReady);
                }

                self.poll_fetch(multiparts)
            }
            SinkStreamState::Streaming(fut) => self.poll_stream(fut),
            SinkStreamState::Either(mut snd_rcv_state) => match snd_rcv_state.poll()? {
                Async::Ready(req_res) => {
                    match req_res {
                        RequestResponse::Request(req) => {
                            *self = SinkStreamState::Sinking(req);
                        }
                        RequestResponse::Response(res) => {
                            *self = SinkStreamState::Streaming(res);
                        }
                    }
                    self.poll_fetch(multiparts)
                }
                Async::NotReady => {
                    *self = SinkStreamState::Either(snd_rcv_state);

                    Ok(Async::NotReady)
                }
            },
            SinkStreamState::Polling => {
                error!("Called polling while polling");
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
    pub fn new(sock: zmq::Socket, buffer_size: usize) -> Self {
        MultipartSinkStream {
            state: SinkStreamState::Pending(sock),
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
        self.state.poll_fetch(&mut self.multiparts)
    }
}
