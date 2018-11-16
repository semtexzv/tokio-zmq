/*
 * This file is part of Tokio ZMQ.
 *
 * Copyright © 2018 Riley Trautman
 *
 * Tokio ZMQ is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Tokio ZMQ is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Tokio ZMQ.  If not, see <http://www.gnu.org/licenses/>.
 */

//! This module defines the `SinkType` type. A wrapper around Sockets that implements
//! `futures::Sink`.

use std::{collections::VecDeque, mem};

use async_zmq_types::Multipart;
use futures::{Async, AsyncSink};
use log::error;
use zmq;

use crate::{
    async_types::{future_types::RequestFuture, EventedFile},
    error::Error,
};

pub(crate) struct SinkType {
    buffer_size: usize,
    inner: SinkState,
}

pub(crate) enum SinkState {
    Ready,
    Pending(VecDeque<Multipart>),
    Polling,
}

impl SinkType {
    pub(crate) fn new(buffer_size: usize) -> Self {
        SinkType {
            buffer_size,
            inner: SinkState::Ready,
        }
    }

    pub(crate) fn polling(&mut self) -> SinkState {
        mem::replace(&mut self.inner, SinkState::Polling)
    }

    fn poll_request(
        &mut self,
        sock: &zmq::Socket,
        file: &EventedFile,
        mut pending: VecDeque<Multipart>,
    ) -> Result<Async<()>, Error> {
        if let Some(mut multipart) = pending.pop_front() {
            match RequestFuture.poll(sock, file, &mut multipart)? {
                Async::Ready(()) => self.poll_request(sock, file, pending),
                Async::NotReady => {
                    pending.push_front(multipart);
                    self.inner = SinkState::Pending(pending);
                    Ok(Async::NotReady)
                }
            }
        } else {
            self.inner = SinkState::Ready;
            Ok(Async::Ready(()))
        }
    }

    pub(crate) fn start_send(
        &mut self,
        multipart: Multipart,
        sock: &zmq::Socket,
        file: &EventedFile,
    ) -> Result<AsyncSink<Multipart>, Error> {
        match self.polling() {
            SinkState::Ready => {
                let mut pending = VecDeque::new();
                pending.push_back(multipart);

                self.inner = SinkState::Pending(pending);
                Ok(AsyncSink::Ready)
            }
            SinkState::Pending(mut pending) => {
                if pending.len() > self.buffer_size {
                    self.inner = SinkState::Pending(pending);

                    if let Async::NotReady = self.poll_complete(sock, file)? {
                        match self.polling() {
                            SinkState::Ready => {
                                self.inner = SinkState::Ready;

                                return self.start_send(multipart, sock, file);
                            }
                            SinkState::Pending(pending) => {
                                let pending_size = pending.len();
                                self.inner = SinkState::Pending(pending);

                                if pending_size > self.buffer_size {
                                    return Ok(AsyncSink::NotReady(multipart));
                                } else {
                                    return self.start_send(multipart, sock, file);
                                }
                            }
                            SinkState::Polling => {
                                error!("Inner polling should not have been polling");
                                return Err(Error::Sink);
                            }
                        }
                    } else {
                        return self.start_send(multipart, sock, file);
                    }
                }
                pending.push_back(multipart);
                self.inner = SinkState::Pending(pending);
                Ok(AsyncSink::Ready)
            }
            SinkState::Polling => {
                error!("Polling called while polling");
                Err(Error::Sink)
            }
        }
    }

    pub(crate) fn poll_complete(
        &mut self,
        sock: &zmq::Socket,
        file: &EventedFile,
    ) -> Result<Async<()>, Error> {
        match self.polling() {
            SinkState::Pending(pending) => self.poll_request(sock, file, pending),
            SinkState::Ready => {
                self.inner = SinkState::Ready;
                Ok(Async::Ready(()))
            }
            SinkState::Polling => {
                error!("Error in poll_complete");
                Err(Error::Sink)
            }
        }
    }
}
