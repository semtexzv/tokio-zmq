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

use std::mem;

use async_zmq_types::Multipart;
use futures::Async;
use zmq;

use crate::{
    async_types::{future_types::ResponseFuture, EventedFile},
    error::Error,
};

pub(crate) struct StreamType {
    inner: StreamState,
}

pub(crate) enum StreamState {
    Ready,
    Pending(Multipart),
    Polling,
}

impl StreamType {
    pub(crate) fn new() -> Self {
        StreamType {
            inner: StreamState::Ready,
        }
    }

    pub(crate) fn polling(&mut self) -> StreamState {
        mem::replace(&mut self.inner, StreamState::Polling)
    }

    pub(crate) fn poll_response(
        &mut self,
        sock: &zmq::Socket,
        file: &EventedFile,
        mut multipart: Multipart,
    ) -> Result<Async<Option<Multipart>>, Error> {
        match ResponseFuture.poll(&sock, &file, &mut multipart)? {
            Async::Ready(item) => {
                self.inner = StreamState::Ready;

                Ok(Async::Ready(Some(item)))
            }
            Async::NotReady => {
                self.inner = StreamState::Pending(multipart);

                Ok(Async::NotReady)
            }
        }
    }

    pub(crate) fn poll(
        &mut self,
        sock: &zmq::Socket,
        file: &EventedFile,
    ) -> Result<Async<Option<Multipart>>, Error> {
        match self.polling() {
            StreamState::Ready => self.poll_response(sock, file, Multipart::new()),
            StreamState::Pending(multipart) => self.poll_response(sock, file, multipart),
            StreamState::Polling => Err(Error::Stream),
        }
    }
}
