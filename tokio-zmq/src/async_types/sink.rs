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

//! This module defines the `MultipartSink` type. A wrapper around Sockets that implements
//! `futures::Sink`.

use async_zmq_types::Multipart;
use futures::{Async, AsyncSink, Sink};
use zmq;

use crate::{
    async_types::{sink_type::SinkType, EventedFile},
    error::Error,
};

/// The `MultipartSink` Sink handles sending streams of data to ZeroMQ Sockets.
///
/// You shouldn't ever need to manually create one. Here's how to get one from a 'raw' `Socket`'
/// type.
///
/// ### Example
/// ```rust
/// extern crate zmq;
/// extern crate futures;
/// extern crate tokio;
/// extern crate tokio_zmq;
///
/// use std::sync::Arc;
///
/// use futures::{Future, Sink};
/// use tokio_zmq::{prelude::*, Error, Multipart, Pub, Socket};
///
/// fn get_sink(socket: Socket) -> impl Sink<SinkItem = Multipart, SinkError = Error> {
///     socket.sink(25)
/// }
///
/// fn main() {
///     let context = Arc::new(zmq::Context::new());
///     let socket = Pub::builder(context)
///         .bind("tcp://*:5568")
///         .build()
///         .unwrap()
///         .socket();
///     let sink = get_sink(socket);
///
///     let msg = zmq::Message::from_slice(b"Some message");
///
///     // tokio::run(sink.send(msg.into())).unwrap();
/// }
/// ```
pub struct MultipartSink {
    sock: zmq::Socket,
    file: EventedFile,
    inner: SinkType,
}

impl MultipartSink {
    pub fn new(buffer_size: usize, sock: zmq::Socket, file: EventedFile) -> Self {
        MultipartSink {
            sock,
            file,
            inner: SinkType::new(buffer_size),
        }
    }
}

impl Sink for MultipartSink {
    type SinkItem = Multipart;
    type SinkError = Error;

    fn start_send(
        &mut self,
        multipart: Self::SinkItem,
    ) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        self.inner.start_send(multipart, &self.sock, &self.file)
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.inner.poll_complete(&self.sock, &self.file)
    }
}