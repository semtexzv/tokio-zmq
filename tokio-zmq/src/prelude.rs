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

//! Provide useful types and traits for working with Tokio ZMQ.

use std::{sync::Arc, time::Duration};

use futures::Stream;
use zmq;

pub use async_zmq_types::{
    ControlHandler, Controllable, EndHandler, IntoInnerSocket, SinkSocket, SinkStreamSocket,
    StreamSocket, WithEndHandler,
};

use crate::{async_types::TimeoutStream, error::Error, socket::config::SocketBuilder};

/* ----------------------------------TYPES----------------------------------- */

/* ----------------------------------TRAITS---------------------------------- */

/// This trait allows adding a timeout to any stream with Error = Error.
pub trait WithTimeout: Stream<Error = Error> + Sized {
    /// Add a timeout to a given stream.
    ///
    /// ### Example, using a Pull wrapper type
    /// ```rust
    /// extern crate futures;
    /// extern crate tokio_zmq;
    /// extern crate zmq;
    ///
    /// use std::{sync::Arc, time::Duration};
    ///
    /// use futures::{Future, Stream};
    /// use tokio_zmq::{prelude::*, Socket, Pull, Multipart};
    ///
    /// fn main() {
    ///     let ctx = Arc::new(zmq::Context::new());
    ///     let pull = Pull::builder(ctx)
    ///         .bind("tcp://*:5574")
    ///         .build()
    ///         .unwrap();
    ///
    ///     // Receive a Timeout after 30 seconds if the stream hasn't produced a value
    ///     let fut = pull.stream().timeout(Duration::from_secs(30));
    ///
    ///     // tokio::run(fut.map(|_| ()).or_else(|e| {
    ///     //     println!("Error: {}", e);
    ///     //     Ok(())
    ///     // }));
    /// }
    /// ```
    fn timeout(self, duration: Duration) -> TimeoutStream<Self>;
}

/// This trait is implemented by all socket types to allow custom builders to be created
pub trait HasBuilder {
    fn builder(ctx: Arc<zmq::Context>) -> SocketBuilder<'static, Self>
    where
        Self: Sized,
    {
        SocketBuilder::new(ctx)
    }
}

/* ----------------------------------impls----------------------------------- */

impl<T> HasBuilder for T where T: IntoInnerSocket {}

impl<T> WithTimeout for T
where
    T: Stream<Error = Error>,
{
    fn timeout(self, duration: Duration) -> TimeoutStream<Self> {
        TimeoutStream::new(self, duration)
    }
}
