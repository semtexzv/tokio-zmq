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

//! Provide useful types and traits for working with Futures ZMQ.

use std::sync::Arc;
use zmq;

pub use async_zmq_types::{
    ControlHandler, Controllable, EndHandler, IntoInnerSocket, SinkSocket, SinkStreamSocket,
    StreamSocket, WithEndHandler,
};

use crate::socket::config::SocketBuilder;

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
