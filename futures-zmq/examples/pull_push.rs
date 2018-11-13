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

extern crate futures;
extern crate futures_zmq;
extern crate tokio;
extern crate zmq;

use std::sync::Arc;

use futures::{Future, Stream};
use futures_zmq::{prelude::*, Multipart, Pull, Push, Sub};

pub struct Stop;

impl ControlHandler for Stop {
    fn should_stop(&mut self, _: Multipart) -> bool {
        println!("Got stop signal");
        true
    }
}

fn main() {
    let ctx = Arc::new(zmq::Context::new());
    let cmd = Sub::builder(Arc::clone(&ctx))
        .connect("tcp://localhost:5559")
        .filter(b"")
        .build()
        .unwrap();
    let stream = Pull::builder(Arc::clone(&ctx))
        .connect("tcp://localhost:5557")
        .build()
        .unwrap();
    let sink = Push::builder(ctx)
        .connect("tcp://localhost:5558")
        .build()
        .unwrap();

    let runner = stream
        .stream()
        .controlled(cmd.stream(), Stop)
        .map(|multipart| {
            for msg in &multipart {
                if let Some(msg) = msg.as_str() {
                    println!("Relaying: {}", msg);
                }
            }
            multipart
        })
        .forward(sink.sink(25));

    tokio::run(runner.map(|_| ()).or_else(|e| {
        println!("Error!: {:?}", e);
        Ok(())
    }));
}
