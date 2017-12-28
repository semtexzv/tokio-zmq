/*
 * This file is part of ZeroMQ Futures.
 *
 * Copyright © 2017 Riley Trautman
 *
 * ZeroMQ Futures is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZeroMQ Futures is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ZeroMQ Futures.  If not, see <http://www.gnu.org/licenses/>.
 */

extern crate futures;
extern crate tokio_core;
extern crate zmq;
extern crate zmq_futures;

use std::io;
use std::time::Duration;

use futures::{Future, Stream};
use futures::stream::iter_ok;
use tokio_core::reactor::{Core, Interval};
use zmq_futures::push::Push;

#[derive(Debug)]
enum Error {
    Zmq(zmq::Error),
    Io(io::Error),
}

impl From<zmq::Error> for Error {
    fn from(e: zmq::Error) -> Self {
        Error::Zmq(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

fn main() {
    let mut core = Core::new().unwrap();
    let workers = Push::new().bind("tcp://*:5557").unwrap();
    let sink = Push::new().connect("tcp://localhost:5558").unwrap();

    let start = zmq::Message::from_slice(b"0").unwrap();

    let interval = Interval::new(Duration::from_secs(1), &core.handle()).unwrap();

    let process = sink.send(start).map_err(Error::from).and_then(|_| {
        iter_ok(0..100)
            .zip(interval)
            .map_err(Error::from)
            .and_then(|(i, _)| {
                println!("Sending: {}", i);

                Ok(zmq::Message::from_slice(format!("{}", i).as_bytes())?)
            })
            .forward(workers.sink::<Error>())
    });

    core.run(process).unwrap();
}