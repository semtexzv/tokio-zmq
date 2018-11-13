# Futures ZMQ

[documentation](https://docs.rs/futures-zmq/)
[crates.io](https://crates.io/crates/futures-zmq)

This crate contains wrappers around ZeroMQ Concepts with Futures.

See the [examples folder](https://git.asonix.dog/asonix/async-zmq/src/branch/development/futures-zmq/examples) for usage examples.

### Getting Started

```toml
futures-zmq = "0.1"
futures = "0.1.25"
zmq = "0.8"
```

In your application:
```rust
use std::sync::Arc;

use futures::Future;
use futures_zmq::{Error, MultipartRequest, MultipartResponse};
use zmq::{Context, Socket, SocketType};

fn main() -> Result<(), Error> {
    let context = Arc::new(Context::new());

    let sock = context.socket(SocketType::REP)?;
    sock.bind("tcp://*:5000")?;

    let fut = MultipartResponse::new(sock)
        .and_then(|(sock, msg)| {
            println!("Echoing {:?}", msg);
            MultipartRequest::new(sock, msg)
        })
        .map(|_: Socket| ());

    fut.wait()?;

    Ok(())
}
```

### Running the examples
The `req.rs` and `rep.rs` examples are designed to be used together. The `rep` example starts a server with a REP socket, and the `req` example queries that server with a REQ socket.


### Contributing
Feel free to open issues for anything you find an issue with. Please note that any contributed code will be licensed under the GPLv3.

### License

Copyright © 2018 Riley Trautman

Futures ZMQ is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

Futures ZMQ is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. This file is part of Futures ZMQ.

You should have received a copy of the GNU General Public License along with Futures ZMQ. If not, see [http://www.gnu.org/licenses/](http://www.gnu.org/licenses/).
