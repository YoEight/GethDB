GethDB
======

The Database For The People.

## Introduction

This project is not a political statement. It is inspired by the Khelish word "Geth," meaning "Servant of the People."

The project is purely experimental and is where I explore various aspects of database design. I aim to test specific
data structures and observe their behavior in real-life scenarios.

This is a side project that I work on during my spare time. It is not production-ready, and most of the code is
undocumented. Although the code is written entirely in Rust, performance is not a primary focus.

## Features

The core engine is a NoSQL, append-only, and immutable database with the following key features:

* Custom binary format for storing data, based on Protobuf 2.
* Indexing is achieved through a homemade LSM tree implementation.
* Client communication is handled via gRPC.
* Full-featured REPL for database interaction.
* Capability to spawn programs using the [Pyro] programming language.
* Supports OpenTelemetry logs and traces, which can be sent to any OpenTelemetry-compatible server when configured, or disabled entirely if preferred.
* Cluster functionality using a homemade Raft implementation (though it is not fully integrated yet).

## Supported platforms

The codebase should compile on any 64-bit platform supported by the Rust compiler. So far, it has been tested on Linux,
macOS, and Windows. Please note that Linux will remain the primary target

## Build from source

You need to have the latest Rust toolchain installed and also `protoc` in your $PATH. Then run this command:

```
cargo build
```

## Docker

```
docker pull yoeight/gethdb:latest
```

At that stage, there is no other version besides `latest`. A new image is pushed every time a commit is pushed to the `master` branch.

The image supports:
- linux/amd64
- linux/arm64/v8

## What's next?

This is the features I want to work on

* Complete benchmark suite
* Support other programming languages for spawn processes, most likely Lua
* Introduce plugins
* Exposes the database core as a programmable platform.

[Pyro]: https://github.com/YoEight/pyro
