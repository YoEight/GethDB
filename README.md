GethDB
======

A database system functioning as a synthetic servant for your data needs.

## Introduction

This project name is inspired by the Khelish word "Geth," meaning "Servant of the People."

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

You need to have the latest Rust toolchain installed, `protoc` in your $PATH and all Google protobuf wellknown types available. Then just run this command:

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

## Kubernetes
The project includes an example configuration for deploying a GethDB node in Kubernetes. You can find the manifest files in the `/k8s` directory. The configuration includes a simple setup with a Datalust Seq instance for collecting OpenTelemetry traces and logs.

To deploy the application (assuming you have Kubernetes properly configured on your machine):
```
kubectl apply -k k8s/manifests/
```

To test the application locally, forward the port to your machine:
```
kubectl port-forward -n geth-app service/gethdb-service 2113:2113
```

To view the traces sent by GethDB to Seq, you can also forward the Seq service port:
```
kubectl port-forward -n geth-app service/seq-service 5341:80
```
The Seq UI will be available at http://localhost:5341

The configuration also includes an OpenTelemetry collector for metrics ingestion, a Prometheus aggregator for data storage and querying, and a Grafana instance pre-configured with a dashboard dedicated to GethDB metrics visualization.

## Metrics and Monitoring

GethDB exposes comprehensive Prometheus-format metrics that provide detailed insights into system performance, I/O operations, resource utilization, and cache efficiency. These metrics include system-level measurements such as CPU and memory usage, detailed I/O operation tracking with histogram distributions for read/write sizes, and performance indicators like cache miss rates. The metrics are designed to give operators full visibility into GethDB's operational health and performance characteristics.

For complete metric definitions, usage examples, and alerting recommendations, see [METRICS.md](./METRICS.md).

A pre-configured Grafana dashboard is available at [`./k8s/manifests/dashboards/geth-engine.json`](./k8s/manifests/dashboards/geth-engine.json) that visualizes all key metrics with organized panels for system overview, I/O operations analysis, and performance monitoring.

## Examples
To quickly see how to interact with GethDB, use the `geth-quickstart` project, which is configured to work with any GethDB node running on `localhost:2113`. Currently, only a Rust client is available, but more clients will be added later.

## What's next?

This is the features I want to work on

* Complete benchmark suite
* Support other programming languages for spawn processes, most likely Lua
* Introduce plugins
* Exposes the database core as a programmable platform.

[Pyro]: https://github.com/YoEight/pyro
