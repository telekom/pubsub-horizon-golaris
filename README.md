<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

<p align="center">
<!--
  <img src="docs/img/Horizon.svg" alt="Golaris logo" width="200">
-->
  <h1 align="center">Golaris</h1>
</p>

<p align="center">
  Horizon component for handling circuit breaker and republishing functionality.
</p>

<p align="center">
  <a href="#prerequisites">Prerequisites</a> •
  <a href="#building-golaris">Building Golaris</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#running-golaris">Running Golaris</a>
</p>

## Overview

Horizon Golaris serves as the circuit breaker within the [Horizon ecosystem](https://github.com/telekom/pubsub-horizon). It ensures the redelivery of failed events by periodically checking the availability of a customer's endpoint using HEAD or GET requests. When the endpoint becomes available again, all events for that customer and endpoint with the status `WAITING` are getting republished and can be picked up by Comet or Pulsar redelivery.

> **Note:** Golaris is an essential part of the Horizon ecosystem. Please refer to [documentation of the entire system](https://github.com/telekom/pubsub-horizon) to get the full picture.

## Prerequisites
For the optimal setup, ensure you have:

- A running instance of Kafka
- A running hazelcast host

## Building Golaris
### Go build

Assuming you have already installed [Go](https://go.dev/), simply run the following to build the executable:
```bash
go build
```

> Alternatively, you can also follow the Docker build in the following section if you want to build a Docker image without the need to have Golang installed locally.

### Docker build
This repository provides a multi-stage Dockerfile that will also take care of compiling the software, as well as dockerizing Quasar. Simply run:

```bash
docker build -t horizon-golaris:latest  . 
```

## Configuration

## Running Golaris
### Locally
Before you can run Golaris locally you must have a running instance of Kafka and MongoDB locally or forwarded from a remote cluster.

After that you can run Golaris in a dev mode using this command:
```shell
go run . serve
```

## Operational Information

## Documentation

Read more about the software architecture and the general process flow of Horizon Golaris in [docs/architecture.md](docs/architecture.md).

## Contributing

We're committed to open source, so we welcome and encourage everyone to join its developer community and contribute, whether it's through code or feedback.  
By participating in this project, you agree to abide by its [Code of Conduct](./CODE_OF_CONDUCT.md) at all times.

## Code of Conduct

This project has adopted the [Contributor Covenant](https://www.contributor-covenant.org/) in version 2.1 as our code of conduct. Please see the details in our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). All contributors must abide by the code of conduct.

By participating in this project, you agree to abide by its [Code of Conduct](./CODE_OF_CONDUCT.md) at all times.

## Licensing

This project follows the [REUSE standard for software licensing](https://reuse.software/). You can find a guide for developers at https://telekom.github.io/reuse-template/.   
Each file contains copyright and license information, and license texts can be found in the [./LICENSES](./LICENSES) folder. For more information visit https://reuse.software/.