<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

# internal/

## Subdirectories

| Directory          | What                                              | When to read                                                  |
| ------------------ | ------------------------------------------------- | ------------------------------------------------------------- |
| `scheduler/`       | gocron-based periodic task scheduler              | Modifying check intervals, adding scheduled tasks             |
| `cache/`           | Hazelcast cache interfaces and initialization     | Adding cache maps, modifying lock behavior                    |
| `republish/`       | Event republishing logic with distributed locking | Debugging republishing, modifying cancellation or throttling  |
| `listener/`        | Hazelcast subscription change event listener      | Debugging delivery type changes, callback URL changes         |
| `circuitbreaker/`  | Circuit breaker state management and health check | Modifying CB loop detection, exponential backoff              |
| `handler/`         | Stuck event handlers (DELIVERING, FAILED, WAITING)| Debugging stuck events, modifying handler lock behavior       |
| `healthcheck/`     | Endpoint health checking with cooldown            | Modifying health check methods, cooldown logic                |
| `api/`             | Fiber HTTP API routes and handlers                | Adding API endpoints, modifying circuit breaker API           |
| `kafka/`           | Kafka message picking and republishing            | Debugging event republishing, modifying Kafka interactions    |
| `mongo/`           | MongoDB queries for event status messages         | Modifying event queries, adding new query patterns            |
| `config/`          | Viper-based configuration loading and types       | Adding config options, modifying defaults                     |
| `auth/`            | OAuth token retrieval                             | Modifying authentication flow                                 |
| `log/`             | Zerolog configuration                             | Modifying log format or levels                                |
| `metrics/`         | Prometheus metrics and middleware                  | Adding metrics, modifying instrumentation                     |
| `tracing/`         | Zipkin distributed tracing                        | Modifying trace propagation                                   |
| `utils/`           | Generic helper functions                          | Adding utility functions                                      |
| `test/`            | Shared test mocks and Docker test infrastructure  | Writing tests, adding mock implementations                    |
