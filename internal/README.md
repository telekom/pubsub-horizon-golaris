<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

# Golaris Internal Architecture

## Overview

Golaris monitors circuit breaker state and republishes events when consumer endpoints recover. It runs as multiple replicas sharing state through Hazelcast distributed caches. The scheduler periodically scans circuit breaker and republishing caches, spawning goroutines for each entry. Subscription changes (delivery type, callback URL, redeliveries-per-second) trigger republishing through a Hazelcast entry listener.

## Multi-Replica Correctness

All state coordination uses Hazelcast distributed primitives. No local-only state participates in cross-replica signaling.

### Distributed Cancellation

Cancellation of in-progress republishing goroutines uses two Hazelcast distributed signals: `ContainsKey` (entry existence) and `IsLocked` (lock ownership). When a republishing entry is deleted from the cache (via `ForceDelete`), `IsLocked` becomes false first (ForceUnlock step), then `ContainsKey` becomes false (Delete step), giving earlier detection. The check runs at three points: per-batch (fatal), per-message (non-fatal), and post-throttle (non-fatal). Cancellation returns `ErrCancelled` to prevent the caller from deleting a replacement entry that may have been created by the canceller.

Previous designs used a local `subscriptionCancelMap` that was invisible to other replicas. Goroutines on replica B would miss cancellation signals issued on replica A.

### Scheduler Loop Continuity

Scheduler loops (`checkOpenCircuitBreakers`, `checkRepublishingEntries`) use `continue` on nil subscription lookups. Each iteration processes all entries in the cache -- a nil subscription for one entry must not prevent processing of remaining entries.

## Invariants

- **No local-only cancellation state**: Republishing cancellation signals must be visible to all replicas. Entry deletion from `RepublishingCache` is the sole cancellation mechanism.
- **Entry existence before cache mutation**: Listener handlers must verify `RepublishingCache` entry existence before performing operations that assume the entry is present.
- **Scheduler loops never early-return on single-entry failures**: `return` in scheduler iteration loops is reserved for cache-level errors, not per-entry conditions.

## Design Decisions

| ID     | Decision                                          | Rationale                                                                                 |
| ------ | ------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| DL-001 | `continue` instead of `return` in scheduler loops | `return` exits entire loop, skipping remaining entries in same iteration                   |
| DL-002 | Hazelcast `ContainsKey` for distributed cancel    | Local map invisible across replicas; entry existence is already a distributed signal       |
| DL-003 | Entry existence check before cancel in listener   | Prevents race condition if goroutine deletes entry between check and set                   |
| DL-004 | `context.Context` on `RepublishPendingEvents`     | `ContainsKey` requires context; propagated from caller's lock context                      |
| DL-005 | Triple-check cancellation (`ContainsKey`+`IsLocked`) | Checked per-batch, per-message, and post-throttle to detect cancellation promptly          |
