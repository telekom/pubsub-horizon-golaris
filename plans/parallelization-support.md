# Fix Multi-Replica Correctness in Golaris Scheduler and Cancellation

> **Plan ID:** `4f884f80-f4c3-4cf9-9b22-36cf5983796d`
> **Created:** 2026-02-18

## Problem

Golaris has four critical multi-replica correctness bugs:

1. Scheduler loops exit early with `return` instead of `continue` when subscription is nil, skipping remaining entries
2. Local `subscriptionCancelMap` provides cancellation signal invisible to other replicas
3. Listener sets cancel status without checking entry existence, creating a race condition
4. Handler/republishing locks use `TryLockWithTimeout` without lease, orphaning locks on process crash until session timeout

## Approach

Four independent milestones targeting each bug:

- **M-001** fixes return-vs-continue in scheduler loops
- **M-002** removes local cancel map and replaces with distributed Hazelcast `ContainsKey` checks with per-batch granularity
- **M-003** adds missing entry check before cancel signal in listener
- **M-004** replaces all `TryLockWithTimeout` with `TryLockWithLeaseAndTimeout` (60s lease)

Each milestone includes unit/integration tests using existing dockertest+testify patterns.

---

## Decision Log

| ID | Decision | Reasoning |
|----|----------|-----------|
| DL-001 | Replace `return` with `continue` in scheduler loops | `scheduler.go:98,131` use `return` when subscription nil -- exits entire loop, skips remaining entries. `continue` allows processing remaining entries in same iteration. |
| DL-002 | Remove local `subscriptionCancelMap` and use Hazelcast `ContainsKey` for distributed cancellation | Local map (`cache.go:47-66`) invisible across replicas -- goroutines on other instances miss cancellation signals. Hazelcast entry existence provides distributed signal visible to all replicas. |
| DL-003 | Add `RepublishingCache` entry existence check before setting cancel status in `listener.go:102-105` | `handleDeliveryTypeChangeFromSSEToCallback` sets cancel=true (`listener.go:119`) but missing entry check -- race if goroutine deletes entry between check and set. |
| DL-004 | Replace `TryLockWithTimeout` with `TryLockWithLeaseAndTimeout` (60s lease) for handler and republishing locks | Process crash while holding lock orphans lock until Hazelcast session timeout (~minutes). Lease-based locks auto-release after 60s, matching `HealthCheckCache` pattern (`healthcheck.go:154`). |
| DL-005 | Add `context.Context` parameter to `RepublishPendingEvents` signature | `ContainsKey` requires `context.Context` but `RepublishPendingEvents` currently lacks context parameter. |
| DL-006 | Check `ContainsKey` per-batch rather than per-message | Per-message `ContainsKey` adds 1-5ms latency per event. Per-batch check balances responsiveness (batch-level granularity) with performance (single check per ~10 messages). |

---

## Milestone 1: Fix scheduler return-vs-continue bugs

**Files:**
- `internal/scheduler/scheduler.go`
- `internal/scheduler/scheduler_test.go`

**Requirements:**
- Scheduler loops continue processing remaining entries when subscription is nil

**Acceptance Criteria:**
- Unit tests verify both `checkOpenCircuitBreakers` and `checkRepublishingEntries` process all entries even when some subscriptions are nil

### Code Intents

| ID | File | Behavior | Decisions |
|----|------|----------|-----------|
| CI-M-001-001 | `scheduler.go` | Change `return` to `continue` at line 98 in `checkOpenCircuitBreakers` when subscription is nil, allowing loop to process remaining circuit breaker entries | DL-001 |
| CI-M-001-002 | `scheduler.go` | Change `return` to `continue` at line 131 in `checkRepublishingEntries` when subscription is nil, allowing loop to process remaining republishing entries | DL-001 |
| CI-M-001-003 | `scheduler_test.go` | Add unit test verifying `checkOpenCircuitBreakers` continues processing after nil subscription (multiple CB entries, subscription exists for only some) | DL-001 |
| CI-M-001-004 | `scheduler_test.go` | Add unit test verifying `checkRepublishingEntries` continues processing after nil subscription (multiple republishing entries, subscription exists for only some) | DL-001 |

---

## Milestone 2: Replace local cancel map with Hazelcast distributed checks

**Files:**
- `internal/cache/cache.go`
- `internal/republish/republish.go`
- `internal/listener/listener.go`
- `internal/circuitbreaker/circuitbreaker.go`
- `internal/api/circuitbreaker.go`
- `internal/republish/republish_test.go`

**Requirements:**
- Cancellation signals visible across all Hazelcast replicas, no local-only state

**Acceptance Criteria:**
- Integration test verifies `ForceDelete` on one replica stops goroutines on different replica via `ContainsKey` check

### Code Intents

| ID | File | Behavior | Decisions |
|----|------|----------|-----------|
| CI-M-002-001 | `cache.go` | Remove `subscriptionCancelMap` (line 47), `cancelMapMutex` (line 48), `SetCancelStatus` function (lines 56-60), and `GetCancelStatus` function (lines 62-66) | DL-002 |
| CI-M-002-002 | `republish.go` | Update `RepublishPendingEvents` signature to accept `context.Context` as first parameter. Replace `SetCancelStatus(false)` call at line 138 with no-op. Replace `GetCancelStatus` checks at lines 142, 173, 183 with `RepublishingCache.ContainsKey` checks (inverted logic: entry exists = continue, entry missing = cancelled). Check `ContainsKey` once per batch before `FindWaitingMessages`/`FindProcessedMessagesByDeliveryTypeSSE`, not in throttle sleep loop. | DL-002, DL-005, DL-006 |
| CI-M-002-003 | `listener.go` | Remove `SetCancelStatus` calls at lines 88, 119, 163, 198, 241 (distributed cancellation now handled by entry deletion alone) | DL-002 |
| CI-M-002-004 | `circuitbreaker.go` | Remove `SetCancelStatus` call at line 188 in `forceDeleteRepublishingEntry` (entry deletion sufficient for cancellation signal) | DL-002 |
| CI-M-002-005 | `api/circuitbreaker.go` | Remove `SetCancelStatus` call at line 95 (entry deletion at line 91 provides cancellation signal) | DL-002 |
| CI-M-002-006 | `republish_test.go` | Add integration test: two concurrent `HandleRepublishingEntry` goroutines on different mock replicas, `ForceDelete` called mid-processing, verify both goroutines detect cancellation via `ContainsKey` returning false | DL-002, DL-006 |

---

## Milestone 3: Add missing entry check in SSE-to-callback listener

**Files:**
- `internal/listener/listener.go`
- `internal/listener/listener_test.go`

**Requirements:**
- Listener only sets cancel status when `RepublishingCache` entry exists, preventing race conditions

**Acceptance Criteria:**
- Unit test verifies `handleDeliveryTypeChangeFromSSEToCallback` handles missing entry without error

### Code Intents

| ID | File | Behavior | Decisions |
|----|------|----------|-----------|
| CI-M-003-001 | `listener.go` | Add `RepublishingCache.Get` check before `SetCancelStatus(true)` at line 119 in `handleDeliveryTypeChangeFromCallbackToSSE`. Only set cancel status if entry exists. Prevents race where goroutine deletes entry between listener check and cancel signal. | DL-003 |
| CI-M-003-002 | `listener_test.go` | Add unit test: `handleDeliveryTypeChangeFromSSEToCallback` called when no `RepublishingCache` entry exists, verify no error and no cancel status set | DL-003 |

---

## Milestone 4: Replace TryLockWithTimeout with TryLockWithLeaseAndTimeout

**Files:**
- `internal/handler/delivering.go`
- `internal/handler/failed.go`
- `internal/handler/waiting.go`
- `internal/republish/republish.go`
- `internal/handler/delivering_test.go`
- `internal/handler/failed_test.go`
- `internal/handler/waiting_test.go`

**Requirements:**
- Handler and republishing locks auto-release after 60 seconds, preventing orphaned locks on process crash

**Acceptance Criteria:**
- Unit tests verify locks acquired with `TryLockWithLeaseAndTimeout` auto-release after 60s when `Unlock` not called

### Code Intents

| ID | File | Behavior | Decisions |
|----|------|----------|-----------|
| CI-M-004-001 | `delivering.go` | Replace `TryLockWithTimeout` at line 23 with `TryLockWithLeaseAndTimeout(ctx, cache.DeliveringLockKey, 60*time.Second, 100*time.Millisecond)` | DL-004 |
| CI-M-004-002 | `failed.go` | Replace `TryLockWithTimeout` at line 24 with `TryLockWithLeaseAndTimeout(ctx, cache.FailedLockKey, 60*time.Second, 100*time.Millisecond)` | DL-004 |
| CI-M-004-003 | `waiting.go` | Replace `TryLockWithTimeout` at line 42 with `TryLockWithLeaseAndTimeout(ctx, cache.WaitingLockKey, 60*time.Second, 100*time.Millisecond)` | DL-004 |
| CI-M-004-004 | `republish.go` | Replace `TryLockWithTimeout` at line 82 with `TryLockWithLeaseAndTimeout(ctx, subscriptionId, 60*time.Second, 100*time.Millisecond)` | DL-004 |
| CI-M-004-005 | `delivering_test.go` | Add unit test: verify `CheckDeliveringEvents` acquires lock with lease, simulate crash (no `Unlock`), verify lock auto-releases after 60s | DL-004 |
| CI-M-004-006 | `failed_test.go` | Add unit test: verify `CheckFailedEvents` acquires lock with lease, simulate crash (no `Unlock`), verify lock auto-releases after 60s | DL-004 |
| CI-M-004-007 | `waiting_test.go` | Add unit test: verify `CheckWaitingEvents` acquires lock with lease, simulate crash (no `Unlock`), verify lock auto-releases after 60s | DL-004 |

---

## Verification

1. Run existing tests to ensure no regressions:
   ```bash
   cd horizon-components/pubsub-horizon-golaris
   go test -tags testing ./...
   ```

2. New tests specifically verify:
   - **Fix 1:** Multiple entries processed even when first subscription is nil
   - **Fix 2:** Republishing stops when entry is deleted from Hazelcast (no local map needed)
   - **Fix 3:** SSE-to-callback change with existing entry triggers ForceDelete
   - **Fix 4:** Locks auto-release after lease expiry

3. Confirm complete removal of cancel map:
   ```bash
   grep -r "CancelStatus\|cancelMap\|subscriptionCancelMap" internal/
   ```
