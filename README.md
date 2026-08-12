# artie-queue

A persistent, composable HTTP queue in Go. FIFO or LIFO, priority, and delay —
composed freely, so a *delayed priority-LIFO* queue is a configuration, not a
feature.

No database, no embedded KV store, no broker. Storage is an append-only log
this repo implements, with group-commit fsync, crash recovery, and compaction.
The only dependency is the Go standard library.

**What it guarantees, stated plainly:** at-least-once delivery, so consumers
must be idempotent. Every enqueue is on disk before its `201`. A message the
server confirmed survives `kill -9`; a message it confirmed the ack for stays
gone. A log it cannot fully trust stops it from starting rather than being
quietly skipped past. It is a single node with no replication, so one disk is
one failure domain.

```bash
go build -o artie-queue ./cmd/artie-queue
go run ./cmd/jobrunner              # supervises the server, serves the dashboard
open http://localhost:8081
```

### The four required answers

Answered in full further down; linked here so they are not buried.

1. [How do you handle replay messages?](#1-how-do-you-handle-replay-messages) —
   two different things share that name, and they get different answers.
2. [How would you refactor this into a Pub/Sub?](#2-how-would-you-refactor-your-queue-into-a-pubsub) —
   the log is already the topic; the hard part is that priority and LIFO are
   *per-subscriber* orderings.
3. [What would you add with more time?](#3-what-would-you-add-with-more-time) —
   five things, in the order I would actually do them.
4. [Why choose this over SQS, RabbitMQ, or Pulsar?](#4-why-would-anyone-choose-this-over-sqs-rabbitmq-or-pulsar) —
   mostly you shouldn't; here is the narrow case where you would, and where it
   loses.

### Requirements, and where each is met

| the assignment asked for | where |
|---|---|
| FIFO or LIFO | `Config.Ordering`, one comparator ([below](#the-one-idea-worth-stealing)) |
| Priority | `Config.PriorityEnabled`, primary over the mode ([decision 1](#1-priority-is-primary-the-ordering-mode-tie-breaks-within-a-priority-level)) |
| Delay | per-message `delay_ms`, gated by the delayed heap |
| Composable into e.g. delayed priority-LIFO | eight compositions from one comparator; the demo switches between five of them live |
| Persisted, durable, survives restarts | append-only log, fsync before every 2xx, [crash test](#tests) |
| Storage not delegated to a queue or database | stdlib only — the log is [`internal/wal/`](internal/wal/) |
| Concurrency | one mutex per queue, group commit, [tested under `-race`](#concurrency) |
| An application that uses it | [`cmd/jobrunner`](cmd/jobrunner) — worker pool + live dashboard |
| The four questions answered | [top of this file](#the-four-required-answers), in full [below](#the-four-questions-answered) |

---

## The one idea worth stealing

Four features were asked for. Implementing four features would have been the
wrong answer. There is **one ordered structure with a configurable
comparator**, and every queue type falls out of it:

```go
func (c *Config) less(a, b *Message) bool {
    if c.PriorityEnabled && a.effPriority != b.effPriority {
        return a.effPriority > b.effPriority   // priority is primary
    }
    if c.Ordering == LIFO {
        return a.Seq > b.Seq                   // mode tie-breaks within a level
    }
    return a.Seq < b.Seq
}
```

| priority | mode | resulting queue |
|---|---|---|
| off | FIFO | plain FIFO |
| off | LIFO | plain LIFO |
| on  | FIFO | priority-FIFO |
| on  | LIFO | priority-LIFO |

Delay is not a fifth case. **Delay is a gate in front of the same comparator**:
a message that is not yet visible simply isn't in the structure that `dequeue`
looks at. So every row above is also its delayed variant — four orderings ×
delay on or off = **eight compositions, one comparator, zero special cases**.
Delay is per message, so a single queue can hold both at once.

Adding a fifth ordering rule would mean writing one more branch in one function,
not another subsystem. That is the test of whether the abstraction is real.

That is the whole design. Everything below is about making it durable.

### If you only have ten minutes

| look at | why |
|---|---|
| [`internal/queue/message.go`](internal/queue/message.go) → `less()` | the comparator above, in context — 12 lines, eight queue types |
| [`internal/wal/record.go`](internal/wal/record.go) | the record format, and why it carries two checksums |
| [`internal/wal/log.go`](internal/wal/log.go) → `Append` / `Wait` | group commit: durable before the 201, without one fsync per message |
| [`internal/integration/crash_test.go`](internal/integration/crash_test.go) | `kill -9` against the real binary — the test the whole design exists to pass |
| the demo's **corruption lab** | the startup policy, as something you can click |

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
 POST /messages ───▶│ enqueue: assign Seq, append, apply       │
                    │   ├── VisibleAt > now ──▶ delayed heap   │──┐
                    │   └── otherwise ────────▶ ready heap     │  │ timer set to
                    └──────────────────────────────────────────┘  │ next VisibleAt
                                   │                              │
 POST /dequeue ◀───────────────────┤◀─────────────────────────────┘
                                   ▼
                          in-flight (lease heap, by deadline)
                                   │
             ack ──▶ deleted       │       deadline passes ──▶ attempts+1
             nack ─▶ requeued ◀────┘                     └──▶ attempts ≥ max ──▶ DLQ
```

**Two heaps, not one.** The delayed heap is ordered by `VisibleAt`; the ready
heap by the comparator. A single timer, set to the next `VisibleAt`, promotes
between them — no polling, and no scanning the ready heap for visibility.
Scanning would be worse than slow: the top of a heap is the only element you
can inspect in O(1), so "skip the invisible ones" degrades into a linear scan
*and* stops the comparator being a total order over the heap's contents.

**Everything is rebuilt from the log.** Both heaps, in-flight state, the DLQ,
the dedup index and the sequence counter are derived state. The log is the
database.

### Layout

| path | what |
|---|---|
| `internal/wal/` | record format, group-commit writer, replay, corruption policy |
| `internal/queue/` | message model, comparator, heaps, leases, DLQ, dedup, compaction |
| `internal/api/` | HTTP handlers (stdlib `ServeMux`, no router) |
| `internal/demo/` | job runner, supervisor, dashboard |
| `internal/integration/` | tests that SIGKILL the real server binary |

---

## The five design decisions

### 1. Priority is primary; the ordering mode tie-breaks within a priority level

In a priority-LIFO queue holding a priority-5 message and a newer priority-1
message, **the priority-5 message comes out first**.

The alternative — mode primary, priority as tie-breaker — is not a weaker
choice, it is a *degenerate* one. `Seq` is monotonic and unique, so if the mode
sorts first, every comparison is already decided before priority is consulted.
The priority field would be dead configuration.

The real cost of this choice is **starvation**: a steady stream of priority-5
work means priority-1 messages never surface. So there is an optional
`aging_interval_ms`, off by default, that raises a waiting message's *effective*
priority one level per interval up to a cap.

Aging has a subtlety worth naming: a comparator that reads the clock silently
invalidates the heap, because the ordering between two elements can change
while they sit in it and the invariant is only maintained at push and pop. So
the boost is materialised into a stored field on a fixed tick and the heap is
rebuilt in O(n) — the comparator stays a pure function of stored fields. It is
off by default so that the ordering tests assert against a strict total order.

### 2. Lease-and-ack, not pop-and-forget. 30s visibility, 5 attempts

Dequeue moves a message to in-flight with a visibility deadline. The consumer
must `ack` (delete) or `nack` (requeue immediately). If the deadline passes
with neither, the message is redelivered with `Attempts+1`; once attempts reach
`max_attempts` it is dead-lettered.

**This is at-least-once delivery, and the consequence is that consumers must be
idempotent.** Pop-and-forget would be simpler and would silently lose a message
every time a consumer died mid-work, which is the wrong trade for anything that
matters.

Nack and lease-expiry are the same state transition — a delivery that did not
succeed — differing only in cause, which the record type already captures.
A requeued message keeps its original `Seq`, so it returns to its place in the
ordering rather than to the back of the queue.

### 3. Group commit: every enqueue is fsynced before its 201, and concurrent enqueues share one fsync

The naive options are fsync-per-write (durable, pinned to the disk's serial
fsync rate) or batch-and-respond-early (fast, loses the last few milliseconds
on power loss). Group commit refuses the trade:

```go
batch, _ := log.Append(rec)   // buffer, under the queue mutex
queue.mu.Unlock()             // release before waiting
err := log.Wait(batch)        // one fsync serves every writer in this batch
```

A background flusher owns the file descriptor. While it is inside one fsync,
every writer that arrives piles into the next batch and is committed together.
Throughput scales with concurrency instead of being capped by fsync latency,
and **no caller is ever told "committed" before the bytes are on the platter**.

Appending happens under the queue mutex so that log order matches
state-transition order; only the durability wait is outside it. That one rule
is what makes replay reproduce exactly the state a crash interrupted.

Measured (`go test ./internal/queue -bench Enqueue`, Apple silicon, APFS):

| producers | enqueues/sec | records per fsync | fsync latency |
|---|---|---|---|
| 1 | 297 | 1.0 | 3.3 ms |
| 4 | 665 | 2.0 | 3.0 ms |
| 16 | 2,307 | 7.7 | 3.4 ms |
| 64 | 7,818 | 29.5 | 4.0 ms |

The fsync itself costs the same throughout — that is the disk, and no amount of
code makes it faster. What changes is how many messages each one carries.
Throughput rises 26× while the durability guarantee is byte-for-byte identical
at every row. The dashboard's **Burst** button shows the same number climbing
live.

Recovery is ~12.5 ms to replay 20,000 messages (1.6M records/sec, 101 bytes per
message on disk), so restart time is not what the log costs you.

There is **no per-message durability override**. Two durability stories in one
queue is a worse product than one honest one, and "you can turn off safety"
undercuts the entire pitch.

A failed write or fsync is **fatal and sticky**: the queue stops accepting work
and every subsequent call returns the error. We do not know what reached the
disk, and accepting messages we cannot persist is the one thing worse than
being unavailable.

### 4. Corruption: a torn tail truncates, any bad checksum refuses to start

| what replay finds | what happens |
|---|---|
| file ends inside a record, header intact | truncate, log a warning, start |
| record header checksum mismatch | **refuse to start**, name the byte offset |
| complete record, payload checksum mismatch — anywhere, including the last record | **refuse to start**, name the byte offset |
| unrecognised record type | **refuse to start**, name the byte offset |

**A record is never silently skipped.** A queue that quietly drops data it
cannot read is worse than one that will not start, because only one of those
two failures is visible to a human.

The tail case is the interesting one. A *complete* record at the end of the log
with a bad checksum is ambiguous — bit rot, or a torn write that happened to
land on a record boundary. Refusing is the deliberate choice: guessing
"probably torn" is exactly how a queue silently loses a record.

#### Why records carry two checksums

The format is:

```
[4B length][1B type][4B header crc32c][4B record crc32c][payload]
```

The record CRC covers length, type and payload. Covering the *length* matters,
because the length is what tells the reader where the next record starts — one
flipped bit there would silently re-frame the rest of the log.

But the record CRC can only be verified *after* reading `length` bytes, and the
nastiest failure is when `length` itself is what got corrupted: an inflated
length runs past the end of the file, which looks exactly like an interrupted
write. Treating that as a torn tail means truncating — throwing away every
valid, already-acknowledged record behind it, with nothing but a warning.

The header checksum is verifiable before `length` is trusted, and separates the
two cases cleanly:

- header CRC **ok**, length runs past EOF → the header committed and the
  payload did not: a genuine torn tail, safe to discard, because the batch was
  never fsynced and no client was ever told it committed.
- header CRC **bad** → the framing is damaged: corruption, refuse.

Four extra bytes per record to turn "probably fine" into "known". This was
found by using the demo's corruption lab against the original single-checksum
format, and there is a regression test for it
(`TestInflatedLengthIsCorruptionNotATornTail`).

#### The operator escape hatch

Refusing to start is only reasonable if a human has a way forward, so
truncation exists as an explicit, auditable command rather than a silent
default:

```console
$ artie-queue verify -dir ./data
FAIL  ./data/jobs/wal.log
      wal: corrupt record in ./data/jobs/wal.log at byte offset 1311: checksum
      mismatch (header says 0x81f9d3b8, computed 0x4c42f7df over 88 payload bytes)

$ artie-queue repair -dir ./data -queue jobs -truncate-at 1311
truncated ./data/jobs/wal.log from 2729 to 1311 bytes (1418 bytes discarded)
original saved as ./data/jobs/wal.log.bak.1786518130660256000
```

`repair` always leaves the original behind. The point is that a person decided
to drop the tail, not that the tool decided it was unimportant.

### 5. Dedup on a client-supplied `DedupID`, 5-minute window, duplicates return 200

A producer that retries after a timeout gets `200` with the **original message
id**, not a `409`. It wants idempotence, not an error to special-case.

The work is not the lookup — it is that the dedup index must be part of the log
and rebuilt on replay, and must survive compaction, or it is theater the moment
the process restarts. Live messages' entries are rebuilt from their own
`ENQUEUE` records; entries whose message has already been acked (but whose
window is still open) are written explicitly into the compaction snapshot as
`DEDUP` records. Both paths are tested across a restart.

---

## The four questions, answered

### 1. How do you handle replay messages?

Two different things share that name, and they get different answers.

**(a) Redelivery.** A visibility deadline that passes without an ack causes
redelivery with `Attempts+1`. That makes this at-least-once, so consumers must
be idempotent. The queue helps on the *producer* side: `DedupID` makes a
retried enqueue idempotent within a 5-minute window, which is exactly the
situation a producer is in when a request times out and it cannot tell whether
the enqueue committed. The crash test measures this directly — it tracks
requests that were in flight when the process was killed as genuinely
*unknown*, because that is what they are.

The queue does **not** dedup on the consumer side. Delivering twice is a
property of at-least-once, not a bug, and the honest fix is idempotent
consumers rather than a broker pretending to offer exactly-once.

**(b) Log replay.** Storage is an append-only log, so history is already there.
Startup replays every record through the same `apply` functions the live write
path uses — one implementation, two entry points, so a recovered queue cannot
drift from a running one. Nothing stops a consumer resetting to an offset and
re-reading history, which is the Kafka model, and which is exactly the bridge
to the next question.

### 2. How would you refactor your queue into a Pub/Sub?

**The log is already the topic.** What makes this a queue rather than a topic
is that consumption is destructive and there is one shared cursor.

The refactor:

1. Keep the log immutable. Stop letting `ACK` mean "delete"; make it advance a
   cursor instead.
2. Give every subscriber group its own cursor and its own ack/in-flight state.
   `ACK` records become `(group, message)` pairs.
3. Fan out on publish: one `ENQUEUE` record, N groups that can see it.
4. Retention stops being "until acked" and becomes time or size based, since a
   message is no longer removable when the first group finishes with it.
   Compaction becomes segment expiry.

**The real tension is ordering.** Priority and LIFO are *per-subscriber*
properties, not properties of the log. Group A consuming priority-first and
group B consuming FIFO cannot share one global heap, because the heap *is* the
ordering. Each group needs its own index over the shared log — its own ready
heap, built from the same records, ordered by its own comparator.

That is the part that would actually cost work. The immutable log, the fan-out
and the per-group cursors are mechanical; N independent indexes over one log is
a real memory and rebuild-time cost, and it is why most brokers make you choose
between "log with offsets" (Kafka, Pulsar) and "queue with per-message
ordering" (SQS, RabbitMQ) rather than offering both.

### 3. What would you add with more time?

Five, in the order I would actually do them:

1. **Long-poll dequeue.** Consumers currently poll every ~60ms; a `wait_time_ms`
   that parks the request until a message arrives cuts idle load to nothing and
   drops delivery latency. Cheapest real win here.
2. **Segmented log with retention.** One file per queue means compaction
   rewrites everything. Segments make it O(dead segments) instead of O(live
   state), and make time/size retention possible — and they are a prerequisite
   for the Pub/Sub refactor above.
3. **Metrics endpoint.** Prometheus-format depth, in-flight, oldest-message age,
   redelivery rate and fsync latency histograms. The numbers already exist in
   `/stats`; they just need a format an operator's dashboard speaks.
4. **Batch enqueue.** One request, N messages, one fsync. Group commit already
   does this for concurrent clients; a batch endpoint gives a single client the
   same benefit.
5. **Replication.** Ship the log to a follower and require an ack from it
   before responding. This is the one that changes the product — everything
   above is a single-node improvement, and this is what removes "single node"
   from the limitations list. It is also the largest by a wide margin, which is
   why it is last.

Deliberately *not* on this list: exactly-once delivery. It is not achievable
without a transactional consumer, and claiming it would be dishonest.

### 4. Why would anyone choose this over SQS, RabbitMQ, or Pulsar?

For most teams, **they shouldn't**. Those are mature, replicated, operated
systems and this is a single-node binary written for a take-home. Anyone who
tells you their weekend queue beats SQS is selling something.

The honest pitch is narrower and, I think, sharper: **the composition is what's
hard to buy off the shelf.**

- **SQS FIFO** has no priority, no LIFO, and caps delay at 15 minutes. Priority
  is usually emulated with one queue per level and a consumer that polls them
  in order — which is a distributed system of its own, and gets the ordering
  subtly wrong the moment a poll comes back empty.
- **RabbitMQ** has priority queues, but the levels are bounded, priority
  interacts awkwardly with consumer prefetch, and there is no LIFO.
- **Pulsar** can genuinely do most of this, and then you are operating a Pulsar
  cluster — ZooKeeper or etcd, BookKeeper, brokers — for a workload that fits
  on one machine.

So: a single static binary, zero external dependencies, arbitrary composition
of ordering semantics, embeddable as a Go package, and a log format you can
read in an afternoon. For a team that needs *delayed priority-LIFO* and does
not want to run a broker to get it, that is a real gap.

**Where it loses, plainly:**

- **Single node. No replication.** The machine is the failure domain. One disk
  failure is data loss. Every system above survives this; this one does not.
- **Throughput is bounded by one disk's fsync rate.** Group commit raises the
  ceiling with concurrency, but the ceiling is still one device.
- **Memory-resident index.** Every live message's metadata is in RAM. Deep
  backlogs are bounded by memory, not disk.
- **No auth, no TLS, no multi-tenancy, no quotas.** It expects a trusted network.
- **`GET /stats` computes oldest-message age with an O(n) scan** over live
  messages. Fine at these depths; it would need a fourth heap keyed by
  `CreatedAt` before it wasn't.
- **Compaction pauses writes** for its duration, because it runs under the
  queue mutex. At this scale that is a few milliseconds; a large queue would
  need an online snapshot with a delta log.

---

## Concurrency

**One `sync.Mutex` per queue**, guarding that queue's index. At this scale it
is the right call: the critical sections are heap operations and map writes
measured in hundreds of nanoseconds, and the expensive part of the write path
(fsync) deliberately happens *outside* the lock.

Each queue owns its own log file and its own mutex, so two queues never
contend. The manager holds an `RWMutex` over the name→queue map only.

**Where it breaks, and what I'd do:** the first bottleneck would be a single
hot queue with many producers, where mutex handoff starts to dominate. In
order: (1) shard by queue name — already free, since queues are independent;
(2) separate the ready-heap lock from the in-flight map lock, since ack/nack
touch a map and dequeue touches a heap; (3) shard one queue's ready heap into N
sub-heaps with a merge on dequeue, which costs strict global ordering and is
therefore the last resort, not the first.

Choosing the simple thing and knowing where it breaks beats being clever early.

---

## Tests

```bash
go test ./... -race
```

| test | what it proves |
|---|---|
| `TestKill9MidLoadLosesNothingAcknowledged` | SIGKILL the real server binary under concurrent load; every message confirmed with a 201 survives, every acked message stays gone |
| `TestKill9PreservesDelayedAndDeadLettered` | delayed and dead-lettered state survives a crash, not just ready messages |
| `TestServerRefusesToStartOnCorruptLog` | end-to-end: mid-log checksum mismatch → non-zero exit naming the offset → `verify` → `repair` → clean start |
| `TestServerTruncatesTornTailAndStarts` | a valid header with a missing payload is truncated with a warning, not refused |
| `TestInflatedLengthIsCorruptionNotATornTail` | regression: a damaged length field is corruption, not a torn write |
| `TestOrderingModes` / `TestDelayedVariantsRespectTheSameComparator` | all eight compositions, against exact expected sequences |
| `TestRestartPreservesOrderingForEveryMode` | ordering after a restart is byte-identical to ordering without one |
| `TestConcurrentProducersAndConsumers` | 8 producers / 4 consumers: everything delivered, nothing leased twice, nothing acked twice |
| `TestGroupCommitBatchesConcurrentWriters` | concurrent writers share fsyncs and all records replay, in order |
| `TestExhaustedAttemptsDeadLetter` / `TestVisibilityExpiryEventuallyDeadLetters` | attempts drive dead-lettering, via both nack and lease expiry |
| `TestCompactionPreservesSequenceCounter` | compacting an empty queue does not reset `Seq` and reorder future messages |
| `TestRandomizedOperationsSurviveRestart` | 300 pseudo-random operations across 4 configurations and 5 seeds, restarted and compared message-by-message, then drained against an independently-written model of the comparator |
| `FuzzReplay` | arbitrary bytes at the log reader: never panics, never surfaces an unverified record, and replaying the verified prefix is stable |
| `TestStructuralInvariantsUnderConcurrentLoad` | every message is in exactly one structure, heap indices point back correctly, and the heap property holds — checked continuously while six goroutines mutate everything |
| `TestCompactionUnderConcurrentLoadLosesNothing` | ~50 compactions while producers and consumers run, then restart: nothing acknowledged is lost, nothing acked returns |
| `TestCompactionDoesNotThrashWhenLiveStateExceedsThreshold` | compaction cannot loop forever when there is nothing to reclaim |
| `TestIncompleteQueueDoesNotBrickStartup` | an interrupted create does not stop the server or permanently poison the queue name |
| `TestWriteFailureIsStickyAndFatal` | a failed write stops the log for good rather than pretending the next one might land |
| `TestLargePrioritiesSurviveRestartUnchanged` | the priority that comes out of the log is the priority that went in, at every 32-bit boundary |
| `TestInstallReportsWhetherTheRenameLanded` | compaction can tell a failure before the swap from one after it, because they need opposite handling |

The crash test asserts on *bounds*, not exact totals, and the width of the band
is exactly the number of requests in flight when the process died — an
unanswered request may or may not have committed, and pretending otherwise
would be testing a fiction.

### Checking the claims yourself

Every claim above is one command away, and they all run in under a minute:

```bash
# durability: SIGKILL the real binary mid-load, restart, assert nothing
# acknowledged was lost and nothing acked came back
go test ./internal/integration -run Kill9 -v

# the corruption policy end to end: refuse, verify, repair, restart
go test ./internal/integration -run 'Corrupt|TornTail' -v

# ordering for all eight compositions, before and after a restart
go test ./internal/queue -run 'Ordering|Delayed' -v

# throw arbitrary bytes at the log reader (seed corpus runs by default)
go test ./internal/wal -run FuzzReplay -fuzz FuzzReplay -fuzztime 60s

# the group-commit numbers in the table above
go test ./internal/queue -bench Enqueue -benchtime 500x
```

The whole suite is `go test ./... -race`, which takes about 30 seconds.

---

## HTTP API

```
POST   /queues                              {name, ordering, priority_enabled, max_attempts,
                                             default_visibility_timeout_ms, aging_interval_ms,
                                             aging_max_boost, dedup_window_ms}
GET    /queues                              list with stats
POST   /queues/{name}/messages              {payload, priority, delay_ms, dedup_id}
POST   /queues/{name}/dequeue               {max_messages, visibility_timeout_ms}
POST   /queues/{name}/messages/{id}/ack
POST   /queues/{name}/messages/{id}/nack
GET    /queues/{name}/stats                 depth, in-flight, delayed, DLQ, oldest age, fsync stats
GET    /queues/{name}/dlq                   inspect dead-lettered messages
GET    /queues/{name}/peek?limit=N          non-destructive view, in comparator order
POST   /queues/{name}/compact               force compaction
GET    /healthz
```

`payload` is any JSON value, stored as the exact bytes you send.

```bash
curl -X POST localhost:8080/queues \
  -d '{"name":"jobs","ordering":"lifo","priority_enabled":true,"max_attempts":5}'

curl -X POST localhost:8080/queues/jobs/messages \
  -d '{"payload":{"task":"resize"},"priority":3,"delay_ms":5000,"dedup_id":"job-42"}'

curl -X POST localhost:8080/queues/jobs/dequeue \
  -d '{"max_messages":10,"visibility_timeout_ms":30000}'
```

Status codes: `201` new message · `200` duplicate, carrying the original id ·
`400` malformed or invalid request · `404` unknown queue or message · `409`
queue name taken, or ack/nack of a message that isn't leased · `413` payload
over 256 KiB · `503` the log has failed and the queue has stopped accepting
work.

The one endpoint not in the assignment's list is `peek`, which reads the queue
in comparator order without taking a lease. It exists because the dashboard
needs to show ordering, and because "what would this queue hand me next, and
why" is the first question anyone debugging a priority queue asks.

---

## Demo

```bash
go build -o artie-queue ./cmd/artie-queue
go run ./cmd/jobrunner
open http://localhost:8081
```

A worker pool consuming over the real HTTP API — with priority, delay, retries
and dead-lettering all happening at once rather than demonstrated one at a
time. Sliders control pool size, failure rate (nack), abandon rate (never ack,
so the lease expires) and visibility timeout.

The demo **supervises the queue server as a child process**, which is what
makes two of its panels honest rather than theatrical:

- **Crash lab** — `kill -9` sends a real SIGKILL to a real pid. Restart, and
  the board repopulates from the log. In-flight leases correctly do not
  survive: they come back as ready.
- **Corruption lab** — flip a byte mid-log and the server *refuses to start*,
  showing you the actual refusal and byte offset; then run the operator repair
  and watch it come back. Or tear the tail and watch it truncate, warn, and
  start. This is the panel that found the header-checksum bug.

Also on the page: a live records-per-fsync readout with a **Burst** button, a
compaction button showing the log shrink, and a dedup demo that submits the
same `dedup_id` twice and shows the second returning the first one's id.

The five composition tabs are five pre-created queues, because a queue's config
is immutable — replay derives dead-lettering from `max_attempts`, so the same
records must always produce the same state. Switching tabs points the workers
at a different composition.

---

## Implementation notes

### What isn't durable, on purpose

**Dequeue writes nothing to the log.** A lease is not durable state: if the
process dies, every in-flight message correctly reappears as ready, which is
exactly what lease expiry would have done anyway. The cost is that a crash can
under-count `Attempts` by one, which is cheaper than an fsync on the read path.
Lease *expiry* is logged, because attempt counts decide dead-lettering and
those must survive.

### Compaction

**Compaction** snapshots live state to a temp file, fsyncs it, atomically
renames it over the log, fsyncs the directory, then swaps the descriptor. A
crash at any point leaves either the whole old log or the whole new one. The
snapshot carries `NextSeq` forward explicitly — without it, compacting a fully
drained queue would reset the sequence counter and reorder new messages against
already-acknowledged history.

Compaction distinguishes a failure *before* the rename from one *after* it,
because they need opposite handling. Before: nothing changed, the old log is
still live, the queue carries on uncompacted. After: the descriptor the queue
holds points at an unlinked inode, so anything appended to it would be
invisible to every future reader — the queue fails closed rather than
acknowledge writes that are already lost. Collapsing both into one error is a
fail-open: a failed directory fsync would silently discard everything written
afterwards.

Compaction triggers on log size, but the log must also have **doubled since the
last snapshot**. Size alone is not enough: a queue whose live state is already
larger than the threshold can never get back under it, so it would re-compact
on every timer tick forever, rewriting the whole log to reclaim nothing. The
doubling rule bounds write amplification to a constant factor.

### Counters and interrupted creates

**Counters** in `/stats` (`enqueued`, `acked`, `dead_lettered`, …) count what
*this process* has done, so a restart resets them; queue depths are recovered
state and do not. They are incremented by the public methods rather than inside
the shared `apply*` functions, because those run during replay too — and a
compacted snapshot restores dead-lettered messages as ordinary records, so a
counter incremented during replay would report a different number depending on
whether compaction had happened.

**An interrupted create** leaves a log with no committed record — either
zero-length, or holding a partially written META. Startup warns and skips it
rather than refusing, because it contains nothing anyone was told was durable,
and refusing would let one interrupted create take every other queue on the box
down with it. "Does this queue already exist?" is answered by asking whether
its log holds a committed record, not by whether a file is present: a torn META
is non-empty but commits nothing, and treating it as existing would leave the
name skipped at boot yet permanently unusable. Creating over a log that is
*corrupt* rather than incomplete still refuses — that file may hold real data.

---

## What this got wrong first

Every one of these passed its tests before it was found. They are here because
the class of bug is the interesting part, and because a durability claim is
only worth as much as the effort spent trying to break it.

**A corrupt length field masqueraded as a torn write.** The original format had
one checksum per record. A flipped bit in a length field made the record claim
to run past EOF — indistinguishable from an interrupted write — so the reader
truncated and discarded every valid record behind it, with only a warning.
Exactly the silent loss the corruption policy exists to prevent. Found by
pointing the demo's corruption lab at the format that was supposed to survive
it. Fixed with the header checksum described above.

**Compaction failed open.** `CommitAs` fsynced the directory *after* the
rename, so a failure there returned an error after the swap had committed. The
caller treated all errors alike: no reopen, no fail-closed. The queue then
appended to an unlinked inode, fsynced it, and answered `200` with a durability
guarantee — while the log a restart would read contained none of it. The fix is
that the installer now reports *whether the rename landed*, separately from the
error, because before and after the swap need opposite handling.

**Priority was stored narrower than it was compared.** Persisted as `int32`
while `Message.Priority` is a Go `int`. A priority past 2³¹ was accepted,
acknowledged, then silently reinterpreted on replay — a running queue ordered
on the value the client sent, a recovered queue on its low 32 bits
sign-extended, so the highest-priority message could come back as the lowest.
Rejecting large priorities would have fixed the symptom; widening the stored
field to match the in-memory type removes the failure mode, leaving no rule for
a future change to forget.

**Compaction could thrash**, rewriting the whole log every tick when live state
already exceeded the threshold. **An interrupted create could brick startup**,
taking every other healthy queue down with it. **A failed queue spun** at 1 ms
forever instead of going quiet.

The through-line: four of the six were *fail-open* paths — an error handled by
carrying on rather than stopping. For a queue whose entire pitch is "we tell
you the truth about what is durable", fail-open is the failure mode worth
hunting deliberately, because it is the one that never shows up as a crash.
