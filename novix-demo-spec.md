# Novix Partition Demo: Build Spec

**Goal:** a demo where the network cable gets pulled on stage, the store keeps selling, and on reconnect the system reconciles and syncs only what policy says matters, with a live bandwidth counter proving the point.

**Non-goal:** building Novix. This is a demo that proves the abstraction, in the smallest amount of code that is honest.

---

## The demo script (write this first, build backwards from it)

Scene: one laptop, three terminal panes or one web dashboard. Two simulated stores plus one cloud.

1. **Steady state (60s).** Store A and Store B are each generating POS order events and fryer telemetry. Dashboard shows: local event counts per store, cloud's synced view, and a running bandwidth meter with two numbers: bytes a naive replicate-everything design would have sent vs bytes Novix policy sync actually sent.
2. **The pull (30s).** `make partition-storeB` (docker network disconnect). Store B's link goes red on the dashboard. Orders KEEP FLOWING at Store B. Local apps (a receipt printer consumer, a kitchen display consumer) keep consuming. Nothing crashes, nothing is queued in application code; the runtime owns it.
3. **The stress (60s).** While partitioned, inject an equipment fault event at Store B (policy: sync immediate) and a price update from the cloud (policy: push to all stores). Show both are pending, correctly classified, in the sync ledger.
4. **The reconnect (60s).** `make heal-storeB`. Watch the sync ledger drain in priority order: fault event first, order events next, telemetry as 5-minute aggregates only. Cloud view converges. Bandwidth meter shows the punchline, e.g. "partition backlog: 84,000 events, 41 MB raw. Synced: 1,900 events, 0.9 MB. Zero orders lost."
5. **The close (30s).** Show the entire behavior was driven by one YAML file. Change `telemetry.sync` from `aggregates_5m` to `all`, re-run, watch the bandwidth number blow up. Policy is the product.

Total: under 5 minutes. Rehearsable, deterministic, no live internet needed.

---

## Architecture

```
  docker compose, single laptop

  +--------------------+   +--------------------+   +--------------------+
  |  store-a           |   |  store-b           |   |  cloud             |
  |  novixd (agent)    |   |  novixd (agent)    |   |  novixd (hub mode) |
  |  - local log       |   |  - local log       |   |  - global view     |
  |  - policy engine   |   |  - policy engine   |   |  - sync sink       |
  |  - sync loop       |   |  - sync loop       |   |  - policy push     |
  |  posgen (producer) |   |  posgen (producer) |   +--------------------+
  |  kds (consumer)    |   |  kds (consumer)    |            |
  +--------------------+   +--------------------+   +--------------------+
       |        |                |        |         |  dashboard (web)   |
   net_a (always up)         net_b (partitionable)  +--------------------+
```

- One binary, `novixd`, runs in agent mode (stores) or hub mode (cloud). One binary matters: it is the future product shape and it makes the demo trivially reproducible.
- `posgen` and `kds` are tiny separate processes talking to novixd over localhost only. This visually proves apps never touch the WAN.
- Partition = `docker network disconnect demo_wan store-b`. Real TCP failure, not a mock.

## Core components inside novixd

1. **Local log.** Append-only, per-topic, SQLite in WAL mode. One table: `(topic, site_seq, ts, key, payload, class)`. SQLite is deliberate: crash-safe, zero ops, and disarms the "did you just demo an in-memory toy" objection. Target: a few thousand events/sec, far more than the demo needs.
2. **Local pub/sub.** HTTP long-poll or gRPC stream on localhost. Consumers track their own offsets (stored in the same SQLite). No cluster, no partitions, no consumer groups. Single writer per site makes ordering trivial: `site_seq` is a monotonic integer.
3. **Policy engine.** Loads `novix.yaml`, classifies every event at write time (class column), and drives the sync loop. Classification at write time, not sync time, so the ledger UI can show pending counts per class during the partition.
4. **Sync loop.** Site to hub, pull-based from the hub or push from the site (push is simpler; do push). Maintains per-topic high-water marks. On reconnect: drain in `priority` order. For `aggregates_5m` classes, the site computes windowed aggregates locally and syncs only the aggregate records; raw stays on site with a retention policy.
5. **Reconciliation.** Keep the demo honest but narrow:
   - Event topics (orders, telemetry, faults): append-only facts, no conflicts by construction. Global order is (site_id, site_seq); the hub never rewrites history, it interleaves.
   - State topic (price list): last-writer-wins with hybrid logical clocks, hub is authoritative for this topic per policy (`authority: hub`). Demo one conflict: store-local price override made during partition loses to a hub update with a later HLC, and the dashboard shows the LWW decision explicitly.
   - Say out loud in the demo what is NOT solved: multi-master writes to the same key with merge semantics. Per-topic authority plus append-only events covers retail POS honestly. CRDTs are a roadmap slide, not demo code.

## The policy file (the actual product)

```yaml
site: store-b
hub: cloud:9400
bandwidth_cap: 2mbps

topics:
  orders:
    process: local
    sync: all
    priority: 1
    authority: site        # facts happen at the store
  equipment_faults:
    process: local
    sync: immediate
    priority: 0
  fryer_telemetry:
    process: local
    sync: aggregates_5m
    retention_local: 72h
    priority: 2
  price_list:
    type: state
    authority: hub         # hub wins conflicts
    sync: push_to_sites

offline:
  continue: true
  buffer: 72h
```

## Tech choices

- **Go.** Single static binary, goroutines map cleanly to log/sync/serve loops, cross-compiles for the future Raspberry Pi version of this demo (a Pi with a physical ethernet cable to yank is the conference-stage upgrade).
- **SQLite via modernc.org/sqlite** (pure Go, no cgo, keeps cross-compile trivial).
- **Dashboard:** one HTML page served by the hub, polling a JSON status endpoint. No framework. The bandwidth meter and the sync ledger are the two UI elements that matter.
- **No Kafka, no NATS, no Redis anywhere in the demo.** The absence is the pitch.

## Milestones (nights and weekends honest sizing)

| # | Deliverable | Definition of done |
|---|---|---|
| 1 | Log + local pub/sub | posgen writes, kds consumes, survives `kill -9` of novixd mid-stream with zero loss |
| 2 | Policy engine + sync | Hub converges in steady state; aggregates class syncs windows only; bandwidth meter counts both real and counterfactual bytes |
| 3 | Partition + reconcile | Full demo script steps 1 to 4 pass, scripted, 10 consecutive clean runs |
| 4 | Dashboard + LWW conflict + polish | 5-minute demo runnable by someone else from the README |

Roughly one focused weekend each. If milestone 3 takes three weekends, that is fine; it is the one that matters.

## Metrics the demo must produce (these become sales collateral)

- Orders lost during partition: **0** (the only acceptable number)
- Sync reduction vs naive replication: target **20x to 50x** on the telemetry-heavy workload
- Reconnect-to-converged time for a 1-hour partition backlog: seconds, displayed live
- Lines of application code that deal with connectivity: **0** (show posgen and kds source on a slide)

## Hygiene

- Personal GitHub, personal laptop, personal time. No PingCAP assets, accounts, code, or customer knowledge in the repo. Review the employment agreement's IP assignment clause before the repo goes public.
- Keep a VISION.md and a DECISIONS.md from commit one. Investors and design partners read decision logs; it demonstrates judgment.
