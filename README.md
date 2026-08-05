# novix

Edge-native event fabric. Process locally, sync what matters, survive partitions.

**Status: pre-alpha. Private. Building toward a single demo, not a product.**

Milestone 1 (durable site log, local pub/sub, crash test) and Milestone 2 (policy engine, sync loop, hub mode, bandwidth meters) are complete and tested. Milestone 3 is the partitioned two-store docker demo.

## What this is

Novix is a runtime for applications that run in many physical locations (stores, ships, warehouses, vehicles) where the network is unreliable and the business cannot stop when it fails.

Apps at a site talk only to the local Novix agent. The agent owns durability, buffering, and synchronization. A policy file, not application code, decides what gets processed locally, what leaves the site, in what order, and at what fidelity.

```yaml
topics:
  orders:            { process: local, sync: all, priority: 1 }
  equipment_faults:  { process: local, sync: immediate, priority: 0 }
  fryer_telemetry:   { process: local, sync: aggregates_5m, retention_local: 72h }
offline:
  continue: true
  buffer: 72h
```

The companies that needed this (Chick-fil-A, Target, Royal Caribbean) built it in-house because there was nothing to buy. Novix is the buy option.

## Current goal

One demo, described in `docs/demo-spec.md`: two simulated stores and a cloud hub on one laptop. Pull the network on a store, it keeps selling. Reconnect, and the backlog drains in policy priority order with a live bandwidth meter comparing policy sync against naive replicate-everything.

Success criteria:

- Orders lost during partition: 0
- Sync bytes vs naive replication on a telemetry-heavy workload: 20x to 50x reduction
- Application code that handles connectivity: 0 lines

## Layout

```
cmd/novixd/      single binary: -mode agent (site) or -mode hub (cloud)
cmd/posgen/      demo producer (POS orders)
cmd/telegen/     demo producer (equipment telemetry)
cmd/kds/         demo consumer (kitchen display)
internal/log/    append-only per-topic log on SQLite (WAL), sync state, meters
internal/policy/ policy loading and write-time event classification
internal/sync/   site-to-hub sync loop: priority drain, aggregation, meters
internal/hub/    hub mode: idempotent sync sink, fleet view
examples/        example site policy (store-a.yaml)
third_party/     vendored yaml.v3 (offline builds, see DECISIONS 010)
docs/            demo spec, why-novix-exists, architecture diagram
```

## Running (once milestone 1 lands)

```
make demo        # docker compose up: store-a, store-b, cloud, dashboard
make partition-storeB
make heal-storeB
```

## Non-negotiables

- One static binary. No external brokers, databases, or coordinators at a site.
- Apps never touch the WAN.
- Crash safety is table stakes: kill -9 the agent mid-stream, lose nothing.

See `VISION.md` for what this is trying to become and `DECISIONS.md` for why things are the way they are.

## License

None yet, intentionally. The repo is private; a license is chosen deliberately before anything becomes public. See DECISIONS.md entry 003.
