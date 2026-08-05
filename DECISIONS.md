# DECISIONS

Append-only log of significant decisions. Newest at the bottom. Format: context, decision, alternatives considered, consequences, revisit trigger. A decision without a revisit trigger is dogma; avoid dogma.

---

## 001: The first artifact is a demo, not a product

**Date:** 2026-08-04
**Context:** Solo founder-stage project, nights and weekends, no design partner yet. Infrastructure projects die by building runtimes for hypothetical users.
**Decision:** All engineering effort targets a single 5-minute partition demo (docs/demo-spec.md) until a design partner exists. The demo doubles as the sales asset for discovery calls.
**Alternatives:** Build a general runtime first (slower to signal, riskier); whitepaper only (nobody believes whitepapers about partitions).
**Consequences:** Code will contain demo-shaped shortcuts. That is acceptable and documented inline.
**Revisit when:** A design partner signs, or 90 days pass without one (then the whole project gets re-evaluated, not just this decision).

## 002: Beachhead is retail/QSR store systems, North America

**Date:** 2026-08-04
**Context:** "Edge" as a horizontal is not a market. Vertical beachheads are. Candidates: retail store systems, industrial telemetry, maritime, vehicles.
**Decision:** Retail and QSR chains in the 200 to 2,000 site range. Refuse to be talked out of it.
**Alternatives:** Industrial (buyer is OT, gated by integrators and certification, slow POCs); maritime (tiny number of accounts); vehicles (OEM sales cycles, embedded constraints).
**Rationale:** The buyer is platform engineering and infrastructure leadership, a persona reachable with a normal enterprise motion. Public reference architectures (Chick-fil-A, Target) mean zero market education. Store outages have recognized P&L cost. One-store POCs take weeks, not quarters.
**Revisit when:** Ten discovery calls fail to surface budgeted pain, or an inbound design partner from another vertical shows up with money.

## 003: No license until the repo goes public, then decided deliberately

**Date:** 2026-08-04
**Context:** GitHub repo created private. License choice for infrastructure is strategic: Apache 2.0 maximizes bottoms-up adoption (Kafka, NATS, Zenoh precedent); BSL/FSL protects against cloud strip-mining at the cost of early trust.
**Decision:** No license file while private (nobody has rights by default). Choose deliberately before first public commit.
**Revisit when:** Two weeks before any public release, as a scheduled decision with its own entry.

## 004: One static binary, agent and hub are the same executable

**Date:** 2026-08-04
**Context:** Product runs at sites with no IT staff in the building. Every additional process, dependency, or coordinator multiplies field failure modes.
**Decision:** Single Go binary, `novixd`, mode selected by config. No external broker, database server, or coordinator at a site.
**Alternatives:** Embed NATS for transport (revisit-worthy later); separate agent/hub codebases (drift, double the ops surface).
**Consequences:** Some hub-side features will feel cramped in the same binary. Accepted until it hurts.
**Revisit when:** Hub-side requirements (multi-tenant fleet views, auth) clearly outgrow the shared binary.

## 005: Site log is SQLite in WAL mode

**Date:** 2026-08-04
**Context:** The site log must survive kill -9 and power loss, run on small hardware, and require zero operations. Demo also needs to disarm the "in-memory toy" objection.
**Decision:** Append-only per-topic log in SQLite (WAL), pure-Go driver (modernc.org/sqlite) to keep cross-compilation trivial (future Raspberry Pi demo).
**Alternatives:** Custom segment files (more work, no ops story yet, premature); embedded key-value stores like Pebble/Badger (better write throughput, worse inspectability; nothing at demo scale needs it).
**Consequences:** Write throughput ceiling in the low thousands of events/sec per site. Fine for the beachhead workload by orders of magnitude.
**Revisit when:** A real workload exceeds SQLite write throughput, measured, not imagined.

## 006: Events are classified by policy at write time, not sync time

**Date:** 2026-08-04
**Context:** The sync ledger UI must show pending counts per class during a partition, and the bandwidth meter must show the counterfactual naive-replication byte count.
**Decision:** The policy engine stamps a class on every event at append. Sync drains by class priority.
**Alternatives:** Classify lazily at sync (simpler write path, but the ledger UI and counterfactual meter become expensive scans).
**Consequences:** Policy changes do not reclassify history. Documented as expected behavior.
**Revisit when:** A design partner needs retroactive reclassification.

## 007: Reconciliation scope is per-topic authority plus append-only events, nothing more

**Date:** 2026-08-04
**Context:** Reconciliation is where edge projects drown. Full multi-master merge semantics are a research program.
**Decision:** Two topic types only. Event topics: append-only facts, globally interleaved by (site_id, site_seq), no conflicts by construction. State topics: one declared authority (hub or site) with last-writer-wins on hybrid logical clocks, and every LWW decision is surfaced, never silent.
**Alternatives:** CRDTs (elegant, unnecessary for POS-shaped workloads, huge surface); application merge callbacks (pushes the hard problem onto users on day one).
**Consequences:** Some workloads are honestly out of scope for now, and the demo says so out loud.
**Revisit when:** A paying design partner has a workload that per-topic authority genuinely cannot express.

## 008: No Kafka, NATS, or Redis anywhere in the demo

**Date:** 2026-08-04
**Context:** The pitch is not operating a distributed system in every store. A demo that apt-installs a broker per site contradicts the pitch on stage.
**Decision:** The demo stack is novixd, two tiny demo apps, docker compose, and a static dashboard page. Nothing else.
**Alternatives:** NATS as embedded transport (fastest path to reliable transport; deliberately deferred so the demo's absence-of-brokers stays visually true; may return as an internal implementation detail post-demo, see 004).
**Revisit when:** Post-demo, when transport hardening begins.

## 009: Naming and IP hygiene

**Date:** 2026-08-04
**Context:** Founder is employed full-time in adjacent infrastructure GTM. Name "Novix" has known collisions in unrelated industries (legal tech, industrial hardware).
**Decision:** Repo private on personal account, personal hardware and time only, no employer assets, accounts, code, or customer-specific knowledge. Employment agreement IP assignment and invention-disclosure clauses to be reviewed this week, before any external sharing. Trademark search (classes 9 and 42) before public naming commitment; treat "Novix" as a working title.
**Revisit when:** Agreement review completes, or before first public artifact, whichever is first.
