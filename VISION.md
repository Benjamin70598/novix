# VISION

## The whiteboard sentence

Build the platform developers choose when the network is no longer something they can assume.

## The problem, in one paragraph

Software increasingly runs in thousands of physical places: restaurants, stores, ships, depots, factories, vehicles. Every one of those places has a WAN link that fails, throttles, or costs real money. Today the only good answer is the one Chick-fil-A, Target, and Royal Caribbean built for themselves: local compute, local messaging, custom buffering, custom sync, custom fleet management, maintained by platform teams most companies will never have. The pieces exist (brokers, edge Kubernetes, sync engines) but the integrated behavior does not come as a product. Everyone below the top 1% either stitches it together badly or accepts that the site goes dark when the internet does.

## What Novix is

An edge-native event fabric with three commitments:

1. **Local-first.** A site is fully functional with zero connectivity. Not degraded, functional. Apps talk to the local agent and nothing else. Offline is the normal case the system is designed around, not an error path.
2. **Policy-driven.** Developers declare intent per topic: process here, sync everything or aggregates or anomalies, this priority, this bandwidth cap, this retention, this authority. The runtime owns the mechanics. Policy is the product; the log and the transport are commodities.
3. **Honest reconciliation.** Append-only events interleave without conflict by construction. Mutable state has a declared per-topic authority and a visible, explainable conflict rule. No hidden magic, no silent loss, ever. Harder merge semantics (CRDTs, application merges) arrive only when a real workload demands them.

## What Novix is not

- Not a faster Kafka. Not a Kafka at all. If a design discussion starts with partitions and consumer groups, it has left the mission.
- Not an edge Kubernetes distribution. Compute placement is someone else's problem; Novix is the data and event layer that runs on whatever is there, down to a single small box.
- Not a general-purpose multi-master database. Per-topic authority plus append-only facts covers the beachhead honestly. Scope creep toward full geo-replication is how this dies.
- Not a protocol project. Zenoh and NATS are excellent protocols. Novix competes on the policy plane, the fleet data model, and the operator experience, and may even ride on existing transports where that is the fastest honest path.

## Who it is for, in order

1. Retail and QSR chains, 200 to 2,000 sites, with in-store compute and no platform army. The buyer is platform engineering and infrastructure leadership.
2. Adjacent site-based operators: c-stores, grocery, pharmacy, logistics depots.
3. The flagships that built DIY stacks, as displacement, once the product has earned it.
4. Industrial, maritime, and vehicles, later, from a position of strength.

## The test for every feature

Does this make disconnected, site-first applications easier to build and operate for a small team?

If no, it is not Novix core, regardless of how interesting it is.

## What must be proven, in order

1. The partition demo: pull the cable, keep selling, reconcile on heal, big bandwidth number. Boring reliability is the wow.
2. Three prospects describe this pain in their own words with a dollar figure attached.
3. One design partner pilots in 1 to 5 real sites.
4. POS-grade correctness under crash and partition: money is never lost or duplicated.

Anything not in service of those four is deferred.
