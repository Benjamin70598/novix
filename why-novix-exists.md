# Why Novix Exists

**A sales document, not an engineering document.**
Working draft v0.1. Personal project. Drafted on personal time and personal hardware.

---

## Positioning statement

For companies that run software in hundreds or thousands of physical locations, Novix is an edge-native event fabric that keeps every site running through network outages and synchronizes only the data that matters. Unlike the DIY stacks that Fortune 500 retailers spent years building in-house, Novix delivers disconnected operation, policy-driven sync, and fleet-scale management as a single product a five-person platform team can run.

**One line:** The biggest retailers built this themselves because they had no choice. Everyone else still has no choice. Novix is the buy option that has never existed.

---

## The proof the pain is real: three named companies

These three companies all hit the same wall. None of them could buy a product that solved it. All three built it in-house at a cost only they could afford. They are proof of pain, reference architectures, and long-term displacement prospects in one.

### 1. Chick-fil-A (QSR, ~2,800 US locations)

**The documented pain.** Chick-fil-A runs a three-node K3s Kubernetes cluster in every restaurant, roughly 2,800 clusters total, specifically so that business-critical workloads run without internet dependency. Their fleet of IoT devices (fryers, grills, tablets) exchanges billions of MQTT messages per month. At peak a restaurant serves one sandwich every 16 seconds, and kitchen automation cannot stall because the ISP hiccuped.

**What they had to build themselves.**

```
CURRENT STATE (all DIY, built and maintained by an internal platform team)

  Restaurant (x2,800)                          Cloud
  +--------------------------------+           +---------------------------+
  |  3x consumer-grade compute     |           |  AWS                      |
  |  K3s cluster (self-bootstrap)  |           |                           |
  |  MQTT broker (local bus)       | intermit. |  Fleet mgmt (DIY)         |
  |  Apps: forecasting, kitchen    |<--------->|  GitOps deploy (DIY)      |
  |  automation, telemetry         |  WAN      |  Identity per-site (DIY)  |
  |  Vector observability pipeline |           |  Observability agg (DIY)  |
  |  Local buffering + retry (DIY) |           |  Analytics / ML training  |
  +--------------------------------+           +---------------------------+

  DIY components they wrote or assembled: cluster bootstrap, fleet GitOps,
  per-site identity, MQTT-to-cloud sync, buffering during outages,
  observability shipping, remote debug with no direct cluster access.
```

**With Novix.**

```
FUTURE STATE

  Restaurant (x2,800)                          Cloud
  +--------------------------------+           +---------------------------+
  |  Novix Edge runtime            |           |  Novix Control            |
  |   - local event fabric         | policy-   |   - fleet state view      |
  |   - offline-first by default   | driven    |   - policy distribution   |
  |   - policy engine              |<--------->|   - selective sync sink   |
  |  Apps subscribe locally,       |  sync     |  Analytics / ML training  |
  |  never talk to the WAN         |           |  (consumes synced stream) |
  +--------------------------------+           +---------------------------+

  policy:
    fryer_telemetry:  { process: local, sync: aggregates_5m }
    order_events:     { process: local, sync: all, priority: high }
    equipment_faults: { process: local, sync: immediate }
    offline:          { continue: true, buffer: 72h }
```

**The sales math.** Chick-fil-A has publicly described years of platform engineering effort across deployment, identity, observability, and sync. Conservatively that is a 6 to 10 person platform team on an ongoing basis, call it $1.5M to $2.5M per year in fully loaded cost, before opportunity cost. That is the budget line Novix attacks.

### 2. Target (retail, ~2,000 US stores)

**The documented pain.** Target needed each store to act as its own mini-cloud while developers experienced one big cloud. Traditional models of clustering, failover, and high availability had to be completely rethought for in-store compute. No product did this, so they built Unimatrix, a custom control plane that presents thousands of store Kubernetes clusters behind a single unified API, plus a per-store agent that applies specs and reports state back.

**What they had to build themselves.**

```
CURRENT STATE

  Store (x~2,000)                              Cloud
  +--------------------------------+           +---------------------------+
  |  In-store K8s cluster          |           |  Unimatrix central (DIY)  |
  |  Unimatrix agent (DIY)         |<--------->|   - merged fleet state    |
  |  POS, inventory, pickup apps   | intermit. |   - K8s-compatible API    |
  |  Local data services           |  WAN      |  Spinnaker CD pipelines   |
  +--------------------------------+           +---------------------------+

  DIY components: entire Unimatrix control plane, state merging across
  ~2,000 clusters, store-capability targeting for rollouts, agent protocol.
```

**With Novix.** Unimatrix solved deployment. It did not solve the data problem: what happens to events and state generated in-store during a partition, and what subset of it should ever leave the store. Novix positions as the data and event layer that sits beside (not against) a deployment control plane. Before/after is the same shape as Chick-fil-A: replace the DIY sync, buffering, and selective replication logic inside every store app with declared policy.

**The sales math.** Target's engineering blog documents multiple teams and years on this problem. Same DIY-tax argument, larger number.

### 3. Royal Caribbean (maritime, ~25+ ships)

**The documented pain.** Each ship runs a full local streaming stack (publicly documented Kafka clusters per vessel) for payments, loyalty, and recommendations, because a ship at sea is the definition of a disconnected site. Satellite bandwidth is expensive and constrained, so bulk data replication waits until the ship reaches port, where sync windows are only a few hours. Ship-shore synchronization across periods of limited connectivity was the core requirement that drove their architecture selection.

**What they had to build themselves.**

```
CURRENT STATE

  Ship (x25+)                                   Shore
  +--------------------------------+            +--------------------------+
  |  Onboard DC (space-constrained)|  satellite |  Central systems of      |
  |  Kafka cluster per ship        |  (narrow,  |  record                  |
  |  Payments, loyalty, recs       |  costly)   |  Analytics               |
  |  Custom ship-shore sync (DIY)  |<---------->|  Fleet ops               |
  |  Port-window bulk replication  |  port      |                          |
  +--------------------------------+  windows   +--------------------------+

  DIY components: prioritized sync over satellite, port-window bulk
  replication, conflict handling for records touched on ship and shore,
  running and operating Kafka on a ship with minimal IT staff aboard.
```

**With Novix.** The policy model is the pitch in its purest form: `sync: { payments: immediate_via_satellite, telemetry: port_window, media: never }`. Bandwidth policy is a first-class primitive instead of custom code. A per-ship Kafka cluster plus custom sync becomes one runtime with declared intent.

**The sales math.** Satellite bandwidth cost is a real, recurring, measurable line item. Novix's selective sync converts directly into dollars per ship per month. This is the rare infrastructure sale with a hard ROI number.

---

## The pattern, stated once

1. The problem is universal: physical sites, unreliable WAN, business cannot stop.
2. The existing answer is DIY: Kafka or MQTT plus Kubernetes plus custom sync, buffering, and fleet glue.
3. Only the top 1% of companies can afford the DIY answer. Chick-fil-A, Target, and Royal Caribbean have world-class platform teams. The other 10,000 companies with the same problem do not.
4. **Novix sells the DIY stack as a product to the companies that cannot build it.**

This is a displacement-of-nothing motion for the beachhead (they have the pain and no solution) and a consolidation motion for the flagships (replace five stitched systems with one). Both motions are familiar: it is the same shape as selling a distributed SQL database to companies running DIY sharded MySQL.

---

## Beachhead decision: retail and QSR store systems, North America

Picked, and the plan is to refuse to be talked out of it.

**Why this vertical and not industrial telemetry:**

1. The buyer is IT/platform engineering, not OT. Factory floors are gated by OT vendors, systems integrators, safety certification, and 20-year equipment lifecycles. Store systems are bought by the same personas who buy databases: VP Infrastructure, Director of Platform Engineering, enterprise architects. That is the buyer profile an enterprise database SE already knows how to find, qualify, and run POCs with.
2. The reference architectures are public. Chick-fil-A and Target published the blueprint. Selling "the Chick-fil-A architecture as a product" requires zero market education. Every retail CTO has read those posts and concluded they cannot afford to replicate them.
3. The pain is chronic and budgeted. Store connectivity outages are a recognized P&L problem (POS down means sales lost), and store-technology modernization is a standing budget line.
4. Deal velocity. A 200-store regional chain can POC in one store in weeks. A factory POC takes quarters and a hard hat.
5. Expansion path is natural: QSR to c-store to grocery to pharmacy to logistics depots, then industrial later from a position of strength.

**Tier 1 prospects (have the pain, lack the platform army). Discovery targets, not yet diagrammed:**

- Regional and national QSR chains in the 200 to 2,000 site range: Raising Cane's, Dutch Bros, Whataburger, Zaxby's, Culver's
- C-store chains: Buc-ee's, Wawa, Sheetz, Casey's, QuikTrip
- Specialty retail with in-store operations complexity: Tractor Supply, Dollar General, Ace Hardware co-ops
- Grocery regionals: H-E-B (note: H-E-B has real engineering, treat as tier 1.5), Publix, Wegmans

**Tier 2 (flagship displacement, year 3+):** Chick-fil-A, Target, Walmart, Home Depot, Starbucks. These validate the category; they are not the first sale.

**Qualification question for every first call:** "When a store loses internet for four hours, what stops working, and what did that cost you last year?" If the answer is "nothing stops" they built it (rare, tier 2) or they have no in-store compute yet (nurture). Everyone else is a prospect.

---

## Objections and answers

**"We'll just use AWS Greengrass / EKS Hybrid / Outposts."**
Those solve compute placement, not data behavior. None of them answers what happens to events during a 12-hour partition, what syncs first on reconnect, or how two sites reconcile. AWS's own retail edge guidance stitches together EKS Hybrid Nodes plus partners plus custom code, which restates the DIY problem with an AWS invoice attached.

**"NATS leaf nodes do this."**
NATS leaf nodes are the closest existing primitive and the honest competitive answer. The gap: leaf nodes give transport-level buffering and reconnection, not policy (aggregate this, drop that, sync anomalies only, respect this bandwidth cap), not application-level reconciliation, and not a fleet data-management plane. Novix should be positioned as the layer NATS users wish existed above their leaf nodes, and early versions could even run atop NATS transport.

**"Zenoh already exists."**
Zenoh is real prior art and strongest in robotics/automotive (ROS 2). It is a protocol and middleware, not a fleet product for retail IT, and its adoption motion is embedded/OEM, not enterprise platform teams. Differentiation is the policy plane, the reconciliation model, and the buyer.

**"Why not just Kafka per site like Royal Caribbean?"**
That is the expensive DIY answer this product replaces. Kafka per site means running a distributed system in every store with no IT staff in the building, plus writing all sync logic yourself. The pitch is not faster Kafka; it is not having to operate Kafka in 400 stores.

---

## What must be true (honest list)

1. Three discovery calls with tier 1 prospects confirm the pain in their words, with a dollar figure attached.
2. The partition demo (see novix-demo-spec.md) works and is boring in its reliability.
3. A design partner signs an LOI to pilot in 1 to 5 stores.
4. The reconciliation model survives contact with a real workload (POS transactions are the acid test: money must never be lost or duplicated).
5. Employment IP review is clean before any of this becomes public or commercial.

If 1 through 3 cannot be achieved in roughly 90 days of nights-and-weekends effort, that is signal, not failure.

---

## Source references (public)

- Chick-fil-A tech blog: Observability at the Edge (2,800 K8s clusters, billions of MQTT messages, no internet dependency): medium.com/chick-fil-atech/observability-at-the-edge-b2385065ab6e
- Chick-fil-A at KubeCon NA 2018 (edge K8s, GitOps, one sandwich per 16 seconds): eweek.com/cloud/how-chick-fil-a-uses-kubernetes-and-gitops-at-the-edge
- Target tech blog: Unimatrix, distributed edge computing at retail scale: tech.target.com/blog/meet-targets-stores-deployment-interface-that-realizes-distributed-edge-computing-at-retail-scale
- Royal Caribbean ship-shore sync under limited connectivity: d2iq.com/blog/royal-caribbean-real-time-microservices
- Kafka-at-the-edge per ship, port-window replication: kai-waehner.de (Royal Caribbean and smart retail hybrid streaming posts)
- AWS retail edge guidance (EKS Hybrid Nodes plus partners): aws.amazon.com/blogs/industries/consolidate-modernize-transform-edge-computing-for-modern-retail
