# Intent Graph Algorithm Specification

*v1 normative draft*

The whitepaper defines decomposition, matching, priority, conflict resolution, dynamic updates, and optional GNN augmentation. This specification tightens those definitions.

## 2.1 Canonical Intent Record

```text
IntentRecord {
  id: IntentID
  issuer: PrincipalID
  kind: IntentKind
  scope: {local, shard, global}
  goal: GoalPredicate
  constraints: [Constraint]
  utility: UtilityFunctionRef
  deadline: Time
  priority_hint: Float
  trust_requirement: Float
  provenance: ProvenanceProof
  status: {proposed, admitted, decomposed, matched, scheduled, executing, satisfied, failed, rejected, superseded}
}
```

## 2.2 Graph Types

`Graph G = (V, E)`

- `V_intent`: intents / sub-intents
- `V_agent`: agents or services
- `V_resource`: compute, memory, bandwidth, tokens, data locality
- `V_policy`: ACL, regulatory, organizational policy
- `V_commitment`: receipts, promises, escrow, SLA commitments

Edges:

- `depends_on(u, v)`
- `conflicts_with(u, v)`
- `satisfiable_by(u, a)`
- `requires(u, r)`
- `governed_by(u, p)`
- `delegates_to(u, v)`
- `derived_from(u, v)`
- `supersedes(u, v)`
- `compatible_with(u, v)`

## 2.3 Admission Algorithm

Input: intent `i`, state `S`

Output: `admit | reject | defer`

Admission conditions:

1. issuer authenticated
2. provenance valid
3. syntax/semantic checks pass
4. trust threshold met
5. policy not violated
6. at least one feasible decomposition path exists

```text
ADMIT(i, S):
  if not AUTH(i.issuer): return reject(auth)
  if not VALIDATE(i): return reject(schema)
  if TRUST(i.issuer) < i.trust_requirement: return reject(trust)
  if not POLICY_OK(i, S.Π): return reject(policy)
  if not FEASIBLE(i, S.R): return defer(resource)
  return admit
```

## 2.4 Decomposition

Goal: transform a high-level intent into atomic executable sub-intents.

`D : i → {i_1,...,i_k}`

Properties:

- decomposition preserves satisfiability
- child intents inherit constraints unless overridden
- graph edges encode dependency order

`goal(i) ⇒ ∧_{j=1}^{k} goal(i_j) ∧ O(i_1,...,i_k)`

Where `O` is ordering/dependency relation.

```text
DECOMPOSE(i):
  templates = SELECT_TEMPLATES(i.kind, i.goal)
  atoms = APPLY_TEMPLATES(i, templates)
  for each atom in atoms:
      normalize(atom)
      inherit_constraints(atom, i)
  add depends_on edges
  return atoms
```

## 2.5 Matching

Each atomic intent is matched against agent capabilities, available resources, and policy compatibility.

`score(i, a) = w_c*CapMatch(i,a) + w_l*Locality(i,a) + w_t*Trust(a) + w_r*ResourceFit(i,a) + w_u*UtilityGain(i,a) - w_x*ConflictRisk(i,a)`

Choose top-k candidates:

`Cand(i) = TopK_{a ∈ A} score(i,a)`

```text
MATCH(i, G, R):
  candidates = INDEX_LOOKUP(capability_index, i.kind, i.scope)
  feasible = [a for a in candidates if RESOURCE_OK(i, a, R)]
  ranked = SORT_BY_SCORE(feasible, score(i, a))
  return TOPK(ranked)
```

## 2.6 Priority Function

`Priority(i) = α*urgency(i) + β*utility(i) + γ*dependency_criticality(i) + δ*trust_weight(i) - ε*resource_cost(i)`

Dynamic reprioritization occurs when:

- deadline approaches
- dependency unblocks
- resource prices change
- higher-level superseding intent arrives

## 2.7 Conflict Resolution

Two intents conflict if:

`Conf(i,j) = overlap(resources(i), resources(j)) ∧ ¬compatible(i,j)`

Resolution order:

1. policy precedence
2. safety precedence
3. deadline precedence
4. utility maximization
5. negotiated compromise
6. human/governance escalation

Optimization form:

`max_{Y ⊆ I} Σ_{i ∈ Y} Utility(i)`

Subject to:

- no conflicts in `Y`
- resources bounded
- policy respected
- fairness constraints

This turns conflict resolution into a maximum feasible weighted subset problem (or ILP/CSP depending on scale).

## 2.8 Incremental Update

When a new intent arrives, do not rebuild the graph globally.

```text
UPDATE_GRAPH(i_new):
  admit = ADMIT(i_new)
  if admit != admit: return admit
  atoms = DECOMPOSE(i_new)
  for atom in atoms:
      add node atom
      connect dependencies / conflicts / policy edges
      cands = MATCH(atom)
      annotate candidate edges
  affected = NEIGHBORHOOD(atom, radius=r)
  REPRIORITIZE(affected)
  RESOLVE_CONFLICTS(affected)
  SCHEDULE(affected)
```

Locality is critical for hyperscale; this aligns with whitepaper guidance on incremental updates and sharding to keep latency low.

## 2.9 Complexity

| Step | Baseline | Indexed / Sharded |
|---|---|---|
| Admission | `O(1)-O(log n)` | `O(log n)` |
| Decomposition | `O(k log k)` | `O(k log k)` |
| Candidate match | `O(n)` | `O(log n + c)` |
| Reprioritize neighborhood | `O(d log d)` | `O(d log d)` |
| Conflict resolution | NP-hard worst case | bounded CSP on local neighborhood |
| Cross-shard reconciliation | `O(s log s)` | `O(s log s)` |

Where:

- `k`: sub-intents
- `c`: returned candidates
- `d`: affected local neighborhood size
- `s`: number of involved shards

## 2.10 Optional GNN Layer

GNN is advisory, not authoritative.

Use only for:

- ranking candidates
- anomaly detection
- predicting conflict likelihood
- prioritization hints

It must never bypass:

- policy checks
- trust threshold
- hard resource constraints
- safety rules

## 2.11 Execution Semantics

Intent lifecycle:

`proposed → admitted → decomposed → matched → scheduled → executing → satisfied | failed | rejected | superseded`

Cross-shard global intents:

`proposed → admitted → decomposed → shard_prepare → shard_vote → global_commit | abort | compensating_commit`
