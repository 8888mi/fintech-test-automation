# Test Strategy

## Distribution

```
        UI (4)              Cross-layer confidence. Slow, wide.
     Integration (6)        API -> Kafka contracts. Real broker.
       API (24)             Business rules, boundaries, validation. Fast.
```

Ratio is roughly 1 : 1.5 : 6. Every assertion sits at the cheapest level that can
actually make it.

---

## Coverage map

| Area | Level | Cases | Rationale |
|---|---|---|---|
| Payment creation, happy path | API | 2 | Cheapest place to prove the core contract |
| Idempotent replay | API + Integration | 2 | Two failure modes: duplicate record, duplicate event |
| Amount boundaries | API | 5 | 0, -1, 1, max, max+1 |
| Currency validation | API | 6 | Unsupported, malformed, case normalisation |
| Idempotency key validation | API | 3 | Empty, too short, too long |
| Business rejections | API | 2 | Machine-readable reason must reach the caller |
| Lifecycle transitions | API | 2 | Capture from an invalid state must conflict, not silently pass |
| Event emission | Integration | 2 | The asynchronous half of the API's promise |
| Event schema contract | Integration | 1 | `extra="forbid"` against an independent model |
| Partition key | Integration | 1 | Wrong key breaks per-account ordering downstream |
| No duplicate event on replay | Integration | 1 | Double notification / double charge |
| UI happy path and rejection | UI | 3 | The user-visible result |
| UI action reaches the topic | UI | 1 | The only genuinely cross-layer assertion |

---

## Boundary analysis: amount

Limit is 1..1_000_000 minor units.

| Value | Class | Expected |
|---|---|---|
| -1 | Invalid | 422 |
| 0 | Invalid — zero is not a payment | 422 |
| 1 | Valid, lower boundary | 201 |
| 1_000_000 | Valid, upper boundary | 201 |
| 1_000_001 | Invalid, first over | 422 |

The limit is restated as a literal in the test rather than imported from the service.
If someone lowers it, this test should fail and force a conversation — not follow along
silently.

---

## Deliberately not covered

**Load and performance.** Different tooling (k6, Locust), different cadence, different
environment. Mixing it into a functional suite makes both slower and neither trustworthy.

**Kafka broker behaviour.** Partition rebalancing, replication, broker failover are
Confluent's tests, not ours. We test our contract with the broker, not the broker.

**Cross-browser.** Chromium only. The UI here is four form fields; the marginal value of
a WebKit run does not pay for the CI minutes. On a real product with a meaningful
frontend, this decision would reverse.

**Visual regression.** Would need a baseline management strategy and a team agreement on
who approves diffs. Worth adding when the UI is a product surface rather than a test
target.

---

## Flakiness

Sources addressed structurally, not with retries:

| Source | Mitigation |
|---|---|
| Consumer assigned after event published | Subscribe and confirm assignment before acting |
| Stale events from earlier runs | `seek_to_end()` before each test |
| Shared account state across tests | Unique `account_id` fixture per test |
| Fixed sleeps | `wait_until` / predicate-based polling with deadlines |
| Service not ready at suite start | Compose healthchecks + `--wait` |

`--reruns 1` in CI covers residual infrastructure noise only. A test needing its retry
more than once a week is quarantined and fixed.

---

## Entry and exit criteria

**Entry.** Environment healthy (`docker compose up --wait` returns clean), migrations
applied, topic exists.

**Exit.** All blocker- and critical-severity tests pass. No test quarantined for more than
one sprint. Allure report published with history so trends are visible rather than
anecdotal.
