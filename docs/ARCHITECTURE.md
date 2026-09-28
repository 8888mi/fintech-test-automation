# Architecture

Design decisions and the reasoning behind them. Each section states the decision,
the alternative that was rejected, and why.

---

## Money as integer minor units

**Decision.** Every amount is an `int` counting the smallest indivisible unit of the
currency. `Decimal` appears only at the presentation boundary (`utils/money.py`).
`float` never touches an amount anywhere in the codebase.

**Rejected.** `float`, which is the path of least resistance because JSON has no other
numeric type.

**Why.** Binary floating point cannot represent 0.10 exactly. Errors accumulate silently
across summation and rounding, and surface weeks later as a reconciliation discrepancy of
a few cents that nobody can reproduce. Integers make the class of bug impossible rather
than unlikely.

**Consequence.** Currency exponent must be explicit — `to_major(1000, "JPY")` is 1000,
not 10.00. Handled by the exponent table in `utils/money.py`.

---

## Independently declared contract models

**Decision.** `src/framework/api/models.py` re-declares the payloads the service produces
instead of importing `demo_service.models`.

**Rejected.** Sharing one schema definition between implementation and tests. DRY,
obviously appealing, and wrong here.

**Why.** With a shared definition, a developer renaming a field updates both sides in one
commit. The suite stays green, and the consumers that were relying on the old field find
out in production. Restating the contract on the test side means the test is an
independent observer, which is the entire point of a contract test.

**Reinforced by** `extra="forbid"`: an unannounced new field fails too, not just a missing
one. In a payments topic, silent schema drift is an incident.

---

## Read-only, bounded Kafka consumer

**Decision.** `KafkaTestConsumer` uses a random `group.id` per instance,
`enable.auto.commit=False`, and every wait carries a deadline.

**Rejected.** Reusing a fixed consumer group; unbounded `while True` polling.

**Why — the group id.** A test suite pointed at a shared staging broker with a fixed group
id will commit offsets, and a real consumer sharing that group will skip messages. The
failure is intermittent, appears in someone else's service, and takes days to trace back
to the test suite.

**Why — the deadline.** An unbounded poll waiting for an event that will never arrive
produces a CI job that runs until the runner-level timeout kills it, with no output
explaining what happened. Every wait here fails at a known point with the events it did
observe attached to the report.

---

## Subscribe before acting

**Decision.** The `kafka_consumer` fixture starts the consumer, blocks until partitions
are actually assigned, and seeks to the end of the topic — all before the test performs
the action that produces the event.

**Rejected.** Acting first, then subscribing to look for the event.

**Why.** Kafka consumer group rebalancing takes time. Subscribing after the action leaves
a window in which the event was produced but the consumer had no assignment, so `poll`
returns nothing and the test fails intermittently. This is the single most common source
of flakiness in event-driven test suites, and it worsens under parallel execution — that
is, it appears in CI and not on the developer's machine.

`seek_to_end()` handles the opposite problem: a topic that already holds data from earlier
runs, where a naive `earliest` consumer would match a stale event and pass for the wrong
reason.

---

## Retry only on transient failures

**Decision.** The HTTP retry policy fires on `429` and `5xx`. `4xx` passes through
untouched.

**Rejected.** Retrying any non-2xx response.

**Why.** A `422` is the service correctly rejecting invalid input — that is the expected
result of a validation test, not a failure to retry. Retrying it hides real behaviour and
triples the runtime of the negative-path suite.

---

## Test distribution across layers

**Decision.** Most coverage at the API layer, a focused set of integration tests for event
contracts, four UI tests.

**Why.** An API test costs tens of milliseconds; a browser test costs seconds and has more
ways to fail for reasons unrelated to the product. Anything provable cheaply is proved
cheaply. The UI tests exist for assertions that genuinely span layers — specifically,
`test_ui_submission_reaches_the_event_stream`, which is the only place where a browser
action is traced through to the topic.

See [TEST_STRATEGY.md](TEST_STRATEGY.md) for the coverage map.

---

## Configuration in one typed module

**Decision.** `framework/config.py` is the only module that reads the environment.
Everything else receives settings by injection.

**Rejected.** `os.getenv` at point of use.

**Why.** Scattered `os.getenv` calls make it impossible to answer "what does this suite
need to run?" without grepping, and typos produce `None` at runtime rather than an error
at startup. Pydantic validates the whole configuration once, at import, with types.

**Consequence.** Retargeting the suite from local to staging is an environment-variable
change, which is also what makes the CI job a copy of the local run rather than a
separate configuration to maintain.

---

## Flaky-test policy

CI runs with `--reruns 1`. This is a pragmatic concession to genuine infrastructure
noise — a broker that needs another second, a container that was not quite ready — and
not a substitute for fixing tests.

The rule that keeps it honest: a test that needs its retry more than once in a working
week gets quarantined and investigated. Without that rule, `--reruns` becomes a way of
not noticing that the suite has stopped meaning anything.
