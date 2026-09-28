# Payment Platform Test Automation

*[Русская версия](README.ru.md)*

Test automation framework for a payment service with asynchronous event processing.
Covers three layers — **REST API**, **Kafka event contracts**, and **browser UI** — in one
suite, against a real broker rather than mocks.

```bash
git clone <repo> && cd fintech-test-automation
make install && make up && make test
```

Three commands, and the whole environment — Kafka broker, service under test, browser —
comes up and the suite runs. No manual setup, no "ask me for the credentials".

[![tests](https://github.com/<user>/<repo>/actions/workflows/tests.yml/badge.svg)](https://github.com/<user>/<repo>/actions/workflows/tests.yml)

---

## Why this exists

Most payment-system test suites stop at the HTTP response. That leaves the harder half
untested: the service returns `201 Created`, the test goes green, and the event that the
ledger and the notification service depend on was never published — or was published
twice.

This framework treats an API call and its resulting event as **one transaction to verify**.

```python
def test_replayed_request_does_not_emit_a_second_event(payments_api, kafka_consumer, account_id):
    key = payments_api.new_idempotency_key()
    payment = payments_api.create(account_id=account_id, amount_minor=11_000,
                                  idempotency_key=key).expect_status(201).json()

    kafka_consumer.wait_for_payment_event(payment["payment_id"], "payment.created")

    # Same key again — must be a no-op, not a second charge.
    payments_api.create(account_id=account_id, amount_minor=11_000,
                        idempotency_key=key).expect_status(200)

    kafka_consumer.assert_no_event(
        lambda e: e.value["payment_id"] == payment["payment_id"]
                  and e.value["event_type"] == "payment.created",
        within=5.0, description="duplicate payment.created",
    )
```

A duplicate event here means a customer charged twice. No API-only test can catch it.

---

## Stack

| Layer | Tooling |
|---|---|
| Language | Python 3.11+, Poetry |
| Test runner | pytest, pytest-xdist, pytest-rerunfailures |
| API | requests + pydantic contract models |
| Events | confluent-kafka |
| UI | Playwright (sync API), Page Object |
| Reporting | Allure — HTML report built locally; CI additionally publishes it to GitHub Pages |
| CI | GitHub Actions with Docker Compose services |
| Quality | ruff, mypy |

---

## Prerequisites

| Needed for | Requirement |
|---|---|
| Everything | Python 3.11+ and [Poetry](https://python-poetry.org/) |
| `make up`, integration and UI tests | Docker with Compose v2, daemon running |
| `make report` | [Allure CLI](https://allurereport.org/docs/install/) and a JRE — **not** installed by `make install` |
| `make` targets | GNU Make. Absent on stock Windows; use `run-tests.bat` or the underlying commands (see below) |

`make install` covers only the Python dependencies and the Chromium build. The Allure
command-line tool is a separate Java program: `allure-pytest` writes the raw results,
the CLI turns them into a report.

---

## Structure

```
demo_service/            System under test: FastAPI payments API + Kafka producer + static UI
src/framework/
  config.py              Typed settings — the only place that reads the environment
  api/
    client.py            HTTP session, transient-only retries, Allure attachments
    payments_api.py      Service object: tests state intent, not transport
    models.py            Response contracts, extra="forbid"
  kafka_client/
    consumer.py          Bounded read-only consumer with predicate-based waits
  ui/
    base_page.py         data-testid locators only
    pages/               Page objects
  utils/
    money.py             Integer minor units, Decimal at the edges, never float
    waiters.py           Polling instead of sleep
tests/
  conftest.py            Fixtures: settings, HTTP client, Kafka consumer, browser options
  api/                   Fast, no broker, no browser — the bulk of the coverage
  integration/           API → Kafka contract and idempotency
  ui/                    Few, wide, cross-layer
docs/
  ARCHITECTURE.md        Design decisions and their rationale
  TEST_STRATEGY.md       What is covered at which level, and what is deliberately not
.dockerignore            Keeps caches and test artefacts out of the build context
run-tests.bat            Windows stand-in for the Makefile
```

---

## Design decisions

Fuller reasoning in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The short version:

**Money is an integer count of minor units.** `float` never touches an amount. `0.1 + 0.2`
is not `0.3` in binary floating point, and in a payments system that gap eventually
surfaces as a reconciliation ticket nobody can reproduce.

**The test suite declares its own copy of the event schema.** `src/framework/api/models.py`
deliberately does not import the service's models. If both sides shared one definition, a
breaking contract change would update the test along with the implementation and the suite
would stay green through the incident. The duplication is the safety mechanism, and
`extra="forbid"` means an unannounced new field also fails.

**The Kafka consumer is read-only and bounded.** A throwaway `group.id` with
`enable.auto.commit=False` means running the suite against a shared environment never
disturbs the offsets of a real consumer group. Every wait has a deadline and fails with
the events it actually observed — instead of hanging until the CI runner kills the job
with no diagnostic.

**The consumer subscribes before the triggering action.** Partition assignment is forced
and confirmed up front, closing the window in which a fast service could publish an event
the test then never sees. This is the difference between a suite that is reliable and one
that is 95% reliable — and 95% is worse than useless, because people start re-running
failures on reflex.

**Retries only on transient failures.** `429` and `5xx` are retried; `4xx` reaches the
assertion untouched, because a business rejection is a result, not a network glitch.

**UI tests are few and wide.** Anything provable at the API level is proven there, where
it costs milliseconds. The browser tests exist for one thing the lower levels cannot
prove: that a click ends up as an event on the topic.

**Locators are `data-testid` only.** CSS classes and copy are design decisions; binding
tests to them means a text change breaks the suite.

---

## Running

| Command | Effect |
|---|---|
| `make install` | Python dependencies + Chromium |
| `make up` | Kafka + service via Docker Compose, waits for health |
| `make test` | Full suite, parallel, one retry on failure |
| `make test-api` | API layer only — no broker or browser needed |
| `make test-integration` | API → Kafka contract tests |
| `make test-ui` | Playwright tests |
| `make report` | Build and serve the Allure report |
| `make lint` | ruff + mypy |
| `make down` | Tear down and remove volumes |

Without `make` — on Windows, for instance — run the same commands directly:

```bash
poetry install && poetry run playwright install chromium
docker compose up -d --wait
poetry run pytest -n auto --reruns 1 --reruns-delay 2
poetry run pytest tests/api          # or tests/integration, tests/ui
poetry run ruff check src tests demo_service && poetry run mypy src
docker compose down -v
```

### Windows: `run-tests.bat`

The same flow in one script. It starts Docker Desktop if the daemon is not answering (waits
up to two minutes), runs `docker compose up -d --wait`, then pytest, and exits with pytest's
exit code.

```bat
run-tests.bat                  :: full suite, parallel, one retry on failure (as in CI)
run-tests.bat api              :: tests/api only; also: integration, ui
run-tests.bat ui --headed      :: anything after the suite name goes straight to pytest
run-tests.bat install          :: poetry install + Chromium
run-tests.bat kafka-ui         :: open the Kafka web UI (see below)
run-tests.bat down             :: tear down, including kafka-ui, and remove volumes
```

The environment is left running after the tests so the next run starts immediately; stop
it with `run-tests.bat down`. Every suite needs it, the API layer included: API tests call
the payments service on `localhost:8000`, which runs in Compose.

Select subsets by path, as above. The markers declared in `pyproject.toml`
(`smoke`, `api`, `integration`, `ui`) are **not yet applied to any test**, so `-m` currently
deselects everything — use directories until the markers are added.

---

## Configuration

All settings live in `src/framework/config.py` and are read from the environment. Copy the
template and edit it:

```bash
cp .env.example .env
```

**`.env.example` is a template only.** The settings classes read `.env`
(`SettingsConfigDict(env_file=".env")`), so editing `.env.example` changes nothing. Without
a `.env` file the defaults in `config.py` apply — which is why the suite runs out of the
box against Docker Compose.

| Variable | Default | Purpose |
|---|---|---|
| `API_BASE_URL` | `http://localhost:8000` | Service under test |
| `API_TIMEOUT_SECONDS` / `API_RETRY_ATTEMPTS` | `10` / `3` | HTTP timeout and transient retries |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker, external listener |
| `KAFKA_PAYMENTS_TOPIC` | `payments.events` | Topic under assertion |
| `KAFKA_EVENT_TIMEOUT_SECONDS` | `15` | Deadline for an expected event |
| `UI_BASE_URL` | `http://localhost:8000` | Page under test |
| `UI_HEADLESS` | `true` | `false` shows the browser window |
| `UI_SLOW_MO_MS` | `0` | Pause before each Playwright action, ms |
| `UI_DEFAULT_TIMEOUT_MS` | `10000` | Locator timeout |

> **Keep the committed defaults CI-safe.** A GitHub Actions `ubuntu-latest` runner has no X
> server, so `headless = False` in `config.py` would fail there with *"launched a headed
> browser without having a XServer running"* unless the job were wrapped in `xvfb-run`.
> Put local debugging preferences in `.env`, which is gitignored and therefore never
> reaches CI.

Against a secured broker, set `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT` with the matching
mechanism and credentials; no code changes are needed.

`--env` overrides `ENVIRONMENT` for a single run: `pytest --env=staging`. Passing nothing
leaves whatever the environment already provides untouched.

### Watching the browser

```bash
poetry run pytest tests/ui --headed --slowmo 500
```

`--headed` and `--slowmo` win over `UI_HEADLESS` / `UI_SLOW_MO_MS`, so the command line
always overrides `.env`. Note that `slow_mo` pauses *between* Playwright actions; it does
not type character by character, because the page objects use `fill()`.

---

## Reports and diagnostics

### Allure

```bash
poetry run pytest                                    # writes allure-results/
allure serve allure-results                          # temporary report + browser
allure generate allure-results -o allure-report --clean && allure open allure-report
```

**Do not open `allure-report/index.html` by double-clicking it.** Over `file://` the page
loads blank: the report fetches its own JSON over XHR and browsers block that on the file
protocol. It has to be served, which is what `allure serve` and `allure open` do.

Every pytest run empties `allure-results/` first (`--clean-alluredir` is in `addopts`), so a
report always shows only the latest run. To see several suites in one report, run them in
one command — `pytest tests/api tests/ui` — since a second, separate run wipes the first.
Trends need `allure-report/history` copied back into `allure-results/history` before
generating — that is what the CI job does through the `gh-pages` branch.

The suite attaches request and response bodies for every HTTP call, the matched Kafka
event, the last 25 observed events when a wait times out, and a full-page screenshot from
the UI happy path.

### Kafka UI

A browser view of topics, messages, partitions and consumer groups on
**http://localhost:8080**:

```bash
docker compose --profile tools up -d kafka-ui      # or: run-tests.bat kafka-ui
```

It sits in the `tools` Compose profile, so plain `docker compose up`, `make up` and CI never
pull or wait on it. For the same reason plain `docker compose down -v` leaves it running —
use `docker compose --profile tools down -v` (which `run-tests.bat down` does). The image is
`kafbat/kafka-ui`, the maintained fork of `provectuslabs/kafka-ui`; it reaches the broker on
the internal listener `kafka:29092`.

### Inspecting the topic directly

```bash
# Live tail — new messages only; leave running while the suite executes (Ctrl+C to exit)
docker exec -it qa-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payments.events --property print.key=true

# Everything published so far
docker exec -it qa-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payments.events --from-beginning --property print.key=true

# First 5 messages — the command exits on its own
docker exec qa-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic payments.events --from-beginning --max-messages 5

# Topic list, description (partitions, leader, replicas) and offsets
docker exec qa-kafka kafka-topics --bootstrap-server localhost:29092 --list
docker exec qa-kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic payments.events
docker exec qa-kafka kafka-get-offsets --bootstrap-server localhost:29092 --topic payments.events
```

The commands are kept on one line so they paste into any shell — PowerShell and cmd do not
understand bash's `\` line continuation. Without `--from-beginning` the consumer shows only
messages that arrive after it starts, so on a quiet topic it looks stuck; that is expected.

Port 29092 is the broker's internal listener, reachable from inside the container; tests on
the host use 9092. On this image `kafka-run-class kafka.tools.GetOffsetShell` no longer
works — the class moved to `org.apache.kafka.tools` in Kafka 3.7, so use the
`kafka-get-offsets` script.

`payments.events` is not created up front: it appears on the service's first write
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`). Messages accumulate across runs and disappear
only after `docker compose down -v`, which removes the volumes.

---

## Platform notes

**Windows has no `make`.** Use `run-tests.bat`, the direct commands above, or install GNU Make. `make clean`
additionally relies on `rm -rf` and `find`, so it needs a POSIX shell such as Git Bash.

**Never hand a `TopicPartition` to logging.** In confluent-kafka 2.15.0 on Windows its
`__repr__` is built in C with `PRId32`, which MSVC expands to `%I32d` — a directive
`PyUnicode_FromFormat` rejects — so `repr()`, `str()` and `%`-formatting all raise
`SystemError`. `consumer.py` logs plain `(topic, partition)` tuples for this reason.

**The Docker build context is the whole repository** (`context: .` in Compose), while the
image needs only `demo_service/`. `.dockerignore` keeps caches and test artefacts out;
without it the context is several megabytes of `.mypy_cache` and Allure output that grow
with every run.

---

## CI

`.github/workflows/tests.yml` lints, brings up Kafka and the service via Docker Compose,
runs the suite across 4 workers, and publishes the Allure report to GitHub Pages with 30
runs of history. Service logs are captured as an artifact on failure, because a red build
with no logs costs more time than no build at all.

**Publishing happens only in CI**, never on a local run. Locally, pytest writes raw results
to `allure-results/` — `--alluredir` is in the `addopts` of `pyproject.toml`, so every run
does it — and turning those into a report is the separate, manual `allure serve` step above.
The `publish-report` job uploads `allure-results` as an artifact, merges it with the history
kept on the `gh-pages` branch, and pushes the built report back there. It is gated on
`github.ref == 'refs/heads/main'`, so it needs the project to be a GitHub repository with
Pages enabled and its source set to the `gh-pages` branch — and the `<user>/<repo>`
placeholders in the badge above filled in.

---

## Scope

The service in `demo_service/` is a purpose-built stand-in, small enough to read in one
sitting but reproducing the properties that make payment systems awkward to test:
idempotency, asynchronous side effects, state transitions with invalid paths, and money
that must not lose precision.

The framework is the deliverable. Pointing it at a different payment API means rewriting
the service objects in `src/framework/api/` and the page objects — the client, consumer,
configuration, fixtures and reporting carry over unchanged.
