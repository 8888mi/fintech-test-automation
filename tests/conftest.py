"""Shared fixtures.

Fixture scope is chosen on cost, not habit:
  * `settings`   - session, immutable
  * `api_client` - session, holds a connection pool worth reusing
  * `payments_api` - function, cheap wrapper, keeps tests independent
  * `kafka_consumer` - function, because each test needs its own offset
    position and its own view of "events since my action"
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator

import pytest

from framework.api.client import ApiClient
from framework.api.payments_api import PaymentsApi
from framework.config import Settings, get_settings
from framework.kafka_client import KafkaTestConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--env",
        action="store",
        default=None,
        help="Target environment name (local, docker, staging). Overrides ENVIRONMENT.",
    )


@pytest.fixture(scope="session")
def settings(pytestconfig: pytest.Config) -> Settings:
    """Environment-derived settings, with `--env` taking precedence.

    The option defaults to None rather than "local" so that a flag nobody
    passed cannot silently outrank an ENVIRONMENT value coming from .env.
    """
    resolved = get_settings()
    env_override = pytestconfig.getoption("--env")
    if env_override:
        resolved = resolved.model_copy(update={"environment": env_override})
    return resolved


@pytest.fixture(scope="session")
def api_client(settings: Settings) -> Iterator[ApiClient]:
    client = ApiClient(settings.api)
    yield client
    client.close()


@pytest.fixture
def payments_api(api_client: ApiClient) -> PaymentsApi:
    return PaymentsApi(api_client)


@pytest.fixture
def kafka_consumer(settings: Settings) -> Iterator[KafkaTestConsumer]:
    """A consumer already positioned at the end of the topic.

    Subscribing *before* the test acts is what makes the assertions reliable:
    there is no window in which a fast service could publish an event we then
    never see.
    """
    consumer = KafkaTestConsumer(settings=settings.kafka, topic=settings.kafka.payments_topic)
    consumer.start()
    consumer.seek_to_end()
    yield consumer
    consumer.stop()


@pytest.fixture
def account_id() -> str:
    """A unique account per test - no shared state, safe for -n auto."""
    return f"ACC-{uuid.uuid4().hex[:12].upper()}"


@pytest.fixture(scope="session")
def browser_type_launch_args(browser_type_launch_args: dict, settings: Settings) -> dict:
    """Feed UI_HEADLESS / UI_SLOW_MO_MS into how the browser is launched.

    pytest-playwright writes a key into its own dict only when the matching
    command-line flag was actually given, so spreading it *last* leaves
    `--headed` and `--slowmo` in charge and lets the .env values apply only
    when the command line says nothing.
    """
    return {
        "headless": settings.ui.headless,
        "slow_mo": settings.ui.slow_mo_ms,
        **browser_type_launch_args,
    }


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args: dict, settings: Settings) -> dict:
    """Extend pytest-playwright's context with tracing-friendly defaults."""
    return {
        **browser_context_args,
        "base_url": settings.ui.base_url,
        "viewport": {"width": 1280, "height": 800},
        "locale": "en-GB",
    }


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Expose the test outcome so fixtures can attach artifacts only on failure."""
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"report_{report.when}", report)
