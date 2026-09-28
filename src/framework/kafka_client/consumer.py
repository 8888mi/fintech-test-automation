"""Read-only Kafka consumer built for tests.

Three properties matter when you point a test suite at a shared broker:

1. **It must not disturb production consumers.** A random `group.id` plus
   `enable.auto.commit=False` means we never commit an offset that a real
   consumer group depends on.

2. **It must not hang forever.** Waiting for an event that never arrives is
   the single most common cause of a CI job that runs for 40 minutes and
   then times out at the runner level with no useful output. Every wait
   here is bounded and fails with a diagnostic listing what *did* arrive.

3. **It must not miss events published before the test subscribed.** The
   consumer is started and assigned *before* the triggering action, so the
   window between "event produced" and "consumer ready" is closed.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import allure

from framework.config import KafkaSettings

log = logging.getLogger(__name__)


@dataclass
class ConsumedEvent:
    key: str | None
    value: dict[str, Any]
    partition: int
    offset: int
    timestamp_ms: int

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"<Event {self.value.get('event_type', '?')} "
            f"payment={self.value.get('payment_id', '?')} "
            f"p{self.partition}@{self.offset}>"
        )


class EventNotFoundError(AssertionError):
    """Raised when an expected event does not arrive within the timeout."""


@dataclass
class KafkaTestConsumer:
    """Bounded, read-only consumer for assertions on emitted events."""

    settings: KafkaSettings
    topic: str
    _consumer: Any = field(default=None, init=False, repr=False)
    _seen: list[ConsumedEvent] = field(default_factory=list, init=False, repr=False)

    def start(self) -> KafkaTestConsumer:
        from confluent_kafka import Consumer

        group_id = f"qa-readonly-{uuid.uuid4().hex[:12]}"
        self._consumer = Consumer(self.settings.client_config(group_id))
        self._consumer.subscribe([self.topic])

        # Force partition assignment now, so events produced immediately
        # after this call are not missed.
        deadline = time.monotonic() + 20
        while not self._consumer.assignment() and time.monotonic() < deadline:
            self._consumer.poll(0.2)

        if not self._consumer.assignment():
            raise RuntimeError(
                f"No partitions assigned for topic '{self.topic}' within 20s. "
                f"Check that the broker at {self.settings.bootstrap_servers} is reachable "
                f"and the topic exists."
            )
        # Log the assignment as plain tuples. TopicPartition.__repr__ is built
        # in C with PRId32, which expands to "%I32d" on MSVC - a directive
        # PyUnicode_FromFormat rejects - so repr() of one raises SystemError on
        # Windows (confluent-kafka 2.15.0). Never hand these objects to logging.
        log.info(
            "Consumer %s assigned: %s",
            group_id,
            [(tp.topic, tp.partition) for tp in self._consumer.assignment()],
        )
        return self

    def seek_to_end(self) -> None:
        """Skip history and only observe events from this moment forward.

        Use before the triggering action when the topic already holds data
        from earlier tests and you only care about what happens next.
        """
        for tp in self._consumer.assignment():
            _low, high = self._consumer.get_watermark_offsets(tp, timeout=10)
            tp.offset = high
            self._consumer.seek(tp)
        log.info("Seeked to end of %s", self.topic)

    def _poll_once(self, timeout: float) -> ConsumedEvent | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            from confluent_kafka import KafkaError

            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(f"Kafka error: {msg.error()}")

        try:
            value = json.loads(msg.value().decode())
        except (ValueError, UnicodeDecodeError) as exc:
            log.warning("Undecodable message at %s@%s: %s", msg.partition(), msg.offset(), exc)
            return None

        event = ConsumedEvent(
            key=msg.key().decode() if msg.key() else None,
            value=value,
            partition=msg.partition(),
            offset=msg.offset(),
            timestamp_ms=msg.timestamp()[1],
        )
        self._seen.append(event)
        return event

    def wait_for(
        self,
        predicate: Callable[[ConsumedEvent], bool],
        *,
        description: str = "matching event",
        timeout: float | None = None,
    ) -> ConsumedEvent:
        """Poll until `predicate` matches, or fail with a readable diagnostic."""
        timeout = timeout or self.settings.event_timeout_seconds
        deadline = time.monotonic() + timeout

        with allure.step(f"Wait for {description} (<= {timeout:.0f}s)"):
            # Check anything already buffered from previous waits first.
            for buffered in self._seen:
                if predicate(buffered):
                    return buffered

            while time.monotonic() < deadline:
                event = self._poll_once(timeout=0.5)
                if event is not None and predicate(event):
                    allure.attach(
                        json.dumps(event.value, indent=2),
                        name="matched event",
                        attachment_type=allure.attachment_type.JSON,
                    )
                    return event

            self._attach_diagnostics()
            raise EventNotFoundError(
                f"Timed out after {timeout:.0f}s waiting for {description} "
                f"on topic '{self.topic}'. Observed {len(self._seen)} event(s): "
                f"{self._seen[-10:]}"
            )

    def wait_for_payment_event(
        self, payment_id: str, event_type: str, timeout: float | None = None
    ) -> ConsumedEvent:
        return self.wait_for(
            lambda e: e.value.get("payment_id") == payment_id
            and e.value.get("event_type") == event_type,
            description=f"{event_type} for {payment_id}",
            timeout=timeout,
        )

    def assert_no_event(
        self,
        predicate: Callable[[ConsumedEvent], bool],
        *,
        within: float = 5.0,
        description: str = "event",
    ) -> None:
        """Assert an event does NOT appear - the idempotency guard.

        Necessarily costs `within` seconds of wall clock. Keep it short and
        use it sparingly, only where a duplicate event would be a real
        financial defect.
        """
        deadline = time.monotonic() + within
        with allure.step(f"Assert no {description} within {within:.0f}s"):
            while time.monotonic() < deadline:
                event = self._poll_once(timeout=0.3)
                if event is not None and predicate(event):
                    raise AssertionError(f"Unexpected {description} received: {event.value}")

    def _attach_diagnostics(self) -> None:
        allure.attach(
            json.dumps([e.value for e in self._seen[-25:]], indent=2, default=str),
            name="events observed before timeout",
            attachment_type=allure.attachment_type.JSON,
        )

    def stop(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    def __enter__(self) -> KafkaTestConsumer:
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
