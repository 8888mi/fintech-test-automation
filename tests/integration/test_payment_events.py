"""API -> Kafka integration tests.

The HTTP 201 is only half of what a payments service promises. The other
half is the event that downstream systems (ledger, notifications,
reconciliation) consume. A test that stops at the response body will pass
happily while the entire asynchronous half of the system is broken.
"""

from __future__ import annotations

import allure
import pytest

from framework.api.models import PaymentEventModel, PaymentStatus


@allure.epic("Payments")
@allure.feature("Event publication")
class TestPaymentEvents:
    @allure.story("Event emission")
    @allure.severity(allure.severity_level.BLOCKER)
    def test_created_payment_emits_event(self, payments_api, kafka_consumer, account_id):
        payment = payments_api.create_ok(account_id=account_id, amount_minor=42_000)

        event = kafka_consumer.wait_for_payment_event(payment.payment_id, "payment.created")

        assert event.value["amount_minor"] == 42_000
        assert event.value["account_id"] == account_id

    @allure.story("Event contract")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.description(
        "Validates the published payload against an independently-declared schema "
        "with extra='forbid'. Both a missing field and an unannounced new field "
        "fail this test - schema drift in a payments topic is an incident, not a "
        "detail."
    )
    def test_event_matches_published_contract(self, payments_api, kafka_consumer, account_id):
        payment = payments_api.create_ok(account_id=account_id, amount_minor=7_700)

        event = kafka_consumer.wait_for_payment_event(payment.payment_id, "payment.created")
        parsed = PaymentEventModel.model_validate(event.value)

        assert parsed.schema_version == 1
        assert parsed.payment_id == payment.payment_id
        assert parsed.status is PaymentStatus.AUTHORIZED

    @allure.story("Partitioning")
    @allure.description(
        "Events are keyed by account_id so that Kafka guarantees ordering per "
        "account. If the key changes, two payments for one account can be "
        "processed out of order downstream and the ledger goes wrong."
    )
    def test_events_are_keyed_by_account_for_ordering(
        self, payments_api, kafka_consumer, account_id
    ):
        payment = payments_api.create_ok(account_id=account_id, amount_minor=3_300)

        event = kafka_consumer.wait_for_payment_event(payment.payment_id, "payment.created")

        assert event.key == account_id

    @allure.story("Idempotency")
    @allure.severity(allure.severity_level.CRITICAL)
    def test_replayed_request_does_not_emit_a_second_event(
        self, payments_api, kafka_consumer, account_id
    ):
        """A duplicate event here means the customer is notified - or charged - twice."""
        key = payments_api.new_idempotency_key()
        payment = payments_api.create(
            account_id=account_id, amount_minor=11_000, idempotency_key=key
        ).expect_status(201).json()

        kafka_consumer.wait_for_payment_event(payment["payment_id"], "payment.created")

        payments_api.create(
            account_id=account_id, amount_minor=11_000, idempotency_key=key
        ).expect_status(200)

        kafka_consumer.assert_no_event(
            lambda e: e.value.get("payment_id") == payment["payment_id"]
            and e.value.get("event_type") == "payment.created",
            within=5.0,
            description="duplicate payment.created",
        )

    @allure.story("Lifecycle")
    def test_capture_emits_state_transition_event(
        self, payments_api, kafka_consumer, account_id
    ):
        payment = payments_api.create_ok(account_id=account_id, amount_minor=6_400)
        kafka_consumer.wait_for_payment_event(payment.payment_id, "payment.created")

        payments_api.capture(payment.payment_id).expect_status(200)

        event = kafka_consumer.wait_for_payment_event(payment.payment_id, "payment.captured")
        assert event.value["status"] == PaymentStatus.CAPTURED.value

    @allure.story("Rejections")
    @pytest.mark.parametrize(
        "blocked_account", ["ACC-EMPTY-0001", "ACC-RISK-0001"]
    )
    def test_rejected_payment_emits_rejection_event(
        self, payments_api, kafka_consumer, blocked_account
    ):
        payment = payments_api.create_ok(account_id=blocked_account, amount_minor=900)

        event = kafka_consumer.wait_for_payment_event(payment.payment_id, "payment.rejected")

        assert event.value["status"] == PaymentStatus.REJECTED.value
