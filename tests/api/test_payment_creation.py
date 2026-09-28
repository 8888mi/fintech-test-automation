"""API-level tests for payment creation."""

from __future__ import annotations

import allure
import pytest

from framework.api.models import PaymentStatus
from framework.utils.money import format_amount


@allure.epic("Payments")
@allure.feature("Payment creation")
class TestPaymentCreation:
    @allure.story("Happy path")
    @allure.severity(allure.severity_level.BLOCKER)
    def test_authorized_payment_is_created(self, payments_api, account_id):
        payment = payments_api.create_ok(
            account_id=account_id, amount_minor=25_000, currency="USD"
        )

        assert payment.status is PaymentStatus.AUTHORIZED
        assert payment.amount_minor == 25_000
        assert payment.rejection_reason is None
        assert payment.payment_id.startswith("pay_")

    @allure.story("Happy path")
    def test_created_payment_is_retrievable(self, payments_api, account_id):
        created = payments_api.create_ok(account_id=account_id, amount_minor=5_000)

        fetched = payments_api.get(created.payment_id).expect_status(200).json()

        assert fetched["payment_id"] == created.payment_id
        assert fetched["amount_minor"] == created.amount_minor

    @allure.story("Idempotency")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.description(
        "A retried request with the same idempotency key must return the original "
        "payment and must not charge the customer twice. This is the single most "
        "expensive defect class in a payments API."
    )
    def test_replayed_idempotency_key_returns_original_payment(self, payments_api, account_id):
        key = payments_api.new_idempotency_key()

        first = payments_api.create(
            account_id=account_id, amount_minor=10_000, idempotency_key=key
        ).expect_status(201).json()

        # Same key, deliberately different amount: the service must ignore the
        # new body entirely rather than create a second payment.
        second = payments_api.create(
            account_id=account_id, amount_minor=99_000, idempotency_key=key
        ).expect_status(200).json()

        assert second["payment_id"] == first["payment_id"]
        assert second["amount_minor"] == 10_000, "Replay must not overwrite the amount"

        all_payments = payments_api.list_for_account(account_id).expect_status(200).json()
        assert len(all_payments) == 1, f"Duplicate payment created: {all_payments}"

    @allure.story("Currencies")
    @pytest.mark.parametrize("currency", ["USD", "EUR", "TRY", "GBP"])
    def test_supported_currencies_are_accepted(self, payments_api, account_id, currency):
        payment = payments_api.create_ok(
            account_id=account_id, amount_minor=1_500, currency=currency
        )
        assert payment.currency == currency
        allure.attach(format_amount(payment.amount_minor, currency), name="amount")

    @allure.story("Business rules")
    @pytest.mark.parametrize(
        ("blocked_account", "expected_reason"),
        [
            ("ACC-EMPTY-0001", "INSUFFICIENT_FUNDS"),
            ("ACC-RISK-0001", "RISK_DECLINED"),
        ],
    )
    def test_rejection_carries_a_machine_readable_reason(
        self, payments_api, blocked_account, expected_reason
    ):
        payment = payments_api.create_ok(account_id=blocked_account, amount_minor=1_000)

        assert payment.status is PaymentStatus.REJECTED
        assert payment.rejection_reason == expected_reason
