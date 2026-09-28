"""Negative and boundary tests.

Boundary values are stated explicitly rather than computed from the
implementation's constant - if someone lowers the limit, this test should
fail and force a conversation, not quietly follow along.
"""

from __future__ import annotations

import allure
import pytest

MAX_AMOUNT_MINOR = 1_000_000


@allure.epic("Payments")
@allure.feature("Input validation")
class TestPaymentValidation:
    @allure.story("Amount boundaries")
    @pytest.mark.parametrize(
        ("amount", "expected_status", "case"),
        [
            (0, 422, "zero is not a payment"),
            (-1, 422, "negative amount"),
            (1, 201, "minimum valid amount"),
            (MAX_AMOUNT_MINOR, 201, "exactly at the limit"),
            (MAX_AMOUNT_MINOR + 1, 422, "one over the limit"),
        ],
    )
    def test_amount_boundaries(self, payments_api, account_id, amount, expected_status, case):
        allure.dynamic.title(f"Amount {amount}: {case}")
        payments_api.create(account_id=account_id, amount_minor=amount).expect_status(
            expected_status
        )

    @allure.story("Currency validation")
    @pytest.mark.parametrize("currency", ["XXX", "US", "USDD", "", "123"])
    def test_unknown_currency_is_rejected(self, payments_api, account_id, currency):
        payments_api.create(
            account_id=account_id, amount_minor=1_000, currency=currency
        ).expect_status(422)

    @allure.story("Currency validation")
    def test_currency_is_normalised_to_upper_case(self, payments_api, account_id):
        payment = payments_api.create_ok(
            account_id=account_id, amount_minor=1_000, currency="usd"
        )
        assert payment.currency == "USD"

    @allure.story("Idempotency key validation")
    @pytest.mark.parametrize("key", ["", "short", "x" * 129])
    def test_malformed_idempotency_key_is_rejected(self, payments_api, account_id, key):
        payments_api.create(
            account_id=account_id, amount_minor=1_000, idempotency_key=key
        ).expect_status(422)

    @allure.story("Lifecycle")
    def test_capturing_a_rejected_payment_conflicts(self, payments_api):
        rejected = payments_api.create_ok(account_id="ACC-RISK-0001", amount_minor=1_000)
        payments_api.capture(rejected.payment_id).expect_status(409)

    @allure.story("Lifecycle")
    def test_unknown_payment_returns_404(self, payments_api):
        payments_api.get("pay_doesnotexist000").expect_status(404)
