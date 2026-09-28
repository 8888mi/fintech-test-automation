"""End-to-end UI tests.

Few by design. Everything provable at the API level is proven there; these
exist only to confirm that the browser, the API and the event stream agree
with each other. That last assertion - UI action produces a Kafka event -
is the one that cannot be made at any lower level.
"""

from __future__ import annotations

import allure
import pytest

from framework.ui.pages.payments_page import PaymentsPage


@pytest.fixture
def payments_page(page, settings) -> PaymentsPage:
    page.set_default_timeout(settings.ui.default_timeout_ms)
    return PaymentsPage(page, settings.ui.base_url).open()


@allure.epic("Payments")
@allure.feature("Web UI")
class TestPaymentFlowUi:
    @allure.story("Happy path")
    @allure.severity(allure.severity_level.BLOCKER)
    def test_user_can_create_a_payment(self, payments_page, account_id):
        payments_page.fill_form(
            account_id=account_id, amount_minor=25_000, currency="EUR",
            description="Loan repayment",
        ).submit()

        assert payments_page.wait_for_result() == "AUTHORIZED"
        assert payments_page.payment_id.startswith("pay_")
        payments_page.screenshot("payment created")

    @allure.story("Rejections")
    def test_rejection_reason_is_shown_to_the_user(self, payments_page):
        payments_page.fill_form(
            account_id="ACC-EMPTY-0001", amount_minor=5_000
        ).submit()

        assert payments_page.wait_for_result() == "REJECTED"
        assert payments_page.rejection_reason == "INSUFFICIENT_FUNDS"

    @allure.story("Validation")
    def test_over_limit_amount_surfaces_a_validation_error(self, payments_page, account_id):
        payments_page.fill_form(account_id=account_id, amount_minor=2_000_000).submit()

        assert payments_page.wait_for_result() == "ERROR 422"

    @allure.story("Cross-layer")
    @allure.severity(allure.severity_level.CRITICAL)
    @allure.description(
        "The only test in the suite that spans all three layers: a click in the "
        "browser must end up as an event on the Kafka topic. Everything narrower "
        "than this is already covered more cheaply elsewhere."
    )
    def test_ui_submission_reaches_the_event_stream(
        self, payments_page, kafka_consumer, account_id
    ):
        payments_page.fill_form(account_id=account_id, amount_minor=18_500).submit()
        assert payments_page.wait_for_result() == "AUTHORIZED"

        event = kafka_consumer.wait_for_payment_event(
            payments_page.payment_id, "payment.created"
        )
        assert event.value["amount_minor"] == 18_500
