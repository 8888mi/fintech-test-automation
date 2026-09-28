"""Page object for the payment creation screen."""

from __future__ import annotations

import allure
from playwright.sync_api import expect

from framework.ui.base_page import BasePage


class PaymentsPage(BasePage):
    path = "/"

    def fill_form(
        self,
        account_id: str,
        amount_minor: int,
        currency: str = "USD",
        description: str = "",
    ) -> PaymentsPage:
        with allure.step(f"Fill payment form: {account_id} / {amount_minor} {currency}"):
            self.testid("account-input").fill(account_id)
            self.testid("amount-input").fill(str(amount_minor))
            self.testid("currency-select").select_option(currency)
            self.testid("description-input").fill(description)
        return self

    def submit(self) -> PaymentsPage:
        with allure.step("Submit payment"):
            self.testid("submit-button").click()
        return self

    def wait_for_result(self) -> str:
        """Return the status text shown in the result panel."""
        panel = self.expect_visible("result-panel")
        expect(panel).to_be_visible()
        return self.testid("result-status").inner_text().strip()

    @property
    def payment_id(self) -> str:
        return self.testid("result-payment-id").inner_text().strip()

    @property
    def rejection_reason(self) -> str:
        return self.testid("result-reason").inner_text().strip()
