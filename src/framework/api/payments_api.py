"""Service object for the payments API.

Tests express intent ("create a payment"), not transport ("POST this path").
When the endpoint moves, exactly one line in this file changes.
"""

from __future__ import annotations

import uuid

from framework.api.client import ApiClient, ApiResponse
from framework.api.models import PaymentResponse


class PaymentsApi:
    ROOT = "/api/v1/payments"

    def __init__(self, client: ApiClient) -> None:
        self._client = client

    @staticmethod
    def new_idempotency_key() -> str:
        return f"idem-{uuid.uuid4().hex}"

    def create(
        self,
        account_id: str,
        amount_minor: int,
        currency: str = "USD",
        description: str = "",
        idempotency_key: str | None = None,
    ) -> ApiResponse:
        payload = {
            "account_id": account_id,
            "amount_minor": amount_minor,
            "currency": currency,
            "description": description,
            # `is None`, not `or` - an empty string is a deliberate negative
            # test case and must reach the service unmodified.
            "idempotency_key": (
                self.new_idempotency_key() if idempotency_key is None else idempotency_key
            ),
        }
        return self._client.post(self.ROOT, json=payload)

    def create_ok(self, **kwargs) -> PaymentResponse:
        """Happy-path helper: assert 201 and return the parsed model."""
        return self.create(**kwargs).expect_status(201).as_model(PaymentResponse)

    def get(self, payment_id: str) -> ApiResponse:
        return self._client.get(f"{self.ROOT}/{payment_id}")

    def capture(self, payment_id: str) -> ApiResponse:
        return self._client.post(f"{self.ROOT}/{payment_id}/capture")

    def list_for_account(self, account_id: str) -> ApiResponse:
        return self._client.get(f"/api/v1/accounts/{account_id}/payments")
