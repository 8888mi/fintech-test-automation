"""Domain models for the demo payments service.

Money is represented as integer minor units (kopecks/cents) everywhere.
Floats are never used for monetary amounts - see docs/ARCHITECTURE.md.
"""

from __future__ import annotations

import enum
from datetime import UTC, datetime
from typing import Annotated

from pydantic import BaseModel, Field, field_validator

SUPPORTED_CURRENCIES = frozenset({"USD", "EUR", "TRY", "GBP"})

# Business rule: a single payment may not exceed 1_000_000 minor units.
MAX_AMOUNT_MINOR = 1_000_000


class PaymentStatus(str, enum.Enum):
    PENDING = "PENDING"
    AUTHORIZED = "AUTHORIZED"
    CAPTURED = "CAPTURED"
    REJECTED = "REJECTED"
    REFUNDED = "REFUNDED"


class RejectionReason(str, enum.Enum):
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    LIMIT_EXCEEDED = "LIMIT_EXCEEDED"
    RISK_DECLINED = "RISK_DECLINED"


class PaymentRequest(BaseModel):
    """Inbound payload for POST /api/v1/payments."""

    account_id: Annotated[str, Field(min_length=3, max_length=64)]
    amount_minor: Annotated[int, Field(gt=0, le=MAX_AMOUNT_MINOR)]
    currency: Annotated[str, Field(min_length=3, max_length=3)]
    idempotency_key: Annotated[str, Field(min_length=8, max_length=128)]
    description: Annotated[str, Field(default="", max_length=256)]

    @field_validator("currency")
    @classmethod
    def _known_currency(cls, value: str) -> str:
        upper = value.upper()
        if upper not in SUPPORTED_CURRENCIES:
            raise ValueError(f"unsupported currency: {value}")
        return upper


class Payment(BaseModel):
    """Persisted payment aggregate, also the response body."""

    payment_id: str
    account_id: str
    amount_minor: int
    currency: str
    status: PaymentStatus
    idempotency_key: str
    description: str = ""
    rejection_reason: RejectionReason | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PaymentEvent(BaseModel):
    """Contract published to the `payments.events` Kafka topic.

    Consumers downstream depend on this shape. The contract test in
    tests/integration/test_payment_events.py is the guard against silent
    breaking changes.
    """

    event_id: str
    event_type: str
    payment_id: str
    account_id: str
    amount_minor: int
    currency: str
    status: PaymentStatus
    occurred_at: datetime
    schema_version: int = 1
