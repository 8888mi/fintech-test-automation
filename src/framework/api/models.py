"""Response models.

These are intentionally *separate* from the service's own models. The test
suite must not import the implementation's schema, otherwise a breaking
contract change would silently update both sides and the test would still
pass. Duplication here is the point.
"""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, ConfigDict


class PaymentStatus(str, enum.Enum):
    PENDING = "PENDING"
    AUTHORIZED = "AUTHORIZED"
    CAPTURED = "CAPTURED"
    REJECTED = "REJECTED"
    REFUNDED = "REFUNDED"


class PaymentResponse(BaseModel):
    # `extra="forbid"` turns an unannounced new field into a failing test.
    # That is deliberate: in fintech, silent schema drift is an incident.
    model_config = ConfigDict(extra="forbid")

    payment_id: str
    account_id: str
    amount_minor: int
    currency: str
    status: PaymentStatus
    idempotency_key: str
    description: str = ""
    rejection_reason: str | None = None
    created_at: datetime
    updated_at: datetime


class PaymentEventModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str
    payment_id: str
    account_id: str
    amount_minor: int
    currency: str
    status: PaymentStatus
    occurred_at: datetime
    schema_version: int
