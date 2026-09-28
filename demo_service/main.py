"""Demo payments service - the System Under Test.

Deliberately small, but it reproduces the three things that make payment
systems awkward to test:

1. Idempotency - the same idempotency_key must never create two payments.
2. Asynchronous side effects - every state change emits a Kafka event, so
   the HTTP 201 is not the end of the story.
3. Money as integers - no floats anywhere near an amount.

Run with: uvicorn demo_service.main:app --reload
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from demo_service.models import (
    Payment,
    PaymentEvent,
    PaymentRequest,
    PaymentStatus,
    RejectionReason,
)

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PAYMENTS_TOPIC = os.getenv("PAYMENTS_TOPIC", "payments.events")

# Accounts with a balance low enough to trigger a rejection. Keeps the
# rejection path deterministic for tests instead of random.
LOW_BALANCE_ACCOUNTS = {"ACC-EMPTY-0001"}
RISK_BLOCKED_ACCOUNTS = {"ACC-RISK-0001"}

app = FastAPI(title="Demo Payments Service", version="1.0.0")

# In-memory stores. A real service would use Postgres; the point here is the
# behaviour under test, not the persistence layer.
_payments: dict[str, Payment] = {}
_by_idempotency_key: dict[str, str] = {}

_producer = None


def _get_producer():
    """Lazily build a Kafka producer so the service still boots without Kafka."""
    global _producer
    if _producer is not None:
        return _producer
    try:
        from confluent_kafka import Producer

        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    except Exception as exc:  # pragma: no cover - infrastructure path
        log.warning("Kafka producer unavailable, events will be dropped: %s", exc)
        _producer = None
    return _producer


def _publish(payment: Payment, event_type: str) -> None:
    event = PaymentEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        payment_id=payment.payment_id,
        account_id=payment.account_id,
        amount_minor=payment.amount_minor,
        currency=payment.currency,
        status=payment.status,
        occurred_at=datetime.now(UTC),
    )
    producer = _get_producer()
    if producer is None:
        return
    producer.produce(
        topic=PAYMENTS_TOPIC,
        # Partition key = account_id guarantees per-account ordering.
        key=payment.account_id.encode(),
        value=event.model_dump_json().encode(),
    )
    producer.poll(0)


def _authorize(request: PaymentRequest) -> tuple[PaymentStatus, RejectionReason | None]:
    if request.account_id in RISK_BLOCKED_ACCOUNTS:
        return PaymentStatus.REJECTED, RejectionReason.RISK_DECLINED
    if request.account_id in LOW_BALANCE_ACCOUNTS:
        return PaymentStatus.REJECTED, RejectionReason.INSUFFICIENT_FUNDS
    return PaymentStatus.AUTHORIZED, None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/payments", response_model=Payment, status_code=status.HTTP_201_CREATED)
def create_payment(request: PaymentRequest, response: Response) -> Payment:
    existing_id = _by_idempotency_key.get(request.idempotency_key)
    if existing_id:
        # Idempotent replay: return the original resource with 200, not 201,
        # and do NOT emit a second event.
        response.status_code = status.HTTP_200_OK
        return _payments[existing_id]

    payment_status, reason = _authorize(request)
    payment = Payment(
        payment_id=f"pay_{uuid.uuid4().hex[:16]}",
        account_id=request.account_id,
        amount_minor=request.amount_minor,
        currency=request.currency,
        status=payment_status,
        idempotency_key=request.idempotency_key,
        description=request.description,
        rejection_reason=reason,
    )
    _payments[payment.payment_id] = payment
    _by_idempotency_key[request.idempotency_key] = payment.payment_id

    _publish(payment, "payment.created")
    if payment.status is PaymentStatus.REJECTED:
        _publish(payment, "payment.rejected")
    return payment


@app.get("/api/v1/payments/{payment_id}", response_model=Payment)
def get_payment(payment_id: str) -> Payment:
    payment = _payments.get(payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="payment not found")
    return payment


@app.post("/api/v1/payments/{payment_id}/capture", response_model=Payment)
def capture_payment(payment_id: str) -> Payment:
    payment = _payments.get(payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="payment not found")
    if payment.status is not PaymentStatus.AUTHORIZED:
        raise HTTPException(
            status_code=409,
            detail=f"cannot capture payment in status {payment.status.value}",
        )
    payment.status = PaymentStatus.CAPTURED
    payment.updated_at = datetime.now(UTC)
    _publish(payment, "payment.captured")
    return payment


@app.get("/api/v1/accounts/{account_id}/payments", response_model=list[Payment])
def list_account_payments(account_id: str) -> list[Payment]:
    return [p for p in _payments.values() if p.account_id == account_id]


_STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")
