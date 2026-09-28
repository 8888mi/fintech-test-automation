"""Money helpers.

Amounts travel as integer minor units. `Decimal` appears only at the
presentation boundary, and `float` never appears at all - a float cannot
represent 0.10 exactly, and in a payments system that difference eventually
becomes a reconciliation ticket.
"""

from __future__ import annotations

from decimal import Decimal

# Currencies whose minor unit is not 1/100.
_EXPONENTS = {"JPY": 0, "KRW": 0, "BHD": 3, "KWD": 3, "TND": 3}


def exponent(currency: str) -> int:
    return _EXPONENTS.get(currency.upper(), 2)


def to_major(amount_minor: int, currency: str = "USD") -> Decimal:
    """25000, 'USD' -> Decimal('250.00')"""
    return Decimal(amount_minor).scaleb(-exponent(currency))


def to_minor(amount_major: Decimal | str, currency: str = "USD") -> int:
    """Decimal('250.00'), 'USD' -> 25000"""
    scaled = Decimal(str(amount_major)).scaleb(exponent(currency))
    if scaled != scaled.to_integral_value():
        raise ValueError(f"{amount_major} has more precision than {currency} allows")
    return int(scaled)


def format_amount(amount_minor: int, currency: str = "USD") -> str:
    return f"{to_major(amount_minor, currency):.{exponent(currency)}f} {currency.upper()}"
