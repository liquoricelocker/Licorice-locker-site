"""Monthly commission tiers and bonuses for Licorice Locker Listening Room (affiliate) program."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

# —— Core rate: Nth qualifying sale in the calendar month (1-based). Not retroactive. ——


def rate_for_nth_sale(n: int) -> float:
    """Commission rate for the Nth completed referral sale in that month."""
    if n < 1:
        n = 1
    if n >= 25:
        return 0.30
    if n >= 10:
        return 0.25
    return 0.20


def commission_cents_for_nth_sale(nth_sale: int, order_total_cents: int) -> int:
    return int(round(order_total_cents * rate_for_nth_sale(nth_sale)))


def tier_name_for_completed_sales(completed_count: int) -> str:
    """Tier label after `completed_count` sales this month."""
    if completed_count >= 25:
        return "Top Performer"
    if completed_count >= 10:
        return "Momentum"
    return "Base"


def current_rate_for_next_sale_after(completed_count: int) -> float:
    """Rate that applies to the next sale after `completed_count` orders this month."""
    return rate_for_nth_sale(completed_count + 1)


def next_tier_sales_threshold(completed_count: int) -> Optional[int]:
    """Next milestone count (10 or 25), or None if already at top tier pace for the month."""
    if completed_count < 10:
        return 10
    if completed_count < 25:
        return 25
    return None


def progress_toward_next_tier_pct(completed_count: int) -> float:
    if completed_count >= 25:
        return 100.0
    if completed_count < 10:
        return min(100.0, (completed_count / 10.0) * 100.0)
    return min(100.0, ((completed_count - 10) / 15.0) * 100.0)


def monthly_milestone_bonus_cents(completed_sales_count: int) -> int:
    """One-time per month per milestone (NZD), paid with end-of-month commission run."""
    bonus = 0
    if completed_sales_count >= 10:
        bonus += 10_000  # $100
    if completed_sales_count >= 25:
        bonus += 30_000  # $300
    return bonus


# Dashboard / docs: tier definitions (matches spec JSON)
COMMISSION_TIERS: List[Dict[str, Any]] = [
    {"tier": 1, "name": "Base", "min_sales": 0, "max_sales": 9, "commission_rate": 0.20},
    {"tier": 2, "name": "Momentum", "min_sales": 10, "max_sales": 24, "commission_rate": 0.25},
    {"tier": 3, "name": "Top Performer", "min_sales": 25, "max_sales": None, "commission_rate": 0.30},
]

# List-price examples for UI (NZD); Soundwave $429, Mini Series $143
EARNINGS_DISPLAY_NZD: Dict[str, Dict[str, float]] = {
    "Soundwave Display": {"20%": 85.80, "25%": 107.25, "30%": 128.70},
    "Mini Series": {"20%": 28.60, "25%": 35.75, "30%": 42.90},
}

LIST_PRICE_SOUNDWAVE_NZD = 429
LIST_PRICE_MINI_SERIES_NZD = 143


def next_payout_date_for_month(year: int, month: int) -> date:
    """Payouts occur halfway through the following month (15th)."""
    if month == 12:
        return date(year + 1, 1, 15)
    return date(year, month + 1, 15)


@dataclass
class MonthlyCommissionSummary:
    year_month: str
    sales_count: int
    total_sales_cents: int
    commission_from_orders_cents: int
    bonus_cents: int
    total_payable_cents: int
    current_rate_next_sale: float
    tier_name: str
    payout_date: date


def summarize_month(
    total_sales_cents: int,
    orders_commission_cents: int,
    sales_count: int,
    year: int,
    month: int,
) -> MonthlyCommissionSummary:
    ym = f"{year:04d}-{month:02d}"
    bonus = monthly_milestone_bonus_cents(sales_count)
    return MonthlyCommissionSummary(
        year_month=ym,
        sales_count=sales_count,
        total_sales_cents=total_sales_cents,
        commission_from_orders_cents=orders_commission_cents,
        bonus_cents=bonus,
        total_payable_cents=orders_commission_cents + bonus,
        current_rate_next_sale=current_rate_for_next_sale_after(sales_count),
        tier_name=tier_name_for_completed_sales(sales_count),
        payout_date=next_payout_date_for_month(year, month),
    )
