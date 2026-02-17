"""
Time Utilities
==============

Time zone normalization and period handling for multi-source data alignment.
EIA uses Eastern Prevailing Time (EPT), PJM uses EPT, NOAA uses UTC.
"""

from datetime import datetime, timezone
from typing import Optional

import pytz

# Standard time zones for grid operators
TIMEZONE_MAP = {
    "PJM": "US/Eastern",
    "ERCO": "US/Central",
    "CISO": "US/Pacific",
    "SOCO": "US/Eastern",
    "BPAT": "US/Pacific",
    "MISO": "US/Central",
    "NEVP": "US/Pacific",
    "SRP": "US/Arizona",
    "PSCO": "US/Mountain",
    "TVA": "US/Central",
}


def normalize_to_utc(dt: datetime, source_tz: str) -> datetime:
    """Convert a timezone-aware or naive datetime to UTC."""
    tz = pytz.timezone(source_tz)
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    return dt.astimezone(pytz.utc)


def eia_period_to_utc(period_str: str, ba_code: str) -> Optional[datetime]:
    """
    Convert an EIA period string to a UTC datetime.

    EIA uses local prevailing time for each BA.
    Format: 'YYYY-MM-DDTHH' (e.g., '2024-01-15T14')

    Args:
        period_str: EIA period string
        ba_code: Balancing authority code for timezone lookup

    Returns:
        UTC datetime, or None if parsing fails
    """
    try:
        dt = datetime.strptime(period_str, "%Y-%m-%dT%H")
        tz_name = TIMEZONE_MAP.get(ba_code, "US/Eastern")
        return normalize_to_utc(dt, tz_name)
    except (ValueError, TypeError):
        return None


def months_between(date1, date2) -> int:
    """Compute the number of months between two dates."""
    return (date2.year - date1.year) * 12 + (date2.month - date1.month)
