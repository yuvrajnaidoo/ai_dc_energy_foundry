"""
PJM Interconnection Data Miner Connector
=========================================

Connects to PJM's Data Miner 2 API for real-time grid data.
PJM is America's largest electric grid operator, covering the region
from Illinois to North Carolina — including Northern Virginia, the
world's largest data center market.

PJM provides 5-minute interval LMP pricing, load forecasts, and
generation data, making it the highest-frequency data source
in this correlation engine.

Registration: Free at https://dataminer2.pjm.com
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from ai_dc_energy.utils.constants import PJMEndpoints

logger = logging.getLogger(__name__)


class PJMConnector:
    """
    Connector for PJM Interconnection Data Miner 2.

    PJM covers ~65 million customers across 13 states + DC, including
    the top data center market globally (Northern Virginia / Loudoun County).

    Key data feeds:
    - Real-time LMPs (5-minute intervals): price signals showing demand stress
    - Load forecasts: predicted vs actual demand
    - Generation by fuel type: source mix changes from DC load
    - Capacity market results: long-term DC impact on capacity costs
    """

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.base_url = PJMEndpoints.BASE_URL
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if api_key:
            self.session.headers.update({"Ocp-Apim-Subscription-Key": api_key})

    def get_realtime_lmp(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        pnode_ids: Optional[List[str]] = None,
        row_count: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch real-time hourly Locational Marginal Prices (LMPs).

        LMPs are the price signals that reflect real-time supply/demand
        balance at specific grid nodes. Spikes in LMP near data center
        clusters indicate demand stress from DC loads.

        Args:
            start: Start datetime (ISO format)
            end: End datetime
            pnode_ids: Specific pricing node IDs (e.g., Loudoun County nodes)
            row_count: Maximum rows to return

        Returns:
            List of LMP records with timestamp, pnode, price components
        """
        params = {
            "rowCount": row_count,
            "sort": "datetime_beginning_ept desc",
            "fields": "datetime_beginning_ept,pnode_id,pnode_name,"
                      "total_lmp_rt,energy_lmp_rt,congestion_lmp_rt,loss_lmp_rt",
        }

        if start:
            params["datetime_beginning_ept"] = f">={start}"
        if end:
            # PJM uses a specific filter syntax
            if "datetime_beginning_ept" in params:
                params["datetime_beginning_ept"] += f" AND <={end}"
            else:
                params["datetime_beginning_ept"] = f"<={end}"

        if pnode_ids:
            params["pnode_id"] = ",".join(str(p) for p in pnode_ids)

        return self._make_request(PJMEndpoints.RT_LMP, params)

    def get_load_forecast(
        self,
        days_ahead: int = 7,
    ) -> List[Dict[str, Any]]:
        """
        Fetch 7-day load forecast data.

        Comparing forecast vs actual helps identify unexpected demand
        growth from new data center loads.

        Args:
            days_ahead: Number of days ahead to fetch

        Returns:
            Forecast records with datetime, forecasted load, actual load
        """
        params = {
            "rowCount": 5000,
            "sort": "forecast_datetime_beginning_ept desc",
        }
        return self._make_request(PJMEndpoints.LOAD_FORECAST, params)

    def get_generation_by_fuel(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch real-time generation mix by fuel type.

        Critical for understanding whether new DC demand is being met
        by gas peakers (higher carbon) or baseload/renewables.

        Args:
            start: Start datetime
            end: End datetime

        Returns:
            Generation records by fuel category and time period
        """
        params = {
            "rowCount": 5000,
            "sort": "datetime_beginning_ept desc",
        }
        if start:
            params["datetime_beginning_ept"] = f">={start}"

        return self._make_request(PJMEndpoints.GEN_BY_FUEL, params)

    def get_capacity_market_results(
        self,
        delivery_year: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch capacity market auction results.

        PJM's capacity market is where the $9.3 billion price increase
        from data center demand was recorded in 2025-26. This data
        quantifies the long-term cost impact.

        Args:
            delivery_year: Specific delivery year (e.g., '2025/2026')

        Returns:
            Auction results with clearing prices, offered/cleared capacity
        """
        params = {"rowCount": 5000}
        if delivery_year:
            params["delivery_year"] = delivery_year

        return self._make_request(PJMEndpoints.CAPACITY_MARKET, params)

    def _make_request(
        self, endpoint: str, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute request to PJM Data Miner."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            # PJM returns items in a list or in an 'items' key
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("items", data.get("data", []))
            return []

        except requests.exceptions.RequestException as e:
            logger.error(f"PJM Data Miner request failed: {e}")
            raise


# PJM pricing node IDs for key data center markets
# These are approximate — actual pnode IDs should be verified in PJM's system
DC_MARKET_PNODES = {
    "loudoun_county_va": [
        # Northern Virginia (world's largest DC market)
        # Dominion Energy transmission nodes in Loudoun/Prince William
    ],
    "columbus_oh": [
        # AEP Ohio transmission nodes near DC clusters
    ],
    "ashburn_va": [
        # Specific Ashburn-area pricing nodes
    ],
}
