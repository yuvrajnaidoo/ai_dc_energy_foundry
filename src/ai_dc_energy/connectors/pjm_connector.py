"""
PJM Interconnection Data Miner 2 Connector
============================================

Connects to PJM's Data Miner 2 API for real-time grid data.
PJM is America's largest electric grid operator, covering the region
from Illinois to North Carolina — including Northern Virginia, the
world's largest data center market.

PJM provides 5-minute interval LMP pricing, load forecasts, and
generation data, making it the highest-frequency data source
in this correlation engine.

Authentication: Uses a public subscription key fetched dynamically
from PJM's Data Miner 2 settings endpoint. No personal API account
required for public data access.
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
    - Real-time LMPs (hourly): price signals showing demand stress
    - Load forecasts: predicted vs actual demand
    - Generation by fuel type: source mix changes from DC load
    - Metered load by zone: actual consumption per transmission zone
    """

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        self.base_url = PJMEndpoints.BASE_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

        # Use provided key or fetch the public key from Data Miner 2
        if api_key:
            self._subscription_key = api_key
        else:
            self._subscription_key = self._fetch_public_subscription_key()

        self.session.headers.update({
            "Ocp-Apim-Subscription-Key": self._subscription_key,
        })

    @staticmethod
    def _fetch_public_subscription_key() -> str:
        """
        Fetch the public subscription key from PJM Data Miner 2 settings.

        PJM publishes this key at a well-known URL so their Angular web
        app can access the API. The key rotates periodically, so we
        fetch it dynamically rather than hardcoding.
        """
        try:
            response = requests.get(
                PJMEndpoints.SETTINGS_URL,
                timeout=10,
            )
            response.raise_for_status()
            settings = response.json()
            key = settings["subscriptionKey"]
            logger.info("Fetched PJM public subscription key successfully")
            return key
        except Exception as e:
            logger.error(f"Failed to fetch PJM subscription key: {e}")
            raise RuntimeError(
                "Cannot fetch PJM Data Miner 2 subscription key from "
                f"{PJMEndpoints.SETTINGS_URL}: {e}"
            )

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
            start: Start datetime (M/D/YYYY HH:MM format for PJM)
            end: End datetime
            pnode_ids: Specific pricing node IDs (e.g., Loudoun County nodes)
            row_count: Maximum rows to return

        Returns:
            List of LMP records with timestamp, pnode, price components
        """
        params = {
            "rowCount": row_count,
            "sort": "datetime_beginning_ept",
            "order": "desc",
            "startRow": 1,
            "isActiveMetadata": "true",
        }

        if start and end:
            params["datetime_beginning_ept"] = f"{start}to{end}"
        elif start:
            params["datetime_beginning_ept"] = f"{start}to"

        if pnode_ids:
            params["pnode_id"] = ";".join(str(p) for p in pnode_ids)

        return self._make_request(PJMEndpoints.RT_LMP, params)

    def get_load_metered(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        row_count: int = 5000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch hourly metered load by transmission zone.

        Provides actual metered consumption per PJM zone, which is
        the most accurate load measurement available.

        Args:
            start: Start datetime
            end: End datetime
            row_count: Maximum rows

        Returns:
            Hourly metered load records per zone
        """
        params = {
            "rowCount": row_count,
            "sort": "datetime_beginning_ept",
            "order": "desc",
            "startRow": 1,
            "isActiveMetadata": "true",
        }
        if start and end:
            params["datetime_beginning_ept"] = f"{start}to{end}"
        return self._make_request(PJMEndpoints.LOAD_METERED, params)

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
            "sort": "forecast_datetime_beginning_ept",
            "order": "desc",
            "startRow": 1,
            "isActiveMetadata": "true",
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
            "sort": "datetime_beginning_ept",
            "order": "desc",
            "startRow": 1,
            "isActiveMetadata": "true",
        }
        if start and end:
            params["datetime_beginning_ept"] = f"{start}to{end}"

        return self._make_request(PJMEndpoints.GEN_BY_FUEL, params)

    def get_instantaneous_load(self) -> List[Dict[str, Any]]:
        """
        Fetch the most recent instantaneous load snapshot.

        Returns:
            Current instantaneous load records
        """
        params = {
            "rowCount": 100,
            "sort": "datetime_beginning_ept",
            "order": "desc",
            "startRow": 1,
            "isActiveMetadata": "true",
        }
        return self._make_request(PJMEndpoints.INST_LOAD, params)

    def _make_request(
        self, endpoint: str, params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute paginated request to PJM Data Miner 2 API."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            # PJM API returns items in an 'items' key with pagination links
            if isinstance(data, dict):
                return data.get("items", [])
            elif isinstance(data, list):
                return data
            return []

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.warning("PJM key expired, refreshing...")
                self._subscription_key = self._fetch_public_subscription_key()
                self.session.headers.update({
                    "Ocp-Apim-Subscription-Key": self._subscription_key,
                })
                # Retry once
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                return data.get("items", []) if isinstance(data, dict) else data
            logger.error(f"PJM Data Miner request failed: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"PJM Data Miner request failed: {e}")
            raise

    def fetch_paginated(
        self, endpoint: str, params: Dict[str, Any], max_pages: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages of results from a PJM endpoint.

        PJM API returns pagination links in the response. This method
        follows 'next' links to retrieve complete datasets.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            max_pages: Safety limit on pagination

        Returns:
            Combined list of all items across pages
        """
        all_items = []
        url = f"{self.base_url}{endpoint}"
        page = 0

        while url and page < max_pages:
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()

                items = data.get("items", [])
                all_items.extend(items)

                if not items:
                    break

                # Follow pagination: find 'next' link
                links = data.get("links", [])
                next_link = next(
                    (link["href"] for link in links if link.get("rel") == "next"),
                    None,
                )
                url = next_link
                params = {}  # Params are embedded in the next URL
                page += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"PJM pagination failed on page {page}: {e}")
                break

        logger.info(f"Fetched {len(all_items)} total items across {page + 1} pages")
        return all_items


# PJM pricing node IDs for key data center markets
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
