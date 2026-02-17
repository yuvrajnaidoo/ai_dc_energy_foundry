"""
U.S. Energy Information Administration (EIA) API v2 Connector
=============================================================

Connects to the EIA Open Data API to pull:
- Hourly electricity demand by balancing authority (RTO data)
- Monthly retail sales by state and sector
- Generation by fuel type
- State electricity profiles

API Documentation: https://www.eia.gov/opendata/documentation.php
Registration: https://www.eia.gov/opendata/ (free API key required)
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from ai_dc_energy.utils.constants import EIAEndpoints

logger = logging.getLogger(__name__)


class EIAConnector:
    """
    Connector for the U.S. EIA API v2.

    Usage in Foundry:
        This connector is used within Foundry transforms to fetch data
        from the EIA API. In production, the API key should be stored
        in Foundry's secret management system.

    Example:
        connector = EIAConnector(api_key="your-key")
        df = connector.get_rto_demand(
            balancing_authorities=["PJM"],
            start="2024-01-01T00",
            end="2024-12-31T23",
            frequency="hourly"
        )
    """

    def __init__(self, api_key: str, timeout: int = 30):
        self.api_key = api_key
        self.base_url = EIAEndpoints.BASE_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _make_request(
        self,
        path: str,
        data_fields: List[str],
        facets: Optional[Dict[str, List[str]]] = None,
        frequency: str = "hourly",
        start: Optional[str] = None,
        end: Optional[str] = None,
        sort_column: str = "period",
        sort_direction: str = "desc",
        offset: int = 0,
        length: int = 5000,
    ) -> Dict[str, Any]:
        """
        Make a request to the EIA API v2.

        Args:
            path: API route path (e.g., '/electricity/rto/region-sub-ba-data/data/')
            data_fields: List of data fields to return (e.g., ['value'])
            facets: Filter facets (e.g., {'parent': ['PJM'], 'subba': ['ZONA']})
            frequency: Data frequency ('hourly', 'daily', 'monthly', 'annual')
            start: Start period (format depends on frequency)
            end: End period
            sort_column: Column to sort by
            sort_direction: Sort direction ('asc' or 'desc')
            offset: Pagination offset
            length: Number of records to return (max 5000)

        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}{path}"

        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "sort[0][column]": sort_column,
            "sort[0][direction]": sort_direction,
            "offset": offset,
            "length": min(length, 5000),
        }

        # Add data fields
        for i, field in enumerate(data_fields):
            params[f"data[{i}]"] = field

        # Add facets
        if facets:
            for facet_name, facet_values in facets.items():
                for value in facet_values:
                    params[f"facets[{facet_name}][]"] = value

        # Add date filters
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"EIA API request failed: {e}")
            raise

    def get_rto_demand(
        self,
        balancing_authorities: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None,
        frequency: str = "hourly",
        subregions: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Fetch real-time electricity demand data by balancing authority.

        This is the primary real-time data feed. The EIA RTO endpoint provides
        hourly demand data for 64 balancing authorities across the U.S. grid.

        Args:
            balancing_authorities: BA codes (e.g., ['PJM', 'ERCO', 'CISO'])
            start: Start datetime (e.g., '2024-01-01T00')
            end: End datetime
            frequency: 'hourly' or 'daily'
            subregions: Optional sub-BA codes for finer granularity

        Returns:
            List of demand records with period, value, respondent info
        """
        if subregions:
            # Use sub-BA endpoint for finer granularity
            path = EIAEndpoints.RTO_SUBREGION
            facets = {
                "parent": balancing_authorities,
                "subba": subregions,
            }
        else:
            path = EIAEndpoints.RTO_REGION
            facets = {"respondent": balancing_authorities}

        all_records = []
        offset = 0

        while True:
            result = self._make_request(
                path=path,
                data_fields=["value"],
                facets=facets,
                frequency=frequency,
                start=start,
                end=end,
                offset=offset,
            )

            data = result.get("response", {}).get("data", [])
            all_records.extend(data)

            total = result.get("response", {}).get("total", 0)
            offset += len(data)

            if offset >= total or len(data) == 0:
                break

            logger.info(f"Fetched {offset}/{total} RTO demand records")

        return all_records

    def get_generation_by_fuel(
        self,
        balancing_authorities: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch electricity generation by fuel type for balancing authorities.

        Provides the energy source mix (natural gas, nuclear, solar, wind, etc.)
        which is critical for computing carbon intensity of data center regions.

        Args:
            balancing_authorities: BA codes
            start: Start datetime
            end: End datetime

        Returns:
            List of generation records by fuel type
        """
        return self._fetch_all(
            path=EIAEndpoints.RTO_FUEL_TYPE,
            data_fields=["value"],
            facets={"respondent": balancing_authorities},
            frequency="hourly",
            start=start,
            end=end,
        )

    def get_retail_sales(
        self,
        states: List[str],
        sectors: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        frequency: str = "monthly",
    ) -> List[Dict]:
        """
        Fetch retail electricity sales data by state and sector.

        Provides sales (MWh), revenue ($), price (cents/kWh), and customer
        counts. Useful for computing per-capita energy impact.

        Args:
            states: State codes (e.g., ['VA', 'TX', 'GA'])
            sectors: Sector codes ('RES', 'COM', 'IND', 'TRA', 'ALL')
            start: Start period (e.g., '2020-01')
            end: End period
            frequency: 'monthly', 'quarterly', or 'annual'

        Returns:
            List of retail sales records
        """
        facets = {"stateid": states}
        if sectors:
            facets["sectorid"] = sectors

        return self._fetch_all(
            path=EIAEndpoints.RETAIL_SALES,
            data_fields=["sales", "revenue", "price", "customers"],
            facets=facets,
            frequency=frequency,
            start=start,
            end=end,
        )

    def get_facility_data(
        self,
        states: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch power plant operational data by state.

        Provides plant-level generation and fuel consumption data.

        Args:
            states: State codes
            start: Start period
            end: End period

        Returns:
            List of facility-level records
        """
        return self._fetch_all(
            path=EIAEndpoints.FACILITY_FUEL,
            data_fields=["generation", "total-consumption"],
            facets={"stateid": states},
            frequency="monthly",
            start=start,
            end=end,
        )

    def _fetch_all(
        self,
        path: str,
        data_fields: List[str],
        facets: Optional[Dict[str, List[str]]] = None,
        frequency: str = "hourly",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> List[Dict]:
        """Paginate through all results for a given query."""
        all_records = []
        offset = 0

        while True:
            result = self._make_request(
                path=path,
                data_fields=data_fields,
                facets=facets,
                frequency=frequency,
                start=start,
                end=end,
                offset=offset,
            )

            data = result.get("response", {}).get("data", [])
            all_records.extend(data)

            total = result.get("response", {}).get("total", 0)
            offset += len(data)

            if offset >= total or len(data) == 0:
                break

        logger.info(f"Fetched {len(all_records)} total records from {path}")
        return all_records

    def get_latest_demand(
        self, balancing_authorities: List[str], hours_back: int = 24
    ) -> List[Dict]:
        """
        Convenience method: fetch the most recent demand data.

        Args:
            balancing_authorities: BA codes to query
            hours_back: How many hours of recent data to fetch

        Returns:
            Recent demand records sorted newest-first
        """
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(hours=hours_back)

        return self.get_rto_demand(
            balancing_authorities=balancing_authorities,
            start=start_dt.strftime("%Y-%m-%dT%H"),
            end=end_dt.strftime("%Y-%m-%dT%H"),
            frequency="hourly",
        )
