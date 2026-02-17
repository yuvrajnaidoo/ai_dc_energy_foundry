"""
NOAA Climate Data Online Connector
===================================

Fetches weather data for temperature normalization of energy demand.
Cooling Degree Days (CDD) and Heating Degree Days (HDD) are essential
for isolating data center load from weather-driven demand.

API: https://www.ncdc.noaa.gov/cdo-web/api/v2/
Token: Free from https://www.ncdc.noaa.gov/cdo-web/token
"""

import logging
from typing import Any, Dict, List, Optional

import requests

from ai_dc_energy.utils.constants import NOAAEndpoints

logger = logging.getLogger(__name__)


class NOAAConnector:
    """
    Connector for NOAA Climate Data Online API.

    Provides temperature, CDD, and HDD data for normalizing energy
    consumption and isolating data center load effects.
    """

    def __init__(self, api_token: str, timeout: int = 30):
        self.base_url = NOAAEndpoints.BASE_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "token": api_token,
            "Accept": "application/json",
        })

    def get_daily_temps(
        self,
        station_ids: List[str],
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Fetch daily temperature data for weather stations.

        Args:
            station_ids: NOAA station IDs near data center regions
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Daily temperature records (TAVG, TMAX, TMIN, CDD, HDD)
        """
        all_records = []
        datatypes = "TAVG,TMAX,TMIN,CDSD,HDSD"  # CDD/HDD season-to-date

        for station_id in station_ids:
            params = {
                "datasetid": "GHCND",
                "stationid": station_id,
                "startdate": start_date,
                "enddate": end_date,
                "datatypeid": datatypes,
                "units": "standard",
                "limit": 1000,
            }

            try:
                response = self.session.get(
                    f"{self.base_url}/data",
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                all_records.extend(results)
                logger.info(
                    f"Fetched {len(results)} weather records for {station_id}"
                )
            except requests.exceptions.RequestException as e:
                logger.error(f"NOAA request failed for {station_id}: {e}")
                continue

        return all_records

    def find_stations_near(
        self,
        latitude: float,
        longitude: float,
        radius_km: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Find NOAA weather stations near a geographic point.

        Used to identify the best weather station for each data center region.

        Args:
            latitude: Center point latitude
            longitude: Center point longitude
            radius_km: Search radius in kilometers

        Returns:
            List of station records with IDs, names, coordinates
        """
        extent = self._bbox_from_point(latitude, longitude, radius_km)
        params = {
            "datasetid": "GHCND",
            "extent": extent,
            "limit": 25,
        }

        try:
            response = self.session.get(
                f"{self.base_url}/stations",
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json().get("results", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"NOAA station search failed: {e}")
            return []

    @staticmethod
    def _bbox_from_point(lat: float, lon: float, radius_km: int) -> str:
        """Create bounding box string from center point and radius."""
        # Approximate: 1 degree latitude ≈ 111 km
        delta_lat = radius_km / 111.0
        delta_lon = radius_km / (111.0 * abs(max(0.01, __import__("math").cos(__import__("math").radians(lat)))))
        return f"{lat - delta_lat},{lon - delta_lon},{lat + delta_lat},{lon + delta_lon}"


# Recommended NOAA stations for top DC markets
DC_MARKET_WEATHER_STATIONS = {
    "nova": ["GHCND:USW00013743"],     # Washington Dulles (near Ashburn)
    "dfw": ["GHCND:USW00003927"],      # Dallas/Fort Worth Airport
    "phx": ["GHCND:USW00023183"],      # Phoenix Sky Harbor
    "chi": ["GHCND:USW00094846"],      # Chicago O'Hare
    "atl": ["GHCND:USW00013874"],      # Atlanta Hartsfield
    "coh": ["GHCND:USW00014821"],      # Columbus OH
    "sjc": ["GHCND:USW00023293"],      # San Jose Airport
    "pdx": ["GHCND:USW00024229"],      # Portland Airport
    "rno": ["GHCND:USW00024128"],      # Reno Airport
    "phl": ["GHCND:USW00014737"],      # Harrisburg PA
}
