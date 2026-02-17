"""
Epoch AI Frontier Data Centers Connector
=========================================

Connects to the Epoch AI open database of AI data centers.
This is the largest publicly available database tracking construction
timelines of major AI data centers through satellite imagery, permits,
and public documents.

Data license: Creative Commons Attribution (CC-BY)
Source: https://epoch.ai/data/data-centers
"""

import json
import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class EpochAIConnector:
    """
    Connector for the Epoch AI Frontier Data Centers database.

    The database tracks:
    - Location, owner, and users of major AI data centers
    - Total power capacity and compute capacity timelines
    - Capital cost estimates
    - Construction timelines via satellite imagery
    - GPU counts and types where available

    Coverage: ~15% of global AI compute delivered by chip manufacturers
    as of late 2025, primarily U.S. facilities.

    Usage in Foundry:
        This data is available as open data under CC-BY license.
        Can be ingested via:
        1. Direct API/CSV download into Foundry Data Connection
        2. Periodic batch sync from the Epoch AI website
        3. This connector class for programmatic access
    """

    BASE_URL = "https://epoch.ai"
    DATA_CENTERS_PATH = "/data/data-centers"

    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "EOXVantage-Foundry-Connector/1.0",
        })

    def fetch_all_data_centers(self) -> List[Dict[str, Any]]:
        """
        Fetch the complete data center database from Epoch AI.

        Returns a list of data center records, each containing:
        - name: Facility name
        - owner: Company that owns the facility
        - users: Companies using the facility (may differ from owner)
        - location: City, state, country
        - latitude, longitude: Coordinates
        - power_capacity_mw: Total facility power capacity
        - compute_capacity: GPU/accelerator counts if known
        - construction_start: Date construction began
        - operational_date: Date facility became operational
        - capital_cost_usd: Estimated total investment
        - status: operational, under_construction, planned, expanding

        Note: The actual download mechanism may vary. Epoch AI provides
        data as downloadable files. In Foundry, this would typically be
        configured as a file-based Data Connection with periodic sync.
        """
        try:
            # Attempt to fetch structured data
            # Note: The actual endpoint/format may require adaptation
            # based on Epoch AI's current data distribution method
            url = f"{self.BASE_URL}/api/data-centers"
            response = self.session.get(url, timeout=self.timeout)

            if response.status_code == 200:
                return response.json()

            # Fallback: the data may need to be downloaded as CSV/JSON file
            logger.warning(
                "Direct API not available. "
                "Download data manually from epoch.ai/data/data-centers "
                "and upload to Foundry as a file-based data source."
            )
            return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Epoch AI request failed: {e}")
            raise

    @staticmethod
    def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize an Epoch AI record to our standard DataCenter schema.

        Maps Epoch AI field names to the schema defined in constants.py.

        Args:
            raw: Raw record from Epoch AI

        Returns:
            Normalized record matching DATA_CENTER_SCHEMA
        """
        def safe_float(val, default=None):
            if val is None:
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        def safe_int(val, default=None):
            if val is None:
                return default
            try:
                return int(val)
            except (ValueError, TypeError):
                return default

        return {
            "dc_id": f"epoch_{raw.get('id', raw.get('name', 'unknown'))}".replace(" ", "_").lower(),
            "name": raw.get("name", "Unknown"),
            "owner": raw.get("owner", raw.get("company", "Unknown")),
            "operator": raw.get("operator", raw.get("owner", "Unknown")),
            "latitude": safe_float(raw.get("latitude", raw.get("lat"))),
            "longitude": safe_float(raw.get("longitude", raw.get("lon", raw.get("lng")))),
            "county_fips": raw.get("county_fips"),  # May need geo-enrichment
            "state": raw.get("state", ""),
            "capacity_mw": safe_float(raw.get("power_capacity_mw", raw.get("capacity_mw"))),
            "it_load_mw": safe_float(raw.get("it_load_mw")),
            "pue": safe_float(raw.get("pue"), default=1.3),  # Default industry avg
            "ai_focused": _classify_ai_focused(raw),
            "gpu_count": safe_int(raw.get("gpu_count", raw.get("accelerator_count"))),
            "gpu_type": raw.get("gpu_type", raw.get("accelerator_type", "")),
            "operational_date": raw.get("operational_date", raw.get("online_date")),
            "construction_start_date": raw.get("construction_start", raw.get("groundbreaking")),
            "construction_status": _normalize_status(raw.get("status", "unknown")),
            "water_usage_gallons_annual": safe_int(raw.get("water_usage")),
            "renewable_pct": safe_float(raw.get("renewable_pct")),
            "data_source": "epoch_ai",
            "last_updated": raw.get("last_updated"),
        }


def _classify_ai_focused(record: Dict) -> bool:
    """
    Classify whether a data center is AI-focused based on available signals.

    Heuristics:
    - Has GPU/accelerator information → AI-focused
    - Owner is known AI company → AI-focused
    - Name contains AI-related keywords → AI-focused
    """
    ai_companies = {
        "openai", "anthropic", "google", "deepmind", "meta", "microsoft",
        "xai", "nvidia", "amazon", "aws", "oracle", "coreweave",
        "lambda", "together", "cerebras", "inflection",
    }

    # Check GPU presence
    if record.get("gpu_count") or record.get("accelerator_count"):
        return True

    # Check owner
    owner = (record.get("owner", "") or "").lower()
    if any(company in owner for company in ai_companies):
        return True

    # Check name
    name = (record.get("name", "") or "").lower()
    ai_keywords = ["ai", "gpu", "training", "inference", "colossus", "stargate"]
    if any(kw in name for kw in ai_keywords):
        return True

    return False


def _normalize_status(status: str) -> str:
    """Normalize construction status to standard values."""
    status_lower = (status or "").lower().strip()
    status_map = {
        "operational": "operational",
        "online": "operational",
        "active": "operational",
        "under construction": "under_construction",
        "construction": "under_construction",
        "building": "under_construction",
        "planned": "planned",
        "announced": "planned",
        "proposed": "planned",
        "expanding": "expanding",
        "expansion": "expanding",
    }
    return status_map.get(status_lower, "unknown")
