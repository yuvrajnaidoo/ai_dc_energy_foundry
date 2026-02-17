"""
Geographic Utilities
====================

Functions for mapping data centers to energy regions using
geographic coordinates and FIPS code lookups.
"""

import math
from typing import Optional, Tuple


def haversine_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """
    Compute the great-circle distance between two points on Earth.

    Used for finding the nearest weather station to a data center
    and for validating region assignments.

    Args:
        lat1, lon1: First point coordinates (degrees)
        lat2, lon2: Second point coordinates (degrees)

    Returns:
        Distance in kilometers
    """
    R = 6371.0  # Earth radius in km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def state_fips_to_code(fips: str) -> Optional[str]:
    """Convert a 2-digit state FIPS code to a state abbreviation."""
    fips_map = {
        "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
        "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
        "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
        "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
        "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
        "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
        "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
        "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
        "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
        "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
        "56": "WY",
    }
    return fips_map.get(fips)


def county_fips_from_coords(
    latitude: float, longitude: float
) -> Optional[str]:
    """
    Determine county FIPS code from coordinates.

    In production, this would use a shapefile lookup (e.g., via
    the Census Bureau TIGER/Line files loaded into Foundry).
    For the initial deployment, use the FCC Area API as a fallback.

    Args:
        latitude: Latitude of the point
        longitude: Longitude of the point

    Returns:
        5-digit county FIPS code, or None if lookup fails
    """
    # Placeholder: In Foundry, load Census TIGER shapefiles and
    # use GeoPandas or Sedona for point-in-polygon lookup.
    # The FCC API can be used as a lightweight alternative:
    # https://geo.fcc.gov/api/census/area?lat={lat}&lon={lon}&format=json

    import requests

    try:
        url = f"https://geo.fcc.gov/api/census/area?lat={latitude}&lon={longitude}&format=json"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            if results:
                return results[0].get("county_fips")
    except Exception:
        pass

    return None
