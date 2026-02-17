"""
Constants and configuration for the AI DC Energy correlation engine.

All threshold values, API endpoints, region definitions, and schema
constants are centralized here for easy configuration.
"""

from enum import Enum
from typing import Dict, List


# =============================================================================
# API ENDPOINTS
# =============================================================================

class EIAEndpoints:
    """U.S. Energy Information Administration API v2 endpoints."""
    BASE_URL = "https://api.eia.gov/v2"
    RTO_SUBREGION = "/electricity/rto/region-sub-ba-data/data/"
    RTO_REGION = "/electricity/rto/region-data/data/"
    RTO_FUEL_TYPE = "/electricity/rto/fuel-type-data/data/"
    RETAIL_SALES = "/electricity/retail-sales/data/"
    STATE_PROFILES = "/electricity/state-electricity-profiles/"
    FACILITY_FUEL = "/electricity/facility-fuel/data/"
    OPERATING_GENERATORS = "/electricity/operating-generator-capacity/data/"


class EpochAIEndpoints:
    """Epoch AI Frontier Data Centers database endpoints."""
    BASE_URL = "https://epoch.ai"
    DATA_CENTERS = "/data/data-centers"
    # The dataset is available as downloadable CSV/JSON
    DOWNLOAD_URL = "https://epoch.ai/data/data-centers"


class PJMEndpoints:
    """PJM Interconnection Data Miner 2 API endpoints."""
    BASE_URL = "https://api.pjm.com/api/v1"
    SETTINGS_URL = "https://dataminer2.pjm.com/config/settings.json"
    RT_LMP = "/rt_hrl_lmps"           # Real-time hourly LMPs
    DA_LMP = "/da_hrl_lmps"           # Day-ahead hourly LMPs
    LOAD_FORECAST = "/load_frcstd_7_day"
    LOAD_METERED = "/hrl_load_metered"  # Hourly metered load by zone
    LOAD_PRELIM = "/hrl_load_prelim"    # Hourly preliminary load
    GEN_BY_FUEL = "/gen_by_fuel"
    INST_LOAD = "/inst_load"           # Instantaneous load
    CAPACITY_MARKET = "/rpm_auction_results"


class NOAAEndpoints:
    """NOAA Climate Data Online API endpoints."""
    BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
    STATIONS = "/stations"
    DATA = "/data"
    DATASETS = "/datasets"


# =============================================================================
# TOP DATA CENTER MARKETS (Regions of Focus)
# =============================================================================

class DCMarket(Enum):
    """Top U.S. data center markets by installed capacity."""
    NORTHERN_VIRGINIA = "nova"
    DALLAS_FORT_WORTH = "dfw"
    PHOENIX_ARIZONA = "phx"
    CHICAGO_ILLINOIS = "chi"
    ATLANTA_GEORGIA = "atl"
    COLUMBUS_OHIO = "coh"
    SILICON_VALLEY = "sjc"
    PORTLAND_OREGON = "pdx"
    RENO_NEVADA = "rno"
    CENTRAL_PENNSYLVANIA = "phl"


# Mapping of DC markets to EIA balancing authority codes
DC_MARKET_TO_BA: Dict[str, List[str]] = {
    "nova": ["PJM"],           # PJM Interconnection covers NoVA
    "dfw": ["ERCO"],           # ERCOT covers Texas
    "phx": ["SRP", "AZPS"],   # Salt River Project / AZ Public Service
    "chi": ["PJM", "MISO"],   # PJM and MISO cover Illinois
    "atl": ["SOCO"],           # Southern Company covers Georgia
    "coh": ["PJM"],            # PJM covers Ohio
    "sjc": ["CISO"],           # CAISO covers California
    "pdx": ["BPAT"],           # Bonneville Power covers Oregon
    "rno": ["NEVP"],           # NV Energy covers Nevada
    "phl": ["PJM"],            # PJM covers Pennsylvania
}

# State FIPS codes for top DC states
TOP_DC_STATES: Dict[str, str] = {
    "VA": "51",  # Virginia (663 operational DCs)
    "TX": "48",  # Texas (405 operational)
    "CA": "06",  # California
    "IL": "17",  # Illinois
    "GA": "13",  # Georgia (162 operational, 285 planned)
    "OH": "39",  # Ohio
    "PA": "42",  # Pennsylvania
    "AZ": "04",  # Arizona
    "OR": "41",  # Oregon
    "NV": "32",  # Nevada
}


# =============================================================================
# ENERGY UNIT CONVERSIONS
# =============================================================================

class EnergyUnits:
    """Conversion factors for energy units."""
    MWH_TO_GWH = 1 / 1000
    MWH_TO_TWH = 1 / 1_000_000
    GWH_TO_TWH = 1 / 1000
    KWH_TO_MWH = 1 / 1000
    MW_TO_GW = 1 / 1000
    # Approximate annual MWh from MW capacity (8760 hours * capacity factor)
    MW_TO_ANNUAL_MWH_DC = 8760 * 0.85  # Data centers run ~85% avg utilization


# =============================================================================
# CORRELATION ENGINE PARAMETERS
# =============================================================================

class CorrelationConfig:
    """Configuration for the correlation computation engine."""
    # Analysis windows
    BASELINE_MONTHS = 36         # Months of pre-DC data for baseline
    MIN_POST_MONTHS = 3          # Minimum months after DC online for analysis
    DEFAULT_ANALYSIS_WINDOW = 12 # Default comparison window in months

    # Statistical thresholds
    MIN_CONFIDENCE_SCORE = 0.5   # Minimum confidence to report correlation
    SIGNIFICANCE_LEVEL = 0.05    # p-value threshold for statistical significance
    MIN_DATA_COMPLETENESS = 0.8  # 80% data completeness required

    # Anomaly detection
    ANOMALY_THRESHOLD_PCT = 10.0  # Alert if demand deviates >10% from baseline
    SMOOTHING_WINDOW_HOURS = 168  # 7-day rolling average for smoothing

    # Control region matching
    MAX_POPULATION_DIFF_PCT = 30  # Max population difference for control region
    MAX_CLIMATE_ZONE_DIFF = 1     # Max ASHRAE climate zone difference


# =============================================================================
# DATA SCHEMAS
# =============================================================================

# Column names for standardized energy readings
ENERGY_READING_SCHEMA = {
    "reading_id": "string",
    "region_id": "string",
    "balancing_authority": "string",
    "timestamp": "timestamp",
    "demand_mw": "double",
    "generation_mw": "double",
    "net_interchange_mw": "double",
    "temperature_f": "double",
    "cooling_degree_days": "double",
    "price_per_mwh": "double",
    "source_mix": "string",       # JSON: {"natural_gas": 0.4, "nuclear": 0.2, ...}
    "carbon_intensity_gco2_kwh": "double",
    "data_source": "string",
    "ingestion_timestamp": "timestamp",
}

# Column names for data center records
DATA_CENTER_SCHEMA = {
    "dc_id": "string",
    "name": "string",
    "owner": "string",
    "operator": "string",
    "latitude": "double",
    "longitude": "double",
    "county_fips": "string",
    "state": "string",
    "capacity_mw": "double",
    "it_load_mw": "double",
    "pue": "double",             # Power Usage Effectiveness
    "ai_focused": "boolean",
    "gpu_count": "long",
    "gpu_type": "string",
    "operational_date": "date",
    "construction_start_date": "date",
    "construction_status": "string",  # operational, under_construction, planned
    "water_usage_gallons_annual": "long",
    "renewable_pct": "double",
    "data_source": "string",
    "last_updated": "timestamp",
}

# Column names for correlation events
CORRELATION_EVENT_SCHEMA = {
    "event_id": "string",
    "dc_id": "string",
    "region_id": "string",
    "analysis_date": "date",
    "pre_period_start": "date",
    "pre_period_end": "date",
    "post_period_start": "date",
    "post_period_end": "date",
    "pre_period_avg_demand_mw": "double",
    "post_period_avg_demand_mw": "double",
    "delta_demand_mw": "double",
    "delta_demand_pct": "double",
    "pearson_correlation": "double",
    "spearman_correlation": "double",
    "p_value": "double",
    "confidence_score": "double",
    "control_region_id": "string",
    "control_delta_pct": "double",
    "adjusted_delta_pct": "double",  # DC delta minus control delta
    "analysis_window_months": "integer",
    "data_completeness": "double",
    "methodology": "string",
}


# =============================================================================
# STREAMING CONFIGURATION
# =============================================================================

class StreamingConfig:
    """Configuration for Foundry streaming pipelines."""
    # EIA RTO data polling interval (the API updates hourly)
    EIA_POLL_INTERVAL_SECONDS = 300   # Poll every 5 minutes for new hourly data
    # PJM real-time data interval
    PJM_POLL_INTERVAL_SECONDS = 60    # Poll every minute for 5-min LMP data
    # Maximum row size for streaming (Foundry limit is 1MB)
    MAX_ROW_SIZE_BYTES = 512_000      # 512KB safety margin
    # Checkpoint interval for streaming state
    CHECKPOINT_INTERVAL_MS = 60_000   # 1 minute
    # Late data tolerance
    WATERMARK_DELAY_MINUTES = 15      # Allow 15 minutes of late data
