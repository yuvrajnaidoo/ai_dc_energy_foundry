"""
Ontology Object Type Definitions
=================================

Defines the Foundry Ontology object types, link types, and action types
for the AI Data Center Energy Correlation engine.

In Foundry, these are registered via the Ontology Manager UI or
through the Ontology SDK. This file serves as the canonical reference
for the data model and can be used to programmatically register objects.

The Ontology maps business concepts to data, enabling semantic queries:
  "Show me the energy impact in Loudoun County since Microsoft's
   data center came online."
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


# =============================================================================
# OBJECT TYPE: EnergyRegion
# =============================================================================

@dataclass
class EnergyRegionObjectType:
    """
    Represents an electric grid region (balancing authority area).

    In Foundry Ontology Manager:
    - Object Type ID: energy-region
    - Primary Key: region_id
    - Backing Dataset: /datasets/enriched/energy_weather_joined (aggregated)

    Example regions:
    - PJM (covers Northern Virginia DC cluster)
    - ERCO (covers Texas DC market)
    - CISO (covers California DC market)
    """
    object_type_id: str = "energy-region"
    display_name: str = "Energy Region"
    description: str = "Electric grid balancing authority region"
    primary_key: str = "region_id"
    title_property: str = "region_name"

    properties: Dict[str, dict] = field(default_factory=lambda: {
        "region_id": {"type": "string", "description": "Balancing authority code (e.g., PJM, ERCO)"},
        "region_name": {"type": "string", "description": "Full name of the balancing authority"},
        "states_covered": {"type": "string", "description": "Comma-separated state codes"},
        "population": {"type": "long", "description": "Total population in region"},
        "area_sq_miles": {"type": "double", "description": "Geographic area in square miles"},
        "baseline_demand_mw": {"type": "double", "description": "Pre-AI-DC average demand (MW)"},
        "current_demand_mw": {"type": "double", "description": "Latest average demand (MW)"},
        "demand_delta_pct": {"type": "double", "description": "Change from baseline (%)"},
        "total_dc_capacity_mw": {"type": "double", "description": "Total DC capacity in region (MW)"},
        "dc_count": {"type": "integer", "description": "Number of operational data centers"},
    })


# =============================================================================
# OBJECT TYPE: DataCenter
# =============================================================================

@dataclass
class DataCenterObjectType:
    """
    Represents an individual data center facility.

    In Foundry Ontology Manager:
    - Object Type ID: data-center
    - Primary Key: dc_id
    - Backing Dataset: /datasets/enriched/data_centers_with_regions
    """
    object_type_id: str = "data-center"
    display_name: str = "Data Center"
    description: str = "AI/cloud data center facility"
    primary_key: str = "dc_id"
    title_property: str = "name"

    properties: Dict[str, dict] = field(default_factory=lambda: {
        "dc_id": {"type": "string", "description": "Unique data center identifier"},
        "name": {"type": "string", "description": "Facility name"},
        "owner": {"type": "string", "description": "Owning company"},
        "operator": {"type": "string", "description": "Operating company"},
        "latitude": {"type": "double", "description": "Latitude coordinate"},
        "longitude": {"type": "double", "description": "Longitude coordinate"},
        "state": {"type": "string", "description": "U.S. state code"},
        "county_fips": {"type": "string", "description": "County FIPS code"},
        "capacity_mw": {"type": "double", "description": "Total power capacity (MW)"},
        "it_load_mw": {"type": "double", "description": "IT load capacity (MW)"},
        "pue": {"type": "double", "description": "Power Usage Effectiveness ratio"},
        "ai_focused": {"type": "boolean", "description": "Whether facility is AI-focused"},
        "gpu_count": {"type": "long", "description": "Number of GPUs/accelerators"},
        "gpu_type": {"type": "string", "description": "GPU model (e.g., NVIDIA H100)"},
        "operational_date": {"type": "date", "description": "Date facility went operational"},
        "construction_status": {"type": "string", "description": "operational/under_construction/planned"},
        "renewable_pct": {"type": "double", "description": "Percentage of energy from renewables"},
        "region_id": {"type": "string", "description": "Linked energy region ID"},
    })


# =============================================================================
# OBJECT TYPE: EnergyReading
# =============================================================================

@dataclass
class EnergyReadingObjectType:
    """
    Time-series energy consumption reading for a region.

    In Foundry Ontology Manager:
    - Object Type ID: energy-reading
    - Primary Key: reading_id
    - Backing Dataset: /datasets/streaming/energy_demand_live
    - Time Series Property: timestamp → demand_mw
    """
    object_type_id: str = "energy-reading"
    display_name: str = "Energy Reading"
    description: str = "Hourly energy demand measurement"
    primary_key: str = "reading_id"
    title_property: str = "reading_id"

    properties: Dict[str, dict] = field(default_factory=lambda: {
        "reading_id": {"type": "string"},
        "region_id": {"type": "string"},
        "timestamp": {"type": "timestamp"},
        "demand_mw": {"type": "double", "description": "Demand in megawatts"},
        "temperature_f": {"type": "double"},
        "price_per_mwh": {"type": "double"},
        "carbon_intensity_gco2_kwh": {"type": "double"},
    })

    # Time series configuration for Quiver
    time_series: dict = field(default_factory=lambda: {
        "timestamp_property": "timestamp",
        "value_properties": ["demand_mw", "temperature_f", "price_per_mwh"],
    })


# =============================================================================
# OBJECT TYPE: CorrelationEvent
# =============================================================================

@dataclass
class CorrelationEventObjectType:
    """
    Computed correlation between a data center coming online
    and the change in regional energy consumption.

    In Foundry Ontology Manager:
    - Object Type ID: correlation-event
    - Primary Key: event_id
    - Backing Dataset: /datasets/analytics/correlation_events
    """
    object_type_id: str = "correlation-event"
    display_name: str = "Correlation Event"
    description: str = "Before/after energy correlation for a DC deployment"
    primary_key: str = "event_id"
    title_property: str = "event_id"

    properties: Dict[str, dict] = field(default_factory=lambda: {
        "event_id": {"type": "string"},
        "dc_id": {"type": "string"},
        "dc_name": {"type": "string"},
        "region_id": {"type": "string"},
        "analysis_date": {"type": "date"},
        "pre_period_avg_demand_mw": {"type": "double"},
        "post_period_avg_demand_mw": {"type": "double"},
        "delta_demand_mw": {"type": "double"},
        "delta_demand_pct": {"type": "double"},
        "pearson_correlation": {"type": "double"},
        "confidence_score": {"type": "double"},
        "adjusted_delta_pct": {"type": "double", "description": "Delta after control region adjustment"},
        "methodology": {"type": "string"},
    })


# =============================================================================
# LINK TYPES
# =============================================================================

LINK_TYPES = [
    {
        "link_type_id": "data-center-in-region",
        "display_name": "Located In",
        "description": "Links a data center to its energy region",
        "source_object_type": "data-center",
        "target_object_type": "energy-region",
        "source_property": "region_id",
        "target_property": "region_id",
        "cardinality": "many-to-one",
    },
    {
        "link_type_id": "reading-for-region",
        "display_name": "Reading For",
        "description": "Links energy readings to their region",
        "source_object_type": "energy-reading",
        "target_object_type": "energy-region",
        "source_property": "region_id",
        "target_property": "region_id",
        "cardinality": "many-to-one",
    },
    {
        "link_type_id": "correlation-for-dc",
        "display_name": "Correlation For",
        "description": "Links correlation events to the triggering DC",
        "source_object_type": "correlation-event",
        "target_object_type": "data-center",
        "source_property": "dc_id",
        "target_property": "dc_id",
        "cardinality": "many-to-one",
    },
    {
        "link_type_id": "correlation-in-region",
        "display_name": "In Region",
        "description": "Links correlation events to their energy region",
        "source_object_type": "correlation-event",
        "target_object_type": "energy-region",
        "source_property": "region_id",
        "target_property": "region_id",
        "cardinality": "many-to-one",
    },
]


# =============================================================================
# ACTION TYPES (for Workshop interactive applications)
# =============================================================================

ACTION_TYPES = [
    {
        "action_type_id": "run-correlation-analysis",
        "display_name": "Run Correlation Analysis",
        "description": "Trigger a new before/after analysis for a data center",
        "parameters": [
            {"name": "dc_id", "type": "string", "required": True},
            {"name": "analysis_window_months", "type": "integer", "default": 12},
        ],
    },
    {
        "action_type_id": "flag-anomaly",
        "display_name": "Flag Demand Anomaly",
        "description": "Flag an energy demand anomaly for investigation",
        "parameters": [
            {"name": "region_id", "type": "string", "required": True},
            {"name": "timestamp", "type": "timestamp", "required": True},
            {"name": "notes", "type": "string"},
        ],
    },
]
