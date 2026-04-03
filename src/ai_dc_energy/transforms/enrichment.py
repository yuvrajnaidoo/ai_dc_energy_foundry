"""
Layer 2: Data Enrichment & Transformation Transforms
=====================================================

Transforms that join, normalize, and enrich raw datasets to produce
analysis-ready data. This layer:
- Normalizes energy units across sources
- Joins energy data with data center locations by region
- Applies weather normalization (CDD/HDD adjustment)
- Computes derived metrics (carbon intensity, per-capita impact)
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType

from ai_dc_energy.utils.constants import (
    EnergyUnits,
    DC_MARKET_TO_BA,
    TOP_DC_STATES,
)

from transforms.api import transform, Input, Output


# =============================================================================
# TRANSFORM: Normalize Energy Readings
# =============================================================================

@transform(
    raw_rto=Input("/datasets/raw/eia_rto_demand"),
    output=Output("/datasets/enriched/energy_readings_normalized"),
)
def normalize_energy_readings(raw_rto, output):
    """
    Normalize raw EIA RTO data into standardized energy readings.

    Transformations:
    1. Parse period strings to proper timestamps
    2. Convert units to consistent MW/MWh
    3. Assign region IDs based on balancing authority
    4. Generate unique reading IDs

    Args:
        raw_rto: Foundry input (raw EIA RTO demand)
        output: Foundry output dataset handle

    Returns:
        Normalized energy readings DataFrame
    """
    df = raw_rto.dataframe()

    # Parse EIA period format (e.g., "2024-01-15T14") to timestamp
    df = df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("period"), "yyyy-MM-dd'T'HH")
    )

    # Map respondent (BA) to region_id
    df = df.withColumn(
        "region_id",
        F.lower(F.col("respondent"))
    )

    # Rename value to demand_mw (EIA RTO reports in megawatthours for hourly)
    df = df.withColumn(
        "demand_mw",
        F.col("value")  # EIA hourly is effectively MW (MWh per hour = MW)
    )

    # Generate unique reading ID
    df = df.withColumn(
        "reading_id",
        F.concat_ws(
            "_",
            F.lit("eia"),
            F.col("region_id"),
            F.date_format(F.col("timestamp"), "yyyyMMddHH"),
        )
    )

    # Select standardized columns
    df = df.select(
        "reading_id",
        "region_id",
        F.col("respondent").alias("balancing_authority"),
        "timestamp",
        "demand_mw",
        F.lit(None).cast(DoubleType()).alias("generation_mw"),
        F.lit(None).cast(DoubleType()).alias("temperature_f"),
        F.lit(None).cast(DoubleType()).alias("cooling_degree_days"),
        F.lit(None).cast(DoubleType()).alias("price_per_mwh"),
        F.lit(None).cast(StringType()).alias("source_mix"),
        F.lit(None).cast(DoubleType()).alias("carbon_intensity_gco2_kwh"),
        F.lit("eia_rto").alias("data_source"),
        F.col("ingestion_timestamp"),
    )

    output.write_dataframe(df)


# =============================================================================
# TRANSFORM: Enrich Data Centers with Region Mapping
# =============================================================================

@transform(
    raw_dcs=Input("/datasets/raw/epoch_ai_data_centers"),
    output=Output("/datasets/enriched/data_centers_with_regions"),
)
def enrich_data_centers_with_regions(raw_dcs, output):
    """
    Enrich data center records with energy region mappings.

    Maps each data center to its corresponding EIA balancing authority
    based on state and geographic location, enabling the join between
    DC capacity and energy consumption data.

    Args:
        raw_dcs: Foundry input (raw data center records from Epoch AI)
        output: Foundry output dataset handle

    Returns:
        Data centers with region_id and balancing_authority columns added
    """
    raw_dcs_df = raw_dcs.dataframe()

    # Build state-to-BA mapping from constants
    state_ba_mapping = []
    for market, bas in DC_MARKET_TO_BA.items():
        for ba in bas:
            state_ba_mapping.append((market, ba))

    # For now, use a simplified state-level mapping
    # In production, use geographic point-in-polygon with BA boundary shapefiles
    state_to_primary_ba = {
        "VA": "PJM", "MD": "PJM", "PA": "PJM", "OH": "PJM",
        "NJ": "PJM", "DE": "PJM", "WV": "PJM", "NC": "PJM",
        "TX": "ERCO",
        "CA": "CISO",
        "GA": "SOCO",
        "IL": "PJM",  # Northern IL is in PJM; southern is MISO
        "OR": "BPAT",
        "WA": "BPAT",
        "NV": "NEVP",
        "AZ": "SRP",
        "CO": "PSCO",
        "UT": "PACE",
        "TN": "TVA",
        "SC": "SCEG",
    }

    # Create broadcast mapping
    spark = raw_dcs_df.sparkSession

    mapping_rows = [(k, v) for k, v in state_to_primary_ba.items()]
    mapping_df = spark.createDataFrame(mapping_rows, ["state_code", "balancing_authority"])

    # Join DCs with BA mapping
    df = raw_dcs_df.join(
        mapping_df,
        raw_dcs_df["state"] == mapping_df["state_code"],
        how="left"
    ).drop("state_code")

    # Create region_id from BA
    df = df.withColumn(
        "region_id",
        F.lower(F.coalesce(F.col("balancing_authority"), F.lit("unknown")))
    )

    output.write_dataframe(df)


# =============================================================================
# TRANSFORM: Join Energy Readings with Weather Data
# =============================================================================

@transform(
    energy=Input("/datasets/enriched/energy_readings_normalized"),
    output=Output("/datasets/enriched/energy_weather_joined"),
)
def pass_energy_readings(energy, output):
    """
    Pass-through for energy readings.

    Weather normalization will be added when NOAA weather data is
    available. For now, this passes normalized energy readings through
    with null temperature/CDD fields preserved.
    """
    output.write_dataframe(energy.dataframe())


# =============================================================================
# TRANSFORM: Compute Regional DC Capacity Timeline
# =============================================================================

@transform(
    dcs=Input("/datasets/enriched/data_centers_with_regions"),
    output=Output("/datasets/enriched/regional_dc_capacity_timeline"),
)
def compute_regional_dc_capacity_timeline(dcs, output):
    """
    Compute cumulative data center capacity over time per region.

    For each region, builds a timeline of when DC capacity came online,
    enabling before/after analysis at any point in time.

    Output columns:
    - region_id
    - date (monthly granularity)
    - cumulative_capacity_mw (total DC MW online by that date)
    - cumulative_ai_capacity_mw (AI-focused DC MW)
    - dc_count (number of operational DCs)
    - total_gpu_count
    - latest_dc_added (name of most recent DC to come online)

    Args:
        dcs: Foundry input (enriched data centers with regions)
        output: Foundry output dataset handle

    Returns:
        Monthly timeline of DC capacity per region
    """
    dcs_df = dcs.dataframe()

    # Filter to operational DCs with known dates
    operational = dcs_df.filter(
        (F.col("construction_status") == "operational")
        & F.col("operational_date").isNotNull()
        & F.col("capacity_mw").isNotNull()
    )

    # Compute cumulative capacity using window function
    from pyspark.sql.window import Window

    # Create monthly date from operational_date
    operational = operational.withColumn(
        "online_month",
        F.date_trunc("month", F.col("operational_date"))
    )

    # Window for cumulative sum per region, ordered by online date
    region_window = (
        Window
        .partitionBy("region_id")
        .orderBy("online_month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    timeline = operational.groupBy("region_id", "online_month").agg(
        F.sum("capacity_mw").alias("period_capacity_mw"),
        F.sum(
            F.when(F.col("ai_focused") == True, F.col("capacity_mw")).otherwise(0)
        ).alias("period_ai_capacity_mw"),
        F.count("*").alias("period_dc_count"),
        F.sum(F.coalesce(F.col("gpu_count"), F.lit(0))).alias("period_gpu_count"),
        F.max("name").alias("latest_dc_added"),
    )

    # Compute cumulative sums
    cum_window = (
        Window
        .partitionBy("region_id")
        .orderBy("online_month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    timeline = timeline.withColumn(
        "cumulative_capacity_mw",
        F.sum("period_capacity_mw").over(cum_window)
    ).withColumn(
        "cumulative_ai_capacity_mw",
        F.sum("period_ai_capacity_mw").over(cum_window)
    ).withColumn(
        "cumulative_dc_count",
        F.sum("period_dc_count").over(cum_window)
    ).withColumn(
        "cumulative_gpu_count",
        F.sum("period_gpu_count").over(cum_window)
    )

    output.write_dataframe(timeline.select(
        "region_id",
        F.col("online_month").alias("date"),
        "cumulative_capacity_mw",
        "cumulative_ai_capacity_mw",
        "cumulative_dc_count",
        "cumulative_gpu_count",
        "latest_dc_added",
    ))
