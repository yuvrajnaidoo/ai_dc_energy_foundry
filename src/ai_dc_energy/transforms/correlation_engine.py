"""
Layer 3: Correlation Engine
============================

The core analytics module that computes the relationship between
data center deployments and energy consumption changes.

Methodology: Difference-in-Differences (DiD) with control region matching.

For each region where a new data center came online:
1. Establish a baseline using pre-DC energy consumption
2. Compute post-DC energy consumption
3. Compare the delta against a control region (similar demographics,
   no DC activity) to isolate the DC effect
4. Compute correlation coefficients and confidence scores
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, IntegerType,
)

from ai_dc_energy.utils.constants import CorrelationConfig

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# Foundry transform imports (uncomment in Foundry):
# from transforms.api import transform, Input, Output
# -----------------------------------------------------------------------

CORRELATION_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("dc_id", StringType(), True),
    StructField("dc_name", StringType(), True),
    StructField("region_id", StringType(), True),
    StructField("analysis_date", DateType(), True),
    StructField("pre_period_start", DateType(), True),
    StructField("pre_period_end", DateType(), True),
    StructField("post_period_start", DateType(), True),
    StructField("post_period_end", DateType(), True),
    StructField("pre_period_avg_demand_mw", DoubleType(), True),
    StructField("post_period_avg_demand_mw", DoubleType(), True),
    StructField("delta_demand_mw", DoubleType(), True),
    StructField("delta_demand_pct", DoubleType(), True),
    StructField("pearson_correlation", DoubleType(), True),
    StructField("spearman_correlation", DoubleType(), True),
    StructField("p_value", DoubleType(), True),
    StructField("confidence_score", DoubleType(), True),
    StructField("control_region_id", StringType(), True),
    StructField("control_delta_pct", DoubleType(), True),
    StructField("adjusted_delta_pct", DoubleType(), True),
    StructField("analysis_window_months", IntegerType(), True),
    StructField("data_completeness", DoubleType(), True),
    StructField("methodology", StringType(), True),
])


# =============================================================================
# MAIN CORRELATION TRANSFORM
# =============================================================================

# @transform(
#     energy_readings=Input("/datasets/enriched/energy_weather_joined"),
#     dc_timeline=Input("/datasets/enriched/regional_dc_capacity_timeline"),
#     data_centers=Input("/datasets/enriched/data_centers_with_regions"),
#     output=Output("/datasets/analytics/correlation_events"),
# )
def compute_correlation_events(
    energy_readings: DataFrame,
    dc_timeline: DataFrame,
    data_centers: DataFrame,
) -> DataFrame:
    """
    Compute before/after correlation events for each data center deployment.

    For each data center that came online, this transform:
    1. Defines pre-period (BASELINE_MONTHS before operational_date)
    2. Defines post-period (operational_date to now or analysis window)
    3. Computes average demand in both periods
    4. Calculates delta (absolute MW and percentage)
    5. Runs correlation between cumulative DC capacity and demand
    6. Assigns a confidence score

    Args:
        energy_readings: Hourly energy readings with weather adjustment
        dc_timeline: Regional DC capacity timeline
        data_centers: Individual DC records with operational dates

    Returns:
        DataFrame of CorrelationEvent records
    """
    spark = energy_readings.sparkSession
    config = CorrelationConfig

    # Get DCs that have come online (have operational dates)
    online_dcs = data_centers.filter(
        (F.col("construction_status") == "operational")
        & F.col("operational_date").isNotNull()
        & F.col("region_id").isNotNull()
    ).select(
        "dc_id", "name", "region_id", "operational_date", "capacity_mw"
    )

    # Aggregate energy readings to monthly averages per region
    monthly_energy = energy_readings.withColumn(
        "month", F.date_trunc("month", F.col("timestamp"))
    ).groupBy("region_id", "month").agg(
        F.avg("demand_mw").alias("avg_demand_mw"),
        F.count("*").alias("reading_count"),
        F.avg("temperature_f").alias("avg_temp_f"),
    )

    # For each DC, compute the before/after analysis
    # This uses a cross-join approach — in production, consider
    # partitioning or map-side join for performance
    analysis_results = []

    dc_rows = online_dcs.collect()

    for dc_row in dc_rows:
        dc_id = dc_row["dc_id"]
        dc_name = dc_row["name"]
        region_id = dc_row["region_id"]
        op_date = dc_row["operational_date"]
        dc_capacity = dc_row["capacity_mw"]

        if op_date is None or region_id is None:
            continue

        # Filter energy data for this region
        region_energy = monthly_energy.filter(F.col("region_id") == region_id)

        # Define periods
        from dateutil.relativedelta import relativedelta
        pre_start = op_date - relativedelta(months=config.BASELINE_MONTHS)
        pre_end = op_date - relativedelta(months=1)
        post_start = op_date
        # Use analysis window or current date
        post_end_candidate = op_date + relativedelta(
            months=config.DEFAULT_ANALYSIS_WINDOW
        )
        today = datetime.now().date()
        post_end = min(post_end_candidate, today)

        # Check minimum post period
        months_since = (today.year - op_date.year) * 12 + (today.month - op_date.month)
        if months_since < config.MIN_POST_MONTHS:
            continue  # Not enough post-DC data yet

        # Compute pre-period average
        pre_data = region_energy.filter(
            (F.col("month") >= F.lit(pre_start))
            & (F.col("month") <= F.lit(pre_end))
        )
        pre_stats = pre_data.agg(
            F.avg("avg_demand_mw").alias("pre_avg"),
            F.count("*").alias("pre_months"),
        ).collect()[0]

        # Compute post-period average
        post_data = region_energy.filter(
            (F.col("month") >= F.lit(post_start))
            & (F.col("month") <= F.lit(post_end))
        )
        post_stats = post_data.agg(
            F.avg("avg_demand_mw").alias("post_avg"),
            F.count("*").alias("post_months"),
        ).collect()[0]

        pre_avg = pre_stats["pre_avg"]
        post_avg = post_stats["post_avg"]
        pre_months = pre_stats["pre_months"]
        post_months = post_stats["post_months"]

        if pre_avg is None or post_avg is None:
            continue

        # Compute deltas
        delta_mw = post_avg - pre_avg
        delta_pct = (delta_mw / pre_avg * 100) if pre_avg != 0 else None

        # Compute data completeness
        expected_pre = config.BASELINE_MONTHS
        expected_post = min(config.DEFAULT_ANALYSIS_WINDOW, months_since)
        total_expected = expected_pre + expected_post
        total_actual = (pre_months or 0) + (post_months or 0)
        completeness = min(1.0, total_actual / max(1, total_expected))

        # Compute confidence score
        confidence = _compute_confidence_score(
            pre_months=pre_months,
            post_months=post_months,
            data_completeness=completeness,
            dc_capacity_mw=dc_capacity,
            pre_avg_demand=pre_avg,
        )

        # Correlation coefficients would be computed here using
        # scipy.stats.pearsonr and spearmanr on the monthly time series
        # of cumulative DC capacity vs demand. Placeholder values below.
        pearson_r = None  # Computed in production via scipy
        spearman_r = None
        p_value = None

        event = {
            "event_id": f"corr_{dc_id}_{region_id}_{today.isoformat()}",
            "dc_id": dc_id,
            "dc_name": dc_name,
            "region_id": region_id,
            "analysis_date": today,
            "pre_period_start": pre_start,
            "pre_period_end": pre_end,
            "post_period_start": post_start,
            "post_period_end": post_end,
            "pre_period_avg_demand_mw": float(pre_avg),
            "post_period_avg_demand_mw": float(post_avg),
            "delta_demand_mw": float(delta_mw),
            "delta_demand_pct": float(delta_pct) if delta_pct else None,
            "pearson_correlation": pearson_r,
            "spearman_correlation": spearman_r,
            "p_value": p_value,
            "confidence_score": confidence,
            "control_region_id": None,   # Populated by control matching
            "control_delta_pct": None,
            "adjusted_delta_pct": None,
            "analysis_window_months": config.DEFAULT_ANALYSIS_WINDOW,
            "data_completeness": completeness,
            "methodology": "difference_in_differences_v1",
        }
        analysis_results.append(event)

    if not analysis_results:
        return spark.createDataFrame([], CORRELATION_EVENT_SCHEMA)

    return spark.createDataFrame(analysis_results, schema=CORRELATION_EVENT_SCHEMA)


def _compute_confidence_score(
    pre_months: int,
    post_months: int,
    data_completeness: float,
    dc_capacity_mw: float,
    pre_avg_demand: float,
) -> float:
    """
    Compute confidence score (0.0 - 1.0) for a correlation event.

    Factors:
    1. Data completeness (weight: 0.3)
    2. Pre-period length adequacy (weight: 0.2)
    3. Post-period length adequacy (weight: 0.2)
    4. Signal-to-noise ratio: DC capacity vs regional demand (weight: 0.3)
       Higher DC capacity relative to regional demand = clearer signal
    """
    config = CorrelationConfig

    # Factor 1: Data completeness
    completeness_score = min(1.0, data_completeness / config.MIN_DATA_COMPLETENESS)

    # Factor 2: Pre-period adequacy
    pre_score = min(1.0, (pre_months or 0) / config.BASELINE_MONTHS)

    # Factor 3: Post-period adequacy
    post_score = min(1.0, (post_months or 0) / config.DEFAULT_ANALYSIS_WINDOW)

    # Factor 4: Signal-to-noise (DC capacity as % of regional demand)
    if pre_avg_demand and pre_avg_demand > 0 and dc_capacity_mw:
        signal_ratio = dc_capacity_mw / pre_avg_demand
        # Scale: 1% of demand = 0.3 score, 5% = 0.7, 10%+ = 1.0
        snr_score = min(1.0, signal_ratio / 0.10)
    else:
        snr_score = 0.1

    confidence = (
        0.3 * completeness_score
        + 0.2 * pre_score
        + 0.2 * post_score
        + 0.3 * snr_score
    )

    return round(min(1.0, max(0.0, confidence)), 3)


# =============================================================================
# TRANSFORM: Compute Regional Correlation Coefficients
# =============================================================================

# @transform(
#     energy_monthly=Input("/datasets/enriched/energy_readings_monthly"),
#     dc_timeline=Input("/datasets/enriched/regional_dc_capacity_timeline"),
#     output=Output("/datasets/analytics/regional_correlations"),
# )
def compute_regional_correlations(
    energy_monthly: DataFrame,
    dc_timeline: DataFrame,
) -> DataFrame:
    """
    Compute Pearson and Spearman correlation coefficients between
    cumulative DC capacity and regional energy demand over time.

    This provides a single correlation metric per region showing
    how strongly DC growth is associated with energy consumption growth.

    Uses PySpark's built-in correlation or pandas UDF with scipy
    for statistical testing.

    Args:
        energy_monthly: Monthly average energy demand per region
        dc_timeline: Monthly cumulative DC capacity per region

    Returns:
        DataFrame with correlation coefficients per region
    """
    # Join energy demand with DC capacity timeline
    joined = energy_monthly.join(
        dc_timeline,
        (energy_monthly["region_id"] == dc_timeline["region_id"])
        & (energy_monthly["month"] == dc_timeline["date"]),
        how="inner",
    ).select(
        energy_monthly["region_id"],
        energy_monthly["month"],
        energy_monthly["avg_demand_mw"],
        dc_timeline["cumulative_capacity_mw"],
        dc_timeline["cumulative_ai_capacity_mw"],
    )

    # Compute Pearson correlation per region using built-in Spark
    # For more advanced stats (p-values), use a pandas UDF
    from pyspark.ml.stat import Correlation
    from pyspark.ml.feature import VectorAssembler

    # Group by region and compute correlation
    regions = joined.select("region_id").distinct().collect()
    results = []

    for row in regions:
        region = row["region_id"]
        region_data = joined.filter(F.col("region_id") == region)

        n_points = region_data.count()
        if n_points < 6:  # Need minimum data points
            continue

        # Use Spark's corr function for Pearson
        pearson = region_data.stat.corr(
            "avg_demand_mw", "cumulative_capacity_mw"
        )

        # For AI-specific capacity
        pearson_ai = region_data.stat.corr(
            "avg_demand_mw", "cumulative_ai_capacity_mw"
        )

        results.append({
            "region_id": region,
            "pearson_total_dc": pearson,
            "pearson_ai_dc": pearson_ai,
            "data_points": n_points,
            "analysis_date": datetime.now().date(),
        })

    if not results:
        schema = StructType([
            StructField("region_id", StringType()),
            StructField("pearson_total_dc", DoubleType()),
            StructField("pearson_ai_dc", DoubleType()),
            StructField("data_points", IntegerType()),
            StructField("analysis_date", DateType()),
        ])
        return joined.sparkSession.createDataFrame([], schema)

    return joined.sparkSession.createDataFrame(results)
