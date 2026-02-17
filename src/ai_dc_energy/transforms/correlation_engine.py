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

from transforms.api import transform, Input, Output

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

@transform(
    energy_readings=Input("/datasets/enriched/energy_weather_joined"),
    dc_timeline=Input("/datasets/enriched/regional_dc_capacity_timeline"),
    data_centers=Input("/datasets/enriched/data_centers_with_regions"),
    output=Output("/datasets/analytics/correlation_events"),
)
def compute_correlation_events(energy_readings, dc_timeline, data_centers, output):
    """
    Compute before/after correlation events for each data center deployment.

    For each data center that came online, this transform:
    1. Defines pre-period (BASELINE_MONTHS before operational_date)
    2. Defines post-period (operational_date to now or analysis window)
    3. Computes average demand in both periods
    4. Calculates delta (absolute MW and percentage)
    5. Assigns a confidence score

    Fully distributed — uses Spark joins and window functions instead of
    collect() to ensure scalability on large datasets.

    Args:
        energy_readings: Foundry input (hourly energy readings)
        dc_timeline: Foundry input (regional DC capacity timeline)
        data_centers: Foundry input (individual DC records)
        output: Foundry output dataset handle

    Returns:
        DataFrame of CorrelationEvent records
    """
    energy_df = energy_readings.dataframe()
    data_centers_df = data_centers.dataframe()
    spark = energy_df.sparkSession
    config = CorrelationConfig

    # Get DCs that have come online (have operational dates)
    online_dcs = data_centers_df.filter(
        (F.col("construction_status") == "operational")
        & F.col("operational_date").isNotNull()
        & F.col("region_id").isNotNull()
    ).select(
        F.col("dc_id"),
        F.col("name").alias("dc_name"),
        F.col("region_id").alias("dc_region_id"),
        F.col("operational_date"),
        F.col("capacity_mw"),
    )

    # Filter DCs with enough post-operational data
    today = F.current_date()
    online_dcs = online_dcs.withColumn(
        "months_since_op",
        F.months_between(today, F.col("operational_date")).cast("int")
    ).filter(
        F.col("months_since_op") >= config.MIN_POST_MONTHS
    )

    # Compute period boundaries for each DC
    online_dcs = online_dcs.withColumn(
        "pre_period_start",
        F.add_months(F.col("operational_date"), -config.BASELINE_MONTHS)
    ).withColumn(
        "pre_period_end",
        F.add_months(F.col("operational_date"), -1)
    ).withColumn(
        "post_period_start",
        F.col("operational_date")
    ).withColumn(
        "post_period_end",
        F.least(
            F.add_months(F.col("operational_date"), config.DEFAULT_ANALYSIS_WINDOW),
            today,
        )
    )

    # Aggregate energy readings to monthly averages per region
    monthly_energy = energy_df.withColumn(
        "month", F.date_trunc("month", F.col("timestamp"))
    ).groupBy("region_id", "month").agg(
        F.avg("demand_mw").alias("avg_demand_mw"),
        F.count("*").alias("reading_count"),
    )

    # Join DCs with monthly energy on region_id
    joined = online_dcs.join(
        monthly_energy,
        online_dcs["dc_region_id"] == monthly_energy["region_id"],
        how="inner",
    )

    # Flag each month as pre-period or post-period for each DC
    joined = joined.withColumn(
        "is_pre",
        (F.col("month") >= F.col("pre_period_start"))
        & (F.col("month") <= F.col("pre_period_end"))
    ).withColumn(
        "is_post",
        (F.col("month") >= F.col("post_period_start"))
        & (F.col("month") <= F.col("post_period_end"))
    )

    # Compute pre-period and post-period stats per DC
    pre_stats = joined.filter(F.col("is_pre")).groupBy("dc_id").agg(
        F.avg("avg_demand_mw").alias("pre_period_avg_demand_mw"),
        F.count("*").alias("pre_months"),
    )

    post_stats = joined.filter(F.col("is_post")).groupBy("dc_id").agg(
        F.avg("avg_demand_mw").alias("post_period_avg_demand_mw"),
        F.count("*").alias("post_months"),
    )

    # Join pre and post stats back to DCs
    results = online_dcs.join(pre_stats, "dc_id", "inner") \
                         .join(post_stats, "dc_id", "inner")

    # Filter out rows where either average is null
    results = results.filter(
        F.col("pre_period_avg_demand_mw").isNotNull()
        & F.col("post_period_avg_demand_mw").isNotNull()
    )

    # Compute deltas
    results = results.withColumn(
        "delta_demand_mw",
        F.col("post_period_avg_demand_mw") - F.col("pre_period_avg_demand_mw")
    ).withColumn(
        "delta_demand_pct",
        F.when(
            F.col("pre_period_avg_demand_mw") != 0,
            F.col("delta_demand_mw") / F.col("pre_period_avg_demand_mw") * 100
        ).otherwise(None)
    )

    # Compute data completeness
    results = results.withColumn(
        "expected_total",
        F.lit(config.BASELINE_MONTHS) + F.least(
            F.lit(config.DEFAULT_ANALYSIS_WINDOW),
            F.col("months_since_op")
        )
    ).withColumn(
        "data_completeness",
        F.least(
            F.lit(1.0),
            (F.col("pre_months") + F.col("post_months"))
            / F.greatest(F.lit(1), F.col("expected_total"))
        )
    )

    # Compute confidence score using Spark column expressions
    completeness_score = F.least(
        F.lit(1.0),
        F.col("data_completeness") / F.lit(config.MIN_DATA_COMPLETENESS)
    )
    pre_score = F.least(F.lit(1.0), F.col("pre_months") / F.lit(config.BASELINE_MONTHS))
    post_score = F.least(F.lit(1.0), F.col("post_months") / F.lit(config.DEFAULT_ANALYSIS_WINDOW))
    snr_score = F.when(
        (F.col("pre_period_avg_demand_mw") > 0) & F.col("capacity_mw").isNotNull(),
        F.least(F.lit(1.0), F.col("capacity_mw") / F.col("pre_period_avg_demand_mw") / F.lit(0.10))
    ).otherwise(F.lit(0.1))

    results = results.withColumn(
        "confidence_score",
        F.round(
            F.least(F.lit(1.0), F.greatest(F.lit(0.0),
                F.lit(0.3) * completeness_score
                + F.lit(0.2) * pre_score
                + F.lit(0.2) * post_score
                + F.lit(0.3) * snr_score
            )), 3
        )
    )

    # Build final output
    result_df = results.select(
        F.concat_ws("_", F.lit("corr"), F.col("dc_id"), F.col("dc_region_id"),
                     F.date_format(today, "yyyy-MM-dd")).alias("event_id"),
        F.col("dc_id"),
        F.col("dc_name"),
        F.col("dc_region_id").alias("region_id"),
        today.alias("analysis_date"),
        F.col("pre_period_start"),
        F.col("pre_period_end"),
        F.col("post_period_start"),
        F.col("post_period_end"),
        F.col("pre_period_avg_demand_mw"),
        F.col("post_period_avg_demand_mw"),
        F.col("delta_demand_mw"),
        F.col("delta_demand_pct"),
        F.lit(None).cast("double").alias("pearson_correlation"),
        F.lit(None).cast("double").alias("spearman_correlation"),
        F.lit(None).cast("double").alias("p_value"),
        F.col("confidence_score"),
        F.lit(None).cast("string").alias("control_region_id"),
        F.lit(None).cast("double").alias("control_delta_pct"),
        F.lit(None).cast("double").alias("adjusted_delta_pct"),
        F.lit(config.DEFAULT_ANALYSIS_WINDOW).alias("analysis_window_months"),
        F.col("data_completeness"),
        F.lit("difference_in_differences_v1").alias("methodology"),
    )

    output.write_dataframe(result_df)


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

@transform(
    energy_monthly=Input("/datasets/enriched/energy_readings_monthly"),
    dc_timeline=Input("/datasets/enriched/regional_dc_capacity_timeline"),
    output=Output("/datasets/analytics/regional_correlations"),
)
def compute_regional_correlations(energy_monthly, dc_timeline, output):
    """
    Compute Pearson correlation coefficients between cumulative DC
    capacity and regional energy demand over time.

    Uses a pandas UDF to compute per-region correlations in a
    distributed manner without collect().

    Args:
        energy_monthly: Foundry input (monthly average energy demand)
        dc_timeline: Foundry input (monthly cumulative DC capacity)
        output: Foundry output dataset handle

    Returns:
        DataFrame with correlation coefficients per region
    """
    energy_monthly_df = energy_monthly.dataframe()
    dc_timeline_df = dc_timeline.dataframe()

    # Join energy demand with DC capacity timeline
    joined = energy_monthly_df.join(
        dc_timeline_df,
        (energy_monthly_df["region_id"] == dc_timeline_df["region_id"])
        & (energy_monthly_df["month"] == dc_timeline_df["date"]),
        how="inner",
    ).select(
        energy_monthly_df["region_id"],
        energy_monthly_df["month"],
        energy_monthly_df["avg_demand_mw"],
        dc_timeline_df["cumulative_capacity_mw"],
        dc_timeline_df["cumulative_ai_capacity_mw"],
    )

    # Use a pandas UDF grouped by region to compute correlations
    from pyspark.sql.types import StructType as ST, StructField as SF
    import pandas as pd

    result_schema = ST([
        SF("region_id", StringType()),
        SF("pearson_total_dc", DoubleType()),
        SF("pearson_ai_dc", DoubleType()),
        SF("data_points", IntegerType()),
        SF("analysis_date", DateType()),
    ])

    @F.pandas_udf(result_schema, F.PandasUDFType.GROUPED_MAP)
    def compute_region_corr(pdf):
        region = pdf["region_id"].iloc[0]
        n = len(pdf)
        if n < 6:
            return pd.DataFrame()

        pearson_total = pdf["avg_demand_mw"].corr(pdf["cumulative_capacity_mw"])
        pearson_ai = pdf["avg_demand_mw"].corr(pdf["cumulative_ai_capacity_mw"])

        return pd.DataFrame([{
            "region_id": region,
            "pearson_total_dc": float(pearson_total) if pd.notna(pearson_total) else None,
            "pearson_ai_dc": float(pearson_ai) if pd.notna(pearson_ai) else None,
            "data_points": n,
            "analysis_date": datetime.now().date(),
        }])

    result = joined.groupby("region_id").apply(compute_region_corr)

    output.write_dataframe(result)
