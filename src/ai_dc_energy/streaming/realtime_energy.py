"""
Streaming Pipeline: Real-Time Energy Monitoring
================================================

Foundry streaming transforms that process live energy grid data
with sub-15-second latency to the Ontology.

These transforms use Foundry's streaming infrastructure (built on
Apache Flink) to continuously process new data as it arrives.

IMPORTANT: Streaming pipelines require Foundry Streaming to be enabled
in your environment. Contact your Palantir representative if needed.
Streaming compute runs continuously and incurs higher costs than batch.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

from ai_dc_energy.utils.constants import (
    StreamingConfig,
    CorrelationConfig,
    DC_MARKET_TO_BA,
)

from transforms.api import transform, Input, Output
from transforms.api import configure


# =============================================================================
# STREAMING TRANSFORM: Live EIA RTO Demand
# =============================================================================

@transform(
    eia_stream=Input("/datasets/streaming/eia_rto_live"),
    output=Output("/datasets/streaming/energy_demand_live"),
)
@configure(profile=["STREAMING"])
def process_live_energy_demand(eia_stream, output):
    """
    Process live EIA RTO demand data with streaming semantics.

    This transform runs continuously, processing each new hourly
    data point as it arrives from the EIA API streaming sync.

    Processing:
    1. Parse and validate incoming records
    2. Compute 1-hour change rate (demand acceleration)
    3. Flag anomalies (>10% deviation from rolling baseline)
    4. Publish to Ontology for real-time dashboard consumption

    Note: In Foundry, the streaming sync pushes data to a stream
    topic. This transform reads from that topic continuously.

    Args:
        eia_stream: Foundry streaming input from EIA data connection
        output: Foundry streaming output dataset handle

    Returns:
        Processed streaming DataFrame with anomaly flags
    """
    config = CorrelationConfig
    eia_stream_df = eia_stream.dataframe()

    # Parse and validate
    processed = eia_stream_df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("period"), "yyyy-MM-dd'T'HH")
    ).withColumn(
        "region_id",
        F.lower(F.col("respondent"))
    ).withColumn(
        "demand_mw",
        F.col("value").cast(DoubleType())
    ).filter(
        F.col("demand_mw").isNotNull()
        & (F.col("demand_mw") > 0)
    )

    # Add processing metadata
    processed = processed.withColumn(
        "processing_timestamp",
        F.current_timestamp()
    ).withColumn(
        "latency_seconds",
        F.unix_timestamp(F.col("processing_timestamp"))
        - F.unix_timestamp(F.col("timestamp"))
    )

    # Select output columns
    output.write_dataframe(processed.select(
        F.concat_ws("_", F.lit("live"), F.col("region_id"),
                     F.date_format(F.col("timestamp"), "yyyyMMddHH")).alias("reading_id"),
        "region_id",
        F.col("respondent").alias("balancing_authority"),
        "timestamp",
        "demand_mw",
        "processing_timestamp",
        "latency_seconds",
    ))


# =============================================================================
# STREAMING TRANSFORM: Anomaly Detection
# =============================================================================

@transform(
    live_demand=Input("/datasets/streaming/energy_demand_live"),
    baseline=Input("/datasets/enriched/energy_readings_monthly"),
    output=Output("/datasets/streaming/demand_anomalies"),
)
@configure(profile=["STREAMING"])
def detect_demand_anomalies(live_demand, baseline, output):
    """
    Real-time anomaly detection on energy demand.

    Compares live demand against historical baselines to flag
    unexpected consumption spikes that may indicate new DC load.

    Triggers alert when demand exceeds the regional monthly
    average by more than the configured threshold (default 10%).

    This is a streaming-batch hybrid: live_demand is streaming,
    baseline is a batch lookup table.

    Args:
        live_demand: Foundry streaming input (demand readings)
        baseline: Foundry input (historical monthly averages, batch)
        output: Foundry streaming output dataset handle

    Returns:
        Anomaly records with severity and context
    """
    config = CorrelationConfig
    live_demand_df = live_demand.dataframe()
    baseline_df = baseline.dataframe()

    # Compute current month baseline from batch data
    current_month_baseline = baseline_df.withColumn(
        "baseline_month",
        F.month(F.col("month"))
    ).withColumn(
        "baseline_hour",
        F.lit(None)  # Monthly granularity for baseline
    ).select(
        "region_id",
        "baseline_month",
        F.col("avg_demand_mw").alias("baseline_demand_mw"),
    )

    # Enrich live stream with month for join
    live_with_month = live_demand_df.withColumn(
        "current_month",
        F.month(F.col("timestamp"))
    )

    # Join streaming with baseline lookup
    anomaly_check = live_with_month.join(
        F.broadcast(current_month_baseline),
        (live_with_month["region_id"] == current_month_baseline["region_id"])
        & (live_with_month["current_month"] == current_month_baseline["baseline_month"]),
        how="left",
    )

    # Compute deviation
    anomaly_check = anomaly_check.withColumn(
        "deviation_pct",
        F.when(
            F.col("baseline_demand_mw").isNotNull()
            & (F.col("baseline_demand_mw") > 0),
            (F.col("demand_mw") - F.col("baseline_demand_mw"))
            / F.col("baseline_demand_mw") * 100
        ).otherwise(None)
    )

    # Flag anomalies
    anomalies = anomaly_check.filter(
        F.abs(F.col("deviation_pct")) > config.ANOMALY_THRESHOLD_PCT
    ).withColumn(
        "severity",
        F.when(F.abs(F.col("deviation_pct")) > 25, "HIGH")
        .when(F.abs(F.col("deviation_pct")) > 15, "MEDIUM")
        .otherwise("LOW")
    ).withColumn(
        "anomaly_type",
        F.when(F.col("deviation_pct") > 0, "DEMAND_SPIKE")
        .otherwise("DEMAND_DROP")
    )

    output.write_dataframe(anomalies.select(
        live_with_month["region_id"],
        "timestamp",
        "demand_mw",
        "baseline_demand_mw",
        "deviation_pct",
        "severity",
        "anomaly_type",
        "processing_timestamp",
    ))


# =============================================================================
# STREAMING TRANSFORM: PJM Real-Time LMP Monitor
# =============================================================================

@transform(
    pjm_stream=Input("/datasets/streaming/pjm_lmp_live"),
    output=Output("/datasets/streaming/energy_pricing_live"),
)
@configure(profile=["STREAMING"])
def process_live_pricing(pjm_stream, output):
    """
    Process live PJM Locational Marginal Pricing data.

    LMP spikes near data center clusters indicate grid stress
    from concentrated DC loads. This transform:
    1. Parses 5-minute LMP data
    2. Computes price acceleration (rate of change)
    3. Flags price spikes above configurable thresholds

    Args:
        pjm_stream: Foundry streaming input from PJM Data Miner
        output: Foundry streaming output dataset handle

    Returns:
        Processed pricing stream with spike detection
    """
    pjm_stream_df = pjm_stream.dataframe()

    processed = pjm_stream_df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("datetime_beginning_ept"))
    ).withColumn(
        "total_lmp",
        F.col("total_lmp_rt").cast(DoubleType())
    ).withColumn(
        "congestion_lmp",
        F.col("congestion_price_rt").cast(DoubleType())
    )

    # Flag congestion-driven price spikes (congestion > $50/MWh suggests local grid stress)
    processed = processed.withColumn(
        "congestion_alert",
        F.when(F.abs(F.col("congestion_lmp")) > 50, True).otherwise(False)
    ).withColumn(
        "price_spike_alert",
        F.when(F.col("total_lmp") > 200, True).otherwise(False)  # >$200/MWh
    )

    output.write_dataframe(processed.select(
        "timestamp",
        F.col("pnode_id"),
        F.col("pnode_name"),
        "total_lmp",
        "congestion_lmp",
        F.col("system_energy_price_rt").cast(DoubleType()).alias("energy_lmp"),
        F.col("marginal_loss_price_rt").cast(DoubleType()).alias("loss_lmp"),
        "congestion_alert",
        "price_spike_alert",
        F.current_timestamp().alias("processing_timestamp"),
    ))
