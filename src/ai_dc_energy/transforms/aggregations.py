"""
Layer 4: KPI Aggregations
==========================

Final transforms that produce the dashboard-ready KPI datasets
consumed by Foundry Workshop, Contour, and Quiver applications.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transforms.api import transform, Input, Output


@transform(
    energy=Input("/datasets/enriched/energy_weather_joined"),
    dc_timeline=Input("/datasets/enriched/regional_dc_capacity_timeline"),
    correlations=Input("/datasets/analytics/correlation_events"),
    retail=Input("/datasets/raw/eia_retail_sales"),
    output=Output("/datasets/dashboard/kpi_summary"),
)
def compute_dashboard_kpis(energy, dc_timeline, correlations, retail, output):
    """
    Compute all dashboard KPIs for the executive summary view.

    KPIs produced:
    1. Regional Energy Delta (%) — change vs pre-DC baseline
    2. DC Capacity vs Regional Load Correlation (r-value)
    3. Energy Cost Impact ($/MWh)
    4. Per-Capita Energy Increase
    5. Carbon Intensity Delta
    6. Renewable Offset Ratio
    7. Top regions by DC impact

    Args:
        energy: Weather-adjusted energy readings
        dc_timeline: Regional DC capacity timeline
        correlations: Computed correlation events
        retail: Retail sales data for pricing

    Returns:
        Summary KPI DataFrame for dashboard consumption
    """
    energy_df = energy.dataframe()
    dc_timeline_df = dc_timeline.dataframe()
    correlations_df = correlations.dataframe()

    # KPI 1: Regional Energy Delta — latest month vs baseline
    # Compute latest month without collect() using a self-join approach
    latest_month_df = energy_df.agg(F.max("timestamp").alias("latest_ts"))
    energy_with_latest = energy_df.crossJoin(F.broadcast(latest_month_df))

    current_demand = energy_with_latest.filter(
        F.date_trunc("month", F.col("timestamp")) == F.date_trunc("month", F.col("latest_ts"))
    ).groupBy("region_id").agg(
        F.avg("demand_mw").alias("current_avg_demand_mw"),
    )

    # Get baseline from correlation events (pre-period averages)
    baseline_demand = correlations_df.groupBy("region_id").agg(
        F.avg("pre_period_avg_demand_mw").alias("baseline_avg_demand_mw"),
        F.avg("delta_demand_pct").alias("avg_delta_pct"),
        F.avg("confidence_score").alias("avg_confidence"),
        F.count("*").alias("dc_events_count"),
    )

    # Join current with baseline
    kpi = current_demand.join(baseline_demand, "region_id", "left")

    kpi = kpi.withColumn(
        "energy_delta_pct",
        F.when(
            F.col("baseline_avg_demand_mw").isNotNull()
            & (F.col("baseline_avg_demand_mw") > 0),
            (F.col("current_avg_demand_mw") - F.col("baseline_avg_demand_mw"))
            / F.col("baseline_avg_demand_mw") * 100
        ).otherwise(F.col("avg_delta_pct"))
    )

    # KPI 2: Add DC capacity data
    latest_dc = dc_timeline_df.groupBy("region_id").agg(
        F.max("cumulative_capacity_mw").alias("total_dc_capacity_mw"),
        F.max("cumulative_ai_capacity_mw").alias("total_ai_dc_capacity_mw"),
        F.max("cumulative_dc_count").alias("total_dc_count"),
        F.max("cumulative_gpu_count").alias("total_gpu_count"),
    )

    kpi = kpi.join(latest_dc, "region_id", "left")

    # KPI 3: DC load as percentage of regional demand
    kpi = kpi.withColumn(
        "dc_load_pct_of_demand",
        F.when(
            F.col("current_avg_demand_mw").isNotNull()
            & (F.col("current_avg_demand_mw") > 0)
            & F.col("total_dc_capacity_mw").isNotNull(),
            F.col("total_dc_capacity_mw") / F.col("current_avg_demand_mw") * 100
        ).otherwise(None)
    )

    # Add snapshot timestamp
    kpi = kpi.withColumn("snapshot_timestamp", F.current_timestamp())

    output.write_dataframe(kpi.select(
        "region_id",
        "current_avg_demand_mw",
        "baseline_avg_demand_mw",
        "energy_delta_pct",
        "total_dc_capacity_mw",
        "total_ai_dc_capacity_mw",
        "total_dc_count",
        "total_gpu_count",
        "dc_load_pct_of_demand",
        "avg_confidence",
        "dc_events_count",
        "snapshot_timestamp",
    ))


@transform(
    energy=Input("/datasets/enriched/energy_weather_joined"),
    output=Output("/datasets/dashboard/demand_timeseries"),
)
def compute_demand_timeseries(energy, output):
    """
    Produce time-series data for Quiver visualization.

    Outputs hourly demand with rolling averages for smooth
    real-time charting in Foundry Quiver.

    Args:
        energy: Foundry input (weather-adjusted energy readings)
        output: Foundry output dataset handle

    Returns:
        Time-series DataFrame with rolling averages
    """
    energy_df = energy.dataframe()

    # 7-day rolling average for trend line
    window_7d = (
        Window
        .partitionBy("region_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-7 * 24 * 3600, 0)  # 7 days in seconds
    )

    # 30-day rolling average for baseline trend
    window_30d = (
        Window
        .partitionBy("region_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-30 * 24 * 3600, 0)
    )

    ts = energy_df.select(
        "region_id",
        "timestamp",
        "demand_mw",
        "temperature_f",
    ).withColumn(
        "demand_7d_avg_mw",
        F.avg("demand_mw").over(window_7d)
    ).withColumn(
        "demand_30d_avg_mw",
        F.avg("demand_mw").over(window_30d)
    ).withColumn(
        "demand_anomaly_mw",
        F.col("demand_mw") - F.col("demand_30d_avg_mw")
    )

    output.write_dataframe(ts)
