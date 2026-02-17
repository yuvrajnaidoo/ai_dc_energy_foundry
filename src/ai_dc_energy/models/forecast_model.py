"""
Energy Demand Forecast Model
==============================

Predicts regional energy demand based on:
- Historical consumption trends
- Announced/planned data center capacity
- Seasonal weather patterns

Uses Foundry's Model Studio or Code Repository ML framework.
Model type: Time-series regression with DC capacity as exogenous variable.
"""

import logging
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# Foundry ML imports (uncomment in Foundry):
# from palantir_models.transforms import ModelOutput
# from palantir_models.models import ModelAdapter
# -----------------------------------------------------------------------


class EnergyDemandForecaster:
    """
    Forecasts future energy demand incorporating planned DC capacity.

    Methodology:
    1. Fit a baseline trend using pre-DC historical data
    2. Compute the marginal energy impact per MW of DC capacity
       (from observed correlation events)
    3. Project future demand = baseline trend + (planned DC MW * marginal impact)

    This enables answering questions like:
    "If Georgia builds all 285 planned data centers, what will
     regional energy demand look like in 2028?"
    """

    def __init__(self, baseline_growth_rate: float = 0.02):
        """
        Args:
            baseline_growth_rate: Annual baseline demand growth rate
                                  (default 2% — general economic/electrification growth)
        """
        self.baseline_growth_rate = baseline_growth_rate
        self.mw_impact_factors: Dict[str, float] = {}

    def fit(
        self,
        correlation_events: DataFrame,
    ) -> "EnergyDemandForecaster":
        """
        Learn the marginal energy impact per MW of DC capacity.

        From observed correlation events, computes:
        impact_mw_per_dc_mw = average(delta_demand_mw / dc_capacity_mw)

        This tells us: for every 1 MW of DC capacity added,
        how much does regional demand increase?

        Theoretical range: 1.0 - 1.6x (PUE factor)
        A PUE of 1.3 means 1 MW of IT load → 1.3 MW total demand.

        Args:
            correlation_events: Computed correlation events DataFrame

        Returns:
            self (fitted model)
        """
        # Group by region and compute average impact factor
        region_impacts = correlation_events.filter(
            F.col("confidence_score") >= 0.5
        ).groupBy("region_id").agg(
            F.avg("delta_demand_mw").alias("avg_delta_mw"),
            F.count("*").alias("event_count"),
        )

        impacts = region_impacts.collect()
        for row in impacts:
            self.mw_impact_factors[row["region_id"]] = row["avg_delta_mw"]

        logger.info(
            f"Fitted forecaster with impact factors for "
            f"{len(self.mw_impact_factors)} regions"
        )
        return self

    def predict(
        self,
        region_id: str,
        current_demand_mw: float,
        planned_dc_capacity_mw: float,
        years_ahead: int = 3,
    ) -> List[Dict]:
        """
        Forecast energy demand for a region.

        Args:
            region_id: Energy region to forecast
            current_demand_mw: Current average demand (MW)
            planned_dc_capacity_mw: Total planned additional DC capacity (MW)
            years_ahead: Number of years to project

        Returns:
            List of annual forecast points
        """
        impact_factor = self.mw_impact_factors.get(region_id, 1.3)  # Default PUE
        forecasts = []

        for year in range(1, years_ahead + 1):
            # Baseline growth (general electrification/economic)
            baseline = current_demand_mw * (1 + self.baseline_growth_rate) ** year

            # DC capacity assumed to come online linearly over the period
            dc_fraction = min(1.0, year / years_ahead)
            dc_impact = planned_dc_capacity_mw * dc_fraction * (impact_factor / current_demand_mw)

            projected = baseline + (planned_dc_capacity_mw * dc_fraction)

            forecasts.append({
                "region_id": region_id,
                "year_ahead": year,
                "baseline_demand_mw": round(baseline, 1),
                "dc_additional_demand_mw": round(planned_dc_capacity_mw * dc_fraction, 1),
                "total_projected_demand_mw": round(projected, 1),
                "demand_increase_pct": round(
                    (projected - current_demand_mw) / current_demand_mw * 100, 2
                ),
            })

        return forecasts
