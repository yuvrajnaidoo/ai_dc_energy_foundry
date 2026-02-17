"""
Tests for AI DC Energy Correlation Engine
==========================================

Run with: pytest tests/ -v
In Foundry: Tests are executed during the build process.
"""

import pytest
from datetime import date, datetime


class TestEIAConnector:
    """Tests for the EIA API connector."""

    def test_connector_initialization(self):
        from ai_dc_energy.connectors.eia_connector import EIAConnector
        connector = EIAConnector(api_key="test-key")
        assert connector.api_key == "test-key"
        assert connector.base_url == "https://api.eia.gov/v2"

    def test_endpoint_constants(self):
        from ai_dc_energy.utils.constants import EIAEndpoints
        assert "/electricity/rto/" in EIAEndpoints.RTO_SUBREGION
        assert "/electricity/retail-sales/" in EIAEndpoints.RETAIL_SALES


class TestEpochAIConnector:
    """Tests for the Epoch AI connector."""

    def test_normalize_record(self):
        from ai_dc_energy.connectors.epoch_ai_connector import EpochAIConnector

        raw = {
            "id": "test-dc-1",
            "name": "Test AI Data Center",
            "owner": "OpenAI",
            "lat": 39.0438,
            "lon": -77.4874,
            "state": "VA",
            "power_capacity_mw": 100.0,
            "gpu_count": 10000,
            "status": "operational",
        }

        normalized = EpochAIConnector.normalize_record(raw)

        assert normalized["name"] == "Test AI Data Center"
        assert normalized["owner"] == "OpenAI"
        assert normalized["capacity_mw"] == 100.0
        assert normalized["ai_focused"] is True  # Has GPUs
        assert normalized["construction_status"] == "operational"
        assert normalized["data_source"] == "epoch_ai"

    def test_ai_classification_by_owner(self):
        from ai_dc_energy.connectors.epoch_ai_connector import _classify_ai_focused

        assert _classify_ai_focused({"owner": "OpenAI"}) is True
        assert _classify_ai_focused({"owner": "xAI"}) is True
        assert _classify_ai_focused({"owner": "Google"}) is True
        assert _classify_ai_focused({"owner": "Random Corp"}) is False

    def test_ai_classification_by_gpu(self):
        from ai_dc_energy.connectors.epoch_ai_connector import _classify_ai_focused

        assert _classify_ai_focused({"gpu_count": 5000}) is True
        assert _classify_ai_focused({"accelerator_count": 100}) is True

    def test_status_normalization(self):
        from ai_dc_energy.connectors.epoch_ai_connector import _normalize_status

        assert _normalize_status("operational") == "operational"
        assert _normalize_status("Online") == "operational"
        assert _normalize_status("under construction") == "under_construction"
        assert _normalize_status("Planned") == "planned"
        assert _normalize_status("Announced") == "planned"
        assert _normalize_status("weird_status") == "unknown"


class TestCorrelationEngine:
    """Tests for the correlation computation logic."""

    def test_confidence_score_computation(self):
        from ai_dc_energy.transforms.correlation_engine import _compute_confidence_score

        # Good data: long periods, good completeness, strong signal
        score = _compute_confidence_score(
            pre_months=36,
            post_months=12,
            data_completeness=0.95,
            dc_capacity_mw=500,
            pre_avg_demand=10000,
        )
        assert 0.0 <= score <= 1.0
        assert score > 0.5  # Should be decent with good data

        # Poor data: short periods, low completeness
        low_score = _compute_confidence_score(
            pre_months=3,
            post_months=2,
            data_completeness=0.3,
            dc_capacity_mw=10,
            pre_avg_demand=50000,
        )
        assert low_score < score  # Should be lower

    def test_confidence_score_bounds(self):
        from ai_dc_energy.transforms.correlation_engine import _compute_confidence_score

        # Edge case: zero demand
        score = _compute_confidence_score(
            pre_months=0,
            post_months=0,
            data_completeness=0,
            dc_capacity_mw=0,
            pre_avg_demand=0,
        )
        assert 0.0 <= score <= 1.0


class TestConstants:
    """Tests for configuration constants."""

    def test_dc_market_ba_mapping(self):
        from ai_dc_energy.utils.constants import DC_MARKET_TO_BA

        assert "nova" in DC_MARKET_TO_BA
        assert "PJM" in DC_MARKET_TO_BA["nova"]
        assert "ERCO" in DC_MARKET_TO_BA["dfw"]
        assert "CISO" in DC_MARKET_TO_BA["sjc"]

    def test_energy_unit_conversions(self):
        from ai_dc_energy.utils.constants import EnergyUnits

        assert EnergyUnits.MWH_TO_TWH == 1 / 1_000_000
        assert EnergyUnits.GWH_TO_TWH == 1 / 1000
        assert EnergyUnits.MW_TO_ANNUAL_MWH_DC == 8760 * 0.85

    def test_correlation_config(self):
        from ai_dc_energy.utils.constants import CorrelationConfig

        assert CorrelationConfig.BASELINE_MONTHS == 36
        assert CorrelationConfig.ANOMALY_THRESHOLD_PCT == 10.0
        assert 0 < CorrelationConfig.SIGNIFICANCE_LEVEL < 1


class TestGeoUtils:
    """Tests for geographic utilities."""

    def test_haversine_distance(self):
        from ai_dc_energy.utils.geo_utils import haversine_distance_km

        # Distance from Ashburn, VA to Washington DC (about 50 km)
        dist = haversine_distance_km(39.0438, -77.4874, 38.9072, -77.0369)
        assert 30 < dist < 70  # Rough bounds

        # Same point should be 0
        dist_zero = haversine_distance_km(39.0, -77.0, 39.0, -77.0)
        assert dist_zero == 0.0

    def test_state_fips_to_code(self):
        from ai_dc_energy.utils.geo_utils import state_fips_to_code

        assert state_fips_to_code("51") == "VA"
        assert state_fips_to_code("48") == "TX"
        assert state_fips_to_code("06") == "CA"
        assert state_fips_to_code("99") is None


class TestTimeUtils:
    """Tests for time normalization utilities."""

    def test_eia_period_parsing(self):
        from ai_dc_energy.utils.time_utils import eia_period_to_utc

        result = eia_period_to_utc("2024-01-15T14", "PJM")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_months_between(self):
        from ai_dc_energy.utils.time_utils import months_between

        d1 = date(2023, 1, 1)
        d2 = date(2024, 1, 1)
        assert months_between(d1, d2) == 12

        d3 = date(2024, 6, 15)
        assert months_between(d1, d3) == 17


class TestForecastModel:
    """Tests for the energy demand forecast model."""

    def test_predict_basic(self):
        from ai_dc_energy.models.forecast_model import EnergyDemandForecaster

        model = EnergyDemandForecaster(baseline_growth_rate=0.02)
        forecasts = model.predict(
            region_id="pjm",
            current_demand_mw=50000,
            planned_dc_capacity_mw=2000,
            years_ahead=3,
        )

        assert len(forecasts) == 3
        for f in forecasts:
            assert f["region_id"] == "pjm"
            assert f["total_projected_demand_mw"] > 50000
            assert f["demand_increase_pct"] > 0
