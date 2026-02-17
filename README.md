# AI Data Centers & Energy Consumption Correlation Engine

## Palantir Foundry Package

Real-time correlation analysis between AI data center deployments and regional energy consumption patterns.

---

## Package Structure

```
ai_dc_energy_foundry/
├── README.md                          # This file
├── setup.py                           # Python package configuration
├── build.gradle                       # Foundry build configuration
├── conda_recipe/
│   └── meta.yaml                      # Conda environment for Foundry
├── pipeline_configs/
│   ├── streaming_config.yml           # Streaming pipeline parameters
│   └── schedule_config.yml            # Batch/incremental scheduling
├── src/
│   └── ai_dc_energy/
│       ├── __init__.py
│       ├── connectors/                # Data source connectors
│       │   ├── __init__.py
│       │   ├── eia_connector.py       # U.S. EIA API v2 connector
│       │   ├── epoch_ai_connector.py  # Epoch AI data center database
│       │   ├── pjm_connector.py       # PJM grid data connector
│       │   └── noaa_connector.py      # NOAA weather data connector
│       ├── transforms/                # Foundry pipeline transforms
│       │   ├── __init__.py
│       │   ├── raw_ingestion.py       # Layer 1: Raw data ingestion
│       │   ├── enrichment.py          # Layer 2: Transformation & enrichment
│       │   ├── correlation_engine.py  # Layer 3: Correlation computation
│       │   └── aggregations.py        # Layer 4: KPI aggregations
│       ├── streaming/                 # Real-time streaming transforms
│       │   ├── __init__.py
│       │   └── realtime_energy.py     # Streaming pipeline for grid data
│       ├── ontology/                  # Ontology object type definitions
│       │   ├── __init__.py
│       │   └── object_types.py        # EnergyRegion, DataCenter, etc.
│       ├── models/                    # ML models
│       │   ├── __init__.py
│       │   └── forecast_model.py      # Energy demand forecasting
│       └── utils/                     # Shared utilities
│           ├── __init__.py
│           ├── constants.py           # Enums, config constants
│           ├── geo_utils.py           # Geographic matching utilities
│           └── time_utils.py          # Time normalization utilities
└── tests/
    ├── __init__.py
    ├── test_eia_connector.py
    ├── test_correlation_engine.py
    └── test_transforms.py
```

---

## Setup Instructions

### 1. Import into Foundry

1. In Foundry, navigate to your project folder
2. Create a new **Code Repository** (Python Transforms)
3. Upload or push this package to the repository
4. The `build.gradle` and `conda_recipe/meta.yaml` configure all dependencies

### 2. Configure Data Connections

Before running pipelines, configure the following Data Connections in Foundry:

| Connection Name          | Type         | Configuration Required            |
|--------------------------|--------------|-----------------------------------|
| `eia-api-v2`             | REST API     | API key from eia.gov/opendata     |
| `epoch-ai-datacenters`   | REST/File    | No auth (CC-BY open data)         |
| `pjm-dataminer`          | REST API     | Free registration at pjm.com      |
| `noaa-climate`           | REST API     | Free API token from NOAA          |

### 3. Configure Secrets

Store API keys in Foundry's secret management:

```
EIA_API_KEY        → Your EIA API key
PJM_API_KEY        → Your PJM Data Miner key (if applicable)
NOAA_API_TOKEN     → Your NOAA Climate Data API token
```

### 4. Run Pipelines

Execute in order:
1. `raw_ingestion.py` transforms (batch) — loads historical baselines
2. `enrichment.py` transforms — joins and normalizes data
3. `realtime_energy.py` streaming pipeline — starts live monitoring
4. `correlation_engine.py` — computes before/after metrics
5. `aggregations.py` — produces dashboard KPIs

### 5. Build Ontology

After initial data loads, register the Ontology object types defined in
`ontology/object_types.py` using Foundry's Ontology Manager.

---

## Data Sources

| Source | Endpoint | Refresh |
|--------|----------|---------|
| EIA API v2 (RTO) | `/v2/electricity/rto/region-sub-ba-data/data/` | Hourly |
| EIA API v2 (Retail) | `/v2/electricity/retail-sales/data/` | Monthly |
| Epoch AI Frontier DCs | `epoch.ai/data/data-centers` | Irregular |
| PJM Data Miner | `dataminer2.pjm.com` | 5-minute |
| NOAA Climate | `ncdc.noaa.gov/cdo-web/api/v2/` | Hourly |
| DOE/Berkeley Lab | Manual upload (annual reports) | Annual |

---

## Notes

- All statistics in this package reference IEA, U.S. EIA, DOE/Berkeley Lab,
  EPRI, Epoch AI, and public company disclosures as of February 2026
- Streaming pipelines require Foundry Streaming to be enabled in your environment
- Contact your Palantir representative if streaming is not available
