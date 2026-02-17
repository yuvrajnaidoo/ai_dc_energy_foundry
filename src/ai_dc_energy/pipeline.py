"""
Pipeline registration for Palantir Foundry.

This module is the entry point referenced in setup.py under
transforms.pipelines. Foundry discovers all @transform-decorated
functions by importing the modules listed here.
"""

from ai_dc_energy.transforms import raw_ingestion
from ai_dc_energy.transforms import enrichment
from ai_dc_energy.transforms import correlation_engine
from ai_dc_energy.transforms import aggregations
from ai_dc_energy.streaming import realtime_energy
