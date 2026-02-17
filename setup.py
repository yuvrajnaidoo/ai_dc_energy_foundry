from setuptools import setup, find_packages

setup(
    name="ai_dc_energy",
    version="1.0.0",
    description="AI Data Center & Energy Consumption Correlation Engine for Palantir Foundry",
    author="EOX Vantage",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.3.0",
        "requests>=2.28.0",
        "pandas>=1.5.0",
        "numpy>=1.23.0",
        "scipy>=1.9.0",
        "pytz>=2022.7",
        "pyyaml>=6.0",
    ],
    entry_points={
        "transforms.pipelines": [
            "ai_dc_energy = ai_dc_energy.pipeline",
        ],
    },
)
