"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from cdii_data_pipelines import __version__

PACKAGE_REQUIREMENTS = [
  "pyyaml",
  "arcgis==1.9.1",
  "datetime==4.4",
  "geoalchemy2==0.10.2",
  "geopandas==0.10.2",
  "pandas==1.2.4",
  "pytz==2021.3",
  "sqlalchemy==1.4.31",
  "psycopg2-binary==2.9.2",
  "rtree==1.0.0"
  ]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    # "pyspark==3.2.1",
    # "delta-spark==1.1.0",
    # "scikit-learn",
    #"mlflow",
    "pandas",
    "databricks-connect==10.4.*"
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.7,<0.8"
]

setup(
    name="ade_recurring_etl",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools","wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    # entry_points = {
    #     "console_scripts": [
    #         "etl = cdii_data_pipelines.tasks.sample_etl_task:entrypoint",
    #         "ml = cdii_data_pipelines.tasks.sample_ml_task:entrypoint",
    # ]},
    version=__version__,
    description="",
    author="",
)
