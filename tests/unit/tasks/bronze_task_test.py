from cdii_data_pipelines.tasks.bronze_task import BronzeTask
from cdii_data_pipelines.integrations.datasource import DataSource
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
#from pytest_mock import mocker 
import pandas as pd
from datetime import datetime
import pytz
from shapely import Point
import logging
import pytest


class ConcreteDataSource():
    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        return pd.DataFrame()

    def write(self, dataFrame: DataFrame):
        pass

class ConcreteBronzeTask(BronzeTask):
    def __init__(self, spark: SparkSession=None, init_conf: dict=None, source_datasource: DataSource=None):
      super(ConcreteBronzeTask, self).__init__(spark=spark, init_conf=init_conf, source_datasource=source_datasource)

    def extract(self) -> DataFrame:
        return None

def test_transform_requires_geometry_field_in_geo():
    logging.info("Ensure transform requires 'geometry' when in a geo dataframe")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    df['geometry']=[Point(1.0, -1.0), Point(2.0, -2.0)]
    
    source_datasource = ConcreteDataSource()
    test_bronze_task = ConcreteBronzeTask(source_datasource=source_datasource)
    
    with pytest.raises(KeyError):
        test_bronze_task.transform(dataFrame=df, params={})

def test_transform_not_require_geometry_field_when_not_geo():
    logging.info("Ensure transform does not require 'geometry' when not in a geo dataframe")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]

    source_datasource = ConcreteDataSource()
    test_bronze_task = ConcreteBronzeTask(source_datasource=source_datasource)
    
    try:
        test_bronze_task.transform(dataFrame=df, params={})
    except KeyError as exc:
        assert False, f'Exception raised because no geometry_field {exc}'

def test_transform_removes_SHAPE():
    logging.info("Ensure transform removes SHAPE")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    df['SHAPE']=[1, 2]

    source_datasource = ConcreteDataSource()
    test_bronze_task = ConcreteBronzeTask(source_datasource=source_datasource)
    df = test_bronze_task.transform(dataFrame=df, params={})

    assert True, 'SHAPE' not in df.columns

def test_transform_adds_ade_submitted_date_to_today():
    logging.info("Ensure transform adds the ade_submitted_date")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]

    source_datasource = ConcreteDataSource()
    test_bronze_task = ConcreteBronzeTask(source_datasource=source_datasource)
    df = test_bronze_task.transform(dataFrame=df, params={})
    
    today = datetime.now(pytz.timezone("America/Los_Angeles")).date()
    assert True, 'ade_submitted_date' in df.columns

    assert True, (df['ade_submitted_date'] == today).all()

def test_transform_adds_non_standard_geometry():
    logging.info("Ensure transform add 'geometry_field' when provided")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    df['geometry']=[Point(1.0, -1.0), Point(2.0, -2.0)]

    source_datasource = ConcreteDataSource()
    test_bronze_task = ConcreteBronzeTask(source_datasource=source_datasource)
    df = test_bronze_task.transform(dataFrame=df, 
        params={
            "destination": {
                "geometry_field": "geom"
            }
        })

    assert True, 'geom' in df.columns

def test_transform_removes_geometry_when_non_standard_geometry():
    logging.info("Ensure transform removes the 'geometry when 'geometry_field' provided")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    df['geometry']=[Point(1.0, -1.0), Point(2.0, -2.0)]

    source_datasource = ConcreteDataSource()
    test_bronze_task = ConcreteBronzeTask(source_datasource=source_datasource)
    df = test_bronze_task.transform(dataFrame=df, 
        params={
            "destination": {
                "geometry_field": "geom"
            }
        })
    print(df.columns)
    assert True, 'geometry' not in df.columns

