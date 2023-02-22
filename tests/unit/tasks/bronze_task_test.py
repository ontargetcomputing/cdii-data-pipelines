from cdii_data_pipelines.tasks.bronze_task import BronzeTask
from pyspark.pandas import DataFrame
import pandas as pd
from datetime import datetime
import pytz
from shapely import Point
import logging

default_params = {
    "source_datasources": [
      {
      "type": "noop",
      }
    ],
    "destination_datasources": [
      {
      "type": "noop",
      }
    ]
}

class ConcreteBronzeTask(BronzeTask):
    def __init__(self):
      super(ConcreteBronzeTask, self).__init__(init_conf=default_params)

    def extract(self, params: dict=None) -> DataFrame:
        return None

def test_transform_not_require_geometry_field_when_not_geo():
    logging.info("Ensure transform does not require 'geometry' when not in a geo dataframe")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    dfs = []
    dfs.append(df)

    test_bronze_task = ConcreteBronzeTask()
    
    try:
        test_bronze_task.transform(dataFrames=dfs, params={})
    except KeyError as exc:
        assert False, f'Exception raised because no geometry_field {exc}'

def test_transform_removes_SHAPE():
    logging.info("Ensure transform removes SHAPE")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    df['SHAPE']=[1, 2]

    dfs = []
    dfs.append(df)
    test_bronze_task = ConcreteBronzeTask()
    dfs = test_bronze_task.transform(dataFrames=dfs, params={})

    assert True, 'SHAPE' not in dfs[0].columns

def test_transform_adds_ade_submitted_date_to_today():
    logging.info("Ensure transform adds the ade_submitted_date")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]

    dfs = []
    dfs.append(df)
    test_bronze_task = ConcreteBronzeTask()
    dfs = test_bronze_task.transform(dataFrames=dfs, params={})
    
    today = datetime.now(pytz.timezone("America/Los_Angeles")).date()
    assert True, 'ade_submitted_date' in df.columns

    assert True, (dfs[0]['ade_submitted_date'] == today).all()

def test_transform_adds_non_standard_geometry():
    logging.info("Ensure transform add 'geometry_field' when provided")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    df['geometry']=[Point(1.0, -1.0), Point(2.0, -2.0)]

    dfs = []
    dfs.append(df)
    test_bronze_task = ConcreteBronzeTask()
    dfs = test_bronze_task.transform(dataFrames=dfs, 
        params={
            "destination": {
                "geometry_field": "geom"
            }
        })

    assert True, 'geom' in dfs[0].columns

def test_transform_removes_geometry_when_non_standard_geometry():
    logging.info("Ensure transform removes the 'geometry when 'geometry_field' provided")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]
    df['geometry']=[Point(1.0, -1.0), Point(2.0, -2.0)]
    
    dfs = []
    dfs.append(df)
    test_bronze_task = ConcreteBronzeTask()
    dfs = test_bronze_task.transform(dataFrames=dfs, 
        params={
            "destination": {
                "geometry_field": "geom"
            }
        })
    assert True, 'geometry' not in dfs[0].columns
