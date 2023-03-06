from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.pandas.geopandas_wrapper import GeoPandasWrapper
from cdii_data_pipelines.pandas.pandas_wrapper import PandasWrapper
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
from array import array
# import pandas as pd
# import geopandas as gpd
from shapely import wkt
import geopy
import geopy.distance
from shapely import geometry

class WildfireGoldTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(WildfireGoldTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        dataFrames = WildfireGoldTask._create_lat_long(dataFrames, params=params)
        dataFrames = WildfireGoldTask._drop_columns(dataFrames, params=params)
        dataFrame = WildfireGoldTask._merge_datasets(dataFrames, params=params)
        dataFrame = WildfireGoldTask._create_poly(dataFrame)
        dataFrame = WildfireGoldTask._convert_NaN_to_None(dataFrame)
        
        dataFramesToReturn = []
        dataFramesToReturn[0] = dataFrame
        return dataFramesToReturn

    @staticmethod 
    def _create_lat_long(dataFrames: array=None, params: dict=None) -> array:
        create_lat_long = params['create_lat_long']
        if create_lat_long:
            dataFrame = dataFrames[1]
            dataFrame["latitude"] = [data["y"] for data in dataFrame["SHAPE"]]
            dataFrame["longitude"] = [data["x"] for data in dataFrame["SHAPE"]]

            dataFrames[1] = dataFrame

        return dataFrames

    @staticmethod 
    def _drop_columns(dataFrames: array=None, params: dict=None) -> array:
        if 'drop_columns' in params.keys():
            cols_to_drop = params['drop_columns']
            dataFrames[0] = dataFrames[0].drop(columns=cols_to_drop)
        
        return dataFrames

    @staticmethod 
    def _merge_datasets(dataFrames: array=None, params: dict=None) -> array:
        geometry_col = 'geometry'

        geo_data_0 = dataFrames[0].toPandas()
        geo_data_0[geometry_col] = geo_data_0[geometry_col].apply(wkt.loads)
        geo_data_0 = GeoPandasWrapper.GeoDataFrame(geo_data_0, geometry=geometry_col)

        geo_data_1 = dataFrames[1].toPandas()
        geo_data_1[geometry_col] = geo_data_1[geometry_col].apply(wkt.loads)
        geo_data_1 = GeoPandasWrapper.GeoDataFrame(geo_data_1, geometry=geometry_col)

        cols_to_mergs = params['merge_columns']
        merged = geo_data_0.merge(geo_data_1[cols_to_mergs], how="left", left_on=params['join_column'], right_on=params['join_column'])
        return merged

    @staticmethod 
    def _handle_row(row):
        if row["geometry"] is not None:
            return row["geometry"]

        origin = geopy.Point(row["latitude"], row["longitude"])
        north_point = geopy.distance.distance(kilometers=0.1).destination(origin, 0)
        southeast_point = geopy.distance.distance(kilometers=0.1).destination(origin, 135)
        southwest_point = geopy.distance.distance(kilometers=0.1).destination(origin, 225)
        return geometry.Polygon(
            [
                [north_point.longitude, north_point.latitude],
                [southeast_point.longitude, southeast_point.latitude],
                [southwest_point.longitude, southwest_point.latitude],
            ]
        )

    @staticmethod 
    def _create_poly(dataFrame) -> DataFrame:
        dataFrame["geometry"] = dataFrame.apply(
            WildfireGoldTask._handle_row, axis=1
        )

        return dataFrame

    @staticmethod 
    def _convert_NaN_to_None(dataFrame) -> array:
          return dataFrame.where(
              PandasWrapper.notnull(dataFrame), None
          )

def entrypoint():  # pragma: no cover
    conf = {
      "source_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.silver.california_fires_historical_points_good",
        },
        {
          "type": "databricks",
          "table": "ahd_wildfires.bronze.california",
        }        
      ],
      "destination_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.gold.california_fires_historical_points_good",
        }
      ]
    } if "true" == os.environ.get('LOCAL') else None
    task = WildfireGoldTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()
