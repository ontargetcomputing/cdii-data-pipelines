from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.pandas.geopandas_wrapper import GeoPandasWrapper
from cdii_data_pipelines.pandas.pandas_wrapper import PandasWrapper
from cdii_data_pipelines.pandas.pandas_helper import PandasHelper
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
from array import array
import geopy
import geopy.distance
from shapely import geometry

class WildfireGoldTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(WildfireGoldTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        if dataFrames[0].count() > 0:
            dataFrames = WildfireGoldTask._create_geodataframes(dataFrames, params=params)
            # dataFrames = WildfireGoldTask._create_lat_long(dataFrames, params=params)
            dataFrame = WildfireGoldTask._merge_datasets(dataFrames, params=params)
            # dataFrame = WildfireGoldTask._create_poly(dataFrame)
            dataFrame = WildfireGoldTask._convert_NaN_to_None(dataFrame)
            dataFrame = WildfireGoldTask._drop_columns(dataFrame, params=params)

            dataFramesToReturn = []
            dataFramesToReturn.append(PandasHelper.geopandas_to_pysparksql(gpd_df=dataFrame, spark=self.spark))
            return dataFramesToReturn
        else:
            print('No Data to Transform')
            return dataFrames
        
    @staticmethod 
    def _create_geodataframes(dataFrames: array=None, params: dict=None) -> array:
        geo_data_0 = PandasHelper.pysparksql_to_geopandas(dataFrames[0])
        geo_data_1 = PandasHelper.pysparksql_to_geopandas(dataFrames[1])
        new_dataFrames = []
        new_dataFrames.append(geo_data_0)
        new_dataFrames.append(geo_data_1)
        return new_dataFrames
    
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
    def _drop_columns(dataFrame, params: dict=None) -> array:
        if 'drop_columns' in params.keys():
            cols_to_drop = params['drop_columns']
            print(f'Dropping columns {cols_to_drop}')
            dataFrame = dataFrame.drop(columns=cols_to_drop)
        
        return dataFrame

    @staticmethod 
    def _merge_datasets(dataFrames: array=None, params: dict=None) -> DataFrame:
        if len(dataFrames[0]) == 0 or len(dataFrames[1]) == 0:
            return dataFrames[0]
        
        cols_to_mergs = params['merge_columns']
        merged = dataFrames[0].merge(dataFrames[1][cols_to_mergs], how="left", left_on=params['join_column'], right_on=params['join_column'])
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
