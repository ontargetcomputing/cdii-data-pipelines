from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.pandas.pandas_helper import PandasHelper
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
from array import array
import geopandas as gpd

class WildfireGoldTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(WildfireGoldTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        points_pyspark_pandas = dataFrames[0]
        ca_pyspark_pandas = dataFrames[1]

        points_gpd = PandasHelper.pysparksql_to_geopandas(dataFrames[0])
        ca_gpd = PandasHelper.pysparksql_to_geopandas(dataFrames[1])
        ca_identified_points = WildfireGoldTask._filter_to_california(data=points_gpd, california=ca_gpd)

        return dataFrames

    @staticmethod 
    def _filter_to_california(data: gpd.GeoDataFrame=None, california: gpd.GeoDataFrame=None) -> DataFrame:
        joined = gpd.sjoin(data, california, how="left", predicate="intersects")
        joined.drop(columns=['index', 'index_right'], inplace=True)
        return joined

    @staticmethod 
    def _merge_datasets(dataFrames: array=None) -> array:
        return dataFrames[0]

    @staticmethod 
    def _create_poly(dataFrame: array=None) -> DataFrame:
        return dataFrame

    @staticmethod 
    def _convert_NaN_to_None(dataFrames: array=None) -> array:
        return dataFrames

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