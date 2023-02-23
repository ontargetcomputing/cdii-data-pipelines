from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode, regexp_replace
import os
from array import array

class WildfireGoldTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(WildfireGoldTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        dataFrame = dataFrames[0]
        dataFrame = WildfireGoldTask._filter_to_california(dataFrame)

        dataFrames[0] = dataFrame
        return dataFrames

    @staticmethod 
    def _filter_to_california(dataFrame: DataFrame=None) -> DataFrame:
        return dataFrame

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