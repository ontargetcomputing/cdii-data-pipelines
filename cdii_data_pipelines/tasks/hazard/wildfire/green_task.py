from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode, regexp_replace
import os
from array import array

# TODO this should be a generic task
class DashboardWildfireGreenTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(DashboardWildfireGreenTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        dataFrame = dataFrames[0]
        dataFrame = DashboardWildfireGreenTask._filter_to_california(dataFrame)

        dataFrames[0] = dataFrame
        return dataFrames

    @staticmethod 
    def _filter_to_california(dataFrame: DataFrame=None) -> DataFrame:
        return dataFrame

    @staticmethod 
    def _merge_datasets(dataFrames: array=None) -> array:
        return dataFrames[0]

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
    task = DashboardWildfireGreenTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()