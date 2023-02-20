from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.integrations.datasource_type import DatasourceType
from pyspark.sql import SparkSession
import os
from array import array

class WildfireSilverTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(WildfireSilverTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        df = dataFrames[0]
        print(len(df))
        return dataFrames


def entrypoint():  # pragma: no cover
    conf = {
      "source_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.bronze.california_fires_historical_points_good",
        }
      ],
      "destination_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.silver.california_fires_points",
        }
      ]
    } if "true" == os.environ.get('LOCAL') else None
    task = WildfireSilverTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()