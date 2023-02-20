from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.integrations.datasource_type import DatasourceType
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
import os

class WildfireSilverTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None, source_datasource_type: DatasourceType=DatasourceType.AGOL, destination_datasource_type: DatasourceType=DatasourceType.DATABRICKS):
      super(WildfireSilverTask, self).__init__(spark=spark, init_conf=init_conf, source_datasource_type=source_datasource_type, destination_datasource_type=destination_datasource_type)
 
    def transform(self, dataFrame: DataFrame, params: dict=None) -> DataFrame:
        print("********transform")
        return dataFrame


def entrypoint():  # pragma: no cover
    conf = {
      "source_datasources": [
        {
          "type": "agol",
          "table": "ahd_wildfires.bronze.california_fires_historical_points_good",
        }
      ],
      "destination_datasources": [
        {
          "type": "agol",
          "table": "ahd_wildfires.silver.california_fires_points",
        }
      ]
    } if "true" == os.environ.get('LOCAL') else None
    task = WildfireSilverTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()