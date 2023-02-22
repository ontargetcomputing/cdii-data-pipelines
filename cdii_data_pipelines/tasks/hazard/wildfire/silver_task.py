from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode, regexp_replace
import os
from array import array

IRWINID = "IRWINID"
BAD_IRWINID = "IrwinId"

class WildfireSilverTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(WildfireSilverTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        dataFrame = dataFrames[0]
        dataFrame = WildfireSilverTask._create_upper_irwinid(dataFrame)
        dataFrame = WildfireSilverTask._unnest(dataFrame)
        dataFrame = WildfireSilverTask._drop_duplicates(dataFrame)
        dataFrames[0] = dataFrame
        return dataFrames

    @staticmethod 
    def _create_upper_irwinid(dataFrame: DataFrame=None) -> DataFrame:
        # points has "IrwinID" while perims has "IRWINID"
        return dataFrame.withColumnRenamed(BAD_IRWINID, IRWINID) if BAD_IRWINID in dataFrame.columns else dataFrame

    @staticmethod 
    def _unnest(dataFrame: DataFrame=None) -> DataFrame:
        cooked_df = dataFrame.withColumn(IRWINID, explode(split(IRWINID,',')))
        cooked_df = cooked_df.withColumn(IRWINID, regexp_replace(IRWINID, '\{|\}', ''))
        return cooked_df       

    @staticmethod 
    def _drop_duplicates(dataFrame: DataFrame=None) -> DataFrame:
        return dataFrame.drop('OBJECTID').dropDuplicates()

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
          "table": "ahd_wildfires.silver.california_fires_historical_points_good",
        }
      ]
    } if "true" == os.environ.get('LOCAL') else None
    task = WildfireSilverTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()