from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.integrations.datasource import DataSource
from pyspark.sql import SparkSession
from pandas import DataFrame
import os

class BronzeTask(ETLTask):
    """
    BronzeTask is an ETLTask where no transformation is done on the data.  It is kept raw.
    ETLTask.transform is provided, however, child classes must implement the.
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None, source_datasource: DataSource=None):
      super(BronzeTask, self).__init__(spark=spark, init_conf=init_conf, source_datasource=source_datasource)

    def transform(self, dataFrame: DataFrame) -> DataFrame:
        return dataFrame
