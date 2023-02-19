from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.integrations.datasource import Datasource
from cdii_data_pipelines.integrations.db_datasource import DatabricksDatasource
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame


class ConcreteETLTask(ETLTask):
    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(ConcreteETLTask, self).__init__(spark=spark, init_conf=init_conf)

    def transform(self, dataFrame: DataFrame,  params: dict=None) -> DataFrame:
      return None

def test_default_source_datasource_is_databricks():
    concreteETLTask = ConcreteETLTask()
    assert type(concreteETLTask.source) is DatabricksDatasource

def test_default_destination_datasource_is_databricks():
    concreteETLTask = ConcreteETLTask()
    assert type(concreteETLTask.destination) is DatabricksDatasource