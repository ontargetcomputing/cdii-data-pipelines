from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# Databricks Datasource
class DatabricksDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None, spark: SparkSession=None ):
        self.params = params
        self.spark = spark

    def read(self) -> DataFrame:
        dataframe = self.spark.read.table(self.params['table'])
        return dataframe

    def write(self, dataFrame: DataFrame):
        table_name = self.params['table']
        dataFrame.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)

    def truncate(self): 
        table_name = self.params['table']
        delta_table = DeltaTable.forPath(self.spark, table_name)
        delta_table.truncate()
