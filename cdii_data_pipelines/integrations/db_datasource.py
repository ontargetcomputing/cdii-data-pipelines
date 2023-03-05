from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# Databricks Datasource
class DatabricksDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        dataframe = spark.read.table(params['table'])
        return dataframe

    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        table_name = params['table']
        dataFrame.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)

    def truncate(self, params: dict=None, spark: SparkSession=None): 
        table_name = params['table']
        delta_table = DeltaTable.forPath(spark, table_name)
        delta_table.truncate()
