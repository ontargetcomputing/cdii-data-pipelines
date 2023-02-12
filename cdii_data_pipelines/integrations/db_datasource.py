from cdii_data_pipelines.integrations.datasource import DataSource
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import *
import pandas as pd
from pandas import DataFrame

# Databricks Datasource
class DatabricksDataSource(DataSource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        raise NotImplementedError
        # if params is None:
        #   raise TypeError("params:NoneType not allowed, params:dict exptected")

        
    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        table_name = params['table']

        spark_dataFrame = spark.createDataFrame(pd.DataFrame(dataFrame))

        spark_dataFrame.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)
        
        print("Dataset successfully written")

