from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
import pandas as pd

# Databricks Datasource
class DatabricksDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        raise NotImplementedError
        # if params is None:
        #   raise TypeError("params:NoneType not allowed, params:dict exptected")

        
    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        print("Writing dataset")
        table_name = params['table']
        spark_dataFrame = spark.createDataFrame(pd.DataFrame(dataFrame))
        spark_dataFrame.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)
        print("Dataset successfully written")

