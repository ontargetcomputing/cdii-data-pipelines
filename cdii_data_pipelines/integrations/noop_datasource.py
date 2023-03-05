from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
import pyspark.sql as pd
from pyspark.pandas import DataFrame
from pyspark.sql.types import StructType

class NoopDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        return spark.createDataFrame([], StructType([]))

    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        print("Dataset successfully written")

    def truncate(self, params: dict=None, spark: SparkSession=None):
        print("Truncating") 