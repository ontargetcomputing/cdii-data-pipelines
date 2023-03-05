from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
import pyspark.sql as pd
from pyspark.pandas import DataFrame
from pyspark.sql.types import StructType

class NoopDatasource(Datasource):
    """
    """
    def __init__(self, spark: SparkSession=None):
        self.spark = spark
        pass

    def read(self) -> DataFrame:
        return self.spark.createDataFrame([], StructType([]))

    def write(self, dataFrame: DataFrame):
        print("Dataset successfully written")

    def truncate(self):
        print("Truncating") 