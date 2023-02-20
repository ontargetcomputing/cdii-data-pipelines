from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
import pyspark.pandas as pd
from pyspark.pandas import DataFrame

class NoopDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        return pd.DataFrame()

        
    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        print("Dataset successfully written")

