from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from abc import abstractmethod

class Datasource():
    """
    """
    def __init__(self, params: dict=None):
        pass

    @abstractmethod
    def read(self, spark: SparkSession=None) -> DataFrame:
        pass 

    @abstractmethod
    def write(self, dataFrame: DataFrame, spark: SparkSession=None):
        pass 

    @abstractmethod
    def truncate(self, params: dict=None, spark: SparkSession=None):
        pass 