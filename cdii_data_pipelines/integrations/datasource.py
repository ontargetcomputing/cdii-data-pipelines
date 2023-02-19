from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
from abc import abstractmethod

class DataSource():
    """
    """
    def __init__(self):
        pass

    @abstractmethod
    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        pass 

    @abstractmethod
    def write(self, dataFrame: DataFrame):
        pass 
