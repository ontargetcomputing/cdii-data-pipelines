from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from abc import abstractmethod

class Datasource():
    """
    """
    def __init__(self, params: dict=None):
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass 

    @abstractmethod
    def write(self, dataFrame: DataFrame):
        pass 

    @abstractmethod
    def truncate(self):
        pass 