from cdii_data_pipelines.tasks.task import Task
from cdii_data_pipelines.integrations.datasource_factory import DataSourceFactory
from cdii_data_pipelines.integrations.datasource import DataSource
from pyspark.sql import SparkSession
from pandas import DataFrame
from abc import abstractmethod
import os

class ETLTask(Task):
    """
    ETLTask is an abstract class provides standard methods for ETL processes.  Child classes must implement the abstract classes.
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None, source_datasource: DataSource=None, destination_datasource: DataSource=None):
      super(ETLTask, self).__init__(spark, init_conf)
      self.source = self.__prepare_source_datasource(source_datasource)
      self.destination = self.__prepare_destination_datasource(destination_datasource)

    def __prepare_source_datasource(self, source_datasource) -> DataSource:
        if not source_datasource:
            return DataSourceFactory.getDataSource('AGOL', params=self.conf, dbutils=self.dbutils, stage=self.stage)
        else:
            return source_datasource

    def __prepare_destination_datasource(self, destination_datasource) -> DataSource:
        if not destination_datasource:
            return DataSourceFactory.getDataSource('DB', params=self.conf, dbutils=self.dbutils, stage=self.stage)
        else:
            return destination_datasource

    def extract(self, params: dict=None) -> DataFrame:
        """
        Extract method - used to pull data from a source
        :return:
        """
        return self.source.read(params['source'], spark=self.spark) 

    def load(self, dataFrame: DataFrame, params: dict=None):
        """
        Load method - used to load the transformed source data into the final data store
        :return:
        """
        self.destination.write(dataFrame=dataFrame, params=params['destination'], spark=self.spark) 

    @abstractmethod
    def transform(self, dataFrame: DataFrame,  params: dict=None) -> DataFrame:
        """
        Transform method - used to transform data received from source in preparation of loading
        :return:
        """
        pass 

    def launch(self):
        """
        Main method of the task.
        :return:
        """
        dataFrame = self.extract(params=self.conf)
        dataFrame = self.transform(dataFrame=dataFrame, params=self.conf)
        self.load(dataFrame=dataFrame, params=self.conf)

