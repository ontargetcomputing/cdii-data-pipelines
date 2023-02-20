from cdii_data_pipelines.tasks.task import Task
from cdii_data_pipelines.integrations.datasource_factory import DatasourceFactory
from cdii_data_pipelines.integrations.datasource_type import DatasourceType
from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
from array import array
from abc import abstractmethod
import os

class ETLTask(Task):
    """
    ETLTask is an abstract class provides standard methods for ETL processes.  Child classes must implement the abstract classes.
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(ETLTask, self).__init__(spark=spark, init_conf=init_conf)
      self.sources = self.__prepare_source_datasources(params=init_conf)
      self.destinations = self.__prepare_destination_datasources(params=init_conf)

    def __prepare_source_datasources(self, params: dict=None) -> array:
        sources = []
        
        params = [] if ( params is None or 'source_datasources' not in params) else params['source_datasources']
        for integration in params:
            print(f'Configuring source:${integration}')
            sources.append(DatasourceFactory.getDatasource(DatasourceType(integration['type']), params=integration, dbutils=self.dbutils, stage=self.stage))

        return sources

    def __prepare_destination_datasources(self, params: dict=None) -> array:
        destinations = []
        print(params)
        params = [] if ( params is None or 'destination_datasources' not in params) else params['destination_datasources']
        for integration in params:
            print(f'Configuring destination:${integration}')
            destinations.append(DatasourceFactory.getDatasource(DatasourceType(integration['type']), params=integration, dbutils=self.dbutils, stage=self.stage))

        return destinations

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

