from cdii_data_pipelines.tasks.task import Task
from cdii_data_pipelines.integrations.datasource_factory import DatasourceFactory
from cdii_data_pipelines.integrations.datasource_type import DatasourceType
from pyspark.sql import SparkSession
from array import array
import pandas as pd
from abc import abstractmethod

class ETLTask(Task):
    """
    ETLTask is an abstract class provides standard methods for ETL processes.  Child classes must implement the abstract classes.
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(ETLTask, self).__init__(spark=spark, init_conf=init_conf)
      self.sources = self.__prepare_source_datasources(params=self.conf)
      self.destinations = self.__prepare_destination_datasources(params=self.conf)

    def __prepare_source_datasources(self, params: dict=None) -> array:
        sources = []
        
        params = [] if ( params is None or 'source_datasources' not in params) else params['source_datasources']
        for integration in params:
            print(f'Configuring source:${integration}')
            sources.append(DatasourceFactory.getDatasource(DatasourceType(integration['type']), params=integration, dbutils=self.dbutils, spark=self.spark, stage=self.stage))

        return sources

    def __prepare_destination_datasources(self, params: dict=None) -> array:
        destinations = []
        print(params)
        params = [] if ( params is None or 'destination_datasources' not in params) else params['destination_datasources']
        for integration in params:
            print(f'Configuring destination:${integration}')
            destinations.append(DatasourceFactory.getDatasource(DatasourceType(integration['type']), params=integration, dbutils=self.dbutils, spark=self.spark, stage=self.stage))

        return destinations

    def extract(self, params: dict=None) -> array:
        """
        Extract method - used to pull data from a source
        :return:
        """
        pd.set_option('expand_frame_repr', False)
        pd.set_option('display.max_rows', False)
        dataFrames = []
        for index, source in enumerate(self.sources):
            print(f'Reading from : {params["source_datasources"][index]}')
            df = source.read()
            print(df.head())
            dataFrames.append(df)

        
        return dataFrames

    def load(self, dataFrames: array, params: dict=None):
        """
        Load method - used to load the transformed source data into the final data store
        :return:
        """
        print(f'Loading: ${self.destinations}')
        for index, destination in enumerate(self.destinations):
            print(f'Writing to : ${params["destination_datasources"][index]}')
            df = dataFrames[index]
            destination_params = params['destination_datasources'][index]

            if df.count() > 0:
                destination.write(dataFrames[index])
            else:
                truncate_on_emtpy = destination_params["truncate_on_emtpy"]
                if truncate_on_emtpy is True:
                    print('Truncating on empty')
                    destination.truncate(spark=self.spark)
                else:
                    print('Dataframe empty - nothing to write')

    @abstractmethod
    def transform(self, dataFrames: array,  params: dict=None) -> array:
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
        print('********************************************')
        print('*************   Extracting     *************')
        print('********************************************')
        dataFrames = self.extract(params=self.conf)
        print('********************************************')
        print('**********   Transforming     **************')
        print('********************************************')
        dataFrames = self.transform(dataFrames=dataFrames, params=self.conf)
        print('********************************************')
        print('*************   Loading ********************')
        print('********************************************')
        self.load(dataFrames=dataFrames, params=self.conf)

