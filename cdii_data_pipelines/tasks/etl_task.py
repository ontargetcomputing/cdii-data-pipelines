from cdii_data_pipelines.tasks.task import Task
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from abc import abstractmethod

class ETLTask(Task):
    """
    ETLTask is an abstract class provides standard methods for ETL processes.  Child classes must implement the abstract classes.
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(ETLTask, self).__init__(spark, init_conf)

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract method - used to pull data from a source
        :return:
        """
        pass 

    @abstractmethod
    def transform(self, dataFrame: DataFrame) -> DataFrame:
        """
        Transform method - used to transform data received from source in preparation of loading
        :return:
        """
        pass 

    @abstractmethod
    def load(self, dataFrame: DataFrame):
        """
        Load method - used to load the transformed source data into the final data store
        :return:
        """
        pass 


    def launch(self):
        """
        Main method of the task.
        :return:
        """
        self.extract()
        self.transform()
        self.load

