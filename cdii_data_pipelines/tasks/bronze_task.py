from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from abc import abstractmethod

class BronzeTask(ETLTask):
    """
    BronzeTask is an ETLTask where no transformation is done on the data.  It is kept raw.
    ETLTask.transform is provided, however, child classes must implement the.
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
    def transform(self, dataFrame: DataFrame) -> DataFrame:
        return dataFrame
        
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

