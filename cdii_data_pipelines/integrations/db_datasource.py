from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# Databricks Datasource
class DatabricksDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None, spark: SparkSession=None ):
        self.params = params
        self.spark = spark

    def read(self) -> DataFrame:
        dataframe = self.spark.read.table(self.params['table'])
        return dataframe

    def write(self, dataFrame: DataFrame):
        table_name = self.params['table']

        if dataFrame.count() > 0:
          DATA_TYPES = 'data_types'
          if DATA_TYPES in self.params.keys():
              data_types = self.params[DATA_TYPES]
              for data_type in data_types:
                  column = data_type['column']
                  type = data_type['type']
                  print(f'Casting {column} to {type}')
                  dataFrame = dataFrame.withColumn(column, dataFrame[column].cast(type))
          else:
              print(f'No datatypes to cast')

          dataFrame.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)



    def truncate(self): 
        table_name = self.params['table']
        delta_table = DeltaTable.forPath(self.spark, table_name)
        delta_table.truncate()
