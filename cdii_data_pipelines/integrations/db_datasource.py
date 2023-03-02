from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# Databricks Datasource
class DatabricksDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        dataframe = spark.read.table(params['table'])
        return dataframe

    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        table_name = params['table']

        if dataFrame.count() > 0:
          DATA_TYPES = 'data_types'
          if DATA_TYPES in params.keys():
              data_types = params[DATA_TYPES]
              for data_type in data_types:
                  column = data_type['column']
                  type = data_type['type']
                  print(f'Casting {column} to {type}')
                  dataFrame = dataFrame.withColumn(column, dataFrame[column].cast(type))
          else:
              print(f'No datatypes to cast')          
          dataFrame.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)
        else:
          truncate_on_empty = False
          TRUNCATE_ON_EMPTY = 'truncate_on_empty'
          if TRUNCATE_ON_EMPTY in params.keys():
              truncate_on_empty = params[TRUNCATE_ON_EMPTY]

          if truncate_on_empty is True:
              dataFrame.truncate()

