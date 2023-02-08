from cdii_data_pipelines.integrations.datasource import DataSource
from pyspark.sql import SparkSession
from delta.tables import *
from shapely import wkt
import pandas as pd
from pandas import DataFrame

# Databricks Datasource
class DatabricksDataSource(DataSource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        raise NotImplementedError
        # if params is None:
        #   raise TypeError("params:NoneType not allowed, params:dict exptected")

        
    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        table_name = params['table']
        primaryKey = params['primary_key']
        modified_field = params['modified_field']
        geometry_field = params['geometry_field']
        
        table = DeltaTable.forName(spark.getActiveSession(), table_name)
        GEOMETRY = 'geometry'
        if geometry_field != GEOMETRY:
          dataFrame[geometry_field] = dataFrame[GEOMETRY]
          dataFrame.drop([GEOMETRY], axis=1)

        dataFrame[geometry_field] = dataFrame[geometry_field].apply(lambda x: wkt.dumps(x))
        df = spark.createDataFrame(pd.DataFrame(dataFrame))
        
        joinCondition = f'table.{primaryKey} = updates.{primaryKey}'
        modifiedCondition = ''
        if modified_field:
            modifiedCondition = f'updates.{modified_field} != table.{modified_field}'

        # Merge to the table
        print(joinCondition)
        table.alias('table').merge(df.alias('updates'), joinCondition).whenMatchedUpdateAll(modifiedCondition).whenNotMatchedInsertAll().execute()
        self.logger.info("Dataset successfully written")

