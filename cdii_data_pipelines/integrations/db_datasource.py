from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
import pandas as pd

# Databricks Datasource
class DatabricksDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None ):
        pass

    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        print(f'Reading from : {params}')
        
        dataframe = spark.read.table(params['table'])
        pandas_df = dataframe.to_pandas_on_spark()
        
        print(f'Read {len(pandas_df)} records')
        return pandas_df

    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        print(f'Writing dataset with {len(dataFrame)} records')
        table_name = params['table']
        spark_dataFrame = spark.createDataFrame(pd.DataFrame(dataFrame))
        spark_dataFrame.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)
        print("Dataset successfully written")

