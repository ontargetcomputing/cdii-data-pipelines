from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.pandas.pandas_helper import PandasHelper
from cdii_data_pipelines.pandas.geopandas_wrapper import GeoPandasWrapper
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode, regexp_replace
import os
from array import array

IRWINID = "IRWINID"
BAD_IRWINID = "IrwinId"

class WildfireSilverTask(ETLTask):
    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(WildfireSilverTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        dataFrame = dataFrames[0]
        dataFrame = WildfireSilverTask._create_upper_irwinid(dataFrame)
        dataFrame = WildfireSilverTask._unnest(dataFrame)
        print(f'Before _drop_duplicates had {dataFrame.count()} rows')
        dataFrame = WildfireSilverTask._drop_duplicates(dataFrame)
        print(f'After _drop_duplicates had {dataFrame.count()} rows')

        data_gpd = PandasHelper.pysparksql_to_geopandas(dataFrame)
        ca_gpd = PandasHelper.pysparksql_to_geopandas(dataFrames[1])
        county_gpd = PandasHelper.pysparksql_to_geopandas(dataFrames[2])
        
        print(f'Before _filter_to_california had {len(data_gpd)} rows')
        ca_identified_data = WildfireSilverTask._filter_to_california(data=data_gpd, california=ca_gpd)
        print(f'After _filter_to_california had {len(ca_identified_data)} rows')
        
        final_data = WildfireSilverTask._add_county_info(data=ca_identified_data, county=county_gpd)
        #final_data = WildfireSilverTask._condense(data=final_data)
        #convert back to sql dataframe
        dataFrames[0] = PandasHelper.geopandas_to_pysparksql(final_data, spark=self.spark)
        return dataFrames

    @staticmethod 
    def _create_upper_irwinid(dataFrame: DataFrame=None) -> DataFrame:
        # points has "IrwinID" while perims has "IRWINID"
        return dataFrame.withColumnRenamed(BAD_IRWINID, IRWINID) if BAD_IRWINID in dataFrame.columns else dataFrame

    @staticmethod 
    def _unnest(dataFrame: DataFrame=None) -> DataFrame:
        print(f'Unnest: started with {dataFrame.count()} records')
        cooked_df = dataFrame.withColumn(IRWINID, explode(split(IRWINID,',')))
        cooked_df = cooked_df.withColumn(IRWINID, regexp_replace(IRWINID, '\{|\}', ''))
        print(f'Unnest: ended with {cooked_df.count()} records')
        return cooked_df

    @staticmethod 
    def _drop_duplicates(dataFrame: DataFrame=None) -> DataFrame:
        return dataFrame.drop('OBJECTID').dropDuplicates()
    
    @staticmethod 
    def _filter_to_california(data, california) -> DataFrame:
        joined = GeoPandasWrapper.sjoin(data, california, how="inner", predicate="intersects")
        joined.drop(columns=['index', 'index_right', 'NAME'], inplace=True)
        return joined

    @staticmethod 
    def _add_county_info(data, county) -> DataFrame:
        joined_data_merged_with_counties = GeoPandasWrapper.sjoin(data, county, how="left", predicate="intersects")
        joined_data_merged_with_counties.rename({"NAME": "county", "caloes_region": "oes_region"}, axis=1, inplace=True)
        joined_data_merged_with_counties.drop(columns=['index_right'], inplace=True)        

        return joined_data_merged_with_counties

    # # @staticmethod 
    # # def _condense(data: gpd.GeoDataFrame=None) -> DataFrame:
    # #     irwin_id = "IRWINID"

    # #     condensed = data.groupby(irwin_id)["county"].apply(
    # #         lambda x: ", ".join(list(set(x)))
    # #     )
    # #     condensed = pd.DataFrame(condensed)
    # #     condensed["oes_region"] = data.groupby(irwin_id)["oes_region"].apply(
    # #         lambda x: ", ".join(list(set(x)))
    # #     )

    # #     data = data.merge(condensed, on=irwin_id)

    # #     return data



def entrypoint():  # pragma: no cover
    conf = {
      "source_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.bronze.california_fires_historical_points_good",
        }
      ],
      "destination_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.silver.california_fires_historical_points_good",
        }
      ]
    } if "true" == os.environ.get('LOCAL') else None
    task = WildfireSilverTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()