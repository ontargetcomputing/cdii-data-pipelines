from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode, regexp_replace
import os
from array import array

# TODO this should be a generic task
class DashboardWildfireGreenTask(ETLTask):

    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(DashboardWildfireGreenTask, self).__init__(spark=spark, init_conf=init_conf)
 
    def transform(self, dataFrames: array, params: dict=None) -> array:
        print("Transforming")
        dataFrame = dataFrames[0]
        dataFrame = DashboardWildfireGreenTask._filter_to_california(dataFrame)

        dataFrames[0] = dataFrame
        return dataFrames

    @staticmethod 
    def _filter_to_california(dataFrame: DataFrame=None) -> DataFrame:
        return dataFrame

    @staticmethod 
    def _merge_datasets(dataFrames: array=None) -> array:
        return dataFrames[0]

    @staticmethod 
    def _convert_NaN_to_None(dataFrames: array=None) -> array:
        return dataFrames

def entrypoint():  # pragma: no cover
    conf = {
      "source_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.silver.california_fires_historical_points_good",
        }
      ],
      "destination_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.gold.california_fires_historical_points_good",
        }
      ]
    } if "true" == os.environ.get('LOCAL') else None
    task = DashboardWildfireGreenTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()



# #DO IN GREEN LAYER
# fire_points_intersecting_ca = fire_points_intersecting_ca.assign(Filtered=lambda x: ((((x.CalculatedAcres > 5) | x.DailyAcres > 5)) 
#     & 
#     (x.IncidentTypeCategory != "RX") 
#     & 
#     (x.PercentContained != 100))
#     ^
#     True
# )      

# fire_perim_merged["geometry"] = fire_perim_merged.apply(
#     create_poly_data.execute, axis=1
# )
# fire_perim_merged["SHAPE"] = fire_perim_merged.apply(
#     create_poly_agol.execute, axis=1
# )


# fire_perim_merged = fire_perim_merged.where(
#       pd.notnull(fire_perim_merged), None
#   )
#   fire_points_merged = fire_points_merged.where(
#       pd.notnull(fire_points_merged), None
#   )

# 

  # fire_points_merged = fire_points_merged.merge(
  #     fire_points_counties_condensed, on="IrwinID"
  # )
  # fire_perim_merged = fire_perim_merged.merge(
  #     fire_perim_counties_condensed, on="IrwinID"
  # )

# dummy_county_data = county_data[['county', 'caloes_region']].copy()
# dummy_county_data = dummy_county_data.rename(columns={"caloes_region": "oes_region"})
# dummy_county_data = dummy_county_data.assign(GISAcres=0)
# dummy_county_data = dummy_county_data.assign(DailyAcres=0)
# dummy_county_data = dummy_county_data.assign(CalculatedAcres=0)

# dummy_county_data = dummy_county_data.assign(IrwinID=None)
# dummy_county_data = dummy_county_data.assign(Label=None)
# dummy_county_data = dummy_county_data.assign(ComplexName=None)
# dummy_county_data = dummy_county_data.assign(ComplexID=None)
# dummy_county_data = dummy_county_data.assign(PercentContained=None)
# dummy_county_data = dummy_county_data.assign(POOCounty=None)
# dummy_county_data = dummy_county_data.assign(SHAPE=None)
# dummy_county_data = dummy_county_data.assign(IncidentName=None)
# dummy_county_data = dummy_county_data.assign(FireDiscoveryDateTime=None)

# final_fire_points = fire_points_merged.copy()
# final_fire_perim = pd.concat([fire_perim_merged, dummy_county_data], ignore_index=True, sort=False)

        # cooked_fire_points = final_fire_points[AGOL_COLS].rename(COLUMN_RENAME, axis=1)
        # cooked_fire_perim = final_fire_perim[AGOL_COLS].rename(COLUMN_RENAME, axis=1)
