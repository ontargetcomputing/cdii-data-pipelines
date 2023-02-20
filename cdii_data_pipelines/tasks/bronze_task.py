from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from datetime import datetime
import pytz
from shapely import wkt
import os
from array import array

class BronzeTask(ETLTask):
    """
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(BronzeTask, self).__init__(spark=spark, init_conf=init_conf)

    def transform(self, dataFrames: array, params: dict=None) -> array:
        new_dataFrames = []
        for dataFrame in dataFrames:
          if 'SHAPE' in dataFrame.columns:
              dataFrame = dataFrame.drop(columns=['SHAPE'])

          STANDARD_GEOMETRY_FIELD = 'geometry'
          if STANDARD_GEOMETRY_FIELD in dataFrame.columns:
              # destination_geomerty_field = params['destination']['geometry_field']
              dataFrame[STANDARD_GEOMETRY_FIELD] = dataFrame[STANDARD_GEOMETRY_FIELD].apply(lambda x: wkt.dumps(x))
              # if destination_geomerty_field != STANDARD_GEOMETRY_FIELD:
              #     dataFrame.rename(columns={STANDARD_GEOMETRY_FIELD: destination_geomerty_field}, inplace=True)

          dataFrame["ade_date_submitted"] = datetime.now(pytz.timezone("America/Los_Angeles")).date()
          new_dataFrames.append(dataFrame)

        return new_dataFrames


def entrypoint():  # pragma: no cover
    conf = {
      "source_datasources": [
        {
          "type": "agol",
          "dataset_id": "d957997ccee7408287a963600a77f61f",
          "layer": 0,
          "url": "https://chhsagency.maps.arcgis.com/home/"
        }
      ],
      "destination_datasources": [
        {
          "type": "databricks",
          "table": "ahd_wildfires.bronze.california_fires_historical_points_good"
        }
      ]
    } if "true" == os.environ.get('LOCAL') else None
    print("Running BronzeTask")
    task = BronzeTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()