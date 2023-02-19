from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.integrations.datasource_type import DatasourceType
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
from datetime import datetime
import pytz
from shapely import wkt
import os

class BronzeTask(ETLTask):
    """
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None, source_datasource_type: DatasourceType=DatasourceType.AGOL, destination_datasource_type: DatasourceType=DatasourceType.DATABRICKS):
      super(BronzeTask, self).__init__(spark=spark, init_conf=init_conf, source_datasource_type=source_datasource_type, destination_datasource_type=destination_datasource_type)

    def transform(self, dataFrame: DataFrame, params: dict=None) -> DataFrame:
        print("888881")
        if 'SHAPE' in dataFrame.columns:
            dataFrame = dataFrame.drop(columns=['SHAPE'])

        STANDARD_GEOMETRY_FIELD = 'geometry'
        if STANDARD_GEOMETRY_FIELD in dataFrame.columns:
            destination_geomerty_field = params['destination']['geometry_field']
            dataFrame[STANDARD_GEOMETRY_FIELD] = dataFrame[STANDARD_GEOMETRY_FIELD].apply(lambda x: wkt.dumps(x))
            if destination_geomerty_field != STANDARD_GEOMETRY_FIELD:
                dataFrame.rename(columns={STANDARD_GEOMETRY_FIELD: destination_geomerty_field}, inplace=True)

        dataFrame["ade_date_submitted"] = datetime.now(pytz.timezone("America/Los_Angeles")).date()

        return dataFrame


def entrypoint():  # pragma: no cover
    conf = {
      "source": {
        "dataset_id": "d957997ccee7408287a963600a77f61f",
        "layer": 0
      },
      "destination": {
        "table": "ahd_wildfires.bronze.california_fires_historical_points_good",
        "geometry_field": "geom"
      },
      "agol_url": "https://chhsagency.maps.arcgis.com/home/",
      } if "true" == os.environ.get('LOCAL') else None
    task = BronzeTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()