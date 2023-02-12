from cdii_data_pipelines.tasks.bronze_task import BronzeTask
import os
from pandas import DataFrame

class WildfirePerimsBronzeTask(BronzeTask):
    """
    """
    def __init__(self, init_conf: dict=None):
      super(WildfirePerimsBronzeTask, self).__init__(init_conf=init_conf)

    def transform(self, dataFrame: DataFrame, params: dict=None) -> DataFrame:
        print("******************************")
        dataFrame['GISAcres'] = dataFrame['GISAcres'].apply(lambda x: x * -1.0)
        return super(WildfirePerimsBronzeTask, self).transform(dataFrame=dataFrame, params=params)


def entrypoint():  # pragma: no cover
    conf = {
      "source": {
        "dataset_id": "d957997ccee7408287a963600a77f61f",
        "layer": 1,
      },
      "destination": {
        "table": "ahd_wildfires.bronze.california_fires_historical_perims_good",
        "geometry_field": "geom"
      },
      "agol_url": "https://chhsagency.maps.arcgis.com/home/",
      } if "true" == os.environ.get('DEVELOPMENT') else None
    task = WildfirePerimsBronzeTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()
