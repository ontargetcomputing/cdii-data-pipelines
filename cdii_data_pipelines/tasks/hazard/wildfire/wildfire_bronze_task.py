from cdii_data_pipelines.tasks.bronze_task import BronzeTask
import os

class WildfireBronzeTask(BronzeTask):
    """
    """
    def __init__(self, init_conf: dict=None):
      super(WildfireBronzeTask, self).__init__(init_conf=init_conf)

def entrypoint():  # pragma: no cover
    conf = {
      "source": {
        "dataset_id": "d957997ccee7408287a963600a77f61f",
        "layer": 0,
      },
      "destination": {
        "table": "california_fires_historical_points",
        "primary_key": "IrwinID",
        "modified_field": "ModifiedOnDateTime",
        "geometry_field": "geom"
      },
      "agol_url": "https://chhsagency.maps.arcgis.com/home/",      
      } if "true" == os.environ.get('DEVELOPMENT') else None
    task = WildfireBronzeTask(init_conf=conf)
    task.launch()

if __name__ == '__main__':
    entrypoint()
