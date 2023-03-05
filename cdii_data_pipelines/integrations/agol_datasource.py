from cdii_data_pipelines.integrations.datasource import Datasource
from cdii_data_pipelines.pandas.pandas_helper import PandasHelper
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from arcgis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayer
import json
import geopandas as gpd
from shapely import wkt

class AgolDatasource(Datasource):
    """
    """
    def __init__(self, params: dict=None, spark: SparkSession=None ):
        self.gis = GIS(params['url'], params['username'], params['password'])
        self.params = params
        self.spark = spark

    def read(self) -> DataFrame:
        if self.params is None:
          raise TypeError("params:NoneType not allowed, params:dict exptected")
        
        datasetId = self.params['dataset_id']
        layer = self.params['layer']

        print(f'loadFeatureLayer { { "source": datasetId, "layer": layer}}')
        dataLayer = self.gis.content.get(datasetId)
        featureLayer = FeatureLayer(dataLayer.layers[layer].url)        
        featureSet = featureLayer.query()
        gdf = featureSet.sdf
        if len(gdf) > 0:
          gjsonString = featureSet.to_geojson
          gjsonDict = json.loads(gjsonString)
          transformed = gpd.GeoDataFrame.from_features(gjsonDict["features"])

          geom = transformed["geometry"]
          gdf: gpd.GeoDataFrame = gpd.GeoDataFrame(gdf, crs=f'EPSG:4326', geometry=geom)

        return PandasHelper.geopandas_to_pysparksql(gpd_df=gdf, spark=self.spark)
        
    def write(self, dataFrame: DataFrame):
        pass 

    def truncate(self):
        pass