from cdii_data_pipelines.integrations.datasource import Datasource
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
    def __init__(self, params: dict=None ):
        self.gis = GIS(params['url'], params['username'], params['password'])

    @staticmethod
    def _transform(dataFrame):
        if type(dataFrame) is gpd.GeoDataFrame:
          if 'SHAPE' in dataFrame.columns:
              dataFrame = dataFrame.drop(columns=['SHAPE'])

          STANDARD_GEOMETRY_FIELD = 'geometry'
          if STANDARD_GEOMETRY_FIELD in dataFrame.columns:
              dataFrame[STANDARD_GEOMETRY_FIELD] = dataFrame[STANDARD_GEOMETRY_FIELD].apply(lambda x: wkt.dumps(x))
        
        return dataFrame
    
    def read(self, params: dict=None, spark: SparkSession=None) -> DataFrame:
        if params is None:
          raise TypeError("params:NoneType not allowed, params:dict exptected")
        
        datasetId = params['dataset_id']
        layer = params['layer']

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

        return spark.createDataFrame(AgolDatasource._transform(gdf))
        
    def write(self, dataFrame: DataFrame, params: dict=None, spark: SparkSession=None):
        pass 
