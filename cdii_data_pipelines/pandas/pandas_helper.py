
import pyspark as pyspark
import geopandas as gpd
from shapely import wkt
from pyspark.sql import SparkSession

class PandasHelper():
    
    @staticmethod
    def pysparksql_to_geopandas(pysparksql_df: pyspark.sql.DataFrame=None):
        if type(pysparksql_df) is not pyspark.sql.DataFrame:
            raise ValueError(f'Unexpected type;{type(pysparksql_df)}') 

        geometry_column = 'geometry'
        pandas_df = pysparksql_df.toPandas() 
        pandas_df[geometry_column] = pandas_df[geometry_column].apply(wkt.loads)
        return gpd.GeoDataFrame(pandas_df, geometry=geometry_column)

    @staticmethod
    def geopandas_to_pysparksql(gpd_df: gpd.GeoDataFrame=None, spark: SparkSession=None):
        if type(gpd_df) is not gpd.GeoDataFrame:
            raise ValueError(f'Unexpected type;{type(gpd_df)}') 

        if 'SHAPE' in gpd_df.columns:
            gpd_df = gpd_df.drop(columns=['SHAPE'])

        STANDARD_GEOMETRY_FIELD = 'geometry'
        if STANDARD_GEOMETRY_FIELD in gpd_df.columns:
            gpd_df[STANDARD_GEOMETRY_FIELD] = gpd_df[STANDARD_GEOMETRY_FIELD].apply(lambda x: wkt.dumps(x))

        return spark.createDataFrame(gpd_df)
