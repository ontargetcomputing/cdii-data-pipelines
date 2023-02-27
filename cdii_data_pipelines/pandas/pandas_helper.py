
import pyspark as pyspark
import geopandas as gpd
from shapely import wkt

class PandasHelper():
    
    @staticmethod
    def pysparksql_to_geopandas(pysparksql_df: pyspark.sql.DataFrame=None):
        if type(pysparksql_df) is not pyspark.sql.DataFrame:
            raise ValueError(f'Unexpected type;{type(pysparksql_df)}') 

        geometry_column = 'geometry'
        pandas_df = pysparksql_df.toPandas() 
        pandas_df[geometry_column] = pandas_df[geometry_column].apply(wkt.loads)
        return gpd.GeoDataFrame(pandas_df, geometry=geometry_column)

