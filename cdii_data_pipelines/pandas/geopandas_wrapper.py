import geopandas as gpd

class GeoPandasWrapper():
    
    @staticmethod
    def sjoin(left, right, how, predicate) -> gpd.GeoDataFrame:
        return gpd.sjoin(left, right, how=how, predicate=predicate)
    
    @staticmethod
    def sGeoDataFramejoin(data, geometery) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(data, geometry=geometery)

    