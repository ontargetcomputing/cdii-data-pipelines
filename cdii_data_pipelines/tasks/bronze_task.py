from cdii_data_pipelines.tasks.etl_task import ETLTask
from cdii_data_pipelines.integrations.datasource import DataSource
from pyspark.sql import SparkSession
from pandas import DataFrame
from datetime import datetime
import pytz
from shapely import wkt

class BronzeTask(ETLTask):
    """
    BronzeTask is an ETLTask where no transformation is done on the data.  It is kept raw.
    ETLTask.transform is provided, however, child classes must implement the.
    """
    def __init__(self, spark: SparkSession=None, init_conf: dict=None, source_datasource: DataSource=None):
      super(BronzeTask, self).__init__(spark=spark, init_conf=init_conf, source_datasource=source_datasource)

    def transform(self, dataFrame: DataFrame, params: dict=None) -> DataFrame:
        
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
