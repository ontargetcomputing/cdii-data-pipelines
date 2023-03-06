from cdii_data_pipelines.integrations.datasource import Datasource
from cdii_data_pipelines.integrations.agol_datasource import AgolDatasource
from cdii_data_pipelines.integrations.db_datasource import DatabricksDatasource
from cdii_data_pipelines.integrations.noop_datasource import NoopDatasource
from cdii_data_pipelines.integrations.datasource_type import DatasourceType
import os
from pyspark.sql import SparkSession

class DatasourceFactory:
    @staticmethod
    def getDatasource(type: DatasourceType=DatasourceType.DATABRICKS, params: dict=None, dbutils=None, spark: SparkSession=None, stage: str='DEV') -> Datasource:
        print(f'Constructing Datasource : ${params}')
        if type == DatasourceType.AGOL:
            return DatasourceFactory.getAgolDatasource(params=params, dbutils=dbutils, spark=spark, stage=stage)
        elif type == DatasourceType.DATABRICKS:
            return DatasourceFactory.getDatabricksDatasource(params=params, spark=spark)
        elif type == DatasourceType.NOOP:
            return DatasourceFactory.getNoopDatasource(params)
        else:
            raise ValueError(type)
    
    @staticmethod
    def getAgolDatasource(params: dict=None, dbutils=None, spark: SparkSession=None, stage: str='DEV') -> AgolDatasource:
          if dbutils is None or os.environ.get('LOCAL') == 'true':
              print('Reading AGOL CREDS from environment')
              agol_user = os.environ.get(f'AGOL_USERNAME_{stage}')
              agol_password = os.environ.get(f'AGOL_PASSWORD_{stage}')
          else:
              print('Reading AGOL CREDS from dbutils')
              agol_user = dbutils.secrets.get("SECRET_KEYS", f'AGOL_USERNAME_{stage}')
              agol_password = dbutils.secrets.get("SECRET_KEYS", f'AGOL_PASSWORD_{stage}')
          
          params['username'] = agol_user
          params['password'] = agol_password
          print(f'*********construct with {params}')
          return AgolDatasource( params=params, spark=spark)

    @staticmethod
    def getDatabricksDatasource(params: dict=None, dbutils=None, spark: SparkSession=None, stage: str='DEV'):
        return DatabricksDatasource(params=params, spark=spark)

    @staticmethod
    def getNoopDatasource(params: dict=None, dbutils=None, stage: str='DEV'):
        return NoopDatasource()