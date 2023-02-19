from cdii_data_pipelines.integrations.datasource import Datasource
from cdii_data_pipelines.integrations.agol_datasource import AgolDatasource
from cdii_data_pipelines.integrations.db_datasource import DatabricksDatasource
from cdii_data_pipelines.integrations.datasource_type import DatasourceType
import os

class DatasourceFactory:
    @staticmethod
    def getDatasource(type: DatasourceType=DatasourceType.DATABRICKS, params: dict=None, dbutils=None, stage: str='DEV') -> Datasource:
        if type == DatasourceType.AGOL:
            return DatasourceFactory.getAgolDatasource(params)
        elif type == DatasourceType.DATABRICKS:
            return DatasourceFactory.getDatabricksDatasource(params)
        else:
            raise ValueError(type)
    
    @staticmethod
    def getAgolDatasource(params: dict=None, dbutils=None, stage: str='DEV') -> AgolDatasource:
          if dbutils is None or os.environ.get('LOCAL') == 'true':
              agol_user = os.environ.get(f'AGOL_USERNAME_{stage}')
              agol_password = os.environ.get(f'AGOL_PASSWORD_{stage}')
          else:
              agol_user = dbutils.secrets.get("SECRET_KEYS", f'AGOL_USERNAME_{stage}')
              agol_password = dbutils.secrets.get("SECRET_KEYS", f'AGOL_PASSWORD_{stage}')
          
          url = params['agol_url']
          return AgolDatasource( params={
            'url': url,
            'username': agol_user,
            'password': agol_password
          })

    @staticmethod
    def getDatabricksDatasource(params: dict=None, dbutils=None, stage: str='DEV'):
        return DatabricksDatasource()