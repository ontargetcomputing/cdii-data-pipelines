from cdii_data_pipelines.integrations.datasource import DataSource
from cdii_data_pipelines.integrations.agol_datasource import AgolDataSource
from cdii_data_pipelines.integrations.db_datasource import DatabricksDataSource
import os

class DataSourceFactory:
    @staticmethod
    def getDataSource(source: str, params: dict=None, dbutils=None, stage: str='DEV') -> DataSource:
        if source == 'AGOL':
            return DataSourceFactory.getAgolDataSource(params)
        elif source == 'DB':
            return DataSourceFactory.getDatabricksDataSource(params)
        else:
            raise ValueError(format)
    
    @staticmethod
    def getAgolDataSource(params: dict=None, dbutils=None, stage: str='DEV') -> AgolDataSource:
          if dbutils is None or os.environ.get('LOCAL') == 'true':
              agol_user = os.environ.get(f'AGOL_USERNAME_{stage}')
              agol_password = os.environ.get(f'AGOL_PASSWORD_{stage}')
          else:
              agol_user = dbutils.secrets.get("SECRET_KEYS", f'AGOL_USERNAME_{stage}')
              agol_password = dbutils.secrets.get("SECRET_KEYS", f'AGOL_PASSWORD_{stage}')
          
          url = params['agol_url']
          return AgolDataSource( params={
            'url': url,
            'username': agol_user,
            'password': agol_password
          })

    @staticmethod
    def getDatabricksDataSource(params: dict=None, dbutils=None, stage: str='DEV'):
        return DatabricksDataSource()