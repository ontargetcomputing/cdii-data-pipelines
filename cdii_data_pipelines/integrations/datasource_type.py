from enum import Enum

class DatasourceType(Enum):
    NOOP = 'noop'
    AGOL = 'agol'
    DATABRICKS = 'databricks'
    S3 = 's3'
