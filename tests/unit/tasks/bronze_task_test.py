from cdii_data_pipelines.tasks.bronze_task import BronzeTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging

class GoodBronzeTask(BronzeTask):
    def extract(self) -> DataFrame:
        return None

    def load(self, dataFrame: DataFrame):
        return None

def test_bronze_task_transform_not_volitile():
    logging.info("Testing the bronze task - ensure transform is not volitile")
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
                        .appName('TestBronzeTask') \
                        .getOrCreate()
    data = [('James','Smith','M',3000), ('Anna','Rose','F',4100),
      ('Robert','Williams','M',6200)
    ]
    columns = ["firstname","lastname","gender","salary"]
    df = spark.createDataFrame(data=data, schema = columns)

    test_bronze_task = GoodBronzeTask()
    new_df = test_bronze_task.transform(df)

    df3 = new_df.subtract(df)
    assert df3.count() == 0
    logging.info("Testing the bronze task")
