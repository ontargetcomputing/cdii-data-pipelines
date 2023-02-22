from cdii_data_pipelines.tasks.hazard.wildfire.silver_task import WildfireSilverTask
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def test_create_upper_irwinid_converts_when_needed():
    data = [(1,"a","b","c"),
            (2,"d","e", "f"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "IrwinId"])
    
    transformed_df = WildfireSilverTask._create_upper_irwinid(df)
    assert True, 'IrwinId' not in transformed_df.columns
    assert True, 'IRWINID' in transformed_df.columns

def test_create_upper_irwinid_no_converts_when_not_needed():
    data = [(1,"a","b","c"),
            (2,"d","e", "f"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "c"])
    
    transformed_df = WildfireSilverTask._create_upper_irwinid(df)
    assert True, 'IRWINID' not in transformed_df.columns

def test_create_upper_irwinid_no_converts_when_already_upper():
    data = [(1,"a","b","c"),
            (2,"d","e", "f"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "IRWINID"])
    
    transformed_df = WildfireSilverTask._create_upper_irwinid(df)
    assert True, 'IRWINID' in transformed_df.columns

def test_unnest_removes_braces():
    data = [(1,"a","b","{abc-def}"),
            (2,"d","e", "{ghi-jkl}"),
            (3,"g","h", "{mno-pqr")]

    df = spark.createDataFrame(data,["id","a","b", "IRWINID"])
    
    transformed_df = WildfireSilverTask._unnest(df)
    assert transformed_df.collect()[0]['IRWINID'] == 'abc-def'
    assert transformed_df.collect()[1]['IRWINID'] == 'ghi-jkl'
    assert transformed_df.collect()[2]['IRWINID'] == 'mno-pqr'

def test_unnest_expands():
    data = [(1,"a","b","{abc-def},{stu-vwx"),
            (2,"d","e", "{ghi-jkl}"),
            (3,"g","h", "{mno-pqr")]

    df = spark.createDataFrame(data,["id","a","b", "IRWINID"])
    
    transformed_df = WildfireSilverTask._unnest(df)
    assert transformed_df.count() == 4
    assert transformed_df.collect()[0]['IRWINID'] == 'abc-def'
    assert transformed_df.collect()[1]['IRWINID'] == 'stu-vwx'

def test_drop_duplicates_drops_correctly():
    data = [(1,"a","b","1"),
            (2,"d","e", "2"),
            (3,"a","b", "1")]

    df = spark.createDataFrame(data,["OBJECTID","a","b", "IRWINID"])

    transformed_df =  WildfireSilverTask._drop_duplicates(df)
    assert transformed_df.count() == 2
    assert transformed_df.collect()[0]['a'] == 'a'
    assert transformed_df.collect()[1]['a'] == 'd'

def test_transform():
    data = [(1,"a","b","{abc-def},{stu-vwx"),
            (2,"d","e", "{ghi-jkl}"),
            (3,"g","h", "{mno-pqr")]

    df = spark.createDataFrame(data,["id","a","b", "IrwinID"])
    
    transformed_df = WildfireSilverTask._unnest(df)
    assert transformed_df.count() == 4
    assert transformed_df.collect()[0]['IRWINID'] == 'abc-def'
    assert transformed_df.collect()[1]['IRWINID'] == 'stu-vwx'
    assert "IrwinID" not in transformed_df.columns

