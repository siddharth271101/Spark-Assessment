import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from delta import *


if __name__ == '__main__':
    builder = pyspark.sql.SparkSession.builder.appName("BizAnalytix") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    scSpark = configure_spark_with_delta_pip(builder).getOrCreate()
    people_path = "data/people.csv"

    people_data = scSpark.read.csv(people_path, header='true', sep=',', inferSchema=False)

    people_data.write.format("delta").saveAsTable("default.people_delta")

    # query table by path
    df = scSpark.read.format("delta").load("spark-warehouse/people_delta")

    # extracting number of rows from the Dataframe
    row = df.count()
    
    # extracting number of columns from the Dataframe
    col = len(df.columns)

    # printing
    print(f'Dimension of the Dataframe is: {(row,col)}')
    print(f'Number of Records/Rows are: {row}')
    print(f'Number of Columns are: {col}')  