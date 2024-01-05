"""
##########################################################################################################################

Utility Methods

This Python file contains a set of utility methods aimed at providing reusable functions for common tasks in a program. 
These functions cover a range of operations including file handling, data manipulation, string formatting, and more.

Usage:
- Import this file in your Python script.
- Call the desired utility functions to perform specific tasks within your program.

########################################################################################################################
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def initialize_spark_session():
    """
    Function to initialize the Spark session
    """
    spark = SparkSession.builder.appName("HeartDiseasePrediction")\
        .master("local[*]")\
        .config ("spark.sql.shuffle.partitions", "50") \
        .config ("spark.driver.maxResultSize","5g") \
        .config ("spark.executor.memory", "6g") \
        .config ("spark.executor.memoryOverhead", "1g") \
        .config ("spark.sql.execution.pyspark.enabled", "true")\
        .config ("spark.executor.cores", "3")\
        .getOrCreate()
    return spark


def count_missing_values(df, column):
    return df.filter(col(column).isNull()).count()


def open_csv(spark, filename):
    """
    Function to open a CSV file
    """
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(filename)
    except Exception as err:
        print(f"ERROR: {err}")
    return df


def data_checkpoint(spark, df, check_num):
    try:
        temp_file_path = "temp/temp_date.parquet" + str(check_num)

        # Save the DataFrame to a temporary file
        df.write.mode("overwrite").parquet(temp_file_path)

        # Re-initialize the spark session
        spark.stop()
        spark = initialize_spark_session()
        
        # Read the DataFrame back from the temporary file
        df = spark.read.parquet(temp_file_path)

    except Exception as err:
        print(f"ERROR: {err}")

    return df, spark