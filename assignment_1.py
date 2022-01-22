from pyspark.sql import SparkSession
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    current_dir = os.path.abspath(os.path.dirname(__file__))

    df1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5]).toDF(["c1"])
    df1.show()

# spark-submit assignment_1.py






