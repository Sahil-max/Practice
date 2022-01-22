from pyspark.sql import *
from pyspark.sql.functions import *
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

    df1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5]).map(lambda rec: (rec, )).toDF(["c1"]) \
        .withColumn('temp_col', lit('abc')) \
        .withColumn('rn', row_number().over(Window.orderBy('temp_col'))) \
        .drop('temp_col')
    df1.show()
    df2 = spark.sparkContext.parallelize([3, 4, 5]).map(lambda rec: (rec, )).toDF(["c1"]) \
        .withColumn('temp_col', lit('abc')) \
        .withColumn('rn', row_number().over(Window.orderBy('temp_col'))) \
        .drop('temp_col')
    df2.show()

    df1.join(df2, df1['rn'] == df2['rn']).show()

# spark-submit assignment_1.py






