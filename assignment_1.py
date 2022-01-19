if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    # Creating the schema
    schema_t1 = StructType([
        StructField("c1", StringType(), True)
    ])

    schema_t2 = StructType([
        StructField("c2", StringType(), True)
    ])

    t1_df = spark.read.format("csv") \
        .option("header", True) \
        .schema(schema_t1) \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/practice_assignmnt-t1.csv")

    t1_df.printSchema()
    t1_df.show(5, False)

    t2_df = spark.read.format("csv") \
        .option("header", True) \
        .schema(schema_t2) \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/practice_assignmnt-t2.csv")

    t2_df.printSchema()
    t2_df.show(5, False)

    spark - submit - -master yarn - -packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe / ingestion / rdd / rdd2df_thru_explicit_schema.py


    #print("\nConvert RDD to Dataframe using SparkSession.createDataframe(),")
    # Creating RDD of Row
    # = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/practice_assignmnt-t1.csv")









