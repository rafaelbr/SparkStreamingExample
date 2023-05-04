from pyspark.sql import SparkSession

def init_spark_session(config):
    spark = SparkSession.builder \
        .appName("SparkStreamingExample") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", config["AWS"]["ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.secret.key", config["AWS"]["SECRET_KEY"]) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.{}.amazonaws.com".format(config["AWS"]["REGION"])) \
        .getOrCreate()

    return spark



