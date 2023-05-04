import os
import configparser

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.streaming import StreamingContext

from spark.spark_session import init_spark_session

script_dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = os.path.join(script_dir, 'config/config.ini')
config.read(config_file_path)

TOPIC = config['KAFKA']['TOPIC']
BOOTSTRAP_SERVER = config['KAFKA']['BOOTSTRAP_SERVER']

json_schema = StructType([
      StructField("id", StringType(), True),
      StructField("name", StringType(), True),
      StructField("address", StringType(), True),
      StructField("email", StringType(), True),
      StructField("job", StringType(), True),
      StructField("company", StringType(), True),
      StructField("phone_number", StringType(), True)])

if __name__ == '__main__':
    spark = init_spark_session(config)

    ssc = StreamingContext(spark.sparkContext, 5)

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
        .option("subscribe", TOPIC) \
        .option("security.protocol", "PLAIN") \
        .load() \
        .select(from_json(col("value").cast("string"), json_schema).alias("value")).select("value.*")

    kafka_df \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://geekfox-sb-raw/spark/events/parquet") \
        .option("checkpointLocation", "s3a://geekfox-sb-awsresources/spark/checkpoint/") \
        .start() \
        .awaitTermination()





