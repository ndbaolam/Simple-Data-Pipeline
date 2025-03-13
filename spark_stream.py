from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils.helpers import load_cfg

from delta import *
import json

CFG_FILe = "./config.yaml"

scala_version = '2.12'
spark_version = '3.5.5'

packages = [
  f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
  'org.apache.kafka:kafka-clients:3.9.0',  
  'org.apache.hadoop:hadoop-aws:3.3.4',
  'io.minio:minio:8.5.2'
]

schema = T.StructType([
  T.StructField("first_name", T.StringType(), False),
  T.StructField("last_name", T.StringType(), False),
  T.StructField("gender", T.StringType(), False),
  T.StructField("address", T.StringType(), False),
  T.StructField("email", T.StringType(), False),
  T.StructField("username", T.StringType(), False),
  T.StructField("dob", T.DateType(), False),
  T.StructField("registered_date", T.DateType(), False),
  T.StructField("phone", T.StringType(), False),
  T.StructField("picture", T.StringType(), False)
])

class StreamingIngestion():
  def __init__(self, config) -> None:
    self.spark = (SparkSession.builder
              .master("local[*]")
              .appName("Spark Data Streaming")
              .config("spark.jars.packages", ",".join(packages))                            
              .getOrCreate())    

    self.load_config(self.spark.sparkContext, config=config)    
    
    self.config = config    

  def load_config(self, spark_context: SparkContext, config):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['datalake']['access_key'])
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['datalake']['secret_key'])
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", config['datalake']['endpoint'])
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")


  def read(self) -> DataFrame:
    return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['source']['kafka.bootstrap.servers']) \
            .option("startingOffsets", self.config['source']['kafka_options']['startingOffsets']) \
            .option("subscribe", self.config['source']['kafka_topics']) \
            .load()
  
  def write_to_console(self, df: DataFrame) -> None:
    try:
      df.writeStream \
      .format("console") \
      .outputMode("append") \
      .start() \
      .awaitTermination()
    except Exception as e:
      print(e)
    
  def write_to_delta(self, df: DataFrame, minio_bucket: str) -> None:
    df.writeStream \
    .format('delta') \
    .outputMode('append') \
    .option("checkpointLocation", f"s3a://{self.config['datalake']['bucket_name']}/checkpoints/") \
    .option("path", f"s3a://{self.config['datalake']['bucket_name']}/streaming-data/") \
    .start() \
    .awaitTermination()    
    
  def process(self, df: DataFrame) -> DataFrame:
    return (df.selectExpr("CAST(value AS STRING) as json_data")
              .select(F.from_json(F.col("json_data"), schema).alias("data"))
              .select("data.*"))

  def execute(self):
    raw_df = self.read()
    transformed_df = self.process(raw_df)
    self.write_to_console(transformed_df)    

if __name__ == "__main__":
  config = load_cfg(CFG_FILe) 

  app = StreamingIngestion(config=config)
  app.execute()  
  app.spark.stop()
