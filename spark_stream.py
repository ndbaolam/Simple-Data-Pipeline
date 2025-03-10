from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from delta import *
from minio import Minio
import json

scala_version = '2.12'
spark_version = '3.5.5'

packages = [
  f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
  'org.apache.kafka:kafka-clients:3.9.0',  
  'org.apache.hadoop:hadoop-aws:3.3.4'
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
  def __init__(self, config: dict) -> None:
    self.spark = (SparkSession.builder
              .master("local[*]")
              .appName("Spark Data Streaming")
              .config("spark.jars.packages", ",".join(packages))              
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .getOrCreate())

    self.load_config(self.spark.sparkContext)    
    
    self.config = config

    self.source_topics = config['source']['kafka_topics']

    self.minio = Minio(
      endpoint=self.config['minio']['endpoint'],
      secret_key=self.config['minio']['secret_key'],
      access_key=self.config['minio']['access_key'],
      secure=False
    )

  def load_config(self, spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_admin")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_admin")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")


  def read(self) -> DataFrame:
    return (self.spark
            .readStream
            .format("kafka")            
            .option("kafka.bootstrap.servers", self.config['source']['kafka_options']['kafka.bootstrap.servers'])
            .option("startingOffsets", self.config['source']['kafka_options']['startingOffsets'])
            .option("subscribe", self.source_topics)            
            .load())
  
  def write_to_console(self, df: DataFrame) -> None:
    (df.writeStream
     .format(self.config["sink"]["write_format"])
     .outputMode(self.config["sink"]["write_output_mode"])    
     .start()
     .awaitTermination())
    
  def write_to_minio(self, df: DataFrame, minio_bucket: str) -> None:
    found = self.minio.bucket_exists(minio_bucket)
    if not found:
      self.minio.make_bucket(minio_bucket)

    df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", f"s3a://{minio_bucket}/delta-lake") \
    .option("checkpointLocation", f"s3a://{minio_bucket}/checkpoint") \
    .start() \
    .awaitTermination()
    
  def process(self, df: DataFrame) -> DataFrame:
    return (df.selectExpr("CAST(value AS STRING) as json_data")
              .select(F.from_json(F.col("json_data"), schema).alias("data"))
              .select("data.*"))

  def execute(self):
    raw_df = self.read()
    transformed_df = self.process(raw_df)
    self.write_to_minio(transformed_df, minio_bucket=self.config['minio']['bucket'])

if __name__ == "__main__":
  config = {
    "source": {
      "kafka_topics": "users_created",
      "kafka_options": {
        "kafka.bootstrap.servers": "localhost:9092",
        "startingOffsets": "earliest",
      }
    },
    "sink": {
      "write_format": "console",
      "write_output_mode": "append",      
    },
    "minio": {
      "endpoint": "localhost:9000",
      "access_key": "minio_admin",
      "secret_key": "minio_admin",   
      "bucket": "my-bucket"
    }
  }

  app = StreamingIngestion(config=config)
  app.execute()  
