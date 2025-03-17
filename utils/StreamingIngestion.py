from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

scala_version = '2.12'
spark_version = '3.5.5'

packages = [
  f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
  'org.apache.kafka:kafka-clients:3.9.0',    
  f'com.datastax.spark:spark-cassandra-connector_{scala_version}:3.5.1'
]

class StreamingIngestion():
  def __init__(self, config) -> None: 
    self.spark = (SparkSession.builder
              #.master("local[*]")
              .appName("Spark Data Streaming")
              .config("spark.jars.packages", ",".join(packages))  
              .config("spark.cassandra.connection.host", "localhost")
              .getOrCreate())        

    self.config = config    

  def read_from_kafka(self) -> DataFrame:
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

  def write_to_cassandra(self, df: DataFrame) -> None:
    print("Writing to Cassandra...")
    try:
      df.writeStream \
      .format("org.apache.spark.sql.cassandra") \
      .option('checkpointLocation', '/tmp/checkpoint') \
      .option('keyspace', f"{self.config['cassandra']['key_space']}") \
      .option('table', f"{self.config['cassandra']['table_name']}") \
      .start() \
      .awaitTermination()
    except Exception as e:
      print(e)
    
  def process(self, df: DataFrame) -> DataFrame:
    schema = T.StructType([
      T.StructField("id", T.StringType(), False),
      T.StructField("first_name", T.StringType(), False),
      T.StructField("last_name", T.StringType(), False),
      T.StructField("gender", T.StringType(), False),
      T.StructField("address", T.StringType(), False),
      T.StructField("post_code", T.StringType(), False),
      T.StructField("email", T.StringType(), False),
      T.StructField("username", T.StringType(), False),
      T.StructField("registered_date", T.StringType(), False),
      T.StructField("phone", T.StringType(), False),
      T.StructField("picture", T.StringType(), False)
    ])
    
    return (df.selectExpr("CAST(value AS STRING) as json_data")
              .select(F.from_json(F.col("json_data"), schema).alias("data"))
              .select("data.*"))   

if __name__ == "__main__":
  pass