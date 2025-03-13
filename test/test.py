from pyspark.sql import SparkSession

# To avoid JAVA_GATEWAY_EXITED
# export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64x
# export PATH=$JAVA_HOME/bin:$PATH

scala_version = '2.12'
spark_version = '3.5.5'

packages = [
  f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
  'org.apache.kafka:kafka-clients:3.9.0',  
  'org.apache.hadoop:hadoop-aws:3.3.4'
  # f'io.delta:delta-spark_{scala_version}:3.3.0'
]

spark = (SparkSession.builder
         .config("spark.jars.packages", ",".join(packages))
         .getOrCreate())

df = (spark.read
      .format('kafka')
      .option('kafka.bootstrap.servers', 'localhost:9092')
      .option('subscribe', 'users_created')
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())

df.show()
