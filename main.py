from utils.helpers import load_cfg
from utils.CassandraCluster import CassandraCluster
from utils.StreamingIngestion import StreamingIngestion

CFG_FILe = "./config.yaml"

if __name__ == "__main__":
  config = load_cfg(CFG_FILe) 

  session = CassandraCluster(config=config)
  session.create_keyspace()
  session.create_table()

  app = StreamingIngestion(config=config)
  df = app.read_from_kafka()
  df = app.process(df)

  app.write_to_cassandra(df)
