from cassandra.cluster import Cluster
from pyspark.sql import DataFrame

import logging

class CassandraCluster():
  def __init__(self, config) -> None:
    self.cluster = Cluster(['localhost'])
    self.session = self.cluster.connect()

    self.key_space = config['cassandra']['key_space']
    self.table_name = config['cassandra']['table_name']

  def create_keyspace(self) -> None:
    try:      
      print("Creating key space...")

      self.session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {self.key_space}
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
      """)

      print(f"Keyspace {self.key_space} has created...")
    except Exception as e:
      logging.error(f"ERROR WHEN CREATING KEYSPACE: {e}")

  def create_table(self) -> None:
    try:
      print("Creating table...")

      self.session.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.key_space}.{self.table_name} (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
      """)

      print(f"Created table {self.table_name}")
    except Exception as e:
      logging.error(f"ERROR ON CREATING TABLE: {e}")

  def insert_data(self, df: DataFrame) -> None:
    print("Inserting data...")

    user_id = df.get('id')
    first_name = df.get('first_name')
    last_name = df.get('last_name')
    gender = df.get('gender')
    address = df.get('address')
    postcode = df.get('post_code')
    email = df.get('email')
    username = df.get('username')
    dob = df.get('dob')
    registered_date = df.get('registered_date')
    phone = df.get('phone')
    picture = df.get('picture')

    try:
      self.session.execute(f"""
        INSERT INTO {self.key_space}.{self.table_name}(id, first_name, last_name, gender, address, 
            post_code, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      """, (user_id, first_name, last_name, gender, address,
            postcode, email, username, dob, registered_date, phone, picture))
    except Exception as e:
      logging.error(f"ERROR ON INSERTING {e}")

if __name__ == "__main__":
  pass