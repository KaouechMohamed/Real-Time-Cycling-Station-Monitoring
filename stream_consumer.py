import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# Configure logging to write to the console
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.velib_stations (
        number INT PRIMARY KEY,
        contract_name TEXT,
        name TEXT,
        address TEXT,
        status TEXT,
        last_update TIMESTAMP
    );
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    logging.info("Inserting data...")

    number = kwargs.get('number')
    contract_name = kwargs.get('contract_name')
    name = kwargs.get('name')
    address = kwargs.get('address')
    status = kwargs.get('status')
    last_update = kwargs.get('last_update')

    try:
        session.execute("""
            INSERT INTO velib_stations(number, contract_name, name, address, status, last_update
                                        )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (number, contract_name, name, address, status, last_update
              ))
        logging.info(f"Data inserted for station {name} in contract {contract_name}")

    except Exception as e:
        logging.error(f"Could not insert data due to: {e}")


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")

        return None


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', "stations") \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("number", StringType(), False),
        StructField("contract_name", StringType(), False),
        StructField("name", StringType(), False),
        StructField("address", StringType(), False),
        StructField("status", StringType(), False),
        StructField("last_update", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    logging.info("Creating Spark connection ...")
    spark_conn = create_spark_connection()

    # kafka topic
    if spark_conn is not None:
        logging.info("Creating Spark-Kafka connection ...")
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()
        logging.info("Cassandra connection established...")

        if session is not None:
            create_keyspace(session)
            create_table(session)

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'velib_stations')
                               .start())

            streaming_query.awaitTermination()