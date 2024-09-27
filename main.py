import sys
from operator import add
from random import random

from pyspark.sql import SparkSession

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("Data Ingestion Pipeline") \
    .getOrCreate()

# JDBC connection details
jdbc_url = "jdbc:oracle:thin:@<your_oracle_host>:<port>:<service_name>"
connection_properties = {
    "user": "<your_username>",
    "password": "<your_password>",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Read data from Oracle
source_table = "your_source_table"
df = spark.read.jdbc(url=jdbc_url, table=source_table, properties=connection_properties)

# Display the ingested data
df.show()
