from pyspark.sql import SparkSession
from pymongo import MongoClient
import pymysql

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MongoDB_to_Datalake_to_MySQL") \
    .getOrCreate()

# Extract data from MongoDB
def extract_from_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["your_database_name"]
    collection = db["your_collection_name"]

    # Convert MongoDB documents to a Spark DataFrame
    df = spark.read.format("mongo") \
        .option("uri", "mongodb://localhost:27017/your_database_name.your_collection_name") \
        .load()

    return df

# Transform data (optional)
def transform_data(df):
    # Add transformation logic here, e.g., filtering, cleaning, joining
    # ...
    return df

# Load data to data lake (e.g., HDFS, S3)
def load_to_datalake(df):
    # Write DataFrame to data lake in a suitable format (e.g., Parquet, CSV)
    df.write.format("parquet").save("path/to/datalake")

# Load data from data lake to MySQL
def load_to_mysql(df):
    # Read DataFrame from data lake
    df = spark.read.format("parquet").load("path/to/datalake")

    # Create a MySQL connection
    conn = pymysql.connect(
        host="your_mysql_host",
        user="your_mysql_user",
        password="your_mysql_password",
        database="your_mysql_database"
    )

    # Write DataFrame to MySQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://your_mysql_host:3306/your_mysql_database") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "your_mysql_table") \
        .option("user", "your_mysql_user") \
        .option("password", "your_mysql_password") \
        .mode("append") \
        .save()

# Main ETL pipeline
if __name__ == "__main__":
    df = extract_from_mongodb()
    df = transform_data(df)  # Optional
    load_to_datalake(df)
    load_to_mysql(df)