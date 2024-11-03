import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, DecimalType, DoubleType


def flatten_document(document, parent_key=""):
  items = []
  for key, value in document.items() if isinstance(document, dict) else []:
    new_key = parent_key + "." + key if parent_key else key
    if isinstance(value, dict):
      items.extend(flatten_document(value, new_key))
    elif isinstance(value, (list, tuple)):
      for i, item in enumerate(value):
        items.extend(flatten_document(item, f"{new_key}.{i}"))
    else:
      # Try converting to string or a supported type
      try:
          items.append((new_key, str(value)))  # Convert to string
      except:
          items.append((new_key, None))  # Handle conversion errors

  return items

def create_mysql_table(spark, df, table_name, jdbc_url, username, password):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .mode("overwrite") \
        .save()


def infer_schema(df):
  # Custom schema inference for unsupported data types
  schema = StructType()
  for field in df.schema.fields:
    if field.dataType == DecimalType():
      # Convert DecimalType to DoubleType
      schema.add(StructField(field.name, DoubleType()))
    else:
      schema.add(field)
  return schema

if __name__ == "__main__":
    # Spark Session
    spark = SparkSession.builder \
        .appName("MongoDB to MySQL Migration") \
        .getOrCreate()

    # MongoDB Connection
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["sample"]
    mongo_collection = mongo_db["sample"]

    # Fetch MongoDB Data
    mongo_data = mongo_collection.find()

    # Flatten and Create DataFrame
    flattened_data = []
    for doc in mongo_data:
      flattened_data.append(flatten_document(doc))

    # Handle nested lists and tuples
    df = spark.createDataFrame(flattened_data)

    # MySQL JDBC URL
    jdbc_url = "jdbc:mysql://localhost:3306/sampledb"
    username = "root"
    password = "hung"

    # Iterate over DataFrame Columns and Create Tables
    for column_name in df.columns:
        # Extract table name from column name (e.g., "address.street" -> "address")
        table_name = column_name.split(".")[0]

        # Filter and select relevant columns
        filtered_df = df.select(col(column_name))

        # Create or overwrite table
        create_mysql_table(spark, filtered_df, table_name, jdbc_url, username, password)