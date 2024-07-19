from dotenv import load_dotenv
import os
load_dotenv() 
USERNAME = os.getenv('MYSQL_USER')
PASSWORK = os.getenv('MYSQL_PASSWORD')
MYSQL_URL= os.getenv('MYSQL_URL') 
MONGODB_URI = os.getenv('MONGODB_URI')


import findspark
findspark.init()
from pyspark.sql import SparkSession


def readData(spark, filename):
    return spark.read.parquet(filename)

def getSparkSession(app_name="MyApp", master="local[*]", config_options=None):
    """
    Create a Spark session with the given app name and optional configurations.
    
    Parameters:
    - app_name (str): Name of the Spark application.
    - master (str): The master URL for the cluster.
    - config_options (dict): Additional Spark configurations as key-value pairs.
    
    Returns:
    - SparkSession: A SparkSession object.
    """
    builder = SparkSession.builder.appName(app_name).master(master)
    
    # Apply additional configurations if provided
    if config_options:
        for key, value in config_options.items():
            builder = builder.config(key, value)
    
    return builder.getOrCreate()

def readMySQL(spark, database, nameTable):
    print('-----------------------------------')
    print(f"--------Read From Database--------")
    print('-----------------------------------')
    df= spark.read.format("jdbc").options(
        url= f"{MYSQL_URL + database}",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = f"{nameTable}",
        user= USERNAME,
        password= PASSWORK).load()
    print("--------Finished--------")
    print("------------------------")
    return df

def importToMySQL(df, database, nameTable):
    print('----------------------------------------')
    print(f"--------Saving data to Database--------")
    print('----------------------------------------')
    df.write.format("jdbc").options(
        url= f"{MYSQL_URL + database}",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = f"{nameTable}",
        user= USERNAME,
        password= PASSWORK).mode('append').save()
    print("--------Data Import Successfully--------")
    print("----------------------------------------")

def readMongoDB(spark, database, nameTable):
    print('-----------------------------------')
    print(f"--------Read From Database--------")
    print('-----------------------------------')
    df = spark.read \
        .format("mongodb") \
        .option("spark.mongodb.read.connection.uri", MONGODB_URI) \
        .option("spark.mongodb.write.connection.uri", MONGODB_URI) \
        .option("database", database) \
        .option("collection", nameTable) \
        .load()
    print("--------Finished--------")
    print("------------------------")
    return df

def importToMySQL(df, database, nameTable):
    print('----------------------------------------')
    print(f"--------Saving data to Database--------")
    print('----------------------------------------')
    df.write.format("mongodb") \
               .mode("append") \
               .option("database", database) \
               .option("collection", nameTable) \
               .save()
    print("--------Data Import Successfully--------")
    print("----------------------------------------")

if __name__ == "__main__":
    pass