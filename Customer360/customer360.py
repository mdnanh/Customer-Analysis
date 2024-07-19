import findspark
findspark.init()

import pyspark
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import os
from datetime import datetime
from functools import reduce
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from functools import reduce
from pyspark.sql.window import Window
import pandas as pd
from utils import readData, importToMySQL, getSparkSession



def processKeyword(df):
    df= df.selectExpr("cast (datetime as date) as datetime", "user_id", "keyword")
    df= df.filter(df.datetime.isNotNull())
    df= df.filter(df.user_id.isNotNull())
    df= df.withColumn('keywordNormalize', F.translate(F.lower(F.regexp_replace('keyword', " +", " ")),
                                'áàảãạăắằẳẵặâấầẩẫậđéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹy',
                                'aaaaaaaaaaaaaaaaadeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyy'))
    return df


def getCategory(df, category_dict):
    # init 2 column
    df= df.withColumn("category_6", when(col("most_search_6") == "", "Other"))
    df= df.withColumn("category_7", when(col("most_search_7") == "", "Other"))
    for key_word, category in category_dict.items():
        # set value with condition else return origin value
        df= df.withColumn("category_6", when(col('Most_search_6').like(f"%{key_word}%"), category[0]).otherwise(col("category_6")))
        df= df.withColumn("category_7", when(col('Most_search_7').like(f"%{key_word}%"), category[0]).otherwise(col("category_7")))
    # drop 2 columns keyword search
    df= df.drop("Most_search_6", "Most_search_7")
    return df

def mostSearch(df):
    df= df.withColumn("month", F.month('datetime'))
    # Group by and aggregate
    agg_df= df.groupBy("user_id", "month", "keywordNormalize").agg(F.count("*").alias("keyword_count"))
    # ignore null val
    agg_df= agg_df.filter(col("keywordNormalize").isNotNull())
    # Define window specification
    window_spec = Window.partitionBy("user_id", "month").orderBy(agg_df["keyword_count"].desc())
    # Apply window function
    ranked_df= agg_df.withColumn("rn", F.row_number().over(window_spec))
    result_df= ranked_df.filter(ranked_df.rn == 1)

    df_select= result_df.select("user_id", "month", "keywordNormalize")
    df_final= df_select.withColumns({"Most_search_6": when(col("month")==6, df_select.keywordNormalize), 
                               "Most_search_7": when(col("month")==7, df_select.keywordNormalize)})
    df_final= df_final.drop("month", "keywordNormalize")
    df_final= df_final.groupBy("user_id") \
             .agg(
                 F.first("Most_search_6", ignorenulls=True).alias("Most_search_6"),
                 F.first("Most_search_7", ignorenulls=True).alias("Most_search_7"))
    return df_final


def trendingType(df):
    df= df.filter(col("category_6").isNotNull() | col("category_7").isNotNull())
    df= df.fillna("Other")
    df= df.withColumn("Trending_type", when(col("category_6")==col("category_7"), "Unchanged").otherwise("Changed"))
    df= df.withColumn("Previous", when(col("Trending_type")== "Changed", concat_ws("-", col("category_6"), col("category_7"))) \
                      .otherwise("Unchanged"))
    return df


def mainProcess(folderPath, category_dict):
    listFiles= list()
    for filename in os.listdir(folderPath):
        print('--------------------------------------------------------------')
        print(f"--------Read and preprocess data from file {filename}--------")
        print('--------------------------------------------------------------')
        df= readData(spark, os.path.join(folderPath,filename))
        df= processKeyword(df)
        listFiles.append(df)
    print('--------------------------------')
    print("--------Concat Dataframe--------")
    print('--------------------------------')
    df_all= reduce(lambda df1, df2: df1.union(df2), listFiles)
    print('------------------------------------')
    print("--------Most Search by Month--------")
    print('------------------------------------')
    df= mostSearch(df_all)
    df.cache()
    print('----------------------------')
    print("--------Get Category--------")
    print('----------------------------')
    df= getCategory(df, category_dict)
    print('-----------------------------')
    print("--------Trending Type--------")
    print('-----------------------------')
    df= trendingType(df)
    return df

if __name__ == "__main__":

    sparkName= "customer360"
    database= "data_marts"
    nameTable= "analyst_customer"

    config= {"spark.jars.packages" : {"com.mysql:mysql-connector-j:8.0.30"}}
    spark = getSparkSession(app_name= sparkName, config_options=config)

    folder_path= os.path.join(os.path.dirname(os.getcwd()), "log_search")
    category_dict= os.path.join(os.path.dirname(os.getcwd()), "most_search_month.csv")
    
    df= mainProcess(folder_path, category_dict)
    importToMySQL(df, database, nameTable)