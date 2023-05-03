import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.functions import min as Fmin
from pyspark.sql.functions import max as Fmax
from pyspark.sql.functions import avg, col, concat, count, desc, asc, explode, lit, split, stddev, udf, isnan, when, rank, from_unixtime
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
import pandas as pd

from etherscan import *

spark = None

def get_data():

    hashes_df = pd.read_csv("hashes.csv")['0']
    hashes_list = hashes_df.to_list()

    print("before scrape")
    scraped_data = scrape(2)
    print("after scrape")
    print("scraped df:", scraped_data)

    global spark
    spark = SparkSession.builder.appName(
    "pandas to spark").getOrCreate()


    df_spark = spark.createDataFrame(scraped_data)
    # df_spark = spark.read.option('header', 'true').csv("data/new_data.csv")


    data_no_duplicates = df_spark.filter(~df_spark['Txn Hash'].isin(hashes_list))

    new_hashes = data_no_duplicates.toPandas()['Txn Hash']
    aggregated_hashes= pd.concat([hashes_df, new_hashes])
    print("aggregated hashes", aggregated_hashes)

    aggregated_hashes.to_csv('hashes.csv')

    print(data_no_duplicates.show())
    return data_no_duplicates

def do_analysis(df_spark):

    df_spark

    # df_spark.show()

    df_spark.columns

    columns_to_drop = ['1', '2','3','4','5']
    df = df_spark.drop(*columns_to_drop)

    # df.show()

    # df.count()

    # df.describe().show()

    def count_missing(df, col):
        """
        A helper function which count how many missing values in a colum of the dataset.
        
        This function is useful because the data can be either three cases below:
        
        1. NaN
        2. Null
        3. "" (empty string)
        """
        return df.filter((isnan(df[col])) | (df[col].isNull()) | (df[col] == "") | (df[col] == "null")).count()

    print("[missing values]\n")
    for col in df.columns:
        missing_count = count_missing(df, col)
        if missing_count > 0:
            print("{}: {}".format(col, missing_count))

    df.schema

    from pyspark.sql.types import IntegerType, LongType, DoubleType
    df = df.withColumn("Txn Fee", df["Txn Fee"].cast(DoubleType()))

    df.schema

    # df.show()

    split_col = pyspark.sql.functions.split(df['Value'], ' ')
    df = df.withColumn('Val', split_col.getItem(0))
    df = df.withColumn('Type', split_col.getItem(1))

    # df.show()

    df = df.withColumn("Val", df["Val"].cast(DoubleType()))

    num_cols = []
    cat_cols = []

    for s in df.schema:
        data_type = str(s.dataType)
        if data_type == "StringType()":
            cat_cols.append(s.name)
        
        if data_type == "LongType()" or data_type == "DoubleType()":
            num_cols.append(s.name)

    num_cols

    cat_cols

    cols = ['_c0','Value']
    df1 = df.drop(*cols)

    df1.show()
    # ### Top 5 Methods of Transactions

    l = df1.groupBy('Method').count()
    l1 = l.sort('count', ascending=False).limit(8)

    l1 = l1.toPandas()
    l1.to_csv('l1.csv')

    # ### Methods with Highest Etherium Transaction Value

    df_cat_val = df1.groupby("Method").agg({'Val': "sum"})

    method_high_transaction = df_cat_val.sort('sum(Val)', ascending=False).limit(5)

    method_high_transaction = method_high_transaction.toPandas()
    method_high_transaction.to_csv('method.csv')

    df1.select("Method", "Val", "Txn Fee").show(5)
    # ### Analysing Top Etherium Transactions

    top_5_eth_transaction = df1.sort('Val', ascending=False).limit(5)

    high_5_gas_fee = df1.sort('Txn Fee', ascending=False).limit(5)

    low_5_gas_fee = df1.sort('Txn Fee', ascending=True).limit(5)

    # ### Comparison b/w gas prices and transaction timings
    split_col = pyspark.sql.functions.split(df1['Age'], ' ')
    df1 = df1.withColumn('Time', split_col.getItem(0))

    comp = df1.select("Txn Fee","Time", "Val")

    comp = comp.toPandas()
    data = comp[['Time', 'Txn Fee', 'Val']]
    print(data.head())

    output = [method_high_transaction, data]
    return output

def add_to_db(analysis):
    pass

data = get_data()
# analysis = do_analysis(data)
# add_to_db(analysis)
