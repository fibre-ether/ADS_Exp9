import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, col, udf, round as psround, sum as pssum
from pyspark.sql.types import FloatType, StringType, DoubleType
from etherscan import *
import time
from datetime import datetime
import sqlite3
from constants import *

spark = None
current_time = time.time()

def get_data():

    hashes_df = pd.read_csv("hashes.csv")['0']
    hashes_list = hashes_df.to_list()

    scraped_data = scrape(2)

    global spark
    spark = SparkSession.builder.appName(
        "pandas to spark").getOrCreate()

    df_spark = spark.createDataFrame(scraped_data)

    data_no_duplicates = df_spark.filter(
        ~df_spark['Txn Hash'].isin(hashes_list))

    new_hashes = data_no_duplicates.toPandas()['Txn Hash']
    aggregated_hashes = pd.concat([hashes_df, new_hashes])
    # print("aggregated hashes", aggregated_hashes)

    aggregated_hashes.to_csv('hashes.csv')

    # print(data_no_duplicates.show())
    return data_no_duplicates


def do_analysis(df_spark=None):
    
    # spark = SparkSession.builder.appName(
    #     "pandas to spark").getOrCreate()
    
    # df_spark = spark.read.option('header', 'true').csv("data/new_data.csv")
    
    columns_to_drop = ['1', '2', '3', '4', '5']
    df = df_spark.drop(*columns_to_drop)



    df = df.withColumn("Txn Fee", df["Txn Fee"].cast(DoubleType()))

    split_col = pyspark.sql.functions.split(df['Value'], ' ')
    df = df.withColumn('Val', split_col.getItem(0))
    df = df.withColumn('Type', split_col.getItem(1))


    df = df.withColumn("Val", df["Val"].cast(DoubleType()))

    num_cols = []
    cat_cols = []

    for s in df.schema:
        data_type = str(s.dataType)
        if data_type == "StringType()":
            cat_cols.append(s.name)

        if data_type == "LongType()" or data_type == "DoubleType()":
            num_cols.append(s.name)

    cols = ['_c0', 'Value']
    df1 = df.drop(*cols)

    # ### Top 5 Methods of Transactions

    l = df1.groupBy('Method').count()
    l1 = l.sort('count', ascending=False).limit(8)

    l1 = l1.toPandas()
    # l1.to_csv('l1.csv')

    # ### Methods with Highest Etherium Transaction Value

    df_cat_val = df1.groupby("Method").agg({'Val': "sum"})

    method_high_transaction = df_cat_val.sort(
        'Method', ascending=False).limit(5)

    method_high_transaction = method_high_transaction.toPandas()
    # method_high_transaction.to_csv('method.csv')

    df1.select("Method", "Val", "Txn Fee").show(5)
    # ### Analysing Top Etherium Transactions

    # ### Comparison b/w gas prices and transaction timings
    split_col = pyspark.sql.functions.split(df1['Age'], ' ')
    df1 = df1.withColumn('Time', split_col.getItem(0))

    global current_time
    
    def get_nearest_hour(x):
        datetime_x = datetime.fromtimestamp(x)
        return str(datetime_x.replace(second=0, microsecond=0, minute=datetime_x.minute))
    
    current_time_udf = udf(lambda x: current_time-float(x), FloatType())
    
    nearest_hour_udf = udf(lambda x: get_nearest_hour(x), StringType())
    
    df1 = df1.withColumn("Time", current_time_udf(col("Time")))
    df1 = df1.withColumn("Time", nearest_hour_udf(col("Time")))
    df1 = df1.withColumn("Val",psround(df['Val'].cast(DoubleType()),5))
    df1 = df1.withColumn("Txn Fee",psround(df['Txn Fee'].cast(DoubleType()),5))

    comp = df1.select("Txn Fee", "Time", "Val")
    
    comp = comp.groupBy("Time") \
        .agg(pssum("Val").alias("total_value"), \
         pssum("Txn Fee").alias("total_txn_fee"))

    comp = comp.toPandas()
    # data = comp[['Time', 'Txn Fee', 'Val']]
    # print(data.head())

    output = [method_high_transaction, comp]
    comp.to_csv("tvf_analysis.csv")
    method_high_transaction.to_csv("mv_analysis.csv")
    return output


def add_to_db(analysis=None):
    print("---analysis---")
    # print(analysis[0])
    # print(analysis[1])
    
    conn = sqlite3.connect('analysis.sqlite')
    
    if analysis:
        analysis_mv = analysis[0]
        analysis_tvf = analysis[1]
    else:
        analysis_tvf = pd.read_csv("tvf_analysis.csv")
        analysis_mv = pd.read_csv("mv_analysis.csv")
    
    #tvf table
    time_value_fee_table = "tvf_analysis"

    query = f'Create table if not Exists {time_value_fee_table} (Time text ,total_value real, total_txn_fee real)'
    conn.execute(query)
    
    tvf_df = pd.read_sql_query(f"select * from {time_value_fee_table}", conn)
    print("fetched tvfdf:")
    print(tvf_df)
    
    if not tvf_df.empty:
        tvf_df = pd.concat([analysis_tvf, tvf_df], axis=0)
    else:
        tvf_df = analysis_tvf
        
    tvf_df = tvf_df.groupby('Time').agg({'total_value' : 'sum', 'total_txn_fee' : 'sum'})
    
    print("new tvfdf:")
    print(tvf_df)
    
    tvf_df.to_sql(time_value_fee_table,conn,if_exists='replace')
    
    #mv table
    method_value_table = "mv_analysis"
    
    query = f'Create table if not Exists {method_value_table} (Method text ,sum_val real)'
    conn.execute(query)
    
    analysis_mv.rename(columns = {'sum(Val)':'sum_val'}, inplace = True)
    
    mv_df = pd.read_sql_query(f"select * from {method_value_table}", conn)
    print("fetched mvdf:")
    print(mv_df)
    
    if not mv_df.empty:
        mv_df = pd.concat([analysis_mv, mv_df], axis=0)
    else:
        mv_df = analysis_mv
        
    mv_df = mv_df.groupby('Method').agg({'sum_val' : 'sum'})
    mv_df = mv_df.where(mv_df['sum_val']>0).dropna()
    
    
    print("new mvdf:")
    print(mv_df)
    
    mv_df.to_sql(method_value_table,conn,if_exists='replace')
    
    conn.commit()
    conn.close()

def analysis_iteration():
    data = get_data()
    analysis = do_analysis(data)
    add_to_db(analysis)

    
def create_db():
    add_to_db()
    print("created db")
    
if __name__ == '__main__':
    analysis_iteration()