#=========================================
###  POSISI   : 28 FEBRUARY 2023
###  PIC      : HARYO SETOWIBOWO
###  CASE     : CREDIT CARD PRODUCT RECOMMENDATION
###  TASK     : ETL CC DATASET INFERENCE
#=========================================
##           Import Dependencies           
#=========================================
from time import time, sleep
from pytz import timezone
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from functools import reduce
from pyspark.sql import SparkSession, functions as F, types as T, window as W

##              Pandas Config              
#=========================================
import pandas as pd
pd.options.display.html.table_schema=True
pd.options.display.max_columns=999
pd.options.display.max_rows=999

##              Spark Session              
#=========================================
spark = SparkSession\
  .builder\
  .appName("ETL CC DATASET INFERENCE")\
  .config("spark.sql.crossJoin.enabled", "true")\
  .config("spark.dynamicAllocation.enabled", "false")\
  .config("spark.executor.instances", "4")\
  .config("spark.executor.cores", "4")\
  .config("spark.executor.memory", "4g")\
  .config("spark.yarn.executor.memoryOverhead", "2g")\
  .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")\
  .config("spark.ui.showConsoleProgress", "false")\
  .config("spark.sql.broadcastTimeout", "36000")\
  .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
  .config("spark.network.timeout", 60)\
  .enableHiveSupport()\
  .getOrCreate()

#  Config  8  5  8  4 = 
  

##               Functions                 
#=========================================
def set_timer():
  global START_TIME
  START_TIME = time()

def get_timer(start_time=None):
  if start_time:
    t = datetime(1,1,1)+timedelta(seconds=int(time()-start_time))
  else:
    t = datetime(1,1,1)+timedelta(seconds=int(time()-START_TIME))
  return "{}:{}:{}".format(str(t.hour).zfill(2),str(t.minute).zfill(2),str(t.second).zfill(2))

def remove_blank_space(x):
  return F.when((F.trim(F.col(x))!='')&(F.trim(F.upper(F.col(x)))!='NAN'), F.trim(F.col(x))).otherwise(F.lit(None))

def cleanse_blank_space(df_to_clean, *exclude):
  col_to_clean = []
  for val in df_to_clean.dtypes:
    if val[1] == "string" and val[0] not in (exclude):
      col_to_clean.append(val[0])
  return reduce(lambda df, x: df.withColumn(x, remove_blank_space(x)), set(col_to_clean), df_to_clean)

def get_list_partition(schema, table):
  try:
    partitions = spark.sql("""
     SHOW PARTITIONS {}.{}
     """.format(schema, table)).sort('partition', ascending=False).collect() # ambil partisi sesuai format
    if len(partitions) != 0: # jika ada partisi
      list_partition = []
      for row in partitions:
        if "__HIVE_DEFAULT_PARTITION__" not in row[0]:
          dict_partition = {}
          for partition in row[0].split("/"):
            value = partition.split("=")
            dict_partition[value[0]] = value[1]
          list_partition.append(dict_partition)
      return list_partition
    else: # selain itu
      return None # tidak ada partisi
  except:
    print("is not a partitioned table")
    return None
  
get_last_partition = lambda schema, table: get_list_partition(schema, table)[0] if get_list_partition(schema, table)!=None else None

def write_periodic(df, hive_schema, hive_table, ds):
#  count = df.count()
#  if True:
  df = df\
    .withColumn("ds", F.lit(ds))\
    .withColumn("modified_date", F.lit(datetime.now(timezone("Asia/Jakarta"))))

  spark.sql("DROP TABLE IF EXISTS temp.sample_{}_{}".format(hive_table, ds))

  df\
    .sample(False,0.1,None)\
    .write\
    .format("parquet")\
    .mode("overwrite")\
    .saveAsTable("temp.sample_{}_{}".format(hive_table, ds))

  path = lambda p: spark._jvm.org.apache.hadoop.fs.Path(p)
  fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
  hdfs_folder = fs.getContentSummary(path("/user/hive/warehouse/temp.db/sample_{}_{}/".format(hive_table, ds)))

  repartition_number = float(hdfs_folder.getLength()*10)/float(128*1024*1024)
  repartition_number = int(1 if repartition_number < 1 else repartition_number)

  spark.sql("DROP TABLE temp.sample_{}_{}".format(hive_table, ds))

  # repartition_number = 1

  df = df.repartition(repartition_number)

  if spark.sql("SHOW TABLES IN {} LIKE '{}'".format(hive_schema, hive_table)).count() != 0:
    query_drop_partition = "ALTER TABLE {}.{} DROP IF EXISTS PARTITION(ds='{}')".format(hive_schema, hive_table, ds)
    spark.sql(query_drop_partition) # menghapus partisi di spark

  df\
    .write\
    .format("parquet")\
    .partitionBy("ds")\
    .mode("append")\
    .saveAsTable("{}.{}".format(hive_schema,hive_table))

  spark.sql("MSCK REPAIR TABLE {}.{}".format(hive_schema, hive_table)) # memperbaiki tabel
  spark.sql("ANALYZE TABLE {}.{} PARTITION (ds={}) COMPUTE STATISTICS".format(hive_schema, hive_table, ds)) # kalkulasi data
  spark.sql("REFRESH TABLE {}.{}".format(hive_schema, hive_table)) # refresh tabel di spark
#  print("Total Record = ", count)


# UDF
def try_or(func, default=None, expected_exc=(Exception,)):
  try:
    return func()
  except expected_exc:
    return default
  
def cek_partisi (df,ds):
  cnt_df = df\
    .filter(df["ds"]==ds).limit(10).count()
  
  if cnt_df!=0 :
    print("{} is not null, bisa digunakan".format(ds))
    return ds
  
  else :    
    print("{} is null".format(ds))
    ds = ((datetime.strptime(ds, "%Y%m%d")) - relativedelta(days=1)).strftime("%Y%m%d")
    return cek_partisi(df,ds)

##             Main Process                
#=========================================


set_timer()

#=>PARAMS

hive_schema = "datamart"
hive_table = "prodrec_cc_fitur_transaction2"

list_date_run = sorted(["06","20"])

curr_date = datetime.now(timezone("Asia/Jakarta"))
date_l5d = curr_date-relativedelta(days=5)

is_proc = curr_date.strftime("%d") in list_date_run

ds = curr_date.strftime("%Y%m%d")

ds_month = curr_date.strftime("%Y%m")

ds_5 = date_l5d.strftime("%Y%m%d")

ds_format = datetime.strptime(ds, "%Y%m%d").strftime('%Y-%m-%d')
ds_format_l5d = date_l5d.strftime('%Y-%m-%d')

ds_l3d = (datetime.strptime(ds_5, "%Y%m%d") + relativedelta(days=0)).strftime("%Y%m%d")
ds_l5d = (datetime.strptime(ds_5, "%Y%m%d") - relativedelta(days=5)).strftime("%Y%m%d")
ds_l30d = (datetime.strptime(ds_5, "%Y%m%d") - relativedelta(days=30)).strftime("%Y%m%d")
ds_l31d = (datetime.strptime(ds_5, "%Y%m%d") - relativedelta(days=31)).strftime("%Y%m%d")
ds_l60d = (datetime.strptime(ds_5, "%Y%m%d") - relativedelta(days=60)).strftime("%Y%m%d")
ds_l61d = (datetime.strptime(ds_5, "%Y%m%d") - relativedelta(days=61)).strftime("%Y%m%d")
ds_l90d = (datetime.strptime(ds_5, "%Y%m%d") - relativedelta(days=90)).strftime("%Y%m%d")
ds_l180d = (datetime.strptime(ds_5, "%Y%m%d") - relativedelta(days=180)).strftime("%Y%m%d")

print("Params `ds` =", ds)
print("Params `ds_format` =", ds_format)
print("Params `ds_month` =", ds_month)
print("Params `ds_l3d` =", ds_l3d)
print("Params `ds_l5d` =", ds_l5d)
print("Params `ds_l30d` =", ds_l30d)
print("Params `ds_l31d` =", ds_l31d)
print("Params `ds_l60d` =", ds_l60d)
print("Params `ds_l61d` =", ds_l61d)
print("Params `ds_l90d` =", ds_l90d)
print("Params `ds_l180d` =", ds_l180d)

#=>END PARAMS

if is_proc:

  ##              Extract               
  #=========================================
  
  #=>EXTRACT
  
  df_userpool = spark.read.table("datamart.prodrec_fact_customer_data")
  df_saving = spark.read.table("datalake.master_saving")
  df_hist = spark.read.table("datalake.trx_history")
  
  print("-- cek partisi saving")
  ds_saving = cek_partisi(df_saving,ds_5)
  print("ds_saving = {}".format(ds_saving))
  ds_userpool = get_last_partition("datamart","prodrec_fact_customer_data")["ds"]
  
  #=>END EXTRACT

  ##             Transform                
  #=========================================

  #=>TRANSFORM

  df_userpool = cleanse_blank_space(df_userpool)
  df_saving = cleanse_blank_space(df_saving)
  df_hist = cleanse_blank_space(df_hist)
  
  df_userpool = df_userpool\
    .filter(df_userpool["ds"]==ds_userpool)
  df_saving = df_saving\
    .filter(df_saving["ds"]==ds_saving,)
    
  #=================================================
  ### FITUR TRANSACTION 2
  #=================================================
  df_user = df_userpool\
    .select(
      F.trim(df_userpool["user_id"]).alias("user_id"),)
    
  df_user_saving = df_user\
    .join(df_saving, df_user["user_id"]==F.trim(df_saving["user_id"]), "INNER",)\
    .select(
      df_user["user_id"].alias("user_id"),
      df_saving["acct_num"].cast(T.IntegerType()).alias("acct_num"),)
  
  #### Fitur income
  df_income = df_hist\
    .filter("CONCAT(year, month, day) between '{0}' and '{1}'".format(ds_l180d, ds_5),)\
    .filter(df_hist["tr_acc"].isNotNull(),)\
    .filter(
        """
            (
              trim(upper(trremk)) like '%SALARY%' 
                or trim(upper(trremk)) like '%CREDIT%' 
                or trim(upper(trremk)) like '%GAJI%'
                or trim(upper(trremk)) like '%PAYROLL%'
                or trancd in ('428','541','555')
            )
            and trim(upper(trremk)) not like '%SHOPE%'
            and trim(upper(trremk)) not like '%BUKALAPA%'
            and trremk not like '%SA Credit GL Debit%'
            and trremk not like '%Salary crediting CashBack Brimo%'
            and trremk not like '%Salary crediting KUPON%'
        """
    )\
    .filter(F.trim(df_hist["tr_type"])=="C")\
    .join(
      df_user_saving, df_hist["tr_acc"].cast(T.IntegerType())==df_user_saving["acct_num"], "INNER",)\
    .groupBy(
      df_user_saving["user_id"].alias("user_id"),)\
    .agg(
      F.sum(df_hist["amt"]).alias("income"),)
  
  #### Fiter Transaction 2
  df_result = df_user\
    .join(df_income, df_user["user_id"]==df_income["user_id"], "LEFT",)\
    .select(
      df_user["user_id"].alias("user_id"),
      F.coalesce(df_income["income"], F.lit(0)).alias("ddhist_na_income_sum_180d"),)
  
  ##                Load                   
  #=========================================

  #=>LOAD

  write_periodic(
    df_result, 
    hive_schema, 
    hive_table, 
    ds)

  #=>END LOAD

##              ETL Time                 
#=========================================
print("Process took {} for ds={}\n".format(get_timer(), ds))

import requests
import re
import json
import os
import glob
import shutil
from time import sleep
api_key = os.environ["api_key"]

def cek_status(id_job):
  data = requests.get(
      os.environ["cek_status"].format(id_job),
      headers = {"Content-Type": "application/json"},
      auth = (api_key,""),
    ).json()
  return data

def eksekusi(id_job):
  requests.post(
        os.environ["eksekusi"].format(id_job),
        headers = {"Content-Type": "application/json"},
        auth = (api_key,"")
      ).json()
  
satpam = '10604'

period_02 = cek_status(satpam)

if period_02['latest']['status'] == 'succeeded':
  eksekusi(satpam)
else :
  print("Satpam sedang mengecek")

spark.stop()