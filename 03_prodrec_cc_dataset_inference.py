#=========================================
###  POSISI   : 01 MARCH 2023
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

##             Main Process                
#=========================================


set_timer()

#=>PARAMS

hive_schema = "datamart"
hive_table = "prodrec_cc_fact_dataset_inference"

list_date_run = sorted(["06","20"])

curr_date = datetime.now(timezone("Asia/Jakarta"))
date_l5d = curr_date-relativedelta(days=5)

is_proc = curr_date.strftime("%d") in list_date_run

ds = curr_date.strftime("%Y%m%d")

ds_month = curr_date.strftime("%Y%m")

ds_5 = date_l5d.strftime("%Y%m%d")

ds_format = datetime.strptime(ds, "%Y%m%d").strftime('%Y-%m-%d')

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

  df_cust_data = spark.read.table("datamart.prodrec_fact_customer_data")
  df_demog = spark.read.table("datamart.prodrec_cc_fitur_demog")
  df_portfolio = spark.read.table("datamart.prodrec_cc_fitur_portfolio")
  df_trans2 = spark.read.table("datamart.prodrec_cc_fitur_transaction2")
  df_trans3 = spark.read.table("datamart.prodrec_cc_fitur_transaction3")
  
  ds_cust_data = get_last_partition("datamart","bribrain_britama_prodrec_fact_customer_data")["ds"]
  ds_demog = get_last_partition("datamart","prodrec_cc_fitur_demog")["ds"]
  ds_portfolio = get_last_partition("datamart","prodrec_cc_fitur_portfolio")["ds"]
  ds_trans2 = get_last_partition("datamart","prodrec_cc_fitur_transaction2")["ds"]
  ds_trans3 = get_last_partition("datamart","prodrec_cc_fitur_transaction3")["ds"]
  
  #=>END EXTRACT

  ##             Transform                
  #=========================================

  #=>TRANSFORM

  df_cust_data = cleanse_blank_space(df_cust_data)
  df_demog = cleanse_blank_space(df_demog)
  df_portfolio = cleanse_blank_space(df_portfolio)
  df_trans2 = cleanse_blank_space(df_trans2)
  df_trans3 = cleanse_blank_space(df_trans3)
    
  df_demog = df_demog\
    .filter(df_demog["ds"]==ds_demog,)
    
  df_portfolio = df_portfolio\
    .filter(df_portfolio["ds"]==ds_portfolio,)
    
  df_trans2 = df_trans2\
    .filter(df_trans2["ds"]==ds_trans2,)
    
  df_trans3 = df_trans3\
    .filter(df_trans3["ds"]==ds_trans3,)

  df_cust_data = df_cust_data\
    .filter(df_cust_data["ds"]==ds_cust_data,)\
    .filter(df_cust_data["is_credit_card"]==0,)\
    .filter(df_cust_data["is_inactive"]==0,)

  #=================================================
  ### UNION FITUR
  #=================================================

  #=================================================
  ### RESULT TABLE
  #=================================================

  df_result = df_cust_data\
    .join(df_demog, F.trim(df_cust_data["user_id"])==F.trim(df_demog["user_id"]), "LEFT",)\
    .join(df_portfolio, F.trim(df_cust_data["user_id"])==F.trim(df_portfolio["user_id"]), "LEFT",)\
    .join(df_trans2,F.trim(df_cust_data["user_id"])==F.trim(df_trans2["user_id"]), "LEFT",)\
    .join(df_trans3,F.trim(df_cust_data["user_id"])==F.trim(df_trans3["user_id"]), "LEFT",)\
    .select(
      F.concat(
        F.lit(ds),
        F.lit("02"),
        F.lpad(
            F.row_number().over(
              W.Window.orderBy(
            F.rand())), 9, '0')).alias("id"),
      df_cust_data["user_id"].alias("user_id"),
      df_demog["demog_personal_usia_na_na"].alias("demog_personal_usia_na_na"),
      df_demog["demog_personal_pendidikan_na_na"].alias("demog_personal_pendidikan_na_na"),
      df_demog["demog_personal_jenis_pekerjaan_na_na"].alias("demog_personal_jenis_pekerjaan_na_na"),
      df_demog["demog_personal_penghasilan_perbulan_na_na"].alias("demog_personal_penghasilan_perbulan_na_na"),
      df_demog["demog_id_rgdesc_na_na"].alias("demog_id_rgdesc_na_na"),
      df_demog["demog_is_pinjaman_aktif_na_na"].alias("demog_is_pinjaman_aktif_na_na"),
      F.coalesce(df_portfolio["saving_casa_aktif_count_na"],F.lit(0)).alias("saving_casa_aktif_count_na"),
      F.coalesce(df_portfolio["saving_casa_aktif_sum_na"],F.lit(0)).alias("saving_casa_aktif_sum_na"),
      F.coalesce(df_trans2["ddhist_na_income_sum_180d"],F.lit(0)).alias("ddhist_na_income_sum_180d"),
      F.coalesce(df_trans3["transaction_ratio_sumcasa_sumtrxcred_na_l30d"],F.lit(0)).alias("transaction_ratio_sumcasa_sumtrxcred_na_l30d"),
      F.coalesce(df_trans3["transaction_ratio_sumcasatd_sumtrxdeb_na_l30d"],F.lit(0)).alias("transaction_ratio_sumcasatd_sumtrxdeb_na_l30d"),)\
    .distinct()\
    .orderBy("id")

  #=>END TRANSFORM


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

spark.stop()