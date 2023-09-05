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
hive_table = "prodrec_cc_fitur_demog"

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
  df_branch = spark.read.table("datalake.master_branch")
  df_hist = spark.read.table("datalake.trx_history")
  df_loan = spark.read.table("datalake.master_loan")
  
  print("-- cek partisi saving")
  ds_saving = cek_partisi(df_saving,ds_5)
  print("ds_saving = {}".format(ds_saving))
  ds_loan = get_last_partition('datalake', 'master_loan')['ds']
  ds_userpool = get_last_partition("datamart","prodrec_fact_customer_data")["ds"]
  
  #=>END EXTRACT

  ##             Transform                
  #=========================================

  #=>TRANSFORM

  df_userpool = cleanse_blank_space(df_userpool)
  df_saving = cleanse_blank_space(df_saving)
  df_branch = cleanse_blank_space(df_branch)
  df_hist = cleanse_blank_space(df_hist)
  df_loan = cleanse_blank_space(df_loan)
  
  df_userpool = df_userpool\
    .filter(df_userpool["ds"]==ds_userpool)
  df_saving = df_saving\
    .filter(df_saving["ds"]==ds_saving,)
  df_loan = df_loan\
    .filter(df_loan["ds"]==ds_loan,)
    
  #=================================================
  ### FITUR DEMOGRAPHY
  #=================================================
  ### Ambil Data Branch
  df_branch = df_branch\
    .filter(~df_branch["rgdesc"].isin('KANINS                                  ','KAS KANPUS                              '))\
    .select(
      F.trim(df_branch["region"]).alias("region"),
      F.trim(df_branch["rgdesc"]).alias("rgdesc"),
      F.trim(df_branch["mainbr"]).alias("mainbr"),
      F.trim(df_branch["mbdesc"]).alias("mbdesc"),
      F.trim(df_branch["branch"]).alias("branch"),
      F.trim(df_branch["brdesc"]).alias("brdesc"),)\
    .distinct()

  #### Pengambilan Usia, Jenis Pekerjaan, Pendidikan, dan first rgdesc
  df_saving = df_saving\
    .filter(
      df_saving["type"].isin('B1','B2','B3','B4','B5','BA','BB',
                             'BC','BD','BE','BF','BG','BH','BI',
                             'BJ','BK','BL','BM','BN','BO','BP',
                             'BQ','BU','BW','BX','BZ','EB','EK',
                             'EL','FG','FW','LN','MN','PX','TA',
                             'TB','TH','TM','TN','TP','PX','TS',
                             'TT','TU','TV','TZ','UA','UB','UC',
                             'UN','VA','VB','VC','VD','VE','VF',
                             'VG','VH','VI','VJ','VK','VL','VM',
                             'VN','VO','VP','VR','VU','VW','VX',
                             'VY','VZ','XA','LM','TR'),)\
    .filter(df_saving["status"]!=2,)\
    .filter(~df_saving["acct_num"].cast(T.StringType()).substr(-3,2).isin('40','53'),)\
    .filter(df_saving["BRANCH"].isNotNull(),)\
    .withColumn(
      "rownum",
      F.row_number().over(W.Window.partitionBy("user_id").orderBy(F.col("OPEN_DATE"))),)\
    
  df_saving = df_saving\
    .filter(
      df_saving["rownum"]==1,)\
    .filter(
      df_saving["OPEN_DATE"]<ds_format_l5d,)
  
  df_user = df_userpool\
    .select(
      F.trim(df_userpool["user_id"]).alias("user_id"),
      df_userpool["open_date_saving"].alias("open_date_saving"),
      df_userpool["usia"].alias("usia"),
      df_userpool["pendidikan"].alias("pendidikan"),
      df_userpool["jenis_pekerjaan"].alias("jenis_pekerjaan"),
      df_userpool["penghasilan_per_bulan"].alias("penghasilan_per_bulan"),)
    
  df_user_saving = df_user\
    .join(df_saving, (df_user["user_id"]==F.trim(df_saving["user_id"])) &
                    (df_user["open_date_saving"]==df_saving["open_date"]), "INNER",)\
    .select(
      df_user["user_id"].alias("user_id"),
      df_user["open_date_saving"].alias("open_date_saving"),
      df_user["usia"].alias("usia"),
      df_user["pendidikan"].alias("pendidikan"),
      df_user["jenis_pekerjaan"].alias("jenis_pekerjaan"),
      df_user["penghasilan_per_bulan"].alias("penghasilan_per_bulan"),
      F.trim(df_saving["BRANCH"]).alias("branch"),)
    
  df_user_saving_branch = df_user_saving\
    .join(df_branch, df_user_saving["branch"]==df_branch["branch"], "INNER",)\
    .select(
      df_user_saving["user_id"].alias("user_id"),
      df_user_saving["open_date_saving"].alias("open_date_saving"),
      df_user_saving["usia"].alias("usia"),
      df_user_saving["pendidikan"].alias("pendidikan"),
      df_user_saving["jenis_pekerjaan"].alias("jenis_pekerjaan"),
      df_user_saving["penghasilan_per_bulan"].alias("penghasilan_per_bulan"),
      df_branch["rgdesc"].alias("first_rgdesc_register"),)\
    .withColumn(
      "first_rgdesc_register",
      F.when(
        F.col("first_rgdesc_register")=="Bandung", F.lit("JABAR"),)\
      .when(
        F.col("first_rgdesc_register").isin("KANWIL MALANG","Surabaya"), F.lit("JATIM"),)\
      .when(
        F.col("first_rgdesc_register").isin("Yogyakarta","Semarang"), F.lit("JATENG-DIY"),)\
      .when(
        F.col("first_rgdesc_register").isin("KANWIL JAKARTA 3","DKI2","DKI"), F.lit("DKI"),)\
      .when(
        F.col("first_rgdesc_register").isin("KANWIL PEKANBARU","Padang","KANWIL BANDAR LAMPUNG","Medan","Palembang"), F.lit("DKI"),)\
      .when(
        F.col("first_rgdesc_register").isin("Makassar","KANWIL JAYAPURA","Manado"), F.lit("SULAWESI-PAPUA"),)\
      .when(
        F.col("first_rgdesc_register")=="Banjarmasin", F.lit("KALIMANTAN"),)\
      .when(
        F.col("first_rgdesc_register")=="Denpasar", F.lit("BALI"),)\
      .when(
        F.col("first_rgdesc_register")=="K C K", F.lit("KCK"),)\
      .otherwise(F.lit(None)),)\
    .distinct()
    
  #### Fitur is briguna
  df_pinjaman = df_loan\
    .filter(
      df_loan["ds"]==ds_loan,)\
    .select(
      F.trim(df_loan["user_id"]).alias("user_id"),
      df_loan["status"].alias("status"),
      df_loan["type"].alias("type"),)

  df_pinjaman = df_pinjaman\
    .filter(
      df_pinjaman["status"]==1,)\
    .filter(
      df_pinjaman["type"].isin('10','1E','1F','3E','5F','CR','F8','F9','FH',
                               'FI','HQ','HT','K1','K2','K3','K4','K5','KJ',
                               'L1','L2','L3','MJ','MS','PH','QP','W0','W1',
                               'W2','W3','W4','W5','W6','W7','W8','WL','WM',
                               'WN','WO','WP','WR','WT','Z8','ZN','ZO','ZP',
                               'ZQ','ZR','ZS','ZT','ZU','ZV','ZW','ZX'),)\
    .select(
      df_pinjaman["user_id"].alias("user_id"),
      F.lit(1).alias("is_pinjaman_aktif"),)\
    .distinct()
    
  df_user_pinjaman = df_user\
    .join(df_pinjaman, df_user["user_id"]==df_pinjaman["user_id"], "INNER",)\
    .select(
      df_user["user_id"].alias("user_id"),
      df_pinjaman["is_pinjaman_aktif"].alias("is_pinjaman_aktif"),)
  
  #### Fiter Demography
  df_result = df_user\
    .join(df_user_saving_branch, df_user["user_id"]==df_user_saving_branch["user_id"], "LEFT",)\
    .join(df_user_pinjaman, df_user["user_id"]==df_user_pinjaman["user_id"], "LEFT",)\
    .select(
      df_user["user_id"].alias("user_id"),
      df_user["usia"].alias("demog_personal_usia_na_na"),
      df_user["pendidikan"].alias("demog_personal_pendidikan_na_na"),
      df_user["jenis_pekerjaan"].alias("demog_personal_jenis_pekerjaan_na_na"),
      df_user["penghasilan_per_bulan"].alias("demog_personal_penghasilan_perbulan_na_na"),
      df_user_saving_branch["first_rgdesc_register"].alias("demog_id_rgdesc_na_na"),
      F.coalesce(df_user_pinjaman["is_pinjaman_aktif"], F.lit(0)).alias("demog_is_pinjaman_aktif_na_na"),)
  
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