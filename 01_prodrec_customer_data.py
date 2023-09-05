#=========================================
###  POSISI   : 15 FEBRUARY 2023
###  PIC      : HARYO SETOWIBOWO
###  CASE     : CREDIT CARD PRODUCT RECOMMENDATION
###  TASK     : ETL ALL USER CUSTOMER DATA
#=========================================
##           Import Dependencies           
#=========================================
from time import time, sleep
from pytz import timezone
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as F, types as T, window as W

import re

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
  .appName("ETL ALL USER CUSTOMER DATA")\
  .config("spark.sql.crossJoin.enabled", "true")\
  .config("spark.dynamicAllocation.enabled", "false")\
  .config("spark.executor.instances", "4")\
  .config("spark.executor.cores", "4")\
  .config("spark.executor.memory", "4g")\
  .config("spark.yarn.executor.memoryOverhead", "2g")\
  .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")\
  .config("spark.ui.showConsoleProgress", "false")\
  .config("spark.network.timeout", 60)\
  .enableHiveSupport()\
  .getOrCreate()

#  Config  8  5  8  4 = Process took 00:12:37 for ds=20220203 Total Record =  7458469
#  Config  8  5  8  4 = Process took 00:19:45 for ds=20220220 Total Record =  7695189 
  

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
#  if count!=0:
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

def get_ktp_cleansed(datum):
  datum = ''.join(e for e in str(datum) if e.isalnum())
  datum = re.sub("\D", "", datum)
  if len(datum)>=16:
    return datum
  else:
    return None
ktp_cleansed = F.udf(lambda col: try_or(lambda:get_ktp_cleansed(col)), T.StringType())

def get_age(birth_date, ds):
  if type(birth_date) == str:
    birth_date = datetime.strptime(birth_date, "%Y-%m-%d")
  today = datetime.strptime(ds, "%Y%m%d")
  return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
age = F.udf(lambda col1, col2: try_or(lambda:get_age(col1, col2)), T.IntegerType())

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
hive_table = "prodrec_fact_customer_data"

list_date_run = sorted(["06","20"])

curr_date = datetime.now(timezone("Asia/Jakarta"))
date_l5d = curr_date-relativedelta(days=5)

is_proc = curr_date.strftime("%d") in list_date_run

ds = curr_date.strftime('%Y%m%d')
ds_l5d = date_l5d.strftime('%Y%m%d')
ds_format_l5d = date_l5d.strftime('%Y-%m-%d')

ds_cc = get_last_partition("datamart","master_cc_customer_data")["ds"]

print("Params `ds` =", ds)
print("Params `ds_l5d` =", ds_l5d)
print("Params `ds_format_l5d` =", ds_format_l5d)  
print("Params `ds_cc` =", ds_cc)

#=>END PARAMS

if is_proc:
  
  ##              Extract               
  #=========================================

  #=>EXTRACT

  df_cust_cc = spark.read.table("datamart.master_cc_customer_data")
  df_saving = spark.read.table("datalake.master_saving")
  df_demog = spark.read.table("datamart.customer_info_all")
  df_penghasilan = spark.read.table("datamart.customer_info_all_backup")

  print("-- cek partisi saving")
  ds_saving = cek_partisi(df_saving,ds_l5d)
  print("ds_saving = {}".format(ds_saving))
  ds_penghasilan = get_last_partition("datamart","customer_info_all_backup")["ds"]

  #=>END EXTRACT

  ##             Transform                
  #=========================================

  #=>TRANSFORM

  df_saving = df_saving\
    .filter(df_saving["ds"]==ds_saving,)\
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
    .groupBy(
      F.trim(df_saving["user_id"]).alias("user_id"),)\
    .agg(
      F.min(df_saving["OPEN_DATE"]).alias("open_date_saving"),)

  df_saving = df_saving\
    .filter(
      df_saving["open_date_saving"]<ds_format_l5d,)
    
  df_penghasilan = df_penghasilan\
    .filter(
      df_penghasilan["ds"]==ds_penghasilan,)

  df_data_demog = df_demog\
    .join(
      df_penghasilan, F.trim(df_demog["user_id"])==df_penghasilan["user_id"], "LEFT",)\
    .select(
      F.trim(df_demog["user_id"]).alias("user_id"),
      F.date_format(df_demog["tanggal_lahir"],"yyyy-MM-dd").alias("tanggal_lahir"),
      df_demog["jenis_kelamin"].alias("jenis_kelamin"),
      df_demog["kode_jenis_pekerjaan"].alias("kode_jenis_pekerjaan"),
      df_demog["jenis_pekerjaan"].alias("jenis_pekerjaan_nf"),
      df_demog["kode_pendidikan"].alias("kode_pendidikan"),
      df_demog["pendidikan"].alias("pendidikan_nf"),
      df_demog["penghasilan_per_bulan"].alias("penghasilan1"),
      df_penghasilan["penghasilan_per_bulan"].alias("penghasilan2"),)\
    .withColumn(
      "penghasilan_per_bulan",
      F.when(
        F.col("penghasilan1").isNotNull(), F.col("penghasilan1"),)\
      .otherwise(F.col("penghasilan2")),)

  df_userpool = df_saving\
    .join(df_data_demog, df_saving["user_id"]==df_data_demog["user_id"], "LEFT",)\
    .select(
      df_saving["user_id"],
      df_saving["open_date_saving"],
      df_data_demog["tanggal_lahir"],
      df_data_demog["jenis_kelamin"],
      df_data_demog["kode_jenis_pekerjaan"],
      df_data_demog["jenis_pekerjaan_nf"],
      df_data_demog["kode_pendidikan"],
      df_data_demog["pendidikan_nf"],
      df_data_demog["penghasilan_per_bulan"],)\
    .withColumn(
      "jenis_pekerjaan",
      F.when(
        F.col("kode_jenis_pekerjaan").isin(18,23,7), F.lit("TIDAK BERPENGHASILAN"),)\
      .when(
        F.col("kode_jenis_pekerjaan").isin(11,10,2,22,5,6,26,15,25,
                                           12,14,19,13,1,27,3,24), F.lit("PEKERJA NON-ASN/BUMN/WIRASWASTA"),)\
      .when(
        F.col("kode_jenis_pekerjaan").isin(16,28), F.lit("WIRASWASTA"),)\
      .when(
        F.col("kode_jenis_pekerjaan").isin(4,17), F.lit("ASN"),)\
      .when(
        F.col("kode_jenis_pekerjaan")==9, F.lit("PENSIUNAN"),)\
      .when(
        F.col("kode_jenis_pekerjaan")==21, F.lit("BUMN"),)\
      .when(
        F.col("kode_jenis_pekerjaan").isin(8,20), F.lit("TNI/POLRI"),)\
      .otherwise(F.lit("UNDEFINED")),)\
    .withColumn(
      "pendidikan",
      F.when(
        F.col("kode_pendidikan")==1, F.lit("SD/SMP"),)\
      .when(
        F.col("kode_pendidikan")==2, F.lit("SMA"),)\
      .when(
        F.col("kode_pendidikan").isin(3,4,5,6), F.lit("PENDIDIKAN LANJUT"),)\
      .otherwise(F.lit("UNDEFINED")),)\
    .withColumn(
      "penghasilan_per_bulan",
      F.when(
        F.col("penghasilan_per_bulan")=="S/D 5 JUTA", F.lit("S/D 5 JUTA"),)\
      .when(
        F.col("penghasilan_per_bulan")=="5 - 10 JUTA", F.lit("5 - 10 JUTA"),)\
      .when(
        F.col("penghasilan_per_bulan")=="10 - 50 JUTA", F.lit("10 - 50 JUTA"),)\
      .when(
        (F.col("penghasilan_per_bulan").isin("50 - 100 JUTA","DIATAS 100 JUTA")) | (F.col("penghasilan_per_bulan")=="DIATAS 50 JUTA"), F.lit("DIATAS 50 JUTA"),)\
      .otherwise(F.lit("S/D 5 JUTA")),)\
    .distinct()

  df_cust_cc = df_cust_cc\
    .filter(df_cust_cc["ds"]==ds_cc,)\
    .filter(df_cust_cc["open_date_cc"].isNotNull(),)\
    .filter(df_cust_cc["open_date_cc"]<ds_format_l5d,)\
    .select(df_cust_cc["user_id"].alias("user_id"),)\
    .distinct()

  df_cust_status = df_savingmaster\
    .filter(df_savingmaster["ds"]==ds_l5d,)\
    .groupBy(df_savingmaster["user_id"].alias("user_id"),)\
    .agg(
      F.max(
        F.when(
          df_savingmaster["status"]==1, 
          F.lit(1))
        .otherwise(F.lit(0))).alias("is_active"),)

  df_cust_inactive = df_cust_status\
    .filter(df_cust_status["is_active"]==0,)

  df_result = df_userpool\
    .join(df_cust_cc, df_userpool["user_id"]==df_cust_cc["user_id"], "LEFT")\
    .join(df_cust_inactive, df_userpool["user_id"]==df_cust_inactive["user_id"], "LEFT")\
    .select(
      df_userpool["user_id"].alias("user_id"),
      df_userpool["tanggal_lahir"].alias("tanggal_lahir"),
      age(df_userpool["tanggal_lahir"], F.lit(ds_l5d)).alias("usia"),
      df_userpool["jenis_kelamin"].alias("jenis_kelamin"),
      df_userpool["pendidikan"].alias("pendidikan"),
      df_userpool["jenis_pekerjaan"].alias("jenis_pekerjaan"),
      df_userpool["penghasilan_per_bulan"].alias("penghasilan_per_bulan"),
      df_userpool["open_date_saving"].alias("open_date_saving"),
      F.lit(ds_format_l5d).alias("event_date"),
      F.when(df_cust_cc["user_id"].isNotNull(),F.lit(1)).otherwise(F.lit(0)).alias("is_credit_card"),
      F.when(df_cust_inactive["user_id"].isNotNull(),F.lit(1)).otherwise(F.lit(0)).alias("is_inactive"),)

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
