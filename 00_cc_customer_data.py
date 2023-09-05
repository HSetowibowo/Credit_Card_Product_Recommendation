#=========================================
###  POSISI   : 14 MARET 2023
###  PIC      : HARYO SETOWIBOWO
###  CASE     : CC CUSTOMER DATA 
###  TASK     : ETL CC CUSTOMER DATA
#=========================================
##           Import Dependencies           
#=========================================
from time import time, sleep
from pytz import timezone
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as F, types as T, window as W
from functools import reduce
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
  .appName("ETL CC CUSTOMER DATA")\
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

#  Config  4  5  8  4 = Process took 00:12:37 for ds=20220203 Total Record =  7458469
  

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
  count = df.count()
  if count!=0:
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
  print("Total Record = ", count)


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

def remove_blank_space(x):
  return F.when((F.trim(F.col(x))!='')&(F.trim(F.upper(F.col(x)))!='NAN'), F.trim(F.col(x))).otherwise(F.lit(None))

def cleanse_blank_space(df_to_clean, *exclude):
  col_to_clean = []
  for val in df_to_clean.dtypes:
    if val[1] == "string" and val[0] not in (exclude):
      col_to_clean.append(val[0])
  return reduce(lambda df, x: df.withColumn(x, remove_blank_space(x)), set(col_to_clean), df_to_clean)


##             Main Process                
#=========================================

set_timer()
#=>PARAMS

hive_schema = "datamart"
hive_table = "master_cc_customer_data"

curr_date = datetime.now(timezone("Asia/Jakarta"))

ds = (curr_date-relativedelta(days=2)).strftime('%Y%m%d')

ds_format = curr_date.strftime('%Y-%m-%d')
ds_crd = get_last_partition("datalake","customer_credit_daily")["ds"]
ds_cus = get_last_partition("datalake","customer_info_daily")["ds"]

print("Params `ds` =", ds)
print("Params `ds_crd` =", ds_crd)
print("Params `ds_cus` =", ds_cus)

#=>END PARAMS

##Extract
##=========================================

#=>EXTRACT

df_cust_info = spark.read.table("datamart.customer_info_all")
df_crd = spark.read.table("datalake.customer_credit_info_daily")
df_cus = spark.read.table("datalake.customer_demography_info_daily")
df_crm = spark.read.table("datalake.master_credit_card")

df_cust_info = cleanse_blank_space(df_cust_info)
df_crd = cleanse_blank_space(df_crd)
df_cus = cleanse_blank_space(df_cus)
df_crm = cleanse_blank_space(df_crm)

#=>END EXTRACT

##Transform
##=========================================

#=>TRANSFORM

### Filter Partisi Table CC
df_cc = df_crd\
  .filter(
    df_crd["ds"]==ds_crd,)

## Filter Partisi Table Master CC
df_master_cc = df_cus\
  .filter(
    df_cus["ds"]==ds_cus,)\
  .filter(
    df_cus["id_number"].isNotNull(),)\
  .select(
    F.trim(df_cus["acc_number"]).alias("acc_number"),
    F.trim(df_cus["id_number"]).alias("id_number"),)\
  .distinct()
  
## Ambil Card CC Number
df_cc_no = df_cc\
  .join(
    df_master_cc, F.trim(df_cc["cust_number"])==df_master_cc["acc_number"], "INNER",)\
  .select(
    F.trim(df_cc["cust_number"]).alias("cust_number"),
    F.trim(df_cc["card_number"]).alias("card_number"),
    df_master_cc["id_number"].alias("id_number"),
    df_cc["open_date"].alias("open_date"),)

df_cc_no = df_cc_no\
  .groupBy(
    df_cc_no["cust_number"].alias("cust_number"),
    df_cc_no["card_number"].alias("card_number"),
    df_cc_no["id_number"].alias("id_number"),)\
  .agg(
    F.min(df_cc_no["open_date"]).alias("open_date"),)
  
df_cc_no = df_cc_no\
  .withColumn(
    "open_date",
    df_cc_no["open_date"] + F.expr("INTERVAL 7 HOURS"),)
  
## Ambil Data KTP
df_ktp = df_cust_info\
  .filter(
    df_cust_info["id_number"].isNotNull(),)\
  .filter(
    ~df_cust_info["id_number"].isin('', ' ','0','123456789'),)\
  .select(
    F.trim(df_cust_info["user_id"]).alias("user_id"),
    F.trim(df_cust_info["id_number"]).alias("id_number"),)\
  .distinct()
  
df_ktp = df_ktp\
  .withColumn(
    "rank",
    F.row_number().over(W.Window.partitionBy("id_number").orderBy(df_ktp["user_id"].desc())),)
  
df_ktp_clean = df_ktp\
  .filter(
    df_ktp["rank"]==1,)
  
## OUTPUT CC Open Date
df_cc_output = df_cc_no\
  .join(
    df_ktp_clean, F.md5(df_cc_no["id_number"])==df_ktp_clean["id_number"], "INNER",)\
  .select(
    df_ktp_clean["user_id"].alias("user_id"),
    df_cc_no["cust_number"].alias("custno_cc"),
    df_cc_no["card_number"].alias("acctno_cc"),
    df_cc_no["open_date"].alias("open_date_cc"),)\
  .distinct()\
  .withColumn(
    "id_len",
    F.length(F.col("user_id")),)
  
df_crm_filter = df_crm\
  .select(
    F.trim(df_crm["user_id"]).alias("user_id_crm"),
    F.trim(df_crm["cust_nmbr"]).alias("cust_nmbr"),
    F.trim(df_crm["cc_nmbr"]).alias("cc_nmbr"),
    df_crm["cc_date_open"].alias("open_date_cc"),)
  
df_cc_filter = df_cc_output\
  .join(
    df_crm_filter, df_cc_output["custno_cc"]==df_crm_filter["cust_nmbr"], "LEFT",)\
  .select(
    F.when(
      ((df_cc_output["id_len"]!=7) & (df_crm_filter["user_id_crm"].isNotNull())), df_crm_filter["user_id_crm"],)\
    .otherwise(df_cc_output["user_id"]).alias("user_id"),
    df_cc_output["acctno_cc"].alias("acctno_cc"),
    df_cc_output["open_date_cc"].alias("open_date_cc"),)
  
df_cust_cc = df_cc_filter\
  .filter(df_cc_filter["open_date_cc"]<ds_format,)\
  .select(
    df_cc_filter["user_id"].alias("user_id"),
    df_cc_filter["acctno_cc"].alias("acctno_cc"),
    df_cc_filter["open_date_cc"].alias("open_date_cc"),)

#=>END TRANSFORM

##Load
##=========================================

write_periodic(
  df_cust_cc, 
  hive_schema, 
  hive_table, 
  ds)

#=>END LOAD

##              ETL Time                 
#=========================================
print("Process took {} for ds={}\n".format(get_timer(), ds))

spark.stop()