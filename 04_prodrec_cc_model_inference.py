#=========================================
###  POSISI   : 09 MARCH 2023
###  PIC      : HARYO SETOWIBOWO
###  CASE     : CREDIT CARD PRODUCT RECOMMENDATION CC
###  TASK     : ETL CC MODEL INFERENCE
#=========================================
##           Import Dependencies           
#=========================================
from time import time, sleep
from pytz import timezone
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as F, types as T, window as W
import warnings
import math
import pickle
import numpy as np

warnings.filterwarnings('ignore')

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
  .appName("ETL CC MODEL INFERENCE")\
  .config("spark.sql.crossJoin.enabled", "true")\
  .config("spark.dynamicAllocation.enabled", "false")\
  .config("spark.executor.instances", "4")\
  .config("spark.executor.cores", "5")\
  .config("spark.executor.memory", "8g")\
  .config("spark.yarn.executor.memoryOverhead", "4g")\
  .config("spark.driver.maxResultSize", "16g")\
  .config("spark.kryoserializer.buffer.max", "512")\
  .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")\
  .config("spark.ui.showConsoleProgress", "false")\
  .config("spark.network.timeout", 60)\
  .enableHiveSupport()\
  .getOrCreate()

#  Config  8  5  8  4 = All Process took 00:39:05
  

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

def write_periodic(df, hive_schema, hive_table, ds, batch):
#  count = df.count()
#  if count!=0:
  df = df\
    .withColumn("ds", F.lit(ds))\
    .withColumn("batch", F.lit(batch))\
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

  repartition_number = 1

  df = df.repartition(repartition_number)

  if spark.sql("SHOW TABLES IN {} LIKE '{}'".format(hive_schema, hive_table)).count() != 0:
    query_drop_partition = "ALTER TABLE {}.{} DROP IF EXISTS PARTITION(ds='{}', batch='{}')".format(hive_schema, hive_table, ds, batch)
    spark.sql(query_drop_partition) # menghapus partisi di spark

  df\
    .write\
    .format("parquet")\
    .partitionBy("ds", "batch")\
    .mode("append")\
    .saveAsTable("{}.{}".format(hive_schema,hive_table))

  spark.sql("MSCK REPAIR TABLE {}.{}".format(hive_schema, hive_table)) # memperbaiki tabel
  spark.sql("ANALYZE TABLE {}.{} PARTITION(ds='{}', batch='{}') COMPUTE STATISTICS".format(hive_schema, hive_table, ds, batch)) # kalkulasi data
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
start_time = START_TIME

#=>PARAMS

hive_schema = "datamart"
hive_table = "prodrec_cc_fact_model_result"

list_date_run = sorted(["06","20"])

curr_date = datetime.now(timezone("Asia/Jakarta"))
is_proc = curr_date.strftime("%d") in list_date_run

threshold_proba=0.0187

list_feature = [
  'demog_personal_usia_na_na',
  'demog_personal_pendidikan_na_na',
  'demog_personal_jenis_pekerjaan_na_na',
  'demog_personal_penghasilan_perbulan_na_na',
  'demog_id_rgdesc_na_na',
  'demog_is_pinjaman_aktif_na_na',
  'saving_casa_aktif_count_na',
  'saving_casa_aktif_sum_na',
  'ddhist_na_income_sum_180d',
  'transaction_ratio_sumcasa_sumtrxcred_na_l30d',
  'transaction_ratio_sumcasatd_sumtrxdeb_na_l30d'
]

categorical = ['demog_personal_pendidikan_na_na','demog_personal_jenis_pekerjaan_na_na','demog_personal_penghasilan_perbulan_na_na','demog_id_rgdesc_na_na']

list_output = [
  'id',
  'cifno',
  'pred_proba',
  'is_treat'
]

ds = get_last_partition("datamart","prodrec_cc_fact_dataset_inference")["ds"]

print("Params `ds` =", ds)

#=>END PARAMS

if is_proc:

  ##              Extract               
  #=========================================

  #=>EXTRACT

  df_source = spark.read.table("datamart.prodrec_cc_fact_dataset_inference")

  #=>END EXTRACT

  ##             Transform                
  #=========================================

  #=>TRANSFORM

  df_source = df_source\
    .filter(df_source["ds"]==ds,)\
    .withColumn("row_number", F.substring(df_source["id"], -9, 9).cast(T.IntegerType()))\

  cnt = df_source.count()

  batch_size = 500000
  batch_total = math.ceil(cnt/batch_size)

  for batch in range(batch_total):

    set_timer()

    row_min = (batch * batch_size)+1
    row_max = (batch+1) * batch_size

    df_pandas = df_source\
      .filter(df_source["row_number"].between(row_min,row_max),)\
      .toPandas()

    df_pandas.dropna(subset=['demog_id_rgdesc_na_na'], inplace=True)

    objects = ['demog_personal_pendidikan_na_na','demog_personal_jenis_pekerjaan_na_na','demog_personal_penghasilan_perbulan_na_na','demog_cif_rgdesc_na_na']
    missing_not_allowed = list(set(list_feature) - set(objects))

#    df_pandas[categorical] = df_pandas[categorical].fillna(value='missing')

    df_pandas[["demog_is_pinjaman_aktif_na_na","saving_casa_aktif_count_na","saving_casa_aktif_sum_na","ddhist_na_income_sum_180d","transaction_ratio_sumcasa_sumtrxcred_na_l30d","transaction_ratio_sumcasatd_sumtrxdeb_na_l30d"]] = \
    df_pandas[["demog_is_pinjaman_aktif_na_na","saving_casa_aktif_count_na","saving_casa_aktif_sum_na","ddhist_na_income_sum_180d","transaction_ratio_sumcasa_sumtrxcred_na_l30d","transaction_ratio_sumcasatd_sumtrxdeb_na_l30d"]].astype(np.float64)      

    for i in range(0, len(missing_not_allowed)):
      df_pandas[missing_not_allowed[i]] = df_pandas[missing_not_allowed[i]].fillna(df_pandas[missing_not_allowed[i]].median())

    for i, feat in enumerate(categorical):
      pkl_file = open('Model/[v2_230309]_{}_{}_encoder_prod_rec_cc.pkl'.format(i, feat), 'rb')
      le = pickle.load(pkl_file)
      pkl_file.close()
      df_pandas[feat] = le.transform(df_pandas[feat])

    model=pickle.load(open('Model/[v2_230309]_cc_prodrec_model_xgboost150.pkl', 'rb'))

    df_pandas['pred_proba'] = model.predict_proba(df_pandas[list_feature])[:,1]
    df_pandas['is_treat'] = np.where(df_pandas['pred_proba'] > threshold_proba, 1, 0)

    df_result = spark.createDataFrame(df_pandas[list_output])

    #=>END TRANSFORM

    ##                Load                   
    #=========================================

    #=>LOAD

    write_periodic(
      df_result, 
      hive_schema, 
      hive_table, 
      ds, str(batch+1).zfill(len(str(batch_total))))

    #=>END LOAD

    ##              ETL Time                 
    #=========================================
    print("Process took {} for batch = {}\n".format(get_timer(), str(batch+1).zfill(len(str(batch_total)))))

  print("All Process took {}".format(get_timer(start_time)))

spark.stop()