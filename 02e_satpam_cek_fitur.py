#=========================================
##           Import Dependencies           
#=========================================
import string, re, os
from time import time, sleep, gmtime, strftime
from pytz import timezone
from functools import reduce
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql import Window as W

#import os
#import glob
#from bob_telegram_tools.bot import TelegramBot
#import json
import requests
#import shutil
#import telegram as tele

##              Pandas Config              
#=========================================
import pandas as pd
pd.options.display.html.table_schema=True
pd.options.display.max_columns=999
pd.options.display.max_rows=999

##               Functions                 
#=========================================
  
api_key = os.environ["api_key"]

curr_date = strftime("%Y-%m-%d", gmtime())

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
  
demog = '9950'
portfolio = '9951'
trx2 = '9952'
trx3 = '9953'
infer = '9954'

period_02 = cek_status(demog)
period_03 = cek_status(portfolio)
period_04 = cek_status(trx2)
period_05 = cek_status(trx3)
period_06 = cek_status(infer)

if((period_02['latest']['status'] == 'succeeded') and
  (period_03['latest']['status'] == 'succeeded') and
  (period_04['latest']['status'] == 'succeeded') and
  (period_05['latest']['status'] == 'succeeded')):
  eksekusi(infer)