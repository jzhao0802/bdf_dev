from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
        
conf = SparkConf().set("spark.executor.memory", "2g") \
  .set("spark.dynamicAllocation.initialExecutors", "2") \
  .set("spark.driver.memory", "2g") \
  .set("spark.kryoserializer.buffer.max", "1g") \
  .set("spark.driver.cores", "4") \
  .set("spark.yarn.queue", "ace") \
  .set("spark.dynamicAllocation.maxExecutors", "32")
  
  
sparkContext = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sparkContext)
hiveContext = HiveContext(sparkContext)


import os
import pandas as pd


def hdfs_to_csv(hdfs_table, csv_name):
  query = "SELECT * FROM prod_rwi_batch.%s" % hdfs_table
  query_df = hiveContext.sql(query)
  query_df.cache()
  df = query_df.toPandas()  

  # save it locally
  csv_file = "%s.csv" % csv_name
  df.to_csv(csv_file)

  # copy it over to BDF
  os.system("hdfs dfs -copyFromLocal %s /user/dhomola" % csv_file) 
  # this didn't work due to access right issues: 
  # hdfs dfs -copyFromLocal initial_pos.csv /production/ace/data/dpt/dhomola

  # delete locally
  os.system("rm %s" % csv_file)

hdfs_to_csv("initialcohort_cp01_1508281365499_1142", "initial_pos")
hdfs_to_csv("restrictivecohort_cp02_1508281365499_1142", "restricted_pos")
hdfs_to_csv("randomsamplecohort_cs03_1508281365499_1142", "random_sample_scoring")
hdfs_to_csv("negativecohort_cn01_1508281365499_1142", "initial_negative")
hdfs_to_csv("cohort_v01_1508281365499_1142", "v01")
hdfs_to_csv("cohort_v02_1508281365499_1142", "v02")