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
import networkx as nx
import numpy as np


# load the SNOMED ancestor table
query = "SELECT * FROM prod_rwi.snomed_concept_relationship WHERE relationship_id='Maps to'"
query_df = hiveContext.sql(query)
query_df.cache()
df = query_df.toPandas()

# build a directed graph from it
G = nx.DiGraph()
G.add_edges_from(df[['concept_id_1', 'concept_id_2']].values)

# test a few values
df[['concept_id_1', 'concept_id_2']].values
nx.ancestors(G, 256142)
nx.descendants(G, 45592608)
