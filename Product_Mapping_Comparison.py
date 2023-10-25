# Databricks notebook source
import pandas as pd
import numpy as np
import plotly as plt
import seaborn as sns
import plotly.express as px
import matplotlib as mpl
import matplotlib.pyplot as plt
%matplotlib inline

import datetime

# COMMAND ----------

def read_csv_storage(container_name,storage_account,path):
    location = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path}"
    df = spark.read.format("csv").option("header","true").load(location)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## EFP Product Mapping

# COMMAND ----------

container_name = 'test'
storage_account = 'devf0d8e58fd9d51d06a2e4b'
path = '/reference_mapping/Mkt_Product_to_Account_Mapping_EFP.csv'

#sample_output = read_csv_storage(container_name,storage_account,path)
df = spark.read.format("csv").option("header", "true").load(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path}")

# COMMAND ----------

pdf = df.toPandas()

# COMMAND ----------

pdf.head()

# COMMAND ----------

pdf.shape
pdf.index
list(pdf)
pdf.columns.values.tolist()

# COMMAND ----------

pdf.isnull().any()

# COMMAND ----------

pdf[pdf.isnull().any(axis=1)]

# COMMAND ----------

pdf.dropna(how='all').dropna(how='all', axis=1)

# COMMAND ----------



# COMMAND ----------

df = pdf.drop(pdf.columns[pdf.isnull().any()], axis=1)
df.drop('ACCOUNT', inplace=True, axis=1)
df.drop_duplicates(subset=['MKT_PROD_CD','PRODUCT','PRODUCT_HIERARCHY'])
df_out = df[['MKT_PROD_CD','PRODUCT','PRODUCT_HIERARCHY']].drop_duplicates()
df_out = df_out.reset_index().drop('index', axis=1)

# COMMAND ----------

df_out.to_csv(r'/dbfs/FileStore/Liang_files/efp_mapping.csv')

# COMMAND ----------

df_out

# COMMAND ----------

import os

#os.mkdir(r'/dbfs/FileStore/Liang_files')
os.listdir(r'/dbfs/FileStore/Liang_files')

# COMMAND ----------

a = pd.read_csv(r'/dbfs/FileStore/Liang_files/efp_mapping.csv')

# COMMAND ----------

a.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## AMEA Product Mapping

# COMMAND ----------

container_name = 'test'
storage_account = 'devf0d8e58fd9d51d06a2e4b'
path = '/reference_mapping/AMEA_product_mapping_FXE.csv'

df = spark.read.format("csv").option("header", "true").load(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path}")

# COMMAND ----------

pdf = df.toPandas()
pdf

# COMMAND ----------

# MAGIC %md 
# MAGIC ## EU Product Mapping

# COMMAND ----------

container_name = 'test'
storage_account = 'devf0d8e58fd9d51d06a2e4b'
path = '/reference_mapping/EU_PROD_FORMAT_LKUP.csv'

pdf = spark.read.format("csv").option("header", "true").load(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path}").toPandas()
pdf.head()

# COMMAND ----------

pdf.isnull().any()
#pdf[pdf.isnull().any(axis=1)]
