"""
Make transactions table features.

## Inputs
   - 

## Outputs
   - 

## Wish List:
    Main:
    - [ ] 

    Steps:
    - [ ] Clean spine
    - [ ] Join spine with transactions
    - [ ] Get past 
    - [ ] Make Features
    - [ ] 
    - [ ] 
    - [ ] 

"""

# %% Imports

import numpy as np
import re

from pyspark.sql import functions as F
from pyspark.sql import types as T 

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

import datetime as dt
import sys

import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import pyspark


def create_vars_on_time(
    col: F.col,                              # column's name
    date_col: F.col,                         # date reference 
    date: str,                               # Year-month column (the period that represents Year-Month of dataset)
    operation: pyspark.sql.functions,        # desired operation (ex: count, max, min, mean, median...)
    alias: str,                              # final column's name         
    months_to_diff: list = [0, 3, 6, 9, 12], # month cuts (in this case: 0, 3, 6, 9, 12, 15, 18, 21 and 24 months) 
    expand: bool = True) -> list:
    
    '''
    Function for creating features
    '''
    
    date = F.to_date(F.lit(date))
    
    vars_on_time = []

    for month_to_diff_i in months_to_diff:
        new_col_var_name = alias + str(month_to_diff_i) + 'M'

        filtered_col = F            .when(date_col >= F.add_months(date, - month_to_diff_i), col)            .otherwise(None)

        vars_on_time.append(operation(filtered_col).alias(new_col_var_name))

        if expand==True:
            for month_to_diff_j in months_to_diff:
                if month_to_diff_j > month_to_diff_i:
                    new_col_var_name_ = alias + str(month_to_diff_i) + 'M' + str(month_to_diff_j) + 'M'

                    filtered_col_ = F                        .when(
                            (date_col < F.add_months(date, - month_to_diff_i)) &
                            (date_col >= F.add_months(date, - (month_to_diff_j))), col)\
                        .otherwise(None)

                    vars_on_time.append(operation(filtered_col_).alias(new_col_var_name_))

    return vars_on_time

# %% Definitions

spark = (SparkSession.builder
                     .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
                     .getOrCreate())

PATH_SPINE = '../../dados/spine/spine_churn_bb_sample'
df_spine = spark.read.parquet(PATH_SPINE)

PATH_TRANSACTIONS = '../../dados/primary/transactions_clean_historical_table_sample'
df_trans = spark.read.parquet(PATH_TRANSACTIONS)

# %% Clean spine

df_customers = df_spine.select(['NR_CT_CRT', 'SAFRA'])

# %% Join spine with transactions

df_vars = df_customers.join(df_trans,
                            on='NR_CT_CRT',
                            how='left')

# %% Get only past

mask_past = F.col('SAFRA') > F.col('DT_MVT_CT_CRT')
df_vars = df_vars[mask_past]

df_temp = df_vars

# %% Format as timestamp


df_temp = df_temp.withColumn('SAFRA',
                             F.to_date(F.col('SAFRA')))

df_temp = df_temp.withColumn('DT_MVT_CT_CRT',
                             F.to_date(F.col('DT_MVT_CT_CRT')))

# %% Make features

df_vars = df_temp.groupby('NR_CT_CRT', 'SAFRA').agg(
                     *create_vars_on_time(F.col('FAT'), F.col('DT_MVT_CT_CRT'), F.col('SAFRA'), F.count, 'TRANSACOES_FAT_COUNT_'),
                     *create_vars_on_time(F.col('FAT'), F.col('DT_MVT_CT_CRT'), F.col('SAFRA'), F.min, 'TRANSACOES_FAT_MIN_'),
                     *create_vars_on_time(F.col('FAT'), F.col('DT_MVT_CT_CRT'), F.col('SAFRA'), F.max, 'TRANSACOES_FAT_MAX_'),
                     *create_vars_on_time(F.col('FAT'), F.col('DT_MVT_CT_CRT'), F.col('SAFRA'), F.mean, 'TRANSACOES_FAT_MEAN_'),
                     *create_vars_on_time(F.col('FAT'), F.col('DT_MVT_CT_CRT'), F.col('SAFRA'), F.stddev, 'TRANSACOES_FAT_STD_'),
                     *create_vars_on_time(F.col('FAT'), F.col('DT_MVT_CT_CRT'), F.col('SAFRA'), F.sum, 'TRANSACOES_FAT_SUM_'),
                     )

# %% Make features


df_vars.columns




