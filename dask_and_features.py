

# Imports

import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db

import featuretools as ft


# ToDo


data = ft.demo.load_mock_customer()
data

data["transactions"]  # 500x4: transaction_id, session_id, transaction_time, product_id, amount
data["sessions"]   # 34 x 4: session_id, customer_id, device, session_start
data["customers"]  # 5x 4: customer_id, zip_code, join_date, birthday
data["products"]  # 5 2: product_id, brand


transactions_df = data["transactions"].merge(data["sessions"]).merge(data["customers"])
transactions_df
transactions_df.shape # 500,11
transactions_df.columns
# ['transaction_id', 'session_id', 'transaction_time', 'product_id', 'amount', 
# 'customer_id', 'device', 'session_start', 'zip_code', 'join_date', 'birthday']

transactions_df.sample(10)

products_df = data["products"]
products_df

es = ft.EntitySet(id="customer_data")
es


from woodwork.logical_types import Categorical, PostalCode

import woodwork as ww
ww.list_semantic_tags()



#data["transactions"]
#     transaction_id  session_id    transaction_time product_id  amount
#0               298           1 2014-01-01 00:00:00          5  127.64
#1                 2           1 2014-01-01 00:01:05          2  109.48

# transactions_df
#     transaction_id  session_id    transaction_time product_id  ...       session_start  zip_code           join_date   birthday
#298             298           1 2014-01-01 00:00:00          5  ... 2014-01-01 00:00:00     13244 2012-04-15 23:31:04 1986-08-18

#list(transactions_df.columns)
#['transaction_id', 'session_id', 'transaction_time', 'product_id', 'amount', 'customer_id', 
#'device', 'session_start', 'zip_code', 'join_date', 'birthday']


es = es.add_dataframe(
    dataframe_name="transactions",
    dataframe=transactions_df,
    index="transaction_id",
    time_index="transaction_time",
    logical_types={
        "product_id": Categorical,
        "zip_code": PostalCode,
    },
)
es


es
es["transactions"].ww.schema


es = es.add_dataframe(
    dataframe_name="products",
    dataframe=products_df,
    index="product_id")
es



es = es.add_relationship("products", "product_id", "transactions", "product_id")
es


# check the raw data frame
transactions_df.iloc[:5,:]
transactions_df.loc[:10,["device", "customer_id", "zip_code", "session_start", "join_date"]]

es = es.normalize_dataframe(
    base_dataframe_name="transactions",
    new_dataframe_name="sessions",
    index="session_id",
    make_time_index="session_start",
    additional_columns=[
        "device",
        "customer_id",
        "zip_code",
        "session_start",
        "join_date",
    ],
)
es


es["transactions"].ww.schema
es["sessions"].ww.schema


es["sessions"].head(5)
es["transactions"].head(5)




es = es.normalize_dataframe(
    base_dataframe_name="sessions",
    new_dataframe_name="customers",
    index="customer_id",
    make_time_index="join_date",
    additional_columns=["zip_code", "join_date"],
)
es
es["customers"].head(5)


# create new features

feature_matrix, feature_defs = ft.dfs(entityset=es, target_dataframe_name="products")


feature_defs

feature_matrix
feature_matrix.columns
feature_matrix



feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="customers",
                                      agg_primitives=["count"],
                                      trans_primitives=["month"],
                                      max_depth=1)
feature_matrix
        
#customer_id     zip_code  COUNT(sessions) MONTH(join_date)                                           
#        5         60091                6                7

es["customers"]

#   customer_id zip_code           join_date
#5            5    60091 2010-07-17 05:27:50
#4            4    60091 2011-04-08 20:08:14
#1            1    60091 2011-04-17 10:48:33
#3            3    13244 2011-08-13 15:42:34
#2            2    13244 2012-04-15 23:31:04


feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="customers",
                                      agg_primitives=["mean", "sum", "mode"],
                                      trans_primitives=["month", "hour"],
                                      max_depth=1)
feature_matrix


feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="customers",
                                      agg_primitives=["mean", "sum", "mode"],
                                      trans_primitives=["month", "hour"],
                                      max_depth=2)
feature_matrix


feature_matrix.columns
es["customers"]
es


# change target to session instead of customers
feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="sessions",
                                      agg_primitives=["mean", "sum", "mode"],
                                      trans_primitives=["month", "hour"],
                                      max_depth=2)
feature_matrix.head(5)




