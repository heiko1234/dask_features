

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



feature_matrix, feature_defs = ft.dfs(entityset=es, target_dataframe_name="products")


feature_defs

feature_matrix



feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="customers",
                                      agg_primitives=["count"],
                                      trans_primitives=["month"],
                                      max_depth=1)
feature_matrix




feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="customers",
                                      agg_primitives=["mean", "sum", "mode"],
                                      trans_primitives=["month", "hour"],
                                      max_depth=2)
feature_matrix







