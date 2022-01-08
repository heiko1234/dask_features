


import pandas as pd
import numpy as np
import graphviz

import featuretools as ft
import plotly.express as px


data = pd.read_csv("/home/heiko/Repos/dask_features/data/chemical_batch_data.csv")

analytic_data = pd.read_csv("/home/heiko/Repos/dask_features/data/chemical_analytic_data.csv")

data
analytic_data


def translate(data, columnlist):
    for element in columnlist:
        try:
            data[element] = data[element].str.replace(",", ".")
            data[element] = pd.to_numeric(data[element], errors='coerce')
        except:
            continue
    return data


analytic_data = translate(data=analytic_data, columnlist=["pH"])
analytic_data
# analytic_data = None

#transactions_df = data.merge(analytic_data)
#transactions_df

# not working
# data.pivot(index="batch", columns="step", values="temp")


from woodwork.logical_types import Categorical, Double, Integer


import woodwork as ww
ww.list_semantic_tags()
ww.list_logical_types()




es = ft.EntitySet(id="batchdata")
es



data
es = es.add_dataframe(
    dataframe_name="batch_data",
    dataframe=data,
    index="index",
    time_index="time",
    logical_types={
        "batch": Integer,
        "step": Categorical,
        "temp": Double
    },
)
es



es["batch_data"].ww.schema
es["batch_data"]

analytic_data


es = es.add_dataframe(
    dataframe_name="analytic",
    dataframe=analytic_data,
    index="batch",
)
es["analytic"]
#           batch  color    pH  Vis
# 2022001  2022001     93  7.70  190
es["analytic"].ww.schema
#batch       Integer       ['index']
#color       Integer     ['numeric']
#pH           Double     ['numeric']
#Vis         Integer     ['numeric']

es["batch_data"]
#     index                time step    batch  temp
#756    756 2021-10-05 23:24:00  600  2022010  25.0
es["batch_data"].ww.schema
#Column                             
#index       Integer       ['index']
#time       Datetime  ['time_index']
#step    Categorical    ['category']
#batch       Integer     ['numeric']
#temp         Double     ['numeric']



es = es.normalize_dataframe(
    base_dataframe_name="batch_data",
    new_dataframe_name="analytic",
    index="batch",
    #additional_columns=["color", "pH", "Vis"],
)
es


## 
es["batch_data"].ww.schema
es["analytic"].ww.schema


#es = es.add_relationship(parent_dataframe_name="batch_data", 
#                        parent_column_name= "batch",
#                        child_dataframe_name="analytic", 
#                        child_column_name="batch")
#es

es["analytic"].ww.schema
#batch       Integer       ['index']
#color       Integer     ['numeric']
#pH           Double     ['numeric']
#Vis         Integer     ['numeric']

es = es.add_relationship(parent_dataframe_name="analytic", 
                        parent_column_name= "batch",
                        child_dataframe_name="batch_data", 
                        child_column_name="batch")
es


es["batch_data"].head(5)
es["analytic"].head(5)
es


feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="batch_data",
                                      agg_primitives=["count", "mean", "sum", "mode", "Trend"],
                                      # trans_primitives=["month"],
                                      max_depth=2)



feature_matrix
feature_matrix.columns

feature_matrix.iloc[:5,:]
feature_matrix.loc[:,["batch", "analytic.pH"]]
es["analytic"]#
es





feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="analytic",
                                      agg_primitives=["count", "mean","min", "max", "sum", "mode", "Trend"],
                                      # trans_primitives=["month"],
                                      max_depth=3)


list(set(es["batch_data"]["batch"]))  #2022005, 2022006, 2022007, 2022008, 2022009, 2022010
es["batch_data"].ww.schema
feature_matrix.columns
# ['color', 'pH', 'Vis', 'COUNT(batch_data)', 'MAX(batch_data.temp)',
#       'MEAN(batch_data.temp)', 'MIN(batch_data.temp)',
#       'MODE(batch_data.step)', 'SUM(batch_data.temp)',
#       'TREND(batch_data.temp, time)', 'MODE(batch_data.DAY(time))',
#       'MODE(batch_data.MONTH(time))', 'MODE(batch_data.WEEKDAY(time))',
#       'MODE(batch_data.YEAR(time))']

feature_matrix.head
i = 2
feature_matrix.iloc[i:i+5,:5]
feature_matrix.iloc[i:i+5,5:10]
feature_matrix.iloc[i:i+5,10:15]


feature_defs
feature = feature_defs[5]  # 'The average of the "temp" of all instances of "batch_data" for each "batch" in "analytic".'
feature

ft.graph_feature(feature)
ft.describe_feature(feature)


list(set(es["batch_data"]["step"]))
values_dict = {'step': [300, 400, 500, 600]}
es.add_interesting_values(dataframe_name='batch_data', values=values_dict)

es['batch_data'].ww.columns['step'].metadata

feature_matrix, feature_defs = ft.dfs(entityset=es,
                                      target_dataframe_name="analytic",
                                      agg_primitives=["count", "avg_time_between", "mean","min", "max", "sum", "mode", "Trend"],
                                      where_primitives=["count", "min", "max", "avg_time_between"],
                                      trans_primitives=['time_since', "weekday"],
                                      max_depth=2)


feature_matrix.columns

es["batch_data"]["time"]  # 2021-10-05 23:24:00
feature_matrix.head
i = 2
feature_matrix.iloc[i:i+5,:5]
feature_matrix.iloc[i:i+5,5:10]
feature_matrix.iloc[i:i+5,10:15]
feature_matrix.iloc[i:i+5,15:20]

feature = feature_defs[11]
ft.describe_feature(feature)



feature_matrix.corr()["color"].sort_values()


fig = px.scatter(feature_matrix, y = "color", x = "MAX(batch_data.temp WHERE step = 400)")
fig = px.scatter(feature_matrix, y = "color", x = "MIN(batch_data.temp WHERE step = 500)")
fig = px.scatter(feature_matrix, y = "color", x = "AVG_TIME_BETWEEN(batch_data.time)")
fig.show()


es["batch_data"]
fig = px.scatter(es["batch_data"], y = "temp", x = "index")
fig = px.scatter(es["batch_data"], y = "step", x = "index")
fig.show()