
import dask
import os
import pandas as pd

from time import sleep
from datetime import datetime


from dask.distributed import Client

client = Client(n_workers=4)



def inc(x):
    sleep(1)
    return x + 1


def add(x, y):
    sleep(1)
    return x + y


# executea and measure time
starttime=datetime.now()
x = inc(1)
y = inc(2)
x   # 2
y   # 3 
z = add(x, y)
z   # 5
timeneeded = datetime.now()-starttime
timeneeded # seconds=3, microseconds=5651



from dask import delayed

x = delayed(inc)(1)
y = delayed(inc)(2)
z = delayed(add)(x, y)
z

# execute and measure time
starttime=datetime.now()
z.compute()  # 5, with waiting time
timeneeded = datetime.now()-starttime
timeneeded   # seconds=2, microseconds=802233

z.visualize()





#####


def double(x):
    sleep(1)
    return 2 * x

def is_even(x):
    return not x % 2

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# normal for loop:
starttime=datetime.now()
results = []
for x in data:
    if is_even(x):
        y = double(x)
    else:
        y = inc(x)
    results.append(y)

total = sum(results)
total   # 90
timeneeded = datetime.now()-starttime
timeneeded   # seconds=10, microseconds=22633


# dask type:
results = []
for x in data:
    if is_even(x):  # even
        #y = delayed(double)(x)
        y = double(x)
    else:          # odd
        #y = delayed(inc)(x)
        y = inc(x)
    results.append(y)

total = delayed(sum)(results)


total   #delay function


starttime=datetime.now()
total.compute()  #90
timeneeded = datetime.now()-starttime
timeneeded   # seconds=1, microseconds=45994


from nycflights13 import flights
flights.dtypes
flights


# prepare the data to folder: data
data_dir = "/home/heiko/Repos/dask_features/data"
df = flights


list(set(flights["month"])) # month in dataset flights: 1..12


for i in list(set(flights["month"])):
    # i = 1
    data = df[df["month"] == i]

    #data.to_json(os.path.join(data_dir, 'flightjson'+"_"+ str(i)+  '.json'), orient='records', lines=True)
    data.to_csv(os.path.join(data_dir, 'flightjson'+"_"+ str(i)+  '.csv'))



data1 = pd.read_csv(os.path.join(data_dir, "flightjson_"+"1"+".csv"))

data1
data1.columns

# group data and check on time
starttime=datetime.now()
data1.groupby('origin').mean()    # works
data1.groupby("origin").dep_delay.mean()   # does not work
timeneeded = datetime.now()-starttime
timeneeded    # microseconds=17098


# example: 
from os import listdir

filenames = [f for f in listdir("/home/heiko/Repos/dask_features/data")]
filenames = sorted(filenames)
filenames
os.path.join("/home/heiko/Repos/dask_features/data", filenames[1])

# Origin = origin
# DepDelay = dep_delay

starttime=datetime.now()
sums = []
counts = []

for f in filenames:

    fn = os.path.join("/home/heiko/Repos/dask_features/data", f)
    # Read in file
    # deplayed with dask:
    df = delayed(pd.read_csv)(fn)
    # normal pandas
    #df = pd.read_csv(fn)

    # Groupby origin airport
    by_origin = df.groupby('origin')

    # Sum of all departure delays by origin
    total = by_origin.dep_delay.sum()

    # Number of flights by origin
    count = by_origin.dep_delay.count()

    # Save the intermediates
    sums.append(total)
    counts.append(count)



# Compute the intermediates
sums, counts = dask.compute(sums, counts)

# Combine intermediates to get total mean-delay-per-origin
total_delays = sum(sums)
n_flights = sum(counts)
mean = total_delays / n_flights

timeneeded = datetime.now()-starttime
timeneeded   # microseconds=528644   # dask: microseconds=526000



client.close()




# working with dask dataframes

filenames = [os.path.join("/home/heiko/Repos/dask_features/data",f) for f in listdir("/home/heiko/Repos/dask_features/data")]
filenames = sorted(filenames)
filenames



# import dask
import dask
import dask.dataframe as dd
df = dd.read_csv(filenames)
df

df.head()

df[df["month"]==1]
df[df["month"]==12].compute()


starttime=datetime.now()
#df.compute()
df[df["month"]==1].compute()
timeneeded = datetime.now()-starttime
timeneeded   # microseconds=464970


df.dep_delay.max().compute()




###


starttime=datetime.now()

timeneeded = datetime.now()-starttime
timeneeded


starttime=datetime.now()
output = []

for f in filenames:

    fn = os.path.join("/home/heiko/Repos/dask_features/data", f)
    # Read in file
    # deplayed with dask:
    df = pd.read_csv(fn)
    output.append(df)


df = pd.concat(output, axis = 0)
dd = df[df["month"]==1]
# dd  #27004 x 20

timeneeded = datetime.now()-starttime
timeneeded  # microseconds=467321

df  # 336776 x 20
dd

