
import dask

from time import sleep


def inc(x):
    sleep(1)
    return x + 1


def add(x, y):
    sleep(1)
    return x + y



x = inc(1)
y = inc(2)
x   # 2
y   # 3 
z = add(x, y)
z   # 5


from dask import delayed

x = delayed(inc)(1)
y = delayed(inc)(2)
z = delayed(add)(x, y)
z

z.compute()  # 5, with waiting time

z.visualize()


#####


def double(x):
    sleep(1)
    return 2 * x

def is_even(x):
    return not x % 2

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

results = []
for x in data:
    if is_even(x):  # even
        y = delayed(double)(x)
    else:          # odd
        y = delayed(inc)(x)
    results.append(y)

total = delayed(sum)(results)


total


total.compute()


