

from datetime import datetime
import featuretools as ft

trend = ft.primitives.Trend()
times =[datetime(2010, 1, 1, 11, 45, 0), 
        datetime(2010, 1, 1, 11, 55, 15), 
        datetime(2010, 1, 1, 11, 57, 39),
        datetime(2010, 1, 1, 11, 12), 
        datetime(2010, 1, 1, 11, 12, 15)]

times

datetime(2010, 1, 1, 11, 45)
         # Year, Month, Day, hour, min

round(trend([1,2,3,4,5], times), 3)



