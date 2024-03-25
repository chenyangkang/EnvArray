import cdsapi
import pandas as pd
import datetime
from .process import process_ERA5


date_list = [datetime.datetime.strptime('2000-01-01','%Y-%m-%d')+datetime.timedelta(days=i) for i in range(99999)]

date_list = [i for i in date_list if (i.year<=2020 and i.year>=2010)]

request_df = pd.DataFrame(pd.Series(date_list,name='date'))
request_df['date'] = pd.to_datetime(request_df['date'])
request_df['year'] = request_df.date.dt.year
request_df['month'] = [str(i).split('-')[-2] for i in request_df.date.dt.date]
request_df['day'] = [str(i).split('-')[-1] for i in request_df.date.dt.date]


def get_data(year,month,subsub):
    c=cdsapi.Client()
    c.retrieve(
        'reanalysis-era5-single-levels',
        {   
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable':[
                '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature',
                'surface_pressure', 'total_cloud_cover', 'total_precipitation',
            ],
            'year':str(year),
            'month':month,
            'day':list(subsub.day.values),
            'time':['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                    '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00',
                    '17:00','18:00','19:00','20:00','21:00','22:00','23:00'],
        },
        f'download_{year}_{month}.nc'
    )


for year in request_df.year.unique():
    sub = request_df[request_df.year==year]
    for month in sub.month.unique():
        subsub = sub[sub.month==month]
        get_data(year,month,subsub)
        process_ERA5(year, month) # aggregate to 1 day and 50 km

