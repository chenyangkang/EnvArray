
import numpy as np
import xarray as xr
# import pandas as pd
# import matplotlib.pyplot as plt

def process_ERA5(year, month):
    data = xr.open_dataset(f'download_{year}_{month}.nc')

    ## Aggregate to 1D
    a = data[['u10','v10','t2m','sp','tcc','tp']].resample(time='1D').mean().rename({
        'u10':'mean_u10',
        'v10':'mean_v10',
        't2m':'mean_t2m',
        'sp':'mean_sp',
        'tcc':'mean_tcc',
        'tp':'mean_tp',
    })

    b = data[['t2m']].resample(time='1D').max().rename({
        't2m':'max_t2m',
    })

    c = data[['t2m']].resample(time='1D').min().rename({
        't2m':'min_t2m',
    })

    comb = xr.combine_by_coords([
        a,b,c
    ])

    ## transforming the longitude to -180, 180
    aa = comb.sel(longitude=slice(180+1e-8, 360))
    aa['longitude'] = aa['longitude']-360
    bb = comb.sel(longitude=slice(0, 180))
    comb = xr.concat([aa,bb], dim='longitude')

    ## Spatial resample 5 to 1
    res = comb.coarsen(
        longitude = 5, # 5 into 1
        latitude = 5,
        boundary='trim'
    ).mean()

    ## Saving
    res.to_netcdf(f'download_{year}_{month}_processed.nc')

