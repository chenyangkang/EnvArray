
import numpy as np
import xarray as xr
import os
# import pandas as pd
# import matplotlib.pyplot as plt

def process_ERA5(year, month, day, time_interval, spatial_resolution, output_folder):
    data = xr.open_dataset(os.path.join(output_folder, f'download_{year}_{month}_{day}.nc'))

    ## Aggregate to 1D
    # a = data[['u10','v10','t2m','sp','tcc','tp']].resample(time=time_interval).mean().rename({
    #     'u10':'mean_u10',
    #     'v10':'mean_v10',
    #     't2m':'mean_t2m',
    #     'sp':'mean_sp',
    #     'tcc':'mean_tcc',
    #     'tp':'mean_tp',
    # })

    # b = data[['t2m']].resample(time=time_interval).max().rename({
    #     't2m':'max_t2m',
    # })

    # c = data[['t2m']].resample(time=time_interval).min().rename({
    #     't2m':'min_t2m',
    # })

    # comb = xr.combine_by_coords([
    #     a,b,c
    # ])

    comb = data.resample(time=time_interval).mean()

    ## transforming the longitude to -180, 180
    aa = comb.sel(longitude=slice(180+1e-8, 360))
    aa['longitude'] = aa['longitude']-360
    bb = comb.sel(longitude=slice(0, 180))
    comb = xr.concat([aa,bb], dim='longitude')

    ## Spatial resample 3 to 1
    coarsen_weight = int(float(spatial_resolution.split('km')[0])/9) # 30km/9km = ~ 3
    res = comb.coarsen(
        longitude = coarsen_weight, # 3 into 1
        latitude = coarsen_weight,
        boundary='trim'
    ).mean()

    ## Saving
    res.to_netcdf(os.path.join(output_folder, f'download_{year}_{month}_{day}_processed.nc'))

