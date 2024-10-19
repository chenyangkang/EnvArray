import cdsapi
import pandas as pd
import datetime
import os
from joblib import Parallel, delayed
import numpy as np
import xarray as xr



class ERA5DataDailyRequestProcessor:
    def __init__(self, start_date, end_date, output_folder, time_interval, spatial_coarsen, n_jobs=1, 
                 delete_raw=False, download_skip_exist=True, process_skip_exist=True):
        self.start_date = start_date
        self.end_date = end_date
        self.output_folder = output_folder
        self.time_interval = time_interval
        self.spatial_coarsen = spatial_coarsen
        self.n_jobs = n_jobs
        self.date_df = self.get_all_dates()
        self.date_df['year'] = self.date_df['year'].astype('int')
        self.date_df['month'] = self.date_df['month'].astype('int')
        self.date_df['day'] = self.date_df['day'].astype('int')
        self.delete_raw = delete_raw
        self.download_skip_exist = download_skip_exist
        self.process_skip_exist = process_skip_exist

        # check output folder
        os.makedirs(self.output_folder, exist_ok=True)


    def get_all_dates(self):
        date_list = [datetime.datetime.strptime('1970-01-01','%Y-%m-%d')+datetime.timedelta(days=i) for i in range(99999)]
        request_df = pd.DataFrame(pd.Series(date_list,name='date'))
        request_df['date'] = pd.to_datetime(request_df['date'])
        request_df['year'] = request_df.date.dt.year
        request_df['month'] = [str(i).split('-')[-2] for i in request_df.date.dt.date]
        request_df['day'] = [str(i).split('-')[-1] for i in request_df.date.dt.date]
        request_df = request_df[
            (request_df['date']>=pd.to_datetime(self.start_date)) & (request_df['date']<=pd.to_datetime(self.end_date))
        ]
        print(request_df)
        return request_df

    def get_ERA5_data_and_process(self):
        dates = [(str(year), str(month).zfill(2), str(day).zfill(2)) for year, month, day in zip(self.date_df.year, self.date_df.month, self.date_df.day)]

        # Using Joblib to parallelize the downloading and processing
        Parallel(n_jobs=self.n_jobs)(delayed(self.get_data_and_process)(date) for date in dates)

    def get_data_and_process(self, date_tuple):
        year, month, day = date_tuple
        # print(date_tuple)
        try:
            # Decide whether download
            download_flag = True
            if self.download_skip_exist:
                if os.path.exists(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}.nc')):
                    print(f'Download exists: download_ERA5_{year}_{month}_{day}.nc')
                    download_flag = False
                if os.path.exists(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}_processed.nc')):
                    if self.process_skip_exist:
                        download_flag = False

            # download
            if download_flag:
                self.get_data(year, month, day)
            
            # Decide whether process
            process_flag=True
            if self.process_skip_exist:
                if os.path.exists(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}_processed.nc')):
                    print(f'Processed exists: download_ERA5_{year}_{month}_{day}.nc')
                    process_flag = False

            # process
            if process_flag:
                self.process_ERA5(year, month, day)
                
        except Exception as e:
            print(f"Error processing {year}-{month}-{day}: {e}")

    def get_data(self,year,month,day):
        
        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": [
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "2m_temperature",
                "mean_sea_level_pressure",
                "mean_wave_direction",
                "sea_surface_temperature",
                "surface_pressure",
                "total_precipitation",
                "100m_u_component_of_wind",
                "100m_v_component_of_wind",
                "surface_net_solar_radiation",
                "surface_net_thermal_radiation",
                "cloud_base_height",
                "total_cloud_cover",
                "lake_cover",
                "lake_mix_layer_temperature",
                "evaporation",
                "snow_density",
                "snow_depth",
                "snowmelt",
                "temperature_of_snow_layer",
                "soil_temperature_level_1",
                "high_vegetation_cover",
                "leaf_area_index_high_vegetation",
                "leaf_area_index_low_vegetation",
                "low_vegetation_cover"
            ],
            
            'year':year,
            'month':month,
            'day':day,
            'time':['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                    '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00',
                    '17:00','18:00','19:00','20:00','21:00','22:00','23:00'],
            "data_format": "netcdf",
            "download_format": "unarchived"
                
        }

        c=cdsapi.Client()
        
        nc_zip_output_dir = os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}.nc.zip')
        c.retrieve(dataset, request, nc_zip_output_dir)
        
        import time
        time.sleep(3)
        
        self.unzip(nc_zip_output_dir, self.output_folder, f'{year}-{month}-{day}', f'download_ERA5_{year}_{month}_{day}.nc')
        
    def process_ERA5(self, year, month, day):
        data = xr.open_dataset(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}.nc'), engine='h5netcdf')
        data = data.rename({'valid_time': 'time'})
        comb = data.resample(time=self.time_interval).mean()

        ## transforming the longitude to -180, 180
        aa = comb.sel(longitude=slice(180+1e-8, 360))
        aa['longitude'] = aa['longitude']-360
        bb = comb.sel(longitude=slice(0, 180))
        comb = xr.concat([aa,bb], dim='longitude')

        ## Spatial resample 3 to 1
        coarsen_weight = self.spatial_coarsen # 30km/9km = ~ 3
        res = comb.coarsen(
            longitude = coarsen_weight, # 3 into 1
            latitude = coarsen_weight,
            boundary='trim'
        ).mean()

        ## Saving
        nc_zip_output_dir_ = os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}_processed.nc')
        res.to_netcdf(nc_zip_output_dir_)

        if self.delete_raw:
            try:
                os.remove(os.path.join(self.output_folder, f'download_ERA5_{year}_{month}_{day}.nc'))
            except Exception as e:
                print(f'Delete raw download_ERA5_{year}_{month}_{day}.nc fail: {e}')

    def unzip(self, nc_zip_output_dir, tmp_output_path, marker, desired_name):
        import zipfile
        import shutil
        
        # Path to your zip file
        zip_path = nc_zip_output_dir

        # Extract the NetCDF file from the zip
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            
            # Extract all contents or a specific file
            zip_ref.extractall(os.path.join(tmp_output_path, marker))  # Adjust path if needed
            
            shutil.move(os.path.join(tmp_output_path, marker, 'data_stream-oper.nc'), os.path.join(tmp_output_path, desired_name))

            shutil.rmtree(os.path.join(tmp_output_path, marker))

        os.remove(zip_path)
            
