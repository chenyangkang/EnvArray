# EnvArray

Efficient daily environmental data processing and annotation with Xarray and netCDF for ecology project

## TODO

### For EAR5 data (https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview)
- [x] 1. Script for downloading data
- [x] 2. Loading data into Xarray
- [x] 3. Query example
- [x] 4. Resampling example (up and down)

Steps:
1. Config: https://cds.climate.copernicus.eu/api-how-to

2. Run:
```py
import os

start_date='2010-01-01'
end_date='2024-01-01'
time_interval='1D'
spatial_resolution='30km'
output_folder='./downloaded_data'
os.makedirs(output_folder, exist_ok=True)

### For EAR5 data
from EnvArray.download.ERA5.request import ERA5DataDownloaderProcessor

# Get and Process combined
ERA5_path = os.path.join(output_folder, 'ERA5')
os.makedirs(ERA5_path, exist_ok=True)
ERA5_datamaker = ERA5DataDownloaderProcessor(start_date=start_date, 
                            end_date=end_date, 
                            output_folder=ERA5_path, 
                            time_interval=time_interval, 
                            spatial_resolution=spatial_resolution, 
                            n_jobs=3, 
                            delete_raw=False)

ERA5_datamaker.get_ERA5_data_and_process()

```


### For land cover data (https://lpdaac.usgs.gov/products/mcd12q1v061/)
- [x] 1. Script for downloading data
- [] 2. Loading data into Xarray
- [] 3. Query example
- [] 4. Resampling example (up and down)

### For MODIS NDVI data (https://lpdaac.usgs.gov/products/mod13a2v061/)
- [x] 1. Script for downloading data
- [] 2. Loading data into Xarray
- [] 3. Query example
- [] 4. Resampling example (up and down)

### For Topography data (EarthEnv, https://www.earthenv.org)

https://data.earthenv.org/topography/elevation_5KMmn_GMTEDmn.tif
https://data.earthenv.org/topography/slope_5KMmn_GMTEDmd.tif
https://data.earthenv.org/topography/eastness_5KMmn_GMTEDmd.tif
https://data.earthenv.org/topography/northness_5KMmn_GMTEDmd.tif
https://data.earthenv.org/topography/roughness_5KMmd_GMTEDmd.tif

### Combining all
