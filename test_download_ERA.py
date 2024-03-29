import os

start_date='2010-01-01'
end_date='2024-01-01'
time_interval='1D'
spatial_resolution='30km'
output_folder='./downloaded_data'
os.makedirs(output_folder, exist_ok=True)

### For EAR5 data
from EnvArray.download.ERA5.request import get_ERA5_data_and_process

# Get and Process combined
ERA5_path = os.path.join(output_folder, 'ERA5')
os.makedirs(ERA5_path, exist_ok=True)
get_ERA5_data_and_process(start_date, end_date, time_interval, spatial_resolution, ERA5_path)

# First get, then process
# get_ERA5_data(start_date, end_date, time_interval, spatial_resolution, os.path.join(output_folder, 'ERA5'))
#ERA5_process(start_date, end_date, time_interval, spatial_resolution, os.path.join(output_folder, 'ERA5'))
