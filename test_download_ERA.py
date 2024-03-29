import os
start_date='2000-01-01'
end_date='2020-01-01'
time_interval='1D'
spatial_resolution='30km'
output_folder='./downloaded_data'

### For EAR5 data
from EnvArray.download.ERA5.request import get_ERA5_data_and_process, get_ERA5_data, ERA5_process

# Get and Process combined
get_ERA5_data_and_process(start_date, end_date, time_interval, spatial_resolution, os.path.join(output_folder, 'ERA5'))

# First get, then process
# get_ERA5_data(start_date, end_date, time_interval, spatial_resolution, os.path.join(output_folder, 'ERA5'))
#ERA5_process(start_date, end_date, time_interval, spatial_resolution, os.path.join(output_folder, 'ERA5'))
