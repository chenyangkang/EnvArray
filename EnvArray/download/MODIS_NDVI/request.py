
import earthaccess
import my_secrets
import os
os.environ['EARTHDATA_USERNAME'] = my_secrets.username
os.environ['EARTHDATA_PASSWORD'] =  my_secrets.password


def MODIS_NDVI16days_download(start_date, end_date, save_path):
    auth = earthaccess.login(strategy='environment')

    results = earthaccess.search_data(
        short_name='MOD13A2', # here the modis product name
        version='061', # for version 61
        bounding_box = (-180,-90,180,90),
        temporal = (start_date,end_date)
    )

    files = earthaccess.download(results, save_path)

