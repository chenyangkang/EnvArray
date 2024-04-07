
import os

def get_spatiotemporal_data(
    start_date, 
    end_date,
    time_interval='1D',
    spatial_resolution='30km',
    output_folder='./downloaded_data'
):
    
    download_data(start_date, end_date, time_interval, spatial_resolution, output_folder)

    ## Combine data

    ## Reample data

    ## Output data


    pass


def download_data(start_date, end_date, time_interval, spatial_resolution, output_folder):
    
    ### For EAR5 data
    from .download.ERA5.daily_request_daily_processor import get_ERA5_data_and_process
    get_ERA5_data_and_process(start_date, end_date, time_interval, spatial_resolution, os.path.join(output_folder, 'ERA5'))

    ### For land cover data
    from .download.MODIS_land_cover.request import MODIS_land_cover_download
    MODIS_land_cover_download(start_date, end_date, os.path.join(output_folder, 'MODIS_land_cover'))
    # Processing

    ### For NDVI data
    from .download.MODIS_NDVI.request import MODIS_NDVI16days_download
    MODIS_NDVI16days_download(start_date, end_date, os.path.join(output_folder, 'MODIS_NDVI'))
    # Processing

    ### For Topography data
    from .download.EarthEnv_Topography.request import EarthEnv_Topography_Download
    EarthEnv_Topography_Download(output_folder, os.path.join(output_folder, 'EarthEnv_Topography'))
    # Processing




    

    
