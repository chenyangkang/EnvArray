import requests
from urllib.parse import urljoin
import os

# Mimic a browser user-agent

def EarthEnv_Topography_Download(output_folder):

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    parent_url = 'https://data.earthenv.org/topography/'

    for file in ['elevation_5KMmn_GMTEDmn.tif',
                'slope_5KMmn_GMTEDmd.tif',
                'eastness_5KMmn_GMTEDmd.tif',
                'northness_5KMmn_GMTEDmd.tif',
                'roughness_5KMmd_GMTEDmd.tif'
                ]:
        
        url = urljoin(parent_url, file)
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Process the response
            with open(os.path.join(output_folder, file), 'wb') as f:
                f.write(response.content)
            print(f"Download successful: {file}")
        else:
            print(f"Failed to download: {file}. Status code: {response.status_code}")

