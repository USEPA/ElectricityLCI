"""
Small utility functions for use throughout the repository

"""

import io
import zipfile

import requests


def download_unzip(url, unzip_path):
    """
    Download a zip file from url and extract contents to a given path
    
    Parameters
    ----------
    url : str
        Valid url to download the zip file
    unzip_path : str or path object
        Destination to unzip the data
    
    """
    r = requests.get(url)
    content_type = r.headers['Content-Type']
    if 'zip' not in content_type and '-stream' not in content_type:
        print(content_type)
        raise ValueError('URL does not point to valid zip file')
        
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path=unzip_path)
