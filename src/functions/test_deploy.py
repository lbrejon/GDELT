# Imports
import os
import logging
import re
import pandas as pd
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

import sys
sys.path.insert(0, '..')
from GDELT.src.utils.utils import create_directory, download_file_from_url, unzip_file, load_yaml
from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())
project_dir = os.path.join(os.path.dirname('.env'), os.getcwd())

CONFIG_DIR = os.path.join(project_dir, os.environ.get("CONFIG_DIR"))
DATA_DIR = os.path.join(project_dir, os.environ.get("DATA_DIR"))
RAW_DATA_DIR = os.path.join(project_dir, os.environ.get("RAW_DATA_DIR"))
RAW_ZIP_DATA_DIR = RAW_DATA_DIR + 'zip/'
RAW_CSV_DATA_DIR = RAW_DATA_DIR + 'csv/'

AWS_ACCESS_KEY_ID = load_yaml(CONFIG_DIR+'secret.yml')['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = load_yaml(CONFIG_DIR+'secret.yml')['AWS_SECRET_ACCESS_KEY']




def get_csv_category(csv_filename, pattern=r'\.(.*?)\.'):
    return re.findall(pattern, csv_filename)[0]



def get_urls_to_parse(data_file_path, n_urls=None):
    df = pd.read_csv(data_file_path, sep=' ', header=None, names=['something', 'id', 'url'])
    urls_to_parse = df['url'].tolist()
    urls_to_parse = urls_to_parse[:n_urls] if n_urls is not None else urls_to_parse
    return urls_to_parse



def dowload_data(urls_to_parse):
    for row, url in enumerate(urls_to_parse):
        zip_file_path = os.path.join(RAW_ZIP_DATA_DIR, Path(url).name)
        csv_file_path = os.path.join(RAW_CSV_DATA_DIR, Path(url).stem)
    
        # Download data from remote url to data/raw/zip
        create_directory(RAW_ZIP_DATA_DIR)
        if not (os.path.isfile(zip_file_path) or os.path.isfile(csv_file_path)):
            download_file_from_url(remote_url=url, local_dir=RAW_ZIP_DATA_DIR, verbose=True)
        else:
            logging.info(f"[{row+1}/{len(urls_to_parse)}] File '{zip_file_path}' already exists.")


        # Unzip file within data/raw/url to data/raw/csv
        create_directory(RAW_CSV_DATA_DIR)
        if not os.path.isfile(csv_file_path):
            unzip_file(zip_file_path, RAW_CSV_DATA_DIR, verbose=True)
        else:
            logging.info(f"[{row+1}/{len(urls_to_parse)}] File '{csv_file_path}' already exists.")

        # Remove zip file
        if os.path.isfile(zip_file_path):
            os.remove(zip_file_path)
    
   
def count_data(directory):
    csv_files = [f for f in os.listdir(directory)]
    logging.info(f"{len(csv_files)} csv files found within {directory}")



def upload_file_to_S3_bucket(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        service_name='s3', region_name='eu-west-1',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



if __name__ == '__main__':
    data_file_path = os.path.join(RAW_DATA_DIR, 'master_file_list.txt')
    n_urls = 4
    
    # Retrieve urls to parse
    urls_to_parse = get_urls_to_parse(data_file_path, n_urls=n_urls)
    
    # Download data from url
    dowload_data(urls_to_parse)
    
    # Count dowloaded data
    count_data(directory=RAW_CSV_DATA_DIR)