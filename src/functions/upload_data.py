# Imports
import argparse
import os
import logging
import re
import pandas as pd
import numpy as np
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import socket

import sys
sys.path.insert(0, '..')
from GDELT.src.utils.utils import create_directory, download_file_from_url, unzip_file
from GDELT.src.utils.utils import save_csv, load_yaml, save_yaml, load_pickle, save_pickle

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





############ Split rows into hosts ###############
def create_host_file(hosts_no, df):
    hosts = [f'tp-hadoop-{host_no}' for host_no in hosts_no]
    rows_splits = np.array_split(range(df.shape[0]), len(hosts))
    hosts2rows = dict(zip(hosts, rows_splits))
    return hosts2rows


def split_data_per_host(df, hosts_file_path, hosts_no=None, verbose=False):
    if not os.path.isfile(hosts_file_path):
        if hosts_no is None:
            raise ValueError("Select host numero for 'tp-hadoop-XX' (Example: [43, 54, 55, 30]")
        hosts2rows = create_host_file(hosts_no, df)
        save_pickle(hosts2rows, pickle_path=hosts_file_path)
    else:
        hosts2rows = load_pickle(hosts_file_path)

    if verbose:
        for host, rows in hosts2rows.items():
            logging.info(f"Host '{host}': {len(rows)} rows.")
    return hosts2rows


def is_in_s3_bucket(url, bucket_files):
    aws_csv_filename = f"{get_csv_category(Path(url).stem)}/{Path(url).stem}"
    flag = True if aws_csv_filename in bucket_files else False
    return flag


############### Dowload data from web to local computer ###############
def get_files_in_bucket(bucket_name="bucket-nosql"):
    s3 = boto3.resource(
            service_name='s3', region_name='eu-west-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    my_bucket = s3.Bucket(bucket_name)
    bucket_files = [my_bucket_object.key for my_bucket_object in my_bucket.objects.all()]
    return bucket_files



def get_csv_category(csv_filename, pattern=r'\.(.*?)\.'):
    category = re.findall(pattern, csv_filename)
    return category[0]



def download_data_from_web(urls_to_process, bucket_name):
    bucket_files = get_files_in_bucket(bucket_name)
    for row, url in enumerate(urls_to_process):
        zip_file_path = os.path.join(RAW_ZIP_DATA_DIR, Path(url).name)
        csv_file_path = os.path.join(RAW_CSV_DATA_DIR, Path(url).stem)
        aws_filename = get_csv_category(Path(url).stem) + "/" + Path(url).stem

        if aws_filename not in bucket_files:
            flag = True
            # Download data from remote url to data/raw/zip
            create_directory(RAW_ZIP_DATA_DIR)
            if not os.path.isfile(zip_file_path):
                flag = download_file_from_url(remote_url=url, local_dir=RAW_ZIP_DATA_DIR, verbose=True)
                if not flag:
                    print(f">>>>>> Url broken: '{url}'")
            else:
                logging.info(f"[{row+1}/{len(urls_to_process)}] File '{zip_file_path}' already exists.")

            # Unzip file within data/raw/url to data/raw/csv
            create_directory(RAW_CSV_DATA_DIR)
            if (not os.path.isfile(csv_file_path) and flag is True):
                unzip_file(zip_file_path, RAW_CSV_DATA_DIR, verbose=True)
            else:
                logging.info(f"[{row+1}/{len(urls_to_process)}] File '{csv_file_path}' already exists.")



############### Upload data from local computer to AWS S3 bucket ###############
def upload_file(file_name, bucket, object_name=None):
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



def upload_file_to_S3_bucket(csv_files, bucket_name):
    bucket_files = get_files_in_bucket(bucket_name)
    for no, filename in enumerate(csv_files):
        file_path_csv = RAW_CSV_DATA_DIR + filename
        file_path_zip = RAW_ZIP_DATA_DIR + filename + '.zip'
        object_name = f"{get_csv_category(filename)}/{filename}"

        # Upload csv file from local computer to AWS S3 bucket    
        if object_name not in bucket_files:
            upload_file(file_path_csv, bucket_name, object_name=object_name)
            logging.info(f"Uploading file '{object_name}'..")
            os.remove(file_path_csv)
            os.remove(file_path_zip)


 

if __name__ == '__main__':
    logging.info(f">>>>>>>>>> START <<<<<<<<<<")
    
    parser = argparse.ArgumentParser(
        description="Argument parser for parameters initialization",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-n_rows", "--n_rows", help="Set number of rows to process", type=int, default=5)
    args = parser.parse_args()

    # Load data
    data_file_path = os.path.join(RAW_DATA_DIR, 'master_file_list.txt')
    df = pd.read_csv(data_file_path, sep=' ', header=None, names=['something', 'id', 'url'])
    
    # Select rows for current host
    # host_name = 'tp-hadoop-43'
    host_name = socket.gethostname()
    logging.info(f"host_name: {host_name}")
    hosts_file_path = CONFIG_DIR + 'hosts.pkl'
    hosts2rows = load_pickle(hosts_file_path)
    df_host = df.iloc[hosts2rows[host_name]].copy()
    
    # Set bucket parameters
    bucket_name = "bucket-nosql"
    bucket_files = get_files_in_bucket(bucket_name)
    logging.info(f"{len(bucket_files)} files into AWS S3 bucket: {bucket_files}")
    
    # Retrieve urls to process
    n_urls = args.n_rows
    df_host['in_aws'] = df_host['url'].apply(lambda x: is_in_s3_bucket(x, bucket_files=bucket_files))
    urls_to_process = df_host[df_host['in_aws']==False]['url'].tolist()
    if urls_to_process:
        urls_to_process = urls_to_process[:min(n_urls, len(urls_to_process))] if n_urls is not None else urls_to_process
        # logging.info(f"urls_to_process: {urls_to_process}")
        logging.info(f"urls_to_process: {[f'{get_csv_category(Path(url).stem)}/{Path(url).stem}' for url in urls_to_process]}")
        
        # Dowload data from web to local computer
        download_data_from_web(urls_to_process, bucket_name)
        
        # Upload data from local computer to AWS S3 bucket
        csv_files = [f for f in os.listdir(RAW_CSV_DATA_DIR)]
        upload_file_to_S3_bucket(csv_files, bucket_name)
                
        # Display number of files added into AWS S3 bucket 
        bucket_files_updated = get_files_in_bucket(bucket_name)
        logging.info(f"{len(bucket_files_updated) - len(bucket_files)} files added into AWS S3 bucket")
    else:
        logging.info(f"Stopping script because len(urls_to_process) = {len(urls_to_process)}")
    logging.info(f">>>>>>>>>> END <<<<<<<<<<")

    
    
    
    