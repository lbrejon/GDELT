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
from multiprocessing import Pool, cpu_count
from functools import partial


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

# AWS_ACCESS_KEY_ID = load_yaml(CONFIG_DIR+'aws_secret.yml')['AWS_ACCESS_KEY_ID']
# AWS_SECRET_ACCESS_KEY = load_yaml(CONFIG_DIR+'aws_secret.yml')['AWS_SECRET_ACCESS_KEY']

NB_CPU = cpu_count()
NB_PROCESSES = 4




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


# def is_in_s3_bucket(url, bucket_files):
#     aws_csv_filename = f"{get_csv_category(Path(url).stem)}/{Path(url).stem}"
#     flag = True if aws_csv_filename in bucket_files else False
#     return flag


def is_uploaded(url, csv_dir):
    csv_filenames = [f for f in os.listdir(csv_dir) if os.path.isfile(f)]
    # logging.info(f"{len(csv_filenames)} files within csv folder '{csv_dir}'")
    flag = True if url in csv_filenames else False
    return flag


############### Dowload data from web to local computer ###############
# def get_files_in_bucket(bucket_name="bucket-nosql"):
#     s3 = boto3.resource(
#             service_name='s3', region_name='eu-west-1',
#             aws_access_key_id=AWS_ACCESS_KEY_ID,
#             aws_secret_access_key=AWS_SECRET_ACCESS_KEY
#         )
#     my_bucket = s3.Bucket(bucket_name)
#     bucket_files = [my_bucket_object.key for my_bucket_object in my_bucket.objects.all()]
#     return bucket_files



def get_csv_category(csv_filename, pattern=r'\.(.*?)\.'):
    category = re.findall(pattern, csv_filename)
    return category[0]



def download_data_from_web(url_to_process, zip_dir=RAW_ZIP_DATA_DIR, csv_dir=RAW_CSV_DATA_DIR):
    # bucket_files = get_files_in_bucket(bucket_name)
    create_directory(zip_dir)
    create_directory(csv_dir)
    # for row, url in enumerate(urls_to_process):
    url = url_to_process
    zip_file_path = os.path.join(zip_dir, Path(url).name)
    csv_file_path = os.path.join(csv_dir, Path(url).stem)
    # aws_filename = get_csv_category(Path(url).stem) + "/" + Path(url).stem

    
    # Download data from remote url to data/raw/zip
    flag = False
    if not os.path.isfile(zip_file_path):
        flag_not_broken = download_file_from_url(remote_url=url, local_dir=zip_dir, verbose=True)
        flag = flag_not_broken
        if not flag_not_broken:
            print(f">>>>>> Url broken: '{url}'")
    else:
        logging.info(f"Zip file '{zip_file_path}' already exists.")
        

    # Unzip file from data/raw/zip to data/raw/csv
    if ((not os.path.isfile(csv_file_path)) and (flag is True)):
        unzip_file(zip_file_path, csv_dir, verbose=True)
        os.remove(zip_file_path)
        filename = zip_file_path
    else:
        if flag_not_broken: 
            logging.info(f"Csv file '{csv_file_path}' already exists.")
        filename = None
        return filename


############### Upload data from local computer to AWS S3 bucket ###############
# def upload_file(file_name, bucket, object_name=None):
#     """Upload a file to an S3 bucket
#     :param file_name: File to upload
#     :param bucket: Bucket to upload to
#     :param object_name: S3 object name. If not specified then file_name is used
#     :return: True if file was uploaded, else False
#     """

#     # If S3 object_name was not specified, use file_name
#     if object_name is None:
#         object_name = os.path.basename(file_name)

#     # Upload the file
#     s3_client = boto3.client(
#         service_name='s3', region_name='eu-west-1',
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY 
#     )
#     try:
#         response = s3_client.upload_file(file_name, bucket, object_name)
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True



# def upload_file_to_S3_bucket(csv_files, bucket_name, zip_dir=RAW_ZIP_DATA_DIR, csv_dir=RAW_CSV_DATA_DIR):
#     bucket_files = get_files_in_bucket(bucket_name)
#     for no, filename in enumerate(csv_files):
#         file_path_csv = csv_dir + filename
#         file_path_zip = zip_dir + filename + '.zip'
#         object_name = f"{get_csv_category(filename)}/{filename}"

#         # Upload csv file from local computer to AWS S3 bucket    
#         if object_name not in bucket_files:
#             upload_file(file_path_csv, bucket_name, object_name=object_name)
#             logging.info(f"Uploading file '{object_name}'..")
#             os.remove(file_path_csv)
#             os.remove(file_path_zip)



 
# python3 src/functions/upload_data.py -n_rows 1000 -zip_dir "D:/NoSQL/zip/" -csv_dir "D:/NoSQL/csv/"

if __name__ == '__main__':
    logging.info(f">>>>>>>>>> START <<<<<<<<<<")
    
    parser = argparse.ArgumentParser(
        description="Argument parser for parameters initialization",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-n_rows", "--n_rows", help="Set number of rows to process", type=int, default=5)
    parser.add_argument("-zip_dir", "--zip_dir", help="Set zip directory path", type=str, default=RAW_ZIP_DATA_DIR)
    parser.add_argument("-csv_dir", "--csv_dir", help="Set csv directory path", type=str, default=RAW_CSV_DATA_DIR)
    
    args = parser.parse_args()
    n_urls = args.n_rows
    zip_dir = args.zip_dir
    csv_dir = args.csv_dir
    
    # Load data
    data_file_path = os.path.join(RAW_DATA_DIR, 'master_file_list.txt')
    df = pd.read_csv(data_file_path, sep=' ', header=None, names=['something', 'id', 'url'])
    
    # Select rows for current host
    # host_name = 'tp-hadoop-30'
    host_name = socket.gethostname()
    print(f"host_name: {host_name}")
    hosts_file_path = CONFIG_DIR + 'hosts.pkl'
    hosts2rows = load_pickle(hosts_file_path)
    df_host = df.iloc[hosts2rows[host_name]].copy()
    
    # Retrieve urls to process
    df_host['is_uploaded'] = df_host['url'].apply(lambda x: is_uploaded(x, csv_dir=csv_dir))
    urls_to_process = df_host[df_host['is_uploaded']==False]['url'].tolist()
    if urls_to_process:
        urls_to_process = urls_to_process[:min(n_urls, len(urls_to_process))] if n_urls is not None else urls_to_process
        logging.info(f"urls_to_process: {urls_to_process}")

        
        # Dowload data from web to local computer
        uploaded_files = []
        with Pool(processes=NB_PROCESSES) as pool: 
            uploaded_files += list(map(partial(download_data_from_web, zip_dir=zip_dir, csv_dir=csv_dir), urls_to_process))
    else:
        logging.info(f"Stopping script because len(urls_to_process) = {len(urls_to_process)}")
    logging.info(f">>>>>>>>>> END <<<<<<<<<<")

    
    
    
    