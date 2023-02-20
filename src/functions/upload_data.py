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

AWS_ACCESS_KEY_ID = load_yaml(CONFIG_DIR+'aws_secret.yml')['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = load_yaml(CONFIG_DIR+'aws_secret.yml')['AWS_SECRET_ACCESS_KEY']

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


def is_in_s3_bucket(url, bucket_files):
    aws_csv_filename = f"{get_csv_category(Path(url).stem)}/{Path(url).stem}"
    flag = True if aws_csv_filename in bucket_files else False
    return flag


def is_uploaded(url, csv_dir=None, csv_files=None):
    if csv_dir:
        csv_filenames = [f for f in os.listdir(csv_dir)]
        url = Path(url).stem
    else:
        csv_filenames = csv_files
        url = get_csv_category(Path(url).stem) + "/" + Path(url).stem
    flag = True if url in csv_filenames else False
    return flag


############### Dowload data from web to local computer ###############
def get_files_in_bucket(bucket_name):
    s3 = boto3.resource(
        service_name='s3', region_name='eu-west-3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    my_bucket = s3.Bucket(bucket_name)
    bucket_files = [my_bucket_object.key for my_bucket_object in my_bucket.objects.all()]
    return bucket_files



def get_csv_category(csv_filename, pattern=r'\.(.*?)\.'):
    category = re.findall(pattern, csv_filename)
    return category[0]



def is_non_zero_file(file_path):  
    return os.path.isfile(file_path) and os.path.getsize(file_path) > 0



def reduce_csv_file_size(raw_csv_path, processed_csv_path, indexes):
    flag = False
    if is_non_zero_file(raw_csv_path):
        df_csv = pd.read_csv(raw_csv_path, sep='\t', header=None, on_bad_lines='skip', encoding="utf-8")[indexes]
        df_csv.to_csv(processed_csv_path)
        flag = True
    return flag



def download_data_from_web(url, zip_dir, csv_dir, category2cols, bucket_name=None):
    filename = None
    raw_zip_file_path = os.path.join(zip_dir, Path(url).name)
    
    category_file = get_csv_category(Path(url).stem)
    # aws_csv_filename = category_file + "/" + Path(url).stem
        
    if os.path.isfile(raw_zip_file_path):
        logging.warning(f"Zip file '{raw_zip_file_path}' already exists.")
    else:
        # Download data from remote url to data/raw/zip/
        flag_url_ok = download_file_from_url(remote_url=url, local_dir=zip_dir, verbose=True)
        
        # Check if url is broken
        if not flag_url_ok:
            logging.warning(f"Url is broken: '{url}'")
        else:
            # Unzip file from data/raw/zip/ to data/raw/csv/<category>/
            raw_csv_dir_by_category = csv_dir + f"{category_file}/"
            create_directory(raw_csv_dir_by_category)
            unzip_file(raw_zip_file_path, raw_csv_dir_by_category, verbose=True)
            
            # Remove raw zip file
            os.remove(raw_zip_file_path)
            
            # Reduce raw csv file size by dropping columns and save it to data/processed/csv/<category>/
            processed_csv_dir_by_category = raw_csv_dir_by_category.replace('raw/', 'processed/')
            create_directory(processed_csv_dir_by_category)
            raw_csv_file_path = os.path.join(raw_csv_dir_by_category, Path(url).stem)
            processed_csv_file_path = raw_csv_file_path.replace('raw/', 'processed/')
            flag_csv_ok = reduce_csv_file_size(raw_csv_file_path, processed_csv_file_path, indexes=category2cols[category_file])
            
            # Remove raw csv file
            # os.remove(raw_csv_file_path)
        
            if not flag_csv_ok:
                logging.warning(f"Csv file is empty: '{raw_csv_file_path}'")
            else:
                # Upload file from local computer to AWS S3 bucket
                aws_filename = category_file + '/' + Path(processed_csv_file_path).name
                # print(f"\naws_filename: {aws_filename}")
                # upload_file(processed_csv_file_path, bucket_name, object_name=aws_filename)
                logging.info(f"Uploading file '{processed_csv_file_path}'..")
                
                # Remove processed csv file
                # os.remove(processed_csv_file_path)
        
                # Set filename used
                filename = processed_csv_file_path
    return filename


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
        service_name='s3', region_name='eu-west-3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY 
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



# def upload_file_to_S3_bucket(csv_files, bucket_name, zip_dir, csv_dir):
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



 
# python src/functions/upload_data.py -n_rows 10 -zip_dir "D:/NoSQL/zip/" -csv_dir "D:/NoSQL/csv/"

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
    host_name = 'tp-hadoop-43'
    # host_name = socket.gethostname()
    print(f"host_name: {host_name}")
    
    bucket_name = 'gdelt-nosql'
    print(f"bucket_name: {bucket_name}")
    
    hosts_file_path = CONFIG_DIR + 'hosts.pkl'
    hosts2rows = load_pickle(hosts_file_path)
    df_host = df.iloc[hosts2rows[host_name]].copy()
    
    # Retrieve urls to process
    flag = 'aws'
    if flag == 'local':
        df_host['is_uploaded'] = df_host['url'].apply(lambda x: is_uploaded(x, csv_dir=csv_dir))
    else:
        csv_filenames = get_files_in_bucket(bucket_name)
        df_host['is_uploaded'] = df_host['url'].apply(lambda x: is_uploaded(x, csv_files=csv_filenames))
    urls_to_process = df_host[df_host['is_uploaded']==False]['url'].tolist()
    urls_to_process = urls_to_process[:min(n_urls, len(urls_to_process))] if n_urls is not None else urls_to_process
    logging.info(f"{len(urls_to_process)} urls to process: {urls_to_process}")
    
    # Select interesting columns within csv
    category2cols = {
        'export': [0, 1, 2, 5, 15, 53],
        'mentions': [0, 1, 2],
        'gkg': [0, 1, 3, 7, 9, 11, 15]
    }
    
    # Download data
    if urls_to_process:
        # urls_to_process = urls_to_process[:min(n_urls, len(urls_to_process))] if n_urls is not None else urls_to_process
        create_directory(zip_dir)
        create_directory(csv_dir)
        
        # Dowload data from web to local computer
        uploaded_files = []
        with Pool(processes=NB_PROCESSES) as pool: 
            uploaded_files += list(map(partial(download_data_from_web, zip_dir=zip_dir, csv_dir=csv_dir, category2cols=category2cols, bucket_name=bucket_name), urls_to_process))
        logging.info(f"[{len(uploaded_files)}/{len(urls_to_process)}] csv files downloaded: {uploaded_files}")
    else:
        logging.info(f"Stopping script because len(urls_to_process) = {len(urls_to_process)}")
    logging.info(f">>>>>>>>>> END <<<<<<<<<<")

    
    
    
    