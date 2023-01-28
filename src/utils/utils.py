import logging
import pandas as pd
import yaml
import pickle
import zipfile
from pathlib import Path, WindowsPath, PosixPath
import wget


###### Load/Save functions
def load_csv(csv_path, verbose=False):
    """ Load data from csv file
    Args:
        csv_path (String): csv path
    Returns:
        df (Dataframe): csv data loaded
    """
    df = pd.read_csv(csv_path)
    if verbose:
        logging.info(f"Csv file successfully loaded from '{csv_path}'")
    return df


def save_csv(df, csv_path, verbose=False):
    """ Save data to csv file
    Args:
        df (Dataframe): Dataframe
        csv_path (String): csv path
    Returns:
        None
    """
    df.to_csv(csv_path, index=False)
    if verbose:
        logging.info(f"Dataframe successfully saved to '{csv_path}'")
        

def load_parquet(parquet_path, engine='fastparquet', verbose=False):
    """ Load data from parquet file
    Args:
        parquet_path (String): parquet path
        engine (String): parquet engine
    Returns:
        df (Dataframe): parquet data loaded
    """
    df = pd.read_parquet(parquet_path, engine=engine)
    if verbose:
        logging.info(f"Parquet file successfully loaded from '{parquet_path}'")
    return df


def save_parquet(df, parquet_path, verbose=False):
    """ Save data to parquet file
    Args:
        df (Dataframe): Dataframe
        parquet_path (String): parquet path
    Returns:
        None
    """
    df.to_parquet(parquet_path, index=False)
    if verbose:
        logging.info(f"Dataframe successfully saved to '{parquet_path}'")
        
    
    
def load_yaml(yaml_path, verbose=False):
    """ Load data from yaml file
    Args:
        yaml_path (String): yaml path
    Returns:
        data (Dictionary): yaml data loaded
    """
    with open(yaml_path, 'rb') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        if verbose:
            logging.info(f"Yaml file successfully loaded from '{yaml_path}'")
    return data



def save_yaml(data, yaml_path, verbose=False):
    """ Save data to yaml file
    Args:
        data (Dictionary): data dictionary to save
        yaml_path (String): yaml path
    Returns:
        None
    """
    with open(yaml_path, 'w', encoding='utf8') as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
        if verbose:
            logging.info(f"Yaml file successfully saved to '{yaml_path}'")
  
    
def load_pickle(pickle_path, verbose=False):
    """ Load data from pickle file
    Args:
        pickle_path (String): pickle path
    Returns:
        data (Dictionary): pickle data loaded
    """
    with open(pickle_path, 'rb') as f:
        data = pickle.load(f)
        if verbose:
            logging.info(f"Pickle file successfully loaded from '{pickle_path}'")
    return data



def save_pickle(data, pickle_path, verbose=False):
    """ Save data to yaml file
    Args:
        data (Dictionary): data dictionary to save
        pickle_path (String): pickle path
    Returns:
        None
    """
    with open(pickle_path, 'wb') as f:
        pickle.dump(data, f)
        if verbose:
            logging.info(f"Pickle file successfully saved to '{pickle_path}'")
  



###### Download/unzip functions ######
 
        
def create_directory(directory_path, force=True):
    directory_path = directory_path if isinstance(directory_path, (WindowsPath, PosixPath)) else Path(directory_path)
    if not directory_path.exists():
        directory_path.mkdir(parents=force)
        logging.info(f"Directory '{directory_path.name}/' created in '{directory_path.parent.resolve()}' folder.")


def download_file_from_url(remote_url, local_dir, verbose=False):
    # filename, _ = request.urlretrieve(remote_url, local_file)
    wget.download(remote_url, local_dir)
    if verbose:
        logging.info(f"Data downloaded from '{remote_url}' url to '{local_dir}' folder.")


def unzip_file(zipfile_path_src, dir_path_dst, verbose=False):
    if verbose:
        logging.info(f"Unzipping file '{zipfile_path_src}' into '{dir_path_dst}' folder.")
    with zipfile.ZipFile(zipfile_path_src, 'r') as zip_ref:
        zip_ref.extractall(dir_path_dst)