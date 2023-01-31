import os
import argparse
import logging



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Argument parser for parameters initialization",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-n_rows", "--n_rows", help="Set number of rows to process", type=int, default=1000)
    parser.add_argument("-n_iters", "--n_iters", help="Set number of iterations", type=int, default=3)
    parser.add_argument("-zip_dir", "--zip_dir", help="Set number of iterations", type=str)
    parser.add_argument("-csv_dir", "--csv_dir", help="Set number of iterations", type=str)
    
    args = parser.parse_args()
    n_rows = args.n_rows
    n_iters = args.n_iters
    zip_dir = args.zip_dir if 'zip_dir' in args else None
    csv_dir = args.csv_dir if 'csv_dir' in args else None
    
    script_name = 'src/functions/upload_data.py' 
    # -zip_dir "D:/NoSQL/zip/" -csv_dir "D:/NoSQL/csv/"
    
    for i in range(n_iters):
        if ((zip_dir is not None) and (csv_dir is not None)):
            command_line = f"python3 {script_name} -n_rows {n_rows} -zip_dir {zip_dir} -csv_dir {csv_dir}"
        else:
            command_line = f"python3 {script_name} -n_rows {n_rows}"
        print(f"[{i+1}/{n_iters}]: {command_line}")
        os.system(command_line)