import argparse





if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Argument parser for parameters initialization",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-n_rows", "--n_rows", help="Set number of rows to process", type=int)
    
    args = parser.parse_args()
    n_rows = args.n_rows
    
    print(f">>>>>>>>>> START <<<<<<<<<<")
    print(n_rows)
    print(f">>>>>>>>>> END <<<<<<<<<<")
    print(f"\n\n")