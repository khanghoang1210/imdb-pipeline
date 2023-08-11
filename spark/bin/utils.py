import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="PySpark Application with Arguments")
   
    parser.add_argument("--table-name", required=True, help="Name of the table to read from Postgres")

    return parser.parse_args()
