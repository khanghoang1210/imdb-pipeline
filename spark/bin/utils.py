import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="PySpark Application with Arguments")
   
    parser.add_argument("--table-name", required=False, help="Name of the table")
    parser.add_argument("--execution-date", required=False, help="Execution date")
    # parser.add_argument("--exec-date", required=False, help="Execution date")

    return parser.parse_args()
