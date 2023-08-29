import argparse


def parse_args(arg):
    parser = argparse.ArgumentParser(description="PySpark Application with Arguments")
   
    parser.add_argument(f"{arg}", required=True, help="Name of the argument")

    return parser.parse_args()
