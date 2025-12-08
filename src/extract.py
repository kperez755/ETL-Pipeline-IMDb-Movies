from logger import logger
import pandas as pd
from pandas import Series, DataFrame
from configs.config import *
import sys
import os
import time

## Logger
from utils.get_logger import get_logger
import logging


logger = get_logger(
    name='ETL',
    log_file='logs/etl',
    level=logging.INFO

)


## Ensure parent directory is in the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

raw_data = 'data/raw/movies.csv'


def extract_data(raw_data):
    logger.info("Reading the CSV")
    df = pd.read_csv(raw_data)


    logger.info("Printing the Head")
    print(f"Head \n{df.head()}")

    logger.info("Printing Information")
    print(f"DataFrame Information \n{df.info()}")


    logger.info("Checking if there are any NULL values")
    print(f"Any Null values: \n{df.isnull().sum()}")

    return df

time.sleep(3)







