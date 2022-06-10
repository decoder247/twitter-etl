import os
from venv import create

import pandas as pd
from google.cloud import bigquery, storage
from sqlalchemy import create_engine, text





def save_to_parquet(df, output_file, compression="GZIP"):
    df.to_parquet(output_file, compression = compression)

