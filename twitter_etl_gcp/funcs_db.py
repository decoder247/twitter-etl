import os
from venv import create

import pandas as pd
from google.cloud import bigquery, storage
from sqlalchemy import create_engine, text


def db_test_connection(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("select 'hello world'"))
            print(f"Connection successful - test string {result.all()}")
            return 0
    except:
        return 1


def db_create_engine(
    connection_string: str,
    echo: bool = False,
    future: bool = False,
    poolclass=None,
    test_connection: bool = True,
):
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    # Create engine -> Not sure if nullpool needed
    if poolclass is None:
        engine = create_engine(
            connection_string,
            echo=echo,
            future=future,
            # poolclass=NullPool
        )
    else:
        engine = create_engine(
            connection_string,
            echo=echo,
            future=future,
            poolclass=poolclass,
        )

    # Test connection if specified
    if test_connection and db_test_connection(engine):
        raise ValueError("Unable to connect to database")

    return engine


def db_select_query_to_df(query, engine):
    return pd.read_sql_query(query, engine)