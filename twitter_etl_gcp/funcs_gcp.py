import os
from venv import create

import pandas as pd
from google.cloud import bigquery, storage
from sqlalchemy import create_engine, text


def gbq_create_client(google_application_credentials_json_path: str):
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = f"{google_application_credentials_json_path}"
    return bigquery.Client()


def gbq_list_datasets(bqclient, echo: bool = False):
    datasets = list(bqclient.list_datasets())  # Make an API request.
    project = bqclient.project
    dataset_names = []

    if datasets:
        if echo:
            print("Datasets in project {}:".format(project))
        for dataset in datasets:
            dataset_names.append(dataset.dataset_id)
            if echo:
                print("\t{}".format(dataset.dataset_id))
    else:
        if echo:
            print("{} project does not contain any datasets.".format(project))
    return datasets, dataset_names


def gbq_list_tables(bqclient, dataset_name: str, echo: bool = False):
    # TODO(developer): Set dataset_id to the ID of the dataset that contains
    #                  the tables you are listing.
    dataset_id = f"{bqclient.project}.{dataset_name}"

    tables = bqclient.list_tables(dataset_id)  # Make an API request.

    table_names = []

    if echo:
        print("Tables contained in '{}':".format(dataset_id))
    for table in tables:
        table_names.append(table.table_id)
        if echo:
            print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    return tables, table_names


def gbq_select_query_to_df(bqclient, query_string: str = None):
    assert query_string is not None, f"query_string not specified"
    return (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )


def gbq_create_table_schema(schema_dict: dict):
    """
    Possible modes - https://cloud.google.com/bigquery/docs/schemas
    * NULLABLE (Default)
    * REQUIRED
    * REPEATED (column contains an array of values of specified type)

    schema = [
        bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("gender", "STRING"),
    ]
    """
    schema = []
    for col_name, col_data_type_and_mode in schema_dict.items():
        col_data_type_and_mode_orig = col_data_type_and_mode
        if isinstance((col_data_type_and_mode), list):
            """
            Possibilities:
            - CASE 1: 'address': ['STRING','REQUIRED']
            - CASE 2: 'address': ['STRING']
            """

            col_data_type = col_data_type_and_mode.pop(0)
            if col_data_type_and_mode:
                # CASE 1
                mode = col_data_type_and_mode.pop(0)
                if col_data_type_and_mode:
                    raise ValueError(
                        f"Too many schema arguments for col {col_name}. Expected max 2 [data type, mode]. Schema - {col_data_type_and_mode_orig}"
                    )
            else:
                # CASE 2
                mode = "NULLABLE"
        else:
            """
            Possibilities:
            - 'address': 'STRING'
            """
            assert isinstance(
                col_data_type_and_mode, str
            ), f"{col_data_type_and_mode} is not of string type"
            col_data_type = col_data_type_and_mode
            mode = "NULLABLE"

        schema.append(bigquery.SchemaField(col_name, col_data_type, mode=mode))
    return schema


def gbq_create_dataset(
    bqclient,
    dataset_name: str,
    dataset_location: str = "europe-west1",
    echo: bool = True,
):
    # TODO(developer): Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(bqclient.project, dataset_name)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = dataset_location

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = bqclient.create_dataset(dataset, timeout=30)  # Make an API request.
    if echo:
        print("Created dataset {}.{}".format(bqclient.project, dataset.dataset_id))
    return 0


def gbq_create_table(bqclient, schema, dataset_name, table_name):
    """ """
    # Set table_id to the ID of the table to create.
    table_id = f"{bqclient.project}.{dataset_name}.{table_name}"

    table = bigquery.Table(table_id, schema=schema)
    table = bqclient.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    return


def create_gbq_job_config(
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect: bool = True,
    skip_leading_rows: int = 1,
):
    if source_format == bigquery.SourceFormat.CSV:
        return bigquery.LoadJobConfig(
            source_format=source_format,
            skip_leading_rows=skip_leading_rows,
            autodetect=autodetect,
        )
    else:
        return bigquery.LoadJobConfig(
            source_format=source_format,
            autodetect=autodetect,
        )


def append_file_to_gbq_table(
    bqclient, input_file_path: str, destination_table_id: str, job_config: dict
):
    # Appends local file into the bigquery table
    with open(input_file_path, "rb") as source_file:
        job = bqclient.load_table_from_file(
            source_file, destination_table_id, job_config=job_config
        )
    return job
