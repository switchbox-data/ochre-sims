import sys
import io
import os
import time
import json
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
import argparse
import duckdb
# AWS
import boto3
from botocore import UNSIGNED
from botocore.client import Config

# AWS Session
session = boto3.Session(profile_name="switchbox")
s3_client = session.client("s3")
athena_client = session.client("athena", region_name="us-west-2")

# S3 Paths
aws_database = "euss_oedi"
bucket = "dummy-123-target"
folder = "dummy_athena_queries"


def drop_table(
    athena_client: boto3.client,
    aws_database: str,
    table_name: str,
):

    drop_query = f"DROP TABLE IF EXISTS {aws_database}.{table_name}"
    print(f"Attempting to drop table: {drop_query}")
    drop_response = athena_client.start_query_execution(
        QueryString=drop_query,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/{folder}/temp"},
    )

    # Wait for drop query to complete
    while True:
        drop_status = athena_client.get_query_execution(
            QueryExecutionId=drop_response["QueryExecutionId"]
        )
        drop_state = drop_status["QueryExecution"]["Status"]["State"]

        if drop_state == "SUCCEEDED":
            print(f"    -> Dropped!")
            break
        elif drop_state == "FAILED":
            raise Exception(f"Failed to drop table: {table_name}")
        elif drop_state == "CANCELLED":
            raise Exception(f"Cancelled drop query for table: {table_name}")

        time.sleep(0.5)

    return drop_state


###############################################################################
# Old Method
def generate_athena_query_string(
    state: str,
    upgrade: str,
    p2columns: str = None,
    agg_frequency: str = "month",
    verbose: bool = False,
    s3_output_location: str = None,
) -> str:

    limit = 10
    timestamp = time.strftime("%m_%d_%H%M")

    s3_output_location = f"s3://{bucket}/{folder}/temp/{timestamp}"

    # if agg_frequency not in ['date' 'month', 'year']:
    # raise ValueError(f"Invalid aggregation frequency: '{agg_frequency}'. Should be ['date', 'month']"
    print(agg_frequency)

    if p2columns:
        with open((p2columns), "r") as f:
            columns = json.load(f)

    if p2columns is None:
        columns = [
            "out.electricity.total.energy_consumption",
            "out.fuel_oil.total.energy_consumption",
            "out.natural_gas.total.energy_consumption",
            "out.propane.total.energy_consumption",
            "out.site_energy.net.energy_consumption",
            "out.site_energy.total.energy_consumption",
        ]

    # Create the column selections and sum expressions
    column_selects = ",\n            ".join(f'"{col}"' for col in columns)
    sum_expressions = ",\n          ".join(
        f'SUM("{col}") as "sum_{col}"' for col in columns
    )

    # Convert agg_frequency to string if it's not already
    if agg_frequency == "month":
        extract_statement = 'EXTRACT(month FROM "timestamp") as month'
    elif agg_frequency == "date":
        # extract_statement = "(DATE('timestamp') - '15' minute) as date"
        extract_statement = (
            """DATE_ADD('minute', -15, CAST("timestamp" AS timestamp)) as date"""
        )
    elif agg_frequency == "hour":
        # For 8760 unique hours, we need both the date and hour
        # extract_statement = """DATE("timestamp") || ' ' ||
        #         LPAD(CAST(EXTRACT(HOUR FROM "timestamp") as VARCHAR), 2, '0')
        #         as hour"""
        extract_statement = (
            """DATE_ADD('minute', -15, CAST("timestamp" AS timestamp)) as date"""
        )

    # DROP TABLE IF EXISTS euss_oedi.temp_{state}_{upgrade}_{agg_frequency};
    # external_location = '{s3_output_location}/temp_{state}_{upgrade}_{agg_frequency}',
    query = f"""
    CREATE TABLE euss_oedi.temp_{timestamp}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = '{s3_output_location}',
        bucketed_by = ARRAY['bldg_id'],
        bucket_count = 1
    ) AS
    WITH base_data AS (
    SELECT 
        bldg_id,
        {extract_statement},
        upgrade,
        {column_selects}
    FROM "euss_oedi"."res_amy18_r1_by_state"
    WHERE "state" = '{state}'
    AND "upgrade" = '{upgrade}'
    )
    SELECT 
    bldg_id,
    {agg_frequency},
    upgrade,
    {sum_expressions}
    FROM base_data
    GROUP BY bldg_id, {agg_frequency}, upgrade
    ORDER BY bldg_id, {agg_frequency};
    """

    if verbose:
        print(query)

    return query, timestamp


def get_athena_query_results_as_parquet(
    s3_client: boto3.client,
    athena_client: boto3.client,
    timestamp: str,
    query_string: str,
    aws_database: str,
    s3_output_location: str,
    local_parquet_path: str,
):
    """
    Run Athena query and save results as local Parquet file
    """

    print(f"S3 output location: {s3_output_location}")

    # --------------------------------------------------------------------------
    # First, delete the table if it exists
    # Drop the table if it exists
    drop_query = f"DROP TABLE IF EXISTS {aws_database}.temp_{timestamp}"
    print(f"Dropping table: {drop_query}")
    drop_response = athena_client.start_query_execution(
        QueryString=drop_query,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    # Wait for drop query to complete
    while True:
        drop_status = athena_client.get_query_execution(
            QueryExecutionId=drop_response["QueryExecutionId"]
        )
        drop_state = drop_status["QueryExecution"]["Status"]["State"]

        if drop_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)
        print(f"Dropped!")

    # --------------------------------------------------------------------------
    # Now, the interesting ResStock query
    print(f"Running query: {query_string}")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    # Wait for query to complete
    start_time = time.time()
    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        q_state = query_status["QueryExecution"]["Status"]["State"]

        if q_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        elapsed_time = time.time() - start_time  # Calculate elapsed time

        sys.stdout.write(f"\r{q_state} ({elapsed_time:.1f}s)")
        sys.stdout.flush()

        time.sleep(1)

    if q_state != "SUCCEEDED":
        raise Exception(f"Query failed with state: {q_state}")

    print(f"\n\nStatus: {q_state}")

    # --------------------------------------------------------------------------
    # Get the S3 bucket and key from the output location
    # Locate the file within s3
    print(f"Bucket: {bucket}")
    print(f"Folder: {folder}")

    p2temp = folder + f"/temp"
    print(f"p2temp: {p2temp}")

    p2timestamp = folder + f"/temp/{timestamp}"
    print(f"p2timestamp: {p2timestamp}")

    # List all objects in the s3_output_location
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=p2timestamp)

    # Download each file found
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                obj_key = obj["Key"]
                # Skip metadata files
                excluded_endings = [".metadata", ".csv"]
                if not any(obj_key.endswith(ending) for ending in excluded_endings):
                    print(f"Downloading {obj_key} to {local_parquet_path}")
                    s3_client.download_file(
                        Bucket=bucket, Key=obj_key, Filename=local_parquet_path
                    )

    # --------------------------------------------------------------------------
    # Optional: Clean up S3 results

    # drop the table from the database
    drop_query = f"DROP TABLE IF EXISTS {aws_database}.temp_{timestamp}"
    print(f"Dropping table: {drop_query}")
    drop_response = athena_client.start_query_execution(
        QueryString=drop_query,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    while True:
        drop_status = athena_client.get_query_execution(
            QueryExecutionId=drop_response["QueryExecutionId"]
        )
        drop_state = drop_status["QueryExecution"]["Status"]["State"]

        if drop_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)

    # Delete all objects in the temp folder
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=p2temp)

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])


###############################################################################
# New Method
def build_intermediate_table(
    state: str,
    upgrade: str,
    s3_client: boto3.client,
    athena_client: boto3.client,
    aws_database: str,
    s3_output_location: str,
    p2columns: str = None,
) -> str:

    intermediate_table = f"{state}_mdh_cols"
    
    
    if p2columns:
        with open((p2columns), "r") as f:
            columns = json.load(f)

    if p2columns is None:
        columns = [
            "out.electricity.total.energy_consumption",
            "out.fuel_oil.total.energy_consumption",
            "out.natural_gas.total.energy_consumption",
            "out.propane.total.energy_consumption"
        ]

    # --------------------------------------------------------------------------
    # 2. Drop the table if it exists
    print(f"\n1. Dropping existing table ({intermediate_table})")
    drop_table(
        athena_client=athena_client,
        aws_database=aws_database,
        table_name=intermediate_table,
    )
    

    # --------------------------------------------------------------------------
    # 2. Write the query
    
    # Create the column selections and sum expressions
    if p2columns:
        with open((p2columns), "r") as f:
            columns = json.load(f)

    if p2columns is None:
        columns = [
            "out.electricity.total.energy_consumption",
            "out.fuel_oil.total.energy_consumption",
            "out.natural_gas.total.energy_consumption",
            "out.propane.total.energy_consumption"
        ]
    
    column_selects = ",\n            ".join(f'"{col}"' for col in columns)
    
    query_string = f"""
    CREATE TABLE {aws_database}.{intermediate_table} AS
    SELECT 
        bldg_id,
        upgrade,
        EXTRACT(MONTH FROM adjusted_time) as month,
        EXTRACT(DAY FROM adjusted_time) as day,
        EXTRACT(HOUR FROM adjusted_time) as hour,
        {column_selects}
    FROM (
        SELECT 
            *,
            DATE_ADD('minute', -15, CAST("timestamp" AS timestamp)) as adjusted_time
        FROM "euss_oedi"."res_amy18_r1_by_state"
        WHERE state = '{state.upper()}'
        AND upgrade IN ({','.join([f"'{u}'" for u in upgrade])})
        );
    """

    print(f"\n2. Query to build Intermediate table ({intermediate_table})")
    print(query_string)


    # --------------------------------------------------------------------------
    # 3. Run the query
    print(f"3. Now running the query intermediate table...")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    start_time = time.time()
    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        q_status = query_status["QueryExecution"]["Status"]["State"]

        if q_status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        elapsed_time = time.time() - start_time  # Calculate elapsed time

        sys.stdout.write(f"\r{q_status} ({elapsed_time:.1f}s)")
        sys.stdout.flush()

        time.sleep(1)

    if q_status == "SUCCEEDED":
        print(f"\n\nQuery Succeeded (Status: {q_status})")
    else:
        raise Exception(f"Query failed with q_status: {q_status}")

    return f"{aws_database}.{intermediate_table}"


def monthly_query(
    intermediate_table: str,
    state: str,
    upgrade: str,
    s3_client: boto3.client,
    athena_client: boto3.client,
    aws_database: str,
    s3_output_location: str,
    local_parquet_path: str,
    duckdb_path: str,
    p2columns: str = None,
):

    monthly_table = f"{state}_monthly"

    # Clean up existing files in the target S3 location
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=f"summaries/{state}/monthly")

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])

    # --------------------------------------------------------------------------
    drop_query = f"DROP TABLE IF EXISTS {aws_database}.{monthly_table}"
    drop_table(
        athena_client=athena_client, aws_database=aws_database, table_name=monthly_table
    )

    # --------------------------------------------------------------------------
    if p2columns:
        with open((p2columns), "r") as f:
            columns = json.load(f)

    if p2columns is None:
        columns = [
            "out.electricity.total.energy_consumption",
            "out.fuel_oil.total.energy_consumption",
            "out.natural_gas.total.energy_consumption",
            "out.propane.total.energy_consumption",
            "out.site_energy.net.energy_consumption",
            "out.site_energy.total.energy_consumption",
        ]

    # Create the column selections and sum expressions
    column_selects = ",\n            ".join(f'"{col}"' for col in columns)
    sum_expressions = ",\n          ".join(
        f'SUM("{col}") as "sum_{col}"' for col in columns
    )

    query_string = f"""
    CREATE TABLE {monthly_table}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = 's3://dummy-123-target/summaries/{state}/monthly'
    ) AS
    SELECT 
        bldg_id,
        month,
        upgrade,
        {sum_expressions}
    FROM {intermediate_table}
    GROUP BY bldg_id, upgrade, month
    ORDER BY bldg_id, upgrade, month;
    """

    print(query_string)

    print(f"Running query: {query_string}")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    # Wait for query to complete
    start_time = time.time()
    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        q_status = query_status["QueryExecution"]["Status"]["State"]

        if q_status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        elapsed_time = time.time() - start_time  # Calculate elapsed time

        sys.stdout.write(f"\r{q_status} ({elapsed_time:.1f}s)")
        sys.stdout.flush()

        time.sleep(1)

    if q_status != "SUCCEEDED":
        raise Exception(f"Query failed with q_status: {q_status}")

    print(f"\n\nStatus: {q_status}")

    # --------------------------------------------------------------------------
    # Get the S3 bucket and key from the output location
    # Locate the file within s3
    print(f"Bucket: {bucket}")
    print(f"Folder: {folder}")

    p2temp = folder + f"/temp"
    print(f"p2temp: {p2temp}")

    # p2timestamp = folder + f"/temp/{timestamp}"
    # print(f"p2timestamp: {p2timestamp}")
    summaries_folder = f"summaries/{state}/monthly"
    print(f"Summaries folder: {summaries_folder}")

    # List all objects in the s3_output_location
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=summaries_folder)
    print(f"Pages: {pages}")

    # Download each file found
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                obj_key = obj["Key"]
                # Skip metadata files
                excluded_endings = [".metadata", ".csv"]
                if not any(obj_key.endswith(ending) for ending in excluded_endings):
                    print(f"Downloading {obj_key} to {local_parquet_path}")
                    s3_client.download_file(
                        Bucket=bucket, Key=obj_key, Filename=local_parquet_path
                    )

    # --------------------------------------------------------------------------
    # Optional: Clean up S3 results

    # drop the table from the database
    drop_query = f"DROP TABLE IF EXISTS {monthly_table}"
    print(f"Dropping table: {drop_query}")
    drop_response = athena_client.start_query_execution(
        QueryString=drop_query,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    while True:
        drop_status = athena_client.get_query_execution(
            QueryExecutionId=drop_response["QueryExecutionId"]
        )
        drop_state = drop_status["QueryExecution"]["Status"]["State"]

        if drop_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(1)

    # Delete all objects in the temp folder
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=p2temp)

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])

 
def hourly_query(
    intermediate_table: str,
    state: str,
    upgrade: str,
    s3_client: boto3.client,
    athena_client: boto3.client,
    aws_database: str,
    s3_output_location: str,
    local_parquet_path: str,
    duckdb_path: str,
    p2columns: str = None,
    download_via: str = "direct_to_duckdb",
    drop_table_after_query: bool = False,
):
    
    hourly_table = f"{state}_hourly"
    
    # --------------------------------------------------------------------------
    # 1. Clean up existing files in the target S3 location
    
    
    print(f"\n1. Cleaning up existing files in the target S3 location")
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=f"summaries/{state}/hourly")

    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
                

    drop_query = f"DROP TABLE IF EXISTS {aws_database}.{hourly_table}"
    drop_table(
        athena_client=athena_client, aws_database=aws_database, table_name=hourly_table
    )

    # --------------------------------------------------------------------------
    # 2. Write the query
    print(f"\n2. Writing the query")
    if p2columns:
        with open((p2columns), "r") as f:
            columns = json.load(f)

    if p2columns is None:
        columns = [
            "out.electricity.total.energy_consumption",
            "out.fuel_oil.total.energy_consumption",
            "out.natural_gas.total.energy_consumption",
            "out.propane.total.energy_consumption"
        ]

    # Create the column selections and sum expressions      
    column_selects = ",\n            ".join(f'"{col}"' for col in columns)
    sum_expressions = ",\n          ".join(
        f'SUM("{col}") as "sum_{col}"' for col in columns
    )   
    
    query_string = f"""
    CREATE TABLE {hourly_table}
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = 's3://dummy-123-target/summaries/{state}/hourly'
    ) AS        
    SELECT 
        bldg_id,
        upgrade,
        month,
        day,
        hour,
        {sum_expressions}
    FROM {intermediate_table}
    GROUP BY bldg_id, upgrade, month, day, hour
    ORDER BY bldg_id, upgrade, month, day, hour;
    """

    print(f"3. Running query: {query_string}")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": aws_database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    # Wait for query to complete
    start_time = time.time()    
    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        q_status = query_status["QueryExecution"]["Status"]["State"]        

        if q_status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        elapsed_time = time.time() - start_time  # Calculate elapsed time

        sys.stdout.write(f"\r{q_status} ({elapsed_time:.1f}s)")
        sys.stdout.flush()

        time.sleep(1)

    if q_status == "SUCCEEDED":
        print(f"\n\nQuery Succeeded (Status: {q_status})")
    else:
        raise Exception(f"Query failed with q_status: {q_status}") 

    # --------------------------------------------------------------------------
    # Get the S3 bucket and key from the output location
    # Locate the file within s3
    print(f"\n4. Downloading via {download_via}")
    print(f"Bucket: {bucket}")
    print(f"Folder: {folder}")          

    summaries_folder = f"summaries/{state}/hourly"
    print(f"Summaries folder: {summaries_folder}")          
    
    if download_via == "paginator":
        # List all objects in the s3_output_location
        print(f"Downloading files via paginator")
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=summaries_folder)
        print(f"Pages: {pages}")    

        # Download each file found
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    obj_key = obj["Key"]
                    # Skip metadata files
                    excluded_endings = [".metadata", ".csv"]                        
                    if not any(obj_key.endswith(ending) for ending in excluded_endings):
                        print(f"Downloading {obj_key} to {local_parquet_path}")
                        s3_client.download_file(
                            Bucket=bucket, Key=obj_key, Filename=local_parquet_path
                        )
    
    elif download_via == "direct_to_duckdb":
        print(f"Downloading files directly to DuckDB")
        conn = duckdb.connect(duckdb_path)
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        
        # Set your AWS credentials if needed
        conn.execute(f"SET s3_access_key_id='{session.get_credentials().access_key}'")
        conn.execute(f"SET s3_secret_access_key='{session.get_credentials().secret_key}'")
        conn.execute(f"SET s3_region='{session.region_name}'")
        
        conn.execute(f"""
            CREATE TABLE {hourly_table} AS
            SELECT * FROM read_parquet('s3://dummy-123-target/summaries/{state}/hourly/*')
            """)
        
    else:
        raise ValueError(f"Invalid download method: '{download_via}'. Should be ['paginator', 'direct_to_duckdb']")
    # --------------------------------------------------------------------------

   # --------------------------------------------------------------------------
    # Optional: Clean up S3 results   
    if drop_table_after_query:  
        print(f"\n5. Dropping the table from the database")
        drop_query = f"DROP TABLE IF EXISTS {hourly_table}"
        print(f"Dropping table: {drop_query}")  
        drop_response = athena_client.start_query_execution(
            QueryString=drop_query,
            QueryExecutionContext={"Database": aws_database},
            ResultConfiguration={"OutputLocation": s3_output_location},
        )

        while True:
            drop_status = athena_client.get_query_execution(
                QueryExecutionId=drop_response["QueryExecutionId"]
            )
            drop_state = drop_status["QueryExecution"]["Status"]["State"]

            if drop_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            time.sleep(1)

        # Delete all objects in the temp folder
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=p2temp)

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
    else:
        print(f"\n4. Skipping dropping the table from the database")


###############################################################################
def main():

    parser = argparse.ArgumentParser(description="Query ResStock data from Athena")

    # Saved for later...passing particular bldg_ids
    # parser.add_argument('--building-ids', type=int, nargs='+', required=True,
    #                   help='List of building IDs to query')

    parser.add_argument(
        "--parquet_path",
        type=str,
        default="../data/resstock/temp",
        help="Path to save the query results locally (default: '../data/resstock/temp')",
    )
    parser.add_argument(
        "--duckdb_path",
        type=str,
        default="../data/resstock/temp/resstock_temp.db",
        help="Path to save the query results locally (default: '../data/resstock/temp/resstock_temp.db')",
    )
    parser.add_argument(
        "--state", type=str, default="MA", help="State to query (default: MA)"
    )
    parser.add_argument(
        "--upgrade", type=int, nargs="+", default=[0], help="Upgrade scenario (default: '0')"
    )
    parser.add_argument(
        "--columns",
        type=str,
        default=None,
        help="Path to a JSON which is simply a list of columns to query",
    )
    parser.add_argument(
        "--agg_frequency",
        type=str,
        default="month",
        help="Aggregation frequency (default: month)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=True,
        help="Print verbose output including the generated query",
    )

    args = parser.parse_args()
    
    print(args.upgrade)

    # ---------------------------------------------------
    # Executing the 2-Step Query

    # Step 1: Intermediate table
    print("\n--------------------------------")
    print("Building intermediate table")
    print("--------------------------------")
    table_name = build_intermediate_table(
        state=args.state.upper(),
        upgrade=args.upgrade,
        s3_client=s3_client,
        athena_client=athena_client,
        aws_database=aws_database,
        s3_output_location=f"s3://{bucket}/{folder}/temp",
        p2columns=args.columns,
    )

    print(f"\nDone with intermediate table: {table_name}")
    print("--------------------------------\n")

    # Step 2: Generate the query string
    if args.agg_frequency == "month":
        print("\n--------------------------------")
        print("Running monthly query")
        print("--------------------------------")
        monthly_query(
            intermediate_table=table_name,
            state=args.state.upper(),
            upgrade=args.upgrade,
            s3_client=s3_client,
            athena_client=athena_client,
            aws_database=aws_database,
            s3_output_location=f"s3://{bucket}/{folder}/temp",
            local_parquet_path=args.parquet_path,
            p2columns=args.columns,
        )
    elif args.agg_frequency == "hour":
        print("\n--------------------------------")
        print("Running hourly query")
        print("--------------------------------")
        hourly_query(
            intermediate_table=table_name,
            state=args.state.upper(),
            upgrade=args.upgrade,
            s3_client=s3_client,
            athena_client=athena_client,
            aws_database=aws_database,
            s3_output_location=f"s3://{bucket}/{folder}/temp",
            local_parquet_path=args.parquet_path,
            duckdb_path=args.duckdb_path,
            p2columns=args.columns,
        )
        print("\nDone with hourly query")
        print("--------------------------------")
    else:
        raise ValueError(f"Invalid aggregation frequency: '{args.agg_frequency}'. Should be ['month', 'hour']")

if __name__ == "__main__":
    main()
