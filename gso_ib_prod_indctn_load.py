"""Glue Script to Load Data into Salesforce Object from GSO_INDCTN_STG staging table
#<mm/dd/yyyy><author><modification-details>:13-09-2024/karthik/v0
"""

import sys
import math
from io import StringIO
import json
import logging
from datetime import datetime

import boto3
import psycopg2
from pyspark.sql.types import (                 # pylint: disable=import-error
    StructType, StructField, StringType,
    IntegerType, LongType, ShortType,
    DecimalType, FloatType, DoubleType,
    BooleanType, BinaryType, DateType,
    TimestampType)
from pyspark.sql.functions import  when, lit, col, to_date,substring  # pylint: disable=import-error
from pyspark.context import SparkContext        # pylint: disable=import-error
from awsglue.utils import getResolvedOptions    # pylint: disable=import-error
from awsglue.context import GlueContext         # pylint: disable=import-error
from awsglue.job import Job                     # pylint: disable=import-error
from botocore.exceptions import ClientError
from psycopg2 import extras
from pyspark.sql.functions import regexp_replace

args = getResolvedOptions(sys.argv, 
                ['JOB_NAME',
                'host',
                'db',
                'port',
                'username',
                'password',
                'job_name',
                'batch_name',
                'region_name',
                'param_name',
                'common_param_name',
                'abc_fetch_param_function',
                'abc_log_stats_func',
                'rds_schema_name',
                'raw_bucket',
                'sql_key_gso_ib_prod_indctn_load',
                'target_folder',
                'archive_folder',
                'flow_name',
                'logging_level'])

host = args["host"]
db = args["db"]
port = args["port"]
username = args["username"]
password = args["password"]
rdsschemaname = args["rds_schema_name"]
job_name = args["job_name"]
batch_name = args["batch_name"]
region_name = args["region_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
rawbucket = args["raw_bucket"]
level = args['logging_level']
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Configure the logger
logging.basicConfig(
    level=level,
    format='%(asctime)s: %(funcName)s: line %(lineno)d: %(levelname)s: %(message)s',
    datefmt=TIME_FORMAT
)
logger = logging.getLogger(__name__)

logger.info("Starting gso_ib_prod_indctn_load Glue job")

ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")


try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e:
    # Handle potential errors during retrieval
    logger.error('Error retrieving secret from Secrets Manager: %s', e)
    raise e

s3_client = boto3.client("s3")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


def connect_db():
    """Creates db connection for the object
    Parameter:
        db_details (dict): Database details
    """
    try:
        conn = psycopg2.connect(database = db,
                            user = username,
                            host= host,
                            password = password,
                            port = port)
        cur = conn.cursor(
            cursor_factory=extras.RealDictCursor)
        return conn, cur
    except Exception as error:
        logger.error("error in connnect_db %s:", error)
        raise error

def execute(conn, cur, query):
    """Execute sql query
    Parameter:
        conn : Database connection object
        cur : cursor object
        query : The query to be processed
    """
    try:
        cur.execute(query)
        conn.commit()

        if query.strip().lower().startswith("select"):
            records = cur.fetchall()
            output = json.loads(json.dumps(records))
            return output

        elif query.strip().lower().startswith("update") or \
                    query.strip().lower().startswith("delete"):
            return cur.rowcount

        else:
            return "Query executed successfully."

    except psycopg2.Error as e:
        logger.error("Error: %s", e)
        conn.rollback()
        return f"Error: {e}"

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error: %s", e)
        conn.rollback()
        return f"Unknown Error: {e}"

def abc_batch_management(job_name):
    """
    This fucntion is used to get Process ID and Execution ID by invoking ABC lambda
    Args:
        job_name : Name of the job
    """
    lambda_client= boto3.client("lambda",region_name="us-east-2")
    abc_batch_execution = {"operation_type": "insert", \
                           "status": "R",  \
                           "batch_name": "edb_cms_{}".format(job_name)}
    invoke_response_status = lambda_client.invoke(FunctionName="edb_abc_log_batch_status", \
                                Payload = json.dumps(abc_batch_execution).encode("utf-8"))
    batch_execution_status_id = json.loads(invoke_response_status["Payload"].read())
    abc_log_process_status = {"operation_type": "insert", \
                "status": "R", \
                "batch_execution_id": batch_execution_status_id.get("batch_execution_id"), \
                "job_name": job_name }
    invoke_response_status = lambda_client.invoke(
                FunctionName="edb_abc_log_process_status", \
                Payload = json.dumps(abc_log_process_status).encode("utf-8"))
    log_process_status_id = json.loads(invoke_response_status["Payload"].read())
    final_json ={'batch_execution_id': batch_execution_status_id.get("batch_execution_id"),
                'process_execution_id':log_process_status_id.get("process_execution_id")}
    return final_json

def init_config(config_common):
    """
    Initialize config and glue job
    Args:
        config_common : Common configuration parameters
    """
    try:
        ex_ids=abc_batch_management(job_name)
        batch_execution_id=ex_ids['batch_execution_id']
        process_execution_id=ex_ids['process_execution_id']
        json_payload = {
            "batch_name": args.get("batch_name"),
            "job_name": args.get("job_name")}
        config_dct = invoke_lambda(args['abc_fetch_param_function'], json_payload)
        # # Read common config file applicable for all tables
        config_common = config_dct
        config_common["job_name"] = job_name
        config_common["batch_name"] = batch_name
        config_common["process_execution_id"] = process_execution_id
        # config_common["snow_func"] = args['snow_func']
        config_common["abc_log_stats_func"] = abc_log_stats_func
        config_common["batch_execution_id"] = batch_execution_id
        return config_common
    except Exception as exep_func:
        logger.error("Error in initialising config: %s", exep_func)
        raise exep_func

def invoke_lambda(func_name,payload,region_name='us-east-2'):
    """
    Function to invoke Lambda
    Args:
        func_name : Name of the lambda function
        payload : Payload to be passed to the lambda function
        region_name : Region where the lambda function is present
    """
    lambda_client = boto3.client('lambda', region_name=region_name)
    invoke_response = lambda_client.invoke(
        FunctionName=func_name,
        Payload=json.dumps(payload))
    config_obj = json.loads(invoke_response['Payload'].read())
    return config_obj


def querydb(curr, query):
    '''
    this function fetches data from s3/pgadmin and store that as dataframe
    Args:
        query : query to fetch data from pgadmin
        curr : cursor object
    '''
    try:
        conn,curr = connect_db()
        curr.execute(query)
        # Fetch all rows from the executed query
        data = curr.fetchall()
        # Get column names and data types from the cursor description
        columns = [desc[0] for desc in curr.description]
        col_types = [desc[1] for desc in curr.description]
        # Define a basic type mapping with date types
        type_mapping = {
            1082: DateType(),          # PostgreSQL DATE type
            1114: TimestampType(),     # PostgreSQL TIMESTAMP WITHOUT TIME ZONE type
            1184: TimestampType(),     # PostgreSQL TIMESTAMP WITH TIME ZONE type
            23: IntegerType(),         # PostgreSQL INTEGER type (int4)
            20: LongType(),            # PostgreSQL BIGINT type (int8)
            21: ShortType(),           # PostgreSQL SMALLINT type (int2)
            1700: DecimalType(),       # PostgreSQL NUMERIC type
            700: FloatType(),          # PostgreSQL REAL type (float4)
            701: DoubleType(),         # PostgreSQL DOUBLE PRECISION type (float8)
            16: BooleanType(),         # PostgreSQL BOOLEAN type (bool)
            25: StringType(),          # PostgreSQL TEXT type
            1043: StringType(),        # PostgreSQL VARCHAR type
            17: BinaryType(),          # PostgreSQL BYTEA type
        }
        # Define the schema, using StringType as the default
        schema = StructType([
            StructField(col, type_mapping.get(col_type, StringType()), True)
            for col, col_type in zip(columns, col_types)
        ])
        if data:
            # Convert data to RDD
            rdd = spark.sparkContext.parallelize(data)
            # Create a PySpark DataFrame
            df = spark.createDataFrame(rdd, schema)
            return df
        else:
            # Create an empty DataFrame
            empty_df = spark.createDataFrame([], schema)
            return empty_df
    except Exception as exp:    # pylint: disable=broad-except
        logger.error("Exceptionin sql queries : %s",exp)
    finally:
        curr.close()

#Transformation on the Source and Lookups
def transform_df(df1):
    '''
    this function will transform the df with suitable transformations
    df1 = df got from query_db function
    '''
    try: # Rename and transform columns
        # Rename columns by reversing the transformations
        temp_df = df1.select(col("approval_dt_glbl__c").cast("date").alias("Approval_Dt_GLBL__c"),
                            col("apprvl_sts_glbl__c").cast("string").alias("Approval_Sts_GLBL__c"),
                            col("cease_pblsh_dt_glbl__c").cast("date").alias("Cease_Pblsh_Dt_GLBL__c"),
                            col("country_cd_glbl__c").cast("string").alias("Country_Cd_GLBL__c"),
                            col("crt_dt_glbl__c").cast("date").alias("Crt_Dt_GLBL__c"),
                            col("dialect_nm_glbl__c").cast("string").alias("Dialect_Nm_GLBL__c"),
                            col("end_dt_glbl__c").cast("date").alias("End_Dt_GLBL__c"),
                            col("indctn_desc_glbl__c").cast("string").alias("Indctn_Desc_GLBL__c"),
                            col("indctn_desc_glbl__c").cast("string").alias("Name"),
                            col("indctn_ext_id_glbl__c").cast("string").alias("Indctn_External_Id_GLBL__c"),
                            col("indctn_id_glbl__c").cast("string").alias("Indctn_Id_GLBL__c"),
                            col("name__c").cast("string").alias("Indctn_Short_Desc_GLBL__c"),
                            col("indctn_sts_glbl__c").cast("string").alias("Indctn_Sts_GLBL__c"),
                            col("indctn_typ_glbl__c").cast("string").alias("Indctn_Typ_GLBL__c"),
                            col("lst_pblsh_dt_glbl__c").cast("date").alias("Lst_Pblsh_Dt_GLBL__c"),
                            col("prod_grp_prnt_id_glbl_sfdc_id").cast("string").alias("Prod_Grp_Prnt_Id_GLBL__c"),
                            col("revoke_dt_glbl__c").cast("date").alias("Revoke_Dt_GLBL__c"),
                            col("start_dt_glbl__c").cast("date").alias("Start_Dt_GLBL__c"),
                            col("trnsltd_indctn_desc_glbl__c").cast("string").alias("Trnsltd_Indctn_Desc_GLBL__c"),
                            col("trnsltd_indctn_sh_desc_glbl__c").cast("string").alias("Trnsltd_Indctn_Short_Desc_GLBL__c"),
                            col("updt_dt_glbl__c").cast("date").alias("Updt_Dt_GLBL__c"))

        # Now use the temporary DataFrame for further processing
        df = temp_df

        # Transformations		# Transformations
        df = df.withColumn("Cease_Pblsh_Dt_GLBL__c",
                    when(
                        col("Cease_Pblsh_Dt_GLBL__c") >= to_date(lit("3999-01-01")),
                        to_date(lit("4000-12-31"))
                    ).otherwise(col("Cease_Pblsh_Dt_GLBL__c"))
                )

        df = df.withColumn("End_Dt_GLBL__c",
                    when(
                        col("End_Dt_GLBL__c") >= to_date(lit("3999-01-01")),
                        to_date(lit("4000-12-31"))
                    ).otherwise(col("End_Dt_GLBL__c"))
                )

        df = df.withColumn("Revoke_Dt_GLBL__c",
                    when(
                        col("Revoke_Dt_GLBL__c") >= to_date(lit("3999-01-01")),
                        to_date(lit("4000-12-31"))
                    ).otherwise(col("Revoke_Dt_GLBL__c"))
                )
        temp_df_string = df.select(*[col(column_name).cast("string")\
                                    .alias(column_name) for column_name in temp_df.columns])
        temp_df_string=fillnull(temp_df_string)
        temp_df_string = temp_df_string.withColumn("Name", substring("Name", 1, 80))
        return temp_df_string

    except Exception as exp:    # pylint: disable=broad-except
        logger.error("Error in the function: %s", exp)
        return None
def fillnull(df):
    columns_to_update_new = [
        "Approval_Dt_GLBL__c",
        "Revoke_Dt_GLBL__c",
        "Start_Dt_GLBL__c",
        "Approval_Sts_GLBL__c",
        "Cease_Pblsh_Dt_GLBL__c"
    ]
    for column in columns_to_update_new:
        df = df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))
        df = df.withColumn(column, when(col(column).isNull(), '#N/A').otherwise(col(column)))
    return df

def move_from_raw_to_archive(bucket_name, directory_path, archive_path):
    """
    Move multiple files from source to archive directory in S3.
    Args:
        bucket_name (str): Name of the S3 bucket.
        directory_path (str): Source directory path.
        archive_path (str): Archive directory path.
    """
    try:
        current_date = datetime.now().date()
        # List all objects in the source directory
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_path)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        
        if not files:
            return False
        # Process each file individually
        for source_key in files:
            # Construct the target key in the archive directory with date prefix
            target_key = source_key.replace(directory_path, f"{archive_path}/{current_date}", 1)

            # Copy the file to the archive location
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=target_key)
            # Delete the original file from the source directory
            s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        return True

    except Exception as exp:
        logger.error("Error in move_from_raw_to_archive: %s", str(exp))
        return False


def store_flat_file_in_s3(df, bucket_name, folder_name):
    """
    Store flat file in S3 target folder. If the file size exceeds `max_file_size_mb`,
    split it into multiple chunks.
    
    Parameters:
        df (DataFrame): Spark DataFrame to upload.
        bucket_name (str): Name of the S3 bucket.
        folder_name (str): Folder path inside the bucket.
        job_name (str): Job name to use for the file naming.
        max_file_size_mb (int): Maximum size for each chunk in MB (default 120MB).
    """
    if df.count() == 0:
        csv_str = df.toPandas().to_csv(index=False)
        # Define S3 bucket and object key
        object_key = f'{folder_name}/{job_name}.csv'
        # Upload CSV string to S3
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_str.encode('utf-8'))
        return
    max_file_size_mb=120
    # Convert Spark DataFrame to Pandas DataFrame
    pdf = df.toPandas()

    # Estimate the size of the DataFrame in memory (in bytes)
    approx_size_bytes = pdf.memory_usage(index=False, deep=True).sum()
    approx_size_mb = approx_size_bytes / (1024 * 1024)
    # Calculate number of chunks if size exceeds the threshold
    if approx_size_mb > max_file_size_mb:
        chunk_size = math.ceil(len(pdf) / math.ceil(approx_size_mb / max_file_size_mb))
    else:
        chunk_size = len(pdf)  # No splitting required

    # Upload in chunks
    for i, chunk in enumerate(range(0, len(pdf), chunk_size)):
        chunk_df = pdf.iloc[chunk:chunk + chunk_size]
        csv_buffer = StringIO()
        chunk_df.to_csv(csv_buffer, index=False)
        object_key = f'{folder_name}/{job_name}_part_{i + 1}.csv'

        # Upload the chunk to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_buffer.getvalue().encode('utf-8')
        )
def invoke_appflow_del(flow_name):
    '''
    this function invoke the delete_function in salesforce using appflow
    flow_name = appflow_name
    '''
    try:
        # Initialize the AppFlow client
        appflow_client = boto3.client('appflow')

        # Trigger AppFlow execution
        response = appflow_client.start_flow(
            flowName=f'{flow_name}'
                )

    except Exception as e:
        logger.error("error in invoking appflow: %s", e)
        raise e

def get_query_from_s3(file_key):
    """
    This Function is to get query from sql file in s3 bucket
    Args:
        file_key : key of the file in s3 bucket
    """
    response = s3_client.get_object(Bucket=rawbucket, Key=file_key)
    sql_script = response['Body'].read().decode('utf-8')

    # Split the SQL script into individual queries
    queries = [query.strip() for query in sql_script.split(';') if query.strip()]
    return queries[0]

def main():
    """
    this function do the task in sequence of functions mentioned below
    """
    try:
        target_folder=args['target_folder']
        archive_folder=args['archive_folder']
        flow_name=args['flow_name']
        # abc_batch_management(job_name)
        conn, curr = connect_db()
        sql_key_gso_ib_prod_indctn_load= args['sql_key_gso_ib_prod_indctn_load']
        bucket_name=rawbucket
        move_from_raw_to_archive(bucket_name,target_folder,archive_folder)
        # get query to a variable from s3 bucket
        query=get_query_from_s3(sql_key_gso_ib_prod_indctn_load)
        df=querydb(curr,query)
        # df.show()
        df1=transform_df(df)
        # df1.show()
        store_flat_file_in_s3(df1,bucket_name,target_folder)
        #Invoke Appflow
        invoke_appflow_del(flow_name)
    except Exception as exp:
        logger.error("Error in main function: %s", exp)
        raise exp
    finally:
        spark.stop()
main()
job.commit()
