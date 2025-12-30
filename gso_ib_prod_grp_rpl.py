"""Glue Script to load GSO_PROD_GRP_RPLCTN table in RDS from Json file in S3
#<mm/dd/yyyy><author><modification-details>:09-11-2024/Saurav Pournami Nair/v0
"""
import sys
import json
import logging
import time
from datetime import datetime
import pytz
import boto3
import psycopg2
from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error
from pyspark.sql.types import StructType
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from psycopg2 import extras
from botocore.exceptions import ClientError


s3_client = boto3.client("s3")
appflow_client = boto3.client('appflow')
lambda_client = boto3.client("lambda", region_name="us-east-2")
secret_manager = boto3.client("secretsmanager")

# Get the current time in UTC
tz = pytz.timezone('UTC')
glue_start_time = datetime.now(tz)

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
                           'json_file_location',
                           "tgt_name",
                           "archive_path",
                           "logging_level"])

# Job Arguments
host = args["host"]
db = args["db"]
port = args["port"]
username = args["username"]
rds_schema_name = args["rds_schema_name"]
batch_name = args["batch_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
level = args['logging_level']
DATEFMT = "%Y-%m-%dT%H:%M:%S"

# Configure the logger
logging.basicConfig(
    level=level,
    format='%(asctime)s: %(funcName)s: line %(lineno)d: %(levelname)s: %(message)s',
    datefmt=DATEFMT
)
logger = logging.getLogger(__name__)

logger.info("Starting gso_ib_prod_grp_rpl Glue job")

# Get the password from Secrets Manager
try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e:
  # Handle potential errors during retrieval
    raise e


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def connect_db():
    """Creates DB connection for the object
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
    except Exception as error: # pylint: disable=broad-except
        logger.error("error in connnect_db: %s",str(error))
        return None


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

        if query.strip().lower().startswith("update") \
            or query.strip().lower().startswith("delete"):
            return cur.rowcount

        return "Query executed successfully."
    except psycopg2.Error as e:
        logger.error("Error: %s",str(e))
        conn.rollback()
        return f"Error: {e}"
    except Exception as e: # pylint: disable=broad-except
        logger.error("Error: %s",str(e))
        conn.rollback()
        return f"Unknown Error: {e}"


def abc_batch_management(job_name):
    """This function is used to get abc details
    Args:
        job_name : Name of the job
    """
    abc_batch_execution = {"operation_type": "insert","status": "R",  "batch_name": f"{batch_name}"}
    invoke_response_status = lambda_client.invoke(
        FunctionName="edb_abc_log_batch_status",
        Payload = json.dumps(abc_batch_execution).encode("utf-8"))

    batch_execution_status_id = json.loads(invoke_response_status["Payload"].read())
    abc_log_process_status = {"operation_type": "insert",
                              "status": "R", 
                              "batch_execution_id": batch_execution_status_id.get("batch_execution_id"), # pylint: disable=line-too-long
                              "job_name": job_name}

    invoke_response_status = lambda_client.invoke(
       FunctionName="edb_abc_log_process_status",
        Payload = json.dumps(abc_log_process_status).encode("utf-8"))

    log_process_status_id = json.loads(invoke_response_status["Payload"].read())
    final_json ={
        'batch_execution_id': batch_execution_status_id.get("batch_execution_id"),
        'process_execution_id':log_process_status_id.get("process_execution_id")
    }
    return final_json


def init_config(config_common):
    """
    Initialize config and glue job
    Args:
        config_common : Common config object
    """
    try:
        job_name = args["job_name"]
        ex_ids = abc_batch_management(job_name)
        batch_execution_id = ex_ids['batch_execution_id']
        process_execution_id = ex_ids['process_execution_id']
        json_payload = {
            "batch_name": args.get("batch_name"),
            "job_name": args.get("job_name")}
        config_dct = invoke_lambda(args['abc_fetch_param_function'], json_payload)

        # Read common config file applicable for all tables
        config_common = config_dct
        config_common["process_execution_id"] = process_execution_id
        config_common["batch_execution_id"] = batch_execution_id
        return config_common
    except Exception as exep_func:
        logger.error("Error in initialising config: %s", str(exep_func))
        raise exep_func


def invoke_lambda(func_name,payload):
    """
    Function to invoke Lambda
    Args:
        func_name : Name of the Lambda function
        payload : Payload to be passed to the Lambda function
    """
    invoke_response = lambda_client.invoke(
        FunctionName=func_name,
        Payload=json.dumps(payload))
    config_obj = json.loads(invoke_response['Payload'].read())
    return config_obj


#Ingesting data by Truncate and Load
def ingest_data_trunc_load(df, table_name):
    """This Function ingestion data to postgresdb by Truncate and load method
    Args:
        df : DataFrame to be ingested
        table_name : Name of the table to be ingested
    """
    options = {
            "url": "jdbc:postgresql://" + host + ":" + port + "/" + db,
            "user": username,
            "password": password,
            "dbtable": rds_schema_name + "."+ table_name,
            "driver": "org.postgresql.Driver"
        }
    connection_properties= {
            "user": username,
            "password": password,
            "driver": "org.postgresql.Driver"
            }

    conn, curr = None, None
    try:
        conn, curr = connect_db()

        #check if table exists
        query = f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=lower('{table_name}'))" # pylint: disable=line-too-long

        output = execute(conn, curr, query)
        table_exists = output[0]['exists']
        if table_exists:
            query = f'TRUNCATE TABLE {rds_schema_name}.{table_name}'
            output = execute(conn, curr, query)
            load_table_name = f'{rds_schema_name}.{table_name}'

            df.write \
                .jdbc(url=options["url"],
                      table=load_table_name,
                      mode="append",
                      properties=connection_properties)

        else:
            df.write \
                .jdbc(url=options["url"],
                      table=f'{rds_schema_name}.{table_name}',
                      mode="overwrite",
                      properties=connection_properties)
        return True
    except Exception as e:
        ex=str(e).replace('"','').replace("'","")
        logger.error("Error %s",str(ex))
        raise e
    finally:
        if conn:
            curr.close()
            conn.close()


def get_data_from_s3(bucket_name , file_key):
    """
    Read the Data in JSON file in S3 Bucket and convert it into pySpark DataFrame
    Args:
        bucket_name : S3 Bucket Name
        file_key : S3 File Key
    """
    try:
        # Retrieve JSON data from S3
        file_key = f"{file_key}/"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_key)
        files = [content['Key'] for content in response.get('Contents', [])]

        # Filter to get only files (not folders)
        files = [f for f in files if not f.endswith('/')]

        # Iterate through the files and read them
        for file in files:
            s3_path = f's3://{bucket_name}/{file}'
            # Read JSON file
            df = spark.read.format("json").load(s3_path)
        return df
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s",str(exp))
        # Define an empty schema
        schema = StructType([])
        # Create an empty DataFrame
        empty_df = spark.createDataFrame([], schema)
        return empty_df


def transform(df):
    """
    DataFrame transformation to get the desired Structure and fields
    Args:
        df : DataFrame to be transformed
    """
    # Select, transform and rename specified columns
    df_transformed = df.withColumn("PROD_GRP_ID_GLBL__C",
                                   df["Product_Group_Id_GLBL__c"].cast("double"))
    df_final = df_transformed.select(
        col("Id").alias("SFDC_ID"),
        col("Product_Group_External_ID_GLBL__c").alias("PROD_GRP_EXTERNAL_ID_GLBL__C"),
        col("Cntry_Cd_GLBL__c").alias("CNTRY_CD_GLBL__C"),
        col("PROD_GRP_ID_GLBL__C"))
    return df_final


def move_from_raw_to_archieve(bucket_name,directory_path,archive_path):
    """
    Move the file from source to Archive
    Args:
        bucket_name : S3 Bucket Name
        directory_path : Directory Path
        archive_path : Archive Path
    """
    try:
        source_key = ''
        target_key = ''
        copy_source = ''
        files = []
        source = f'{directory_path}'
        target = f'{archive_path}/'

        current_date = datetime.now().date()
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=source)

        # List objects directly under the specified prefix
        for obj in response.get('Contents', []):
            key = obj['Key']
            # Only add keys that are immediate children of the prefix
            if len(key[len(source):].split('/')) == 2:
                files.append(key)

        for obj in files:
            source_key = obj
            target_key = source_key.replace(source, f"{target}{current_date}", 1)
        if source_key != target_key:
            copy_source = {
               'Bucket': bucket_name,
               'Key': source_key
           }

        s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=target_key)
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        return True
    except Exception as e: # pylint: disable=broad-except
        logger.error("move_from_s3_to_s3 exception :: %s",str(e))
        return False


def start_appflow(flow_name):
    """Start the AppFlow execution
    Args:
        flow_name : Name of the AppFlow
    """
    response = appflow_client.start_flow(
        flowName=flow_name
    )
    execution_id = response['executionId']
    return execution_id


def check_appflow_status(flow_name, execution_id):
    """Check the status of the AppFlow execution
    Args:  
        flow_name : Name of the AppFlow
        execution_id : ID of the AppFlow execution
    """
    while True:
        response = appflow_client.describe_flow(
            flowName=flow_name
        )
        last_run_details = response.get('lastRunExecutionDetails', {})
        status = last_run_details.get('mostRecentExecutionStatus')
        timestmp = last_run_details.get('mostRecentExecutionTime')
        if status is None or timestmp is None:
            time.sleep(2)
            continue
        if status == 'Successful' and timestmp > glue_start_time:
            break
        if status == 'Error' and timestmp > glue_start_time:
            raise Exception(f"AppFlow execution failed with ID: {execution_id}") # pylint: disable=broad-exception-raised

        time.sleep(2)  # Check every 2 seconds


def main():
    """Main Function"""
    try:
        flow_name = args['json_file_location'].split('/')[-1]
        execution_id = start_appflow(flow_name)
        check_appflow_status(flow_name, execution_id)
        raw_bucket = args["raw_bucket"]
        file_key = args['json_file_location']
        tgt_name = args['tgt_name']
        archive_path = args['archive_path']
        config_common = {}
        config_common = init_config(config_common)
        df = get_data_from_s3(raw_bucket, file_key)
        tgt_df = transform(df)
        ingest_data_trunc_load(tgt_df,tgt_name)
        move_from_raw_to_archieve(raw_bucket,file_key,archive_path)
    except Exception as exp:
        logger.error("Error in main function: %s",str(exp))
        raise exp
    finally:
        logger.info("gso_ib_prod_grp_rpl Glue job completed")
        spark.stop()
main()
job.commit()
