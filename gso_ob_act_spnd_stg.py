"""Glue Script to load GSO_OB_ACT_SPND_STG table  from Json file in S3 to RDS
#<mm/dd/yyyy><author><modification-details>:11-09-2024/Helen monisha/v0
"""
import sys
import json
import logging
from datetime import datetime
import time
import pytz # pylint: disable=import-error
import boto3 # pylint: disable=import-error
import psycopg2 # pylint: disable=import-error
from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error
from pyspark.context import SparkContext # pylint: disable=import-error
from pyspark.sql.types import StructType # pylint: disable=import-error
from pyspark.sql.functions import col, to_date, lit # pylint: disable=import-error
from botocore.exceptions import ClientError # pylint: disable=import-error
from psycopg2 import extras # pylint: disable=import-error


appflow_client = boto3.client('appflow')
lambda_client = boto3.client("lambda", region_name="us-east-2")
secret_manager = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


args = getResolvedOptions(
    sys.argv,
    [
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
        'hist_t_name',
        'tgt_table',
        'archive_path',
        'logging_level',
        'load_metadata_file_path'])


host = args["host"]
db = args["db"]
port = args["port"]
username = args["username"]
password = args["password"]
rdsschemaname = args["rds_schema_name"]
batch_name = args["batch_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
rawbucket = args["raw_bucket"]
level = args['logging_level']
DATEFMT = "%Y-%m-%dT%H:%M:%S"
URL = f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require"


# Configure the logger
logging.basicConfig(
    level=level,
    format='%(asctime)s: %(funcName)s: line %(lineno)d: %(levelname)s: %(message)s',
    datefmt=DATEFMT
)
logger = logging.getLogger(__name__)

logger.info("Starting gso_ob_act_spnd_stg Glue job")


try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e: # pylint: disable=broad-except
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
    except psycopg2.Error as error:
        logger.error("Error in connect_db: %s", str(error))
        return None,None


def abc_batch_management(job_name):
    """Get Process ID and Execution ID by invoking ABC lambda"""
    abc_batch_execution = {"operation_type": "insert","status": "R",  "batch_name": f"{batch_name}"}
    invoke_response_status = lambda_client.invoke(
    FunctionName="edb_abc_log_batch_status",
    Payload=json.dumps(abc_batch_execution).encode("utf-8"))

    batch_execution_status_id = json.loads(invoke_response_status["Payload"].read())
    logger.info("batch_execution_id: %s", batch_execution_status_id.get('batch_execution_id'))
    abc_log_process_status = {
    "operation_type": "insert",
    "status": "R",
    "batch_execution_id": batch_execution_status_id.get("batch_execution_id"),
    "job_name": job_name}

    invoke_response_status = lambda_client.invoke(
    FunctionName="edb_abc_log_process_status",
    Payload=json.dumps(abc_log_process_status).encode("utf-8"))

    log_process_status_id = json.loads(invoke_response_status["Payload"].read())
    logger.info("process_execution_id: %s", log_process_status_id.get('process_execution_id'))
    final_json ={
        'batch_execution_id': batch_execution_status_id.get("batch_execution_id"),
        'process_execution_id':log_process_status_id.get("process_execution_id")}

    return final_json


def init_config(config_common):
    """
    Initialize config and glue job
    """
    try:
        job_name = args["job_name"]
        ex_ids = abc_batch_management(job_name)
        batch_execution_id = ex_ids['batch_execution_id']
        process_execution_id = ex_ids['process_execution_id']
        logger.info("process_id: %s, batch_id: %s", process_execution_id, batch_execution_id)

        json_payload = {
            "batch_name": args.get("batch_name"),
            "job_name": args.get("job_name")}

        config_dct = invoke_lambda(args['abc_fetch_param_function'], json_payload)
        config_common = config_dct
        config_common["job_name"] = job_name
        config_common["batch_name"] = batch_name
        config_common["process_execution_id"] = process_execution_id
        config_common["abc_log_stats_func"] = abc_log_stats_func
        config_common["batch_execution_id"] = batch_execution_id
        return config_common
    except Exception as exep_func:
        logger.error("Error in initializing config: %s", str(exep_func))
        raise exep_func


def invoke_lambda(func_name, payload, region_name=args["region_name"]):
    """
    Function to invoke Lambda
    """
    lambda_client = boto3.client('lambda', region_name=region_name) # pylint: disable=redefined-outer-name
    invoke_response = lambda_client.invoke(
        FunctionName=func_name,
        Payload=json.dumps(payload))
    config_obj = json.loads(invoke_response['Payload'].read())
    return config_obj


def ingest_data(df,tname,truncate,mode):
    """This Function ingestion data to postgresdb"""
    try:
        load_table_name = f'{rdsschemaname}.{tname}'
        df.write.format("jdbc").options(url=URL, \
                                            user=username,\
                                            dbtable=load_table_name,\
                                            password=password,\
                                            truncate=truncate) \
                                            .mode(mode).save()
        return True
    except Exception as e:
        ex = str(e).replace('"','').replace("'","")
        logger.error("Error occured in ingest_data function: %s", ex)
        raise e


def get_data_from_s3(bucket_name , file_key):
    """
    Read the Data in JSON file in S3 Bucket and convert it into pySpark DataFrame
    """
    try:
        # Get JSON object from S3
        # Retrieve JSON data from S3
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_key)
        files = [content['Key'] for content in response.get('Contents', [])]
        # Filter to get only files (not folders)
        files = [f for f in files if not f.endswith('/')]
        logger.info("Files found: %s", files)
        # Iterate through the files and read them
        for file in files:
            s3_path = f's3://{bucket_name}/{file}'
            logger.info("Reading file: %s", s3_path)
            # Read JSON file
            df = spark.read.format("json").load(s3_path)
        return df
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s", str(exp))
        # Define an empty schema
        schema = StructType([])
        # Create an empty DataFrame
        empty_df = spark.createDataFrame([], schema)
        return empty_df


# pylint: disable=too-many-arguments, too-many-positional-arguments
def transform(df,cntry_cd,merc_value,prev_load_date,current_datetime_str,process_execution_id):
    """
    DataFrame transformation to get the desired Structure and fields
    """
    df = df.withColumn("DATE_OF_EVENT_MERC__C", to_date(col("Date_of_Event_MERC__c"), "yyyy-MM-dd"))

    filtered_df = df.filter(
        (col('Participant_ToV_Updated_Date_MERC__c') > prev_load_date) &
        (col('Participant_ToV_Updated_Date_MERC__c') <= current_datetime_str) &
        (col('Participant_ToV_Final_MERC__c') == merc_value) &
        ((~col('Event_Country_MERC__c').isin(*cntry_cd)) &
        (col('Event_Country_MERC__c').isNotNull()))
        )

    # Select and rename specified columns
    df_final = filtered_df.select(
        col("Event_Id_MERC__c").alias("EVENT_ID_MERC__C"),
        col("Event_Country_MERC__c").alias("EVENT_COUNTRY_MERC__C"),
        col("DATE_OF_EVENT_MERC__C"),
        col("Owner_Master_Id_MERC__c").alias("OWNER_MASTER_ID_MERC__C"),
        col("Name").alias("NAME")
    )

    current_time = datetime.now()
    result_df = df_final.withColumn("is_active",lit("Y")) \
            .withColumn("updated_ts",lit(current_time)) \
            .withColumn("created_ts",lit(current_time)) \
            .withColumn("created_by_job_exec_id",lit(process_execution_id).cast('bigint')) \
            .withColumn("updated_by_job_exec_id",lit(process_execution_id).cast('bigint'))
    return result_df


def move_from_raw_to_archieve(bucket_name,directory_path,archive_path):
    """
    Move the file from source to Archive
    """
    try:
        logger.info('init move_from raw to archive')
        source = f'{directory_path}'
        target = f'{archive_path}/'
        source_key = ''
        target_key = ''
        copy_source = ''
        current_date = datetime.now().date()
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=source)
        files = []
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
                logger.info("Files moved successfully.")
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                logger.info("Files deleted successfully.")
                logger.info('exit move_from raw to archive')
        return True
    except Exception as exp: # pylint: disable=broad-except
        logger.error("move_from_s3_to_s3 exception :: %s", str(exp))
        return False

tz = pytz.timezone('UTC')
glue_start_time = datetime.now(tz)
def start_appflow(flow_name):
    '''
    run appflow
    '''
    response = appflow_client.start_flow(
        flowName=flow_name
    )
    execution_id = response['executionId']
    return execution_id

def check_appflow_status(flow_name, execution_id):
    '''
    check appflow status
    '''
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
        if status == 'Successful' and timestmp > glue_start_time: # pylint: disable=no-else-break
            break
        elif status == 'Error' and timestmp > glue_start_time:
            raise Exception(f"AppFlow execution failed with ID: {execution_id}") # pylint: disable=broad-exception-raised
        else:
            time.sleep(2)  # Check every 2 seconds


def get_load_metadata(s3_path, load_param, bucket_name):
    "This function is used to get current load parameter"
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
        load_metadata = json.loads(response['Body'].read().decode('utf-8'))
        metadata_load_value = load_metadata["metadata_load_value"]
        logger.info("metadata_load_value:: %s", metadata_load_value)
        return metadata_load_value
    except ClientError as err:
        logger.error("Error in function get_load_metadata:: %s",str(err))
        if err.response['Error']['Code'] == 'NoSuchKey':
            metadata_load_value = load_param
            logger.info("metadata_load_value:: %s", metadata_load_value)
            return metadata_load_value
        raise err


def write_metadata_file(s3_path, load_param, bucket_name):
    "This function is used to write metadata file in s3"
    try:
        data = json.dumps({"metadata_load_value": f"{load_param}"})
        s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=data)
    except ClientError as err:
        logger.error("Error in function write_metadata_file:: %s", str(err))
        raise err


def rollback_database_table_process(schema, tb_name, process_id, cursor, conn):
    """
    Rollback from Refined Db if load is unsuccessful
    """
    try:
        delete_query = f"DELETE FROM {schema}.{tb_name} \
        WHERE created_by_job_exec_id = {process_id}"
        cursor.execute(delete_query)
        conn.commit()
    except KeyError as ex:
        logger.error("Rollback failed due to Error %s", str(ex))
        raise ex


def load_history_table(conn,cursor,tgt_table,stg_table,exec_id):
    """Function to load the history table"""
    updt_query = """UPDATE <schema>.<tgt_table> a SET is_active= 'N',
                    updated_ts = current_timestamp,
                    updated_by_job_exec_id = <process_execution_id>
                    where event_id_merc__c in 
                    (select a.event_id_merc__c from <schema>.<tgt_table> a 
                    inner join <schema>.<stg_table> b 
                    on a.event_id_merc__c = b.event_id_merc__c) 
                    and a.is_active = 'Y'"""

    insert_query = """INSERT INTO <schema>.<tgt_table>
                    (event_id_merc__c,event_country_merc__c,date_of_event_merc__c,owner_master_id_merc__c,name,is_active,created_ts,updated_ts,updated_by_job_exec_id,created_by_job_exec_id)
                    (select event_id_merc__c,event_country_merc__c,date_of_event_merc__c,owner_master_id_merc__c,name,'Y'::varchar AS is_active,current_timestamp::timestamp as created_ts,current_timestamp::timestamp as updated_ts,<process_execution_id> as created_by_job_exec_id,<process_execution_id> as updated_by_job_exec_id 
                    FROM <schema>.<stg_table>);"""

    try:
        updt_query = updt_query.replace('<schema>',rdsschemaname) \
                                .replace('<tgt_table>',tgt_table) \
                                .replace('<stg_table>',stg_table) \
                                .replace('<process_execution_id>',str(exec_id))
        cursor.execute(updt_query)
        conn.commit()

        insert_query = insert_query.replace('<process_execution_id>',str(exec_id)) \
                                .replace('<schema>',rdsschemaname) \
                                .replace('<tgt_table>',tgt_table) \
                                .replace('<stg_table>', stg_table)
        cursor.execute(insert_query)
        conn.commit()
    except ValueError as exp_msg:
        logger.error("Error while writing data %s", str(exp_msg))
        rollback_database_table_process(rdsschemaname, tgt_table,
                                        exec_id, cursor, conn)
        raise exp_msg
    finally:
        if cursor is not None:
            cursor.close()


# pylint: disable=too-many-locals
def main():
    """
    The main entry point for the Glue job.
    """
    try:
        # current_datetime_str = datetime.now().strftime("%Y-%m-%dT00:00:00.000Z")
        current_datetime_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        logger.info("Current datetime: %s", current_datetime_str)
        flow_name = args['json_file_location'].split('/')[-1]
        execution_id = start_appflow(flow_name)
        check_appflow_status(flow_name, execution_id)
        config_common = init_config({})
        logger.info("The config_common values in main are: %s", config_common)
        file_key = args['json_file_location']
        logger.info("file_key: %s", file_key)
        process_execution_id = config_common["process_execution_id"]

        # Extract the directory path
        directory_path = file_key
        archive_path = args['archive_path']
        load_metadata_file_path = args["load_metadata_file_path"]
        bucket_name = rawbucket
        logger.info("bucket_name: %s", bucket_name)


        # To get prev_load_date
        load_param = str(config_common['gso_ob_act_spnd_stg_batch_params']['PREV_LOAD_DATE'])
        logger.info("load_param %s", load_param)
        prev_load_date = get_load_metadata(load_metadata_file_path, load_param, bucket_name)


        cntry_cd = config_common['gso_ob_act_spnd_stg_batch_params']['CNTRY_CD']
        logger.info("cntry_cd %s", cntry_cd)
        merc_value = config_common['gso_ob_act_spnd_stg_batch_params']['MERC_VALUE']
        logger.info("merc_value %s", merc_value)

        df = get_data_from_s3(bucket_name, file_key)

        tgt_df = transform(df, cntry_cd, merc_value,
                        prev_load_date, current_datetime_str,
                        process_execution_id)
        tgt_df = tgt_df.dropDuplicates()

        ingest_data(tgt_df, args['tgt_table'], True, 'overwrite')
        conn,curr = connect_db()
        load_history_table(conn, curr, args['hist_t_name'],
                    args['tgt_table'], process_execution_id)
        move_from_raw_to_archieve(bucket_name, directory_path, archive_path)

        # To write metadata file in s3
        write_metadata_file(load_metadata_file_path, current_datetime_str, bucket_name)

    except Exception as exp:
        logger.error("Error in main function: %s", str(exp))
        raise exp
    finally:
        logger.info("gso_ob_act_spnd_stg Glue job completed")
        conn.close()
        curr.close()


main()
job.commit()
