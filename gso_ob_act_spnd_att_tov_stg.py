"""Glue Script to load GSO_OB_ACT_SPND_ATT_TOV_STG table from Json file in S3 to RDS
#<mm/dd/yyyy><author><modification-details>:10-01-2024/Nikita Koli/v0
"""
import sys
import json
import logging
from datetime import datetime
import time

import pytz # pylint: disable=import-error
import boto3 # pylint: disable=import-error
import psycopg2 # pylint: disable=import-error
from pyspark.sql.types import StructType # pylint: disable=import-error
from pyspark.context import SparkContext # pylint: disable=import-error
from pyspark.sql.functions import col, lit, when # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error
from botocore.exceptions import ClientError # pylint: disable=import-error
from psycopg2 import extras # pylint: disable=import-error
from pyspark.sql.types import DecimalType


ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


# Extract arguments
args = getResolvedOptions(sys.argv, [
    'host','db','port','username','password','job_name', 'batch_name', 'region_name', 
    'param_name', 'common_param_name', 'abc_fetch_param_function', 'abc_log_stats_func', 
    'rds_schema_name', 'rawbucket', 'json_file_location1',
    'json_file_location2','json_file_location3','tgt_name', 'archive_path1',
    'archive_path2', 'archive_path3', 'logging_level',
    'load_metadata_file_path', 'hist_table'])


# Database and AWS configuration
host = args["host"]
db = args["db"]
port = args["port"]
username = args["username"]
password = args["password"]
rds_schema_name = args["rds_schema_name"]
job_name = args["job_name"]
batch_name = args["batch_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
rawbucket = args["rawbucket"]
tgt_name = args["tgt_name"]
json_file_location1 = args["json_file_location1"]
json_file_location2 = args["json_file_location2"]
json_file_location3 = args["json_file_location3"]
archive_path1=args["archive_path1"]
archive_path2=args["archive_path2"]
archive_path3=args["archive_path3"]
level = args['logging_level']
URL = f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require"


# Configure the logger
logging.basicConfig(
    level=args['logging_level'],
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Log job start
logger.info("Starting AWS Glue job")


try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e:
    # Handle potential errors during retrieval
    raise e

appflow_client = boto3.client('appflow')
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
        logger.error("error in connnect_db: %s",error)
        raise error


def abc_batch_management(job_name1):
    """
    This fucntion is used to get Process ID and Execution ID by invoking ABC lambda
    """
    lambda_client = boto3.client("lambda",region_name=args["region_name"])
    abc_batch_execution = {"operation_type": "insert","status": "R",  "batch_name": f"{batch_name}"}
    invoke_response_status = lambda_client.invoke(FunctionName="edb_abc_log_batch_status",
                                                  Payload = json.dumps(abc_batch_execution)
                                                  .encode("utf-8"))
    batch_execution_status_id = json.loads(invoke_response_status["Payload"].read())
    logger.info(batch_execution_status_id.get("batch_execution_id"))


    abc_log_process_status = {"operation_type": "insert","status": "R",
                               "batch_execution_id": batch_execution_status_id
                               .get("batch_execution_id"), "job_name": job_name1}

    invoke_response_status = lambda_client.invoke(FunctionName="edb_abc_log_process_status",
                                                  Payload = json.dumps(abc_log_process_status)
                                                  .encode("utf-8") )
    log_process_status_id = json.loads(invoke_response_status["Payload"].read())
    logger.info(log_process_status_id.get("process_execution_id"))
    final_json ={
        'batch_execution_id': batch_execution_status_id.get("batch_execution_id"),
        'process_execution_id':log_process_status_id.get("process_execution_id")
    }
    return final_json


def init_config(config_common):
    """
    Initialize config and glue job
    """
    try:
        ex_ids = abc_batch_management(job_name)
        batch_execution_id = ex_ids['batch_execution_id']
        process_execution_id = ex_ids['process_execution_id']

        logger.info("Initializing config")
        logger.info("Process Execution ID: %s, Batch Execution ID: %s",
                     process_execution_id, batch_execution_id)

        json_payload = {
            "batch_name": args.get("batch_name"),
            "job_name": args.get("job_name")
        }

        config_dct = invoke_lambda(args['abc_fetch_param_function'], json_payload)
        logger.info("Config dictionary retrieved: %s", config_dct)

        # Update the common config
        config_common = config_dct
        config_common["job_name"] = job_name
        config_common["batch_name"] = args.get("batch_name")  # Ensure batch_name is
        #retrieved from args
        config_common["process_execution_id"] = process_execution_id
        config_common["abc_log_stats_func"] = abc_log_stats_func
        config_common["batch_execution_id"] = batch_execution_id

        return config_common

    except Exception as exep_func:
        logger.error("Error in initializing config: %s", exep_func, exc_info=True)
        raise exep_func


def invoke_lambda(func_name,payload,region_name=args["region_name"]):
    """
    Function to invoke Lambda
    """
    lambda_client = boto3.client('lambda', region_name=region_name)
    invoke_response = lambda_client.invoke(
        FunctionName=func_name,
        Payload=json.dumps(payload))
    config_obj = json.loads(invoke_response['Payload'].read())
    return config_obj


def ingest_data(df,tname,truncate,mode):
    """This Function ingestion data to postgresdb"""
    try:
        load_table_name = f'{rds_schema_name}.{tname}'
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
        # Iterate through the files and read them
        for file in files:
            s3_path = f's3://{bucket_name}/{file}'
            logger.info('Reading file: %s', s3_path)

            # Read JSON file
            df = spark.read.format("json").load(s3_path)

        return df
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s", exp)
        # Define an empty schema
        schema = StructType([])
        # Create an empty DataFrame
        empty_df = spark.createDataFrame([], schema)
        return empty_df


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def transform(
    df1,
    df2,
    df3,
    prev_load_date_time,
    attendee_status,
    participant_typ_merc__c,
    participant_tov_final_merc__c,
    cntry_cd,
    curr_load_date_time,
    process_execution_id):
    """Transform the input dataframes based on meeting status, last modified date, and country
        code filters.
        df1: Daily_Attendance_TOV_MERC__c DataFrame
        df2: Meeting_Participant_MERC__c DataFrame
        df3: Meeting_MERC__c DataFrame
    """
    try:
        # Renaming the columns
        df2 = df2.withColumnRenamed("Id", "Id2")
        df3 = df3.withColumnRenamed("Id", "Id3")

        # Joining df1, df2 and df3
        df_join1 = df1.alias('a').join(df2.alias('b'), col('a.Meeting_Participant_MERC__c')
                                             == col('b.Id2'), how='left')

        df_join2 = df_join1.alias('a').join(df3.alias('b'), col('a.Meeting_MERC__c')
                                                  == col('b.Id3'), how='left')

        # Filter df
        df_filter = df_join2.filter((col("Participant_Typ_MERC__c") == participant_typ_merc__c) &
			(col("Status_MERC__c").isin(*attendee_status)) &
            (col("Participant_ToV_Updated_Date_MERC__c") > prev_load_date_time) &
            (col("Participant_ToV_Updated_Date_MERC__c") <= curr_load_date_time) &
            ((col("Event_Country_MERC__c").isNotNull()) &
            (~col('Event_Country_MERC__c').isin(*cntry_cd))) &
            (col("Participant_ToV_Final_MERC__c") == participant_tov_final_merc__c))

        # Rename columns by reversing the transformations
        df = df_filter.withColumnRenamed("Meeting_Participant_MERC__c",
                                         "MEETING_PARTICIPANT_MERC__C") \
            .withColumnRenamed("Meeting_Day_Date_MERC__c", "MEETING_DAY_DATE_MERC__C") \
            .withColumnRenamed("CurrencyIsoCode", "CURRENCYISOCODE") \
            .withColumnRenamed("Est_Hotel_ToV_MERC__c", "EST_HOTEL_TOV_MERC__C") \
            .withColumnRenamed("Est_Grp_Grnd_Transp_MERC__c", "EST_GRND_TRANSP_MERC__C") \
            .withColumnRenamed("Est_Food_Bev_ToV_MERC__c", "EST_FOOD_BEV_TOV_MERC__C") \
            .withColumnRenamed("Est_Reg_Amt_ToV_MERC__c", "EST_REG_AMT_TOV_MERC__C") \
            .withColumnRenamed("Est_Indv_Transfer_MERC__c", "EST_INDV_TRANSFER_MERC__C") \
            .withColumnRenamed("Id", "ID")

        # Transform the DataFrame
        df = df.withColumn(
            "Meeting_Day_Date_MERC__c",
            col("Meeting_Day_Date_MERC__c").cast("date")) \
        .withColumn(
            "Est_Indv_Transfer_MERC__c",
            col("Est_Indv_Transfer_MERC__c").cast("decimal(10, 2)"))

        df = df.select("MEETING_PARTICIPANT_MERC__C","MEETING_DAY_DATE_MERC__C",
                      "CURRENCYISOCODE","EST_HOTEL_TOV_MERC__C", "EST_GRND_TRANSP_MERC__C",
                      "EST_FOOD_BEV_TOV_MERC__C", "EST_REG_AMT_TOV_MERC__C",
                      "EST_INDV_TRANSFER_MERC__C","ID")

        current_time = datetime.now()
        result_df = df.withColumn("is_active",lit("Y")) \
            .withColumn("updated_ts",lit(current_time)) \
            .withColumn("created_ts",lit(current_time)) \
            .withColumn("created_by_job_exec_id",lit(process_execution_id).cast('bigint')) \
            .withColumn("updated_by_job_exec_id",lit(process_execution_id).cast('bigint'))
        return result_df

    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s", exp)
        return None


# pylint: disable=too-many-locals
def move_from_raw_to_archieve(bucket_name, directory_paths, archive_paths):
    """
    Directory files paths (3_files) are added as lists in directory_paths,
    Archive files paths (3_files) are added as lists in Archive_paths,
    """
    try:
        logger.info('init move_from_raw_to_archive')

        for directory_path, archive_path in zip(directory_paths, archive_paths):
            logger.info("Processing: %s", directory_path)
            source = f'{directory_path}'
            target = f'{archive_path}/'
            source_key = ''
            target_key = ''
            copy_source = ''
            current_date = datetime.now().date()
            response = s3_client.list_objects(Bucket=bucket_name, Prefix=source)
            files = []

            for obj in response.get('Contents', []):
                key = obj['Key']

                # Only add keys that are immediate children of the prefix
                if len(key[len(source):].split('/')) >= 2:
                    files.append(key)

            logger.info("Files to move from %s: %s", source, files)

            # Move each file from the current source to the archive path
            for obj in files:
                source_key = obj
                target_key = source_key.replace(source, f"{target}{current_date}", 1)

                if source_key != target_key:
                    copy_source = {
                        'Bucket': bucket_name,
                        'Key': source_key
                    }

                    s3_client.copy_object(CopySource=copy_source,
                                          Bucket=bucket_name,
                                          Key=target_key)
                    logger.info("Files moved successfully.")
                    s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                    logger.info("Files deleted successfully.")

        logger.info('exit move_from_raw_to_archive')
        return True

    except Exception as e:  # pylint: disable=broad-except
        logger.error("move_from_s3_to_s3 exception :: %s", e)
        return False


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
                    where id in 
                    (select a.id from <schema>.<tgt_table> a 
                    inner join <schema>.<stg_table> b 
                    on a.id = b.id) 
                    and a.is_active = 'Y'"""

    insert_query = """INSERT INTO <schema>.<tgt_table>
                    (meeting_participant_merc__c,meeting_day_date_merc__c,currencyisocode,est_hotel_tov_merc__c,est_grnd_transp_merc__c,est_food_bev_tov_merc__c,est_reg_amt_tov_merc__c,est_indv_transfer_merc__c,id,is_active,created_ts,updated_ts,updated_by_job_exec_id,created_by_job_exec_id)
                    (select meeting_participant_merc__c,meeting_day_date_merc__c,currencyisocode,est_hotel_tov_merc__c,est_grnd_transp_merc__c,est_food_bev_tov_merc__c,est_reg_amt_tov_merc__c,est_indv_transfer_merc__c,id,'Y'::varchar AS is_active,current_timestamp::timestamp as created_ts,current_timestamp::timestamp as updated_ts,<process_execution_id> as created_by_job_exec_id,<process_execution_id> as updated_by_job_exec_id 
                    FROM <schema>.<stg_table>);"""

    try:
        updt_query = updt_query.replace('<schema>',rds_schema_name) \
                                .replace('<tgt_table>',tgt_table) \
                                .replace('<stg_table>',stg_table) \
                                .replace('<process_execution_id>',str(exec_id))
        cursor.execute(updt_query)
        conn.commit()

        insert_query = insert_query.replace('<process_execution_id>',str(exec_id)) \
                                .replace('<schema>',rds_schema_name) \
                                .replace('<tgt_table>',tgt_table) \
                                .replace('<stg_table>', stg_table)
        cursor.execute(insert_query)
        conn.commit()
    except ValueError as exp_msg:
        logger.error("Error while writing data %s", str(exp_msg))
        rollback_database_table_process(rds_schema_name, tgt_table,
                                        exec_id, cursor, conn)
        raise exp_msg
    finally:
        if cursor is not None:
            cursor.close()

tz = pytz.timezone('UTC')
glue_start_time = datetime.now(tz)

def start_appflow(flow_name):
    '''
    this will start a appflow
    '''
    response = appflow_client.start_flow(
        flowName=flow_name
    )
    execution_id = response['executionId']
    return execution_id

def check_appflow_status(flow_name, execution_id):
    '''
    this will check the appflow status
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
        elif status == 'Error' and timestmp > glue_start_time: # pylint: disable=no-else-break
            raise Exception(f"AppFlow execution failed with ID: {execution_id}") # pylint: disable=broad-exception-raised
        else:
            time.sleep(2)  # Check every 2 seconds

def fillnull(df):
    columns_to_update = [
        "meeting_participant_merc__c",
        "meeting_day_date_merc__c",
        "currencyisocode",
        "est_hotel_tov_merc__c",
        "est_grnd_transp_merc__c",
        "est_food_bev_tov_merc__c",
        "est_reg_amt_tov_merc__c",
        "est_indv_transfer_merc__c",
        "id"
    ]
    for column in columns_to_update:
        df = df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))
        df = df.withColumn(column, when(col(column).isNull(), None).otherwise(col(column)))
    return df
def main():
    """ Main Function """
    try:
        # To get previous load date
        # curr_load_date_time = datetime.now().strftime("%Y-%m-%dT00:00:00.000Z")
        curr_load_date_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        json_file_loaction = [json_file_location1,json_file_location2,json_file_location3]
        for file in json_file_loaction:
            flow_name = file.split('/')[-1]
            execution_id = start_appflow(flow_name)
            check_appflow_status(flow_name, execution_id)
        # Initialize common configuration
        config_common = {}
        config_common = init_config(config_common)
        logger.info("The config_common values in main are: %s", config_common)
        bucket_name = rawbucket
        process_execution_id = config_common["process_execution_id"]

        df1 = get_data_from_s3(bucket_name, json_file_location1)
        df2 = get_data_from_s3(bucket_name, json_file_location2)
        df3 = get_data_from_s3(bucket_name, json_file_location3)

        # To get variables
        load_metadata_file_path = args["load_metadata_file_path"]
        attendee_status = config_common['gso_ob_act_spnd_att_tov_stg_batch_params']['attendee_status'] # pylint: disable=line-too-long
        cntry_cd = config_common['gso_ob_act_spnd_att_tov_stg_batch_params']['CNTRY_CD']
        participant_typ_merc__c = str(config_common['gso_ob_act_spnd_att_tov_stg_batch_params']['Participant_Typ_MERC__c']) # pylint: disable=line-too-long
        participant_tov_final_merc__c = str(config_common['gso_ob_act_spnd_att_tov_stg_batch_params']['Participant_ToV_Final_MERC__c']) # pylint: disable=line-too-long
        load_param = config_common['gso_ob_act_spnd_att_tov_stg_batch_params']['prev_load_date_time'] # pylint: disable=line-too-long

        prev_load_date_time = get_load_metadata(load_metadata_file_path, load_param, bucket_name)

        # Transform the data
        df = transform(
            df1,
            df2,
            df3,
            prev_load_date_time=prev_load_date_time,
            attendee_status=attendee_status,
            participant_typ_merc__c=participant_typ_merc__c,
            participant_tov_final_merc__c=participant_tov_final_merc__c,
            cntry_cd=cntry_cd,
            curr_load_date_time=curr_load_date_time,
            process_execution_id=process_execution_id
        )
        df=df.dropDuplicates()
        # Ingest data into the target table and history table
        logger.info("Ingesting data from S3 into table: %s", tgt_name)
        df = fillnull(df)
        precision = 16 # Total number of digits
        scale = 2       # Number of digits to the right of the decimal
        # Cast the column to Decimal (Numeric)
        df = df.withColumn("est_hotel_tov_merc__c", col("est_hotel_tov_merc__c").cast(DecimalType(precision, scale)))
        df = df.withColumn("est_grnd_transp_merc__c", col("est_grnd_transp_merc__c").cast(DecimalType(precision, scale)))
        df = df.withColumn("est_food_bev_tov_merc__c", col("est_food_bev_tov_merc__c").cast(DecimalType(precision, scale)))
        df = df.withColumn("est_reg_amt_tov_merc__c", col("est_reg_amt_tov_merc__c").cast(DecimalType(precision, scale)))
        df = df.withColumn("est_indv_transfer_merc__c", col("est_indv_transfer_merc__c").cast(DecimalType(precision, scale)))
        ingest_data(df, tgt_name, True, 'overwrite')
        conn,curr = connect_db()
        logger.info("Ingesting data from S3 into table: %s", args['hist_table'])
        load_history_table(conn, curr, args['hist_table'],
                    tgt_name, process_execution_id)

        # Move files to archive folder
        logger.info("Moving files to archive folder")
        directory_paths = [json_file_location1, json_file_location2, json_file_location3]
        archive_paths = [archive_path1, archive_path2, archive_path3]
        move_from_raw_to_archieve(bucket_name, directory_paths, archive_paths)
        logger.info("Files moved and task is completed")

        # To write metadata file in s3
        write_metadata_file(load_metadata_file_path, curr_load_date_time, bucket_name)

    except Exception as exp:
        logger.error("Error in main function: %s", exp)
        raise exp

    finally:
        logger.info("Inside finally block")
        conn.close()
        curr.close()


# Call the main function
main()
job.commit()
