"""Glue Script to load GSO_OB_ACT_SPND_ATT_STG table  from Json file in S3 to RDS
#<mm/dd/yyyy><author><modification-details>:19-09-2024/karthikrm/v0
"""
import sys
import json
from datetime import datetime
import logging
import ast
import time

import pytz                                     # pylint: disable=import-error
import boto3
import psycopg2
from awsglue.utils import getResolvedOptions    # pylint: disable=import-error
from awsglue.context import GlueContext         # pylint: disable=import-error
from awsglue.job import Job                     # pylint: disable=import-error
from pyspark.sql.types import StructType        # pylint: disable=import-error
from pyspark.context import SparkContext        # pylint: disable=import-error
from pyspark.sql.functions import col, lit  # pylint: disable=import-error
from botocore.exceptions import ClientError
from psycopg2 import extras

ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")

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
                'json_file_location_mpmc',
                'json_file_location_mmc',
                'json_file_location_acc',
                'tgt_table',
                'archive_path_mmpc',
                'archive_path_mmc',
                'archive_path_acc',
                'prev_load_date_time',
                'curr_load_date_time',
                'previous_load_file_location',
                'previous_load_file_name',
                'logging_level',
                'hist_table',
                'attendee_status'])

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
bucket_name = args["raw_bucket"]
json_file_location_mpmc = \
    args['json_file_location_mpmc']
json_file_location_mmc = args['json_file_location_mmc']
json_file_location_acc =args['json_file_location_acc']
tgt_table = args['tgt_table']
hist_table = args['hist_table']
archive_path_mmpc = \
    args['archive_path_mmpc']
archive_path_mmc = args['archive_path_mmc']
archive_path_acc = args['archive_path_acc']
prev_load_date_time = args['prev_load_date_time']
curr_load_date_time = args['curr_load_date_time']
previous_load_file_location = args['previous_load_file_location']
previous_load_file_name =args['previous_load_file_name']
attendee_status = ast.literal_eval(args['attendee_status'])
level = args['logging_level']
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
URL = f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require"

# Configure the logger
logging.basicConfig(
    level=level,
    format='%(asctime)s: %(funcName)s: line %(lineno)d: %(levelname)s: %(message)s',
    datefmt=TIME_FORMAT
)
logger = logging.getLogger(__name__)

logger.info("Starting gso_ob_act_spnd_att_stg Glue job")

try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e:    # pylint: disable=broad-except
    logger.error("Error retrieving secret from Secrets Manager: %s",e)
    # Handle potential errors during retrieval
    raise e

s3_client = boto3.client("s3")
appflow_client = boto3.client('appflow') # pylint: disable=redefined-outer-name
lambda_client = boto3.client("lambda", region_name="us-east-2") # pylint: disable=redefined-outer-name
secret_manager = boto3.client("secretsmanager")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def connect_db():
    """Creates db connection for the object
    Parameter:
        db_details (dict): Database details
    """
    try:
        conn = psycopg2.connect(database=db,
                                user=username,
                                host=host,
                                password=password,
                                port=port)
        cur = conn.cursor(
            cursor_factory=extras.RealDictCursor)
        return conn, cur
    except Exception as error:
        logger.error("error in connnect_db: %s", error)
        raise error


def abc_batch_management(job_name):
    '''
    takes abc params names
    '''
    lambda_client = boto3.client("lambda", region_name=args["region_name"]) # pylint: disable=redefined-outer-name
    abc_batch_execution = {"operation_type": "insert",
                           "status": "R",
                           "batch_name": f"{batch_name}"}
    invoke_response_status = lambda_client.invoke(
                    FunctionName="edb_abc_log_batch_status",
                    Payload=json.dumps(abc_batch_execution).encode("utf-8"))
    batch_execution_status_id = json \
                    .loads(invoke_response_status["Payload"].read())
    logger.info(batch_execution_status_id.get("batch_execution_id"))

    abc_log_process_status = {
        "operation_type": "insert",
        "status": "R",
        "batch_execution_id": batch_execution_status_id.get("batch_execution_id"),
        "job_name": job_name}

    invoke_response_status = lambda_client.invoke(
            FunctionName="edb_abc_log_process_status",
            Payload=json.dumps(abc_log_process_status).encode("utf-8"))
    log_process_status_id = json.loads(invoke_response_status["Payload"].read())
    logger.info(log_process_status_id.get("process_execution_id"))
    final_json = {
        'batch_execution_id': batch_execution_status_id.get("batch_execution_id"),
        'process_execution_id': log_process_status_id.get("process_execution_id")
        }
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
        logger.info("in init config")
        # logger.info(process_execution_id, batch_execution_id)
        json_payload = {
            "batch_name": args.get("batch_name"),
            "job_name": args.get("job_name")}
        config_dct = invoke_lambda(args['abc_fetch_param_function'],
                                   json_payload)
        logger.info("config_dct: %s", config_dct)
        # # Read common config file applicable for all tables
        config_common = config_dct
        config_common["job_name"] = job_name
        config_common["batch_name"] = batch_name
        config_common["process_execution_id"] = process_execution_id
        # config_common["snow_func"] = args['snow_func']
        config_common["abc_log_stats_func"] = abc_log_stats_func
        config_common["batch_execution_id"] = batch_execution_id
        return config_common
    except Exception as exep_func:  # pylint: disable=broad-except
        logger.error("Error in initialising config: %s", exep_func)
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

# pylint: disable=redefined-outer-name
def get_data_from_s3(bucket_name, file_key):
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
    except Exception as exp:    # pylint: disable=broad-except
        logger.error("Error in the function : %s", exp)
        # Define an empty schema
        schema = StructType([])
        # Create an empty DataFrame
        empty_df = spark.createDataFrame([], schema)
        return empty_df

# pylint: disable=too-many-arguments, too-many-locals, redefined-outer-name
def move_from_raw_to_archieve(bucket_name, directory_paths, archive_paths):
    """
    Directory files paths (3_files) are added as lists in directory_paths,
    Archive files paths (3_files) are added as lists in Archive_paths,
    """
    try:
        logger.info('init move_from_raw_to_archive')

        for directory_path, archive_path in zip(directory_paths, archive_paths):
            # logger.info("Processing: %s", directory_path)

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

            # logger.info("Files to move from %s: %s", source, files)

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

##################################### transformations ####################################

# pylint: disable=too-many-arguments, too-many-positional-arguments, trailing-whitespace
def transform(df1, df2, df3, prev_load_date_str,
              curr_load_date_str, attendee_status, exec_id):
    """
    DataFrame transformation to get the desired structure and fields.
    mpmc means: Meeting_Participant_MERC__c object from Salesforce (saved in s3)
    mmc means: Meeting_MERC__c object from Salesforce (saved in s3)
    account means: account object from Salesforce (saved in s3)
    df1: mpmc_df
    df2: mmc_df
    df3: account_df
    prev_load_date_time : previous loaded date_time of Meeting_MERC__c
    curr_load_date_time : current loaded date_time of Meeting_MERC__c
    """
    # Select and rename specified columns
    # fltr means filter
    # jn means join
    # Filter and join operations
    try:
        # Create temporary views for the DataFrames
        df1 = df1.select([col(column).cast("string").alias(column) for column in df1.columns])
        df2 = df2.select([col(column).cast("string").alias(column) for column in df2.columns])
        df3 = df3.select([col(column).cast("string").alias(column) for column in df3.columns])

        result_df = df1.alias("mp") \
            .join(df2.alias("mm"), col("mp.Meeting_MERC__c") == col("mm.Id"), "left_outer") \
            .join(df3.alias("a"), col("mp.Account_MERC__c") == col("a.Id"), "left_outer")

        # Apply the necessary filters
        result_df = result_df.filter(
            (col("mp.Participant_Typ_MERC__c") == "HCP") &
            (col("mp.Status_MERC__c").isin(*attendee_status)) &
            (col("mm.Participant_ToV_Updated_Date_MERC__c") > prev_load_date_str) &
            (col("mm.Participant_ToV_Updated_Date_MERC__c") <= curr_load_date_str) &
            (col("mm.Event_Country_MERC__c").isNotNull()) &
            (col("mm.Event_Country_MERC__c") != "") &
            (col("mm.Participant_ToV_Final_MERC__c") == "Yes"))

        # Select and cast columns as in the SQL query
        result_df = result_df.select(
            col("mm.Event_Id_MERC__c").cast("string").alias("EVENT_ID_MERC__C"),
            col("mp.Id").cast("string").alias("ID"),
            col("mp.Id").cast("string").alias("MEETING_PARTICIPANT_ID_MERC__C"),
            col("mp.Customer_Id_GLBL__c").cast("string").alias("CUSTOMER_ID_GLBL__C"),
            col("a.Name").cast("string").alias("NAME_GLBL__C"),
            col("mp.ToV_Currency_MERC__c").cast("string").alias("CURRENCYISOCODE"),
            col("mp.Total_Hotel_ToV_MERC__c").cast("decimal(16,2)") \
                .alias("TOTAL_HOTEL_TOV_MERC__C"),
            col("mp.Total_Ground_Transporation_ToV_MERC__c").cast("decimal(16,2)") \
                .alias("TOTAL_GROUND_TRNSPRTN_TOV_MERC"),
            col("mp.Total_Registration_ToV_MERC__c").cast("decimal(16,2)") \
                .alias("TOTAL_REGISTRATION_TOV_MERC__C"),
            col("mp.Total_Food_Beverage_ToV_MERC__c").cast("decimal(16,2)") \
                .alias("TOTAL_FOOD_BEVERAGE_TOV_MERC"),
            col("mp.Total_Individual_Transport_ToV_MERC__c").cast("decimal(16,2)") \
                .alias("TOTAL_INDIV_TRANS_TOV_MERC__C"),
            col("mp.CoPay_AODS_Hotel_MERC__c").cast("decimal(16,2)").alias("COPAY_HOTEL_MERC__C"),
            col("mp.CoPay_AODS_Ground_Transport_MERC__c").cast("decimal(16,2)") \
                .alias("COPAY_GROUND_TRANSPORT_MERC__C"),
            col("mp.CoPay_AODS_Food_Beverage_MERC__c").cast("decimal(16,2)") \
                .alias("COPAY_FOOD_BEVERAGE_MERC__C"),
            col("mp.CoPay_AODS_Registration_MERC__c").cast("decimal(16,2)") \
                .alias("COPAY_REGISTRATION_MERC__C"),
            col("mp.CoPay_AODS_Flight_Rail_MERC__c").cast("decimal(16,2)") \
                .alias("COPAY_FLIGHT_RAIL_MERC__C"),
            col("mp.CoPay_AODS_Indiv_Transportation_MERC__c").cast("decimal(16,2)") \
                .alias("COPAY_AODS_INDIV_TRANS_MERC__C"),
            col("mp.Record_Type_Name_MERC__c").cast("string").alias("RECORD_TYPE_NAME__C"))

        # df1.createOrReplaceTempView("Meeting_Participant_MERC__c")
        # df2.createOrReplaceTempView("Meeting_Merc__c")
        # df3.createOrReplaceTempView("Account")

        # query = \
        #     f"""SELECT 
        #             CAST(mm.Event_Id_MERC__c AS STRING) 
        #         		AS EVENT_ID_MERC__C,
        #             CAST(mp.Id AS STRING) AS ID,
        #             CAST(mp.Id AS STRING) 
        #         		AS MEETING_PARTICIPANT_ID_MERC__C,
        #             CAST(mp.Customer_Id_GLBL__c AS STRING) 
        #         		AS CUSTOMER_ID_GLBL__C,
        #             CAST(a.Name AS STRING) AS NAME_GLBL__C,
        #             CAST(mp.ToV_Currency_MERC__c AS STRING) 
        #         		AS CURRENCYISOCODE,
        #             CAST(mp.Total_Hotel_ToV_MERC__c AS DECIMAL(16,2)) 
        #         		AS TOTAL_HOTEL_TOV_MERC__C,
        #             CAST(mp.Total_Ground_Transporation_ToV_MERC__c AS DECIMAL(16,2)) 
        #         		AS TOTAL_GROUND_TRNSPRTN_TOV_MERC,
        #             CAST(mp.Total_Registration_ToV_MERC__c AS DECIMAL(16,2)) 
        #         		AS TOTAL_REGISTRATION_TOV_MERC__C,
        #             CAST(mp.Total_Food_Beverage_ToV_MERC__c AS DECIMAL(16,2)) 
        #         		AS TOTAL_FOOD_BEVERAGE_TOV_MERC,
        #             CAST(mp.Total_Individual_Transport_ToV_MERC__c AS DECIMAL(16,2)) 
        #         		AS TOTAL_INDIV_TRANS_TOV_MERC__C,
        #             CAST(mp.CoPay_AODS_Hotel_MERC__c AS DECIMAL(16,2)) 
        #         		AS COPAY_HOTEL_MERC__C,
        #             CAST(mp.CoPay_AODS_Ground_Transport_MERC__c AS DECIMAL(16,2)) 
        #         		AS COPAY_GROUND_TRANSPORT_MERC__C,
        #             CAST(mp.CoPay_AODS_Food_Beverage_MERC__c AS DECIMAL(16,2)) 
        #         		AS COPAY_FOOD_BEVERAGE_MERC__C,
        #             CAST(mp.CoPay_AODS_Registration_MERC__c AS DECIMAL(16,2)) 
        #         		AS COPAY_REGISTRATION_MERC__C,
        #             CAST(mp.CoPay_AODS_Flight_Rail_MERC__c AS DECIMAL(16,2)) 
        #         		AS COPAY_FLIGHT_RAIL_MERC__C,
        #             CAST(mp.CoPay_AODS_Indiv_Transportation_MERC__c AS DECIMAL(16,2)) 
        #         		AS COPAY_AODS_INDIV_TRANS_MERC__C,
        #             CAST(mp.Record_Type_Name_MERC__c AS STRING) AS RECORD_TYPE_NAME__C
        #         FROM 
        #             Meeting_Participant_MERC__c mp
        #         LEFT OUTER JOIN 
        #             Meeting_Merc__c mm ON mp.Meeting_MERC__c = mm.Id
        #         LEFT OUTER JOIN 
        #             Account a ON mp.Account_MERC__c = a.Id
        #         WHERE 
        #             mp.Participant_Typ_MERC__c = 'HCP'
        #             AND mp.Status_MERC__c IN ('Attended','No Show')
        #             AND mm.Participant_ToV_Updated_Date_MERC__c > '{prev_load_date_str}'
        #             AND mm.Participant_ToV_Updated_Date_MERC__c <= '{curr_load_date_str}'
        #             AND mm.Event_Country_MERC__c IS NOT NULL
        #             AND mm.Event_Country_MERC__c != ''
        #             AND mm.Participant_ToV_Final_MERC__c = 'Yes'
        #         """

        # # Execute the query
        # result_df = spark.sql(query)

        # spark.catalog.dropTempView('Meeting_Participant_MERC__c')
        # spark.catalog.dropTempView('Meeting_MERC__c')
        # spark.catalog.dropTempView('Account_MERC__c')

        current_time = datetime.now()
        result_df = result_df.withColumn("is_active",lit("Y")) \
                .withColumn("updated_ts",lit(current_time)) \
                .withColumn("created_ts",lit(current_time)) \
                .withColumn("created_by_job_exec_id",lit(exec_id).cast('bigint')) \
                .withColumn("updated_by_job_exec_id",lit(exec_id).cast('bigint'))

        return result_df

    except Exception as exp:    # pylint: disable=broad-except
        logger.error("Error in the function: %s", exp)
        return None


def get_load_metadata(s3_path, load_param):
    "This function is used to get current load parameter"
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
        load_metadata = json.loads(response['Body'].read().decode('utf-8'))
        metadata_load_value = load_metadata["metadata_load_value"]
        logger.info("metadata_load_value:: %s", metadata_load_value)
        return metadata_load_value
    except ClientError as err:
        logger.info("Error in function get_load_metadata:: %s", err)
        if err.response['Error']['Code'] == 'NoSuchKey':
            metadata_load_value = load_param
            logger.info("metadata_load_value:: %s",metadata_load_value)
            return metadata_load_value
        logger.error("Error in function get_load_metadata:: %s", err)
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
                    where meeting_participant_id_merc__c in 
                    (select a.meeting_participant_id_merc__c from <schema>.<tgt_table> a 
                    inner join <schema>.<stg_table> b 
                    on a.meeting_participant_id_merc__c = b.meeting_participant_id_merc__c) 
                    and a.is_active = 'Y'"""

    insert_query = """INSERT INTO <schema>.<tgt_table>
                    (event_id_merc__c,meeting_participant_id_merc__c,customer_id_glbl__c,name_glbl__c,currencyisocode,total_hotel_tov_merc__c,total_ground_trnsprtn_tov_merc,total_registration_tov_merc__c,total_food_beverage_tov_merc,total_indiv_trans_tov_merc__c,copay_hotel_merc__c,copay_ground_transport_merc__c,copay_food_beverage_merc__c,copay_registration_merc__c,copay_flight_rail_merc__c,copay_aods_indiv_trans_merc__c,record_type_name__c,id,is_active,created_ts,updated_ts,updated_by_job_exec_id,created_by_job_exec_id)
                    (select event_id_merc__c,meeting_participant_id_merc__c,customer_id_glbl__c,name_glbl__c,currencyisocode,total_hotel_tov_merc__c,total_ground_trnsprtn_tov_merc,total_registration_tov_merc__c,total_food_beverage_tov_merc,total_indiv_trans_tov_merc__c,copay_hotel_merc__c,copay_ground_transport_merc__c,copay_food_beverage_merc__c,copay_registration_merc__c,copay_flight_rail_merc__c,copay_aods_indiv_trans_merc__c,record_type_name__c,id,'Y'::varchar AS is_active,current_timestamp::timestamp as created_ts,current_timestamp::timestamp as updated_ts,<process_execution_id> as created_by_job_exec_id,<process_execution_id> as updated_by_job_exec_id 
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


def write_metadata_file(s3_path, load_param):
    "This function is used to write metadata file in s3"
    try:
        data = json.dumps({"metadata_load_value": f"{load_param}"})
        s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=data)
    except ClientError as err:
        logger.error("Error in function write_metadata_file:: %s", err)
        raise err
    
# Get the current time in UTC
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
        last_run_details = response.get('lastRunExecutionDetails', {}) # pylint: disable=no-else-break
        status = last_run_details.get('mostRecentExecutionStatus')
        timestmp = last_run_details.get('mostRecentExecutionTime')
        if status is None or timestmp is None: # pylint: disable=broad-exception-raised
            time.sleep(2)
            continue
        if status == 'Successful' and timestmp > glue_start_time: # pylint: disable=no-else-break
            break
        elif status == 'Error' and timestmp > glue_start_time:
            raise Exception(f"AppFlow execution failed with ID: {execution_id}") # pylint: disable=broad-exception-raised
        else:
            time.sleep(2)  # Check every 2 seconds


# pylint: disable=too-many-locals,too-many-statements
def main():
    '''
    this function does sequence of operations one by one
    '''
    try:
        # current_datetime_str = datetime.now().strftime("%Y-%m-%dT00:00:00.000Z")
        current_datetime_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        # logger.info(" prev_load_date is : %s",prev_load_date)
        # logger.info("current_datetime_str is : %s", current_datetime_str)
        json_file_loaction = [json_file_location_mpmc,json_file_location_mmc,json_file_location_acc]
        for file in json_file_loaction:
            flow_name = file.split('/')[-1]
            execution_id = start_appflow(flow_name)
            check_appflow_status(flow_name, execution_id)
        previous_file_loaded_location = previous_load_file_location
        previous_loaded_file_name = previous_load_file_name
        meeting_participant_file_key = json_file_location_mpmc
        meeting_file_key = json_file_location_mmc
        account_file_key = json_file_location_acc
        trgt_table = tgt_table

        s3_path = f"{previous_file_loaded_location}/{previous_loaded_file_name}.json"
        prev_load_date = get_load_metadata(s3_path, prev_load_date_time)
        logger.info('trgt_table is : %s', trgt_table)

        # mpmc means : Meeting_Participant_MERC__c
        mpmc_directory_path = meeting_participant_file_key

        # mmc means : Meeting_MERC__c
        mmc_directory_path = meeting_file_key
        account_directory_path = account_file_key
        mpmc_archive_path = archive_path_mmpc
        mmc_archive_path = archive_path_mmc
        account_archive_path = archive_path_acc

        config_common = {}
        config_common = init_config(config_common)
        process_execution_id = config_common["process_execution_id"]
        logger.info("The config_common values in main are: %s", config_common)

        mpmc_df = get_data_from_s3(bucket_name, mpmc_directory_path)
        mmc_df = get_data_from_s3(bucket_name, mmc_directory_path)
        account_df = get_data_from_s3(bucket_name, account_directory_path)

        trgt_df = transform(mpmc_df, mmc_df, account_df,
                           prev_load_date, current_datetime_str, 
                           attendee_status, process_execution_id)
        tgt_df = trgt_df.dropDuplicates()

        logger.info("ingseting data from s3 into table : %s", trgt_table)
        ingest_data(tgt_df, trgt_table, True, 'overwrite')
        conn,curr = connect_db()
        logger.info("Ingesting data from S3 into table: %s", hist_table)
        load_history_table(conn, curr, hist_table,
                    trgt_table, process_execution_id)

        logger.info("moving files to archive folder")
        directory_paths = [
                            mpmc_directory_path,  # meeting_participant_file_key
                            mmc_directory_path,  # meeting_file_key
                            account_directory_path  # account_file_key
                            ]

        archive_paths = [
                            mpmc_archive_path,  # Meeting_Participant_archive_file_key
                            mmc_archive_path,  # Meeting_MERC_archive_file_key
                            account_archive_path  # Account_file_archive_key
                            ]

        move_from_raw_to_archieve(bucket_name, directory_paths, archive_paths)
        logger.info("files moved and task is completed")

        # To write metadata file in s3
        write_metadata_file(s3_path, current_datetime_str)

    except Exception as exp:
        logger.error("Error in main function : %s", exp)
        raise exp
    finally:
        logger.info("inside finally block")
        conn.close()
        curr.close()


main()
job.commit()
