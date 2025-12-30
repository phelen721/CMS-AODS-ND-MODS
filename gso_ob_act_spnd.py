"""
Glue Script to Load Data into a Flat File from the Consent Staging Table
# <mm/dd/yyyy><author><modification-details>: 09-11-2024/Helen Monisha/v0
"""
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from botocore.exceptions import ClientError
from pyspark.context import SparkContext  # pylint: disable=import-error
from pyspark.sql import functions as F # pylint: disable=import-error
from awsglue.context import GlueContext       # pylint: disable=import-error
from awsglue.job import Job                   # pylint: disable=import-error
from awsglue.utils import getResolvedOptions  # pylint: disable=import-error
from psycopg2 import extras


ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


args = getResolvedOptions(sys.argv, [
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
    'logging_level', 
    'sql_file_location',
    'target_folder_spnd',
    'target_folder_cms',
    'archive_folder',
    'target_file_name'])


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
URL = f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require"


# Configure the logger
logging.basicConfig(
    level=level,
    format='%(asctime)s: %(funcName)s: line %(lineno)d: %(levelname)s: %(message)s',
    datefmt=TIME_FORMAT)
logger = logging.getLogger(__name__)

logger.info("Starting gso_ob_act_spnd Glue job")


try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e:
    # Handle potential errors during retrieval
    logger.error('Error retrieving secret from Secrets Manager: %s', e)
    raise e


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


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
    finally:
        # Ensure closing of resources
        if conn is not None and cur is not None:
            cur.close()
            conn.close()


# pylint: disable=redefined-outer-name
def abc_batch_management(job_name):
    """Get Process ID and Execution ID by invoking ABC lambda"""
    lambda_client = boto3.client("lambda", region_name="us-east-2")
    abc_batch_execution = {
        "operation_type": "insert",
        "status": "R",
        "batch_name": f"edb_cms_{job_name}"
    }
    invoke_response_status = lambda_client.invoke(
        FunctionName="edb_abc_log_batch_status",
        Payload=json.dumps(abc_batch_execution).encode("utf-8")
    )
    batch_execution_status_id = json.loads(invoke_response_status["Payload"].read())
    abc_log_process_status = {
        "operation_type": "insert",
        "status": "R",
        "batch_execution_id": batch_execution_status_id.get("batch_execution_id"),
        "job_name": job_name
    }
    invoke_response_status = lambda_client.invoke(
        FunctionName="edb_abc_log_process_status",
        Payload=json.dumps(abc_log_process_status).encode("utf-8")
    )
    log_process_status_id = json.loads(invoke_response_status["Payload"].read())
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


def invoke_lambda(func_name,payload,region_name='us-east-2'):
    """
    Function to invoke Lambda
    """
    lambda_client = boto3.client('lambda', region_name=region_name)
    invoke_response = lambda_client.invoke(
        FunctionName=func_name,
        Payload=json.dumps(payload))
    config_obj = json.loads(invoke_response['Payload'].read())
    return config_obj


def querydb(query):
    """Execute the query and return the result as a PySpark DataFrame."""
    try:
        if ";" in query:
            query = query.replace(";", "")
        df = (
            spark.read.format("jdbc")
            .options(
                url=URL,
                user=username,
                password=password,
                dbtable=f"({query}) AS union_query",  # Enclose the query and give it an alias
            )
            .load()
        )
        return df
    except Exception as error: # pylint: disable=broad-except
        logger.error("Exception in sql queries: %s", str(error))
        return "No Data"
    finally:
        logger.info("exit querydb")


# pylint: disable=line-too-long
def exp_constant_value(df):
    """This function is used to add columns with constant value to dataframe"""
    # Rename column
    df = df.withColumnRenamed("id1", "meeting_participant_id_merc__c")

    # Add column
    df = df.withColumn(
            "val_id_var",
            F.when(F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("meeting_participant_id_merc__c"))
            .otherwise(F.col("id"))
        )
    df = df.withColumn(
            "date_of_event_merc__c",
            F.concat(F.date_format(F.col("date_of_event_merc__c"), "ddMMyyyy"), F.lit("000000"))
        )

    # Apply transformation
    df_exp_constant_value = df.select(F.col("event_id_merc__c"),
            F.col("event_country_merc__c"),
            F.col("date_of_event_merc__c"),
            (F.lpad(F.col("owner_master_id_merc__c"), 8, '0')).alias("owner_master_id_merc__c_out"),
            F.col("name"),
            F.col("meeting_participant_id_merc__c"),
            F.col("customer_id_glbl__c"),
            F.col("name_glbl__c"),
            F.col("record_type_name__c"),
            F.col("currencyisocode"),
            F.col("copay_hotel_merc__c"),
            F.col("copay_ground_transport_merc__c"),
            F.col("copay_food_beverage_merc__c"),
            F.col("copay_registration_merc__c"),
            F.col("copay_flight_rail_merc__c"),
            F.col("copay_aods_indiv_trans_merc__c"),
            (F.when(
                F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("currencyisocode"))
                .otherwise(F.col("currencyisocode_tov"))).alias("currencyisocode1_out"),
            (F.when(
                F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("total_hotel_tov_merc__c"))
                .otherwise(F.col("est_hotel_tov_merc__c"))).alias("hotel_tov_merc__c_out"),
            (F.when(
                F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("total_ground_trnsprtn_tov_merc"))
                .otherwise(F.col("est_grnd_transp_merc__c"))).alias("grnd_transp_merc__c_out"),
            (F.when(
                F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("total_food_beverage_tov_merc"))
                .otherwise(F.col("est_food_bev_tov_merc__c"))).alias("food_bev_tov_merc__c_out"),
            (F.when(
                F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("total_registration_tov_merc__c"))
                .otherwise(F.col("est_reg_amt_tov_merc__c"))).alias("reg_amt_tov_merc__c_out"),
            (F.when(
                F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("total_indiv_trans_tov_merc__c"))
                .otherwise(F.col("est_indv_transfer_merc__c"))).alias("indv_transfer_merc__c_out"),
            (F.lit('Transfer of Value Co Pay')).alias("val_typ_cd_copay"),
            (F.lit('Transfer of Value')).alias("val_typ_cd"),
            (F.lit('Lodging')).alias("val_typ_prps_cd_htl"),
            (F.lit('Transportation')).alias("val_typ_prps_cd_trnsprtn"),
            (F.lit('Food and Beverage')).alias("val_typ_prps_cd_food"),
            (F.lit('Registration Fee')).alias("val_typ_prps_cd_rgstrtn"),
            (F.concat(F.col("val_id_var"), F.lit("_HTL"))).alias("value_id_hotel"),
            (F.concat(F.col("val_id_var"), F.lit("_GRND_TRNSPRT"))).alias("value_id_grand_transport"),
            (F.concat(F.col("val_id_var"), F.lit("_FOOD_BVRG"))).alias("value_id_food_beverage"),
            (F.concat(F.col("val_id_var"), F.lit("_RGSTRN_FEE"))).alias("value_id_registration_fee"),
            (F.concat(F.col("val_id_var"), F.lit("_INDV_GRND_TRNSPRT"))).alias("value_id_individual_transport"),
            (F.concat(F.col("meeting_participant_id_merc__c"), F.lit("_COPAY_HTL"))).alias("value_id_copay_hotel"),
            (F.concat(F.col("meeting_participant_id_merc__c"), F.lit("_COPAY_GRND_TRNSPRT"))).alias("value_id_copay_grand_transport"),
            (F.concat(F.col("meeting_participant_id_merc__c"), F.lit("_COPAY_FOOD_BVRG"))).alias("value_id_copay_food_beverage"),
            (F.concat(F.col("meeting_participant_id_merc__c"), F.lit("_COPAY_RGSTRN_FEE"))).alias("value_id_copay_registration_fee"),
            (F.concat(F.col("meeting_participant_id_merc__c"), F.lit("_COPAY_RAIL_FLGHT_TRNSPRT"))).alias("value_id_copay_rail_flight_transport"),
            (F.concat(F.col("meeting_participant_id_merc__c"), F.lit("_COPAY_INDV_GRND_TRNSPRT"))).alias("value_id_copay_individual_transport"),
            (F.lit('Hotel')).alias("value_purpose_secondary_code_hotel"),
            (F.lit('Ground Transportation')).alias("value_purpose_secondary_code_ground_transportation"),
            (F.lit('Food and Beverage')).alias("value_purpose_secondary_code_food_beverage"),
            (F.lit('Registration Fee')).alias("value_purpose_secondary_code_registration_fee"),
            (F.lit('Rail/Flight Transportation')).alias("value_purpose_secondary_code_rail_flight_transportation"),
            (F.lit('Private Transfer')).alias("value_purpose_secondary_code_individual_transport"),
            (F.when(
                F.upper(F.col("record_type_name__c")) == 'MERC_INDIVIDUAL_SPONSORSHIP',
                F.col("date_of_event_merc__c"))
                .otherwise(
                F.concat(F.date_format(F.col("meeting_day_date_merc__c"), "ddMMyyyy"), F.lit("000000"))
                )).alias("valuepaiddt")
    )

    return df_exp_constant_value


# pylint: disable=line-too-long
def split_dataframe(df, select_columns):
    "This function is used to split dataframe"
    df_list = []

    # df1: COPAY_HOTEL
    df1 = df.select(*select_columns,
                    (F.col("currencyisocode")),
                    (F.col("copay_hotel_merc__c")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_htl")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd_copay")).alias("val_typ_cd"),
                    (F.col("value_id_copay_hotel")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_hotel")).alias("value_purpose_secondary_cd"))
    df_list.append(df1)
    # df2: COPAY_GROUND_TRANSPORT
    df2 = df.select(*select_columns,
                    (F.col("currencyisocode")),
                    (F.col("copay_ground_transport_merc__c")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_trnsprtn")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd_copay")).alias("val_typ_cd"),
                    (F.col("value_id_copay_grand_transport")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_ground_transportation")).alias("value_purpose_secondary_cd"))
    df_list.append(df2)
    # df3: COPAY_FOOD_AND_BEVERAGE
    df3 = df.select(*select_columns,
                    (F.col("currencyisocode")),
                    (F.col("copay_food_beverage_merc__c")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_food")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd_copay")).alias("val_typ_cd"),
                    (F.col("value_id_copay_food_beverage")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_food_beverage")).alias("value_purpose_secondary_cd"))
    df_list.append(df3)
    # df4: COPAY_REGISTRATION
    df4 = df.select(*select_columns,
                    (F.col("currencyisocode")),
                    (F.col("copay_registration_merc__c")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_rgstrtn")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd_copay")).alias("val_typ_cd"),
                    (F.col("value_id_copay_registration_fee")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_registration_fee")).alias("value_purpose_secondary_cd"))
    df_list.append(df4)
    # df5: COPAY_FLIGHT_AND_RAIL
    df5 = df.select(*select_columns,
                    (F.col("currencyisocode")),
                    (F.col("copay_flight_rail_merc__c")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_trnsprtn")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd_copay")).alias("val_typ_cd"),
                    (F.col("value_id_copay_rail_flight_transport")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_rail_flight_transportation")).alias("value_purpose_secondary_cd"))
    df_list.append(df5)
    # df6: HOTEL
    df6 = df.select(*select_columns,
                    (F.col("currencyisocode1_out")).alias("currencyisocode"),
                    (F.col("hotel_tov_merc__c_out")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_htl")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd")),
                    (F.col("value_id_hotel")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_hotel")).alias("value_purpose_secondary_cd"))
    df_list.append(df6)
    # df7: GROUND_TRANSPORT
    df7 = df.select(*select_columns,
                    (F.col("currencyisocode1_out")).alias("currencyisocode"),
                    (F.col("grnd_transp_merc__c_out")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_trnsprtn")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd")),
                    (F.col("value_id_grand_transport")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_ground_transportation")).alias("value_purpose_secondary_cd"))
    df_list.append(df7)
    # df8: REGISTRATION
    df8 = df.select(*select_columns,
                    (F.col("currencyisocode1_out")).alias("currencyisocode"),
                    (F.col("reg_amt_tov_merc__c_out")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_rgstrtn")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd")),
                    (F.col("value_id_registration_fee")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_registration_fee")).alias("value_purpose_secondary_cd"))
    df_list.append(df8)
    # df9: FOOD_AND_BEVERAGE
    df9 = df.select(*select_columns,
                    (F.col("currencyisocode1_out")).alias("currencyisocode"),
                    (F.col("food_bev_tov_merc__c_out")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_food")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd")),
                    (F.col("value_id_food_beverage")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_food_beverage")).alias("value_purpose_secondary_cd"))
    df_list.append(df9)
    # df10: INDIVIDUAL_TRANSPORT
    df10 = df.select(*select_columns,
                    (F.col("currencyisocode1_out")).alias("currencyisocode"),
                    (F.col("indv_transfer_merc__c_out")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_trnsprtn")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd")),
                    (F.col("value_id_individual_transport")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_individual_transport")).alias("value_purpose_secondary_cd"))
    df_list.append(df10)
    # df11: COPAY_INDIV_TRANSPORT
    df11 = df.select(*select_columns,
                    (F.col("currencyisocode")),
                    (F.col("copay_aods_indiv_trans_merc__c")).alias("value_amount"),
                    (F.col("val_typ_prps_cd_trnsprtn")).alias("val_typ_prps_cd"),
                    (F.col("val_typ_cd_copay")).alias("val_typ_cd"),
                    (F.col("value_id_copay_individual_transport")).alias("value_id"),
                    (F.col("value_purpose_secondary_code_individual_transport")).alias("value_purpose_secondary_cd"))
    df_list.append(df11)

    return df_list


def un_expense(df):
    """This function is used to union records"""
    # Rename columns
    df_renamed = df.withColumnRenamed("event_id_merc__c", "event_id_mercury__c") \
        .withColumnRenamed("date_of_event_merc__c", "date_of_evnt_merc__c") \
        .withColumnRenamed("owner_master_id_merc__c_out", "owner_master_id_merc__c") \
        .withColumnRenamed("name", "event_desc") \
        .withColumnRenamed("customer_id_glbl__c", "customer_id_global__c") \
        .withColumnRenamed("name_glbl__c", "name_global__c") \
        .withColumnRenamed("record_type_name__c", "record_type")

    select_columns = ["event_id_mercury__c",
                      "event_country_merc__c",
                      "date_of_evnt_merc__c",
                      "owner_master_id_merc__c",
                      "event_desc",
                      "meeting_participant_id_merc__c",
                      "customer_id_global__c",
                      "name_global__c",
                      "record_type",
                      "valuepaiddt"]

    # Split dataframe into multiple dataframes
    df_list = split_dataframe(df_renamed, select_columns)

    # Union dataframes
    df_final = df_list[0]
    for df_initial in df_list[1:]:
        df_final = df_final.union(df_initial)

    return df_final


def exp_collate(df):
    """This function is used to apply expression collate"""
    df_exp_collate = df.select((F.lit('LLY')).alias("COMPANYCD"),
        (F.lit('MERCURYMS')).alias("SRCSYSCD"),
        (F.lit('MEETINGSPEND')).alias("SRCSYSTYPECD"),
        (F.lit('Meeting TOV')).alias("EVENTTYPECD"),
        (F.lit('LLY')).alias("EVENTPARENTCOMPANYCD"),
        (F.lit('MERCURY')).alias("EVENTPARENTSRCSYSCD"),
        (F.lit('Submitted')).alias("EVENTSTATUSCD"),
        (F.lit('Recipient')).alias("EVENTVALUECUSTOMEREVENTROLE"),
        F.col("event_id_mercury__c").alias("EVENT_ID_MERC__C"),
        F.col("event_country_merc__c").alias("EVENT_COUNTRY_MERC__C"),
        F.col("date_of_evnt_merc__c").alias("DATE_OF_EVENT_MERC__C"),
        F.col("owner_master_id_merc__c").alias("OWNER_MASTER_ID_MERC__C"),
        F.col("meeting_participant_id_merc__c").alias("MEETING_PARTICIPANT_ID_MERC__C"),
        F.col("customer_id_global__c").alias("CUSTOMER_ID_GLBL__C"),
        F.col("name_global__c").alias("NAME_GLBL__C"),
        F.col("currencyisocode").alias("CURRENCYISOCODE"),
        F.col("value_amount").alias("VALUE_AMOUNT"),
        F.col("val_typ_prps_cd").alias("VALUE_TYP_PRPS_CD"),
        F.col("val_typ_cd").alias("VALUE_TYP_CD"),
        F.col("value_id").alias("VALUE_ID"),
        F.col("value_purpose_secondary_cd").alias("VALUE_PURPOSE_SECONDARY_CD"),
        (F.lit('Attended')).alias("EVENT_VALUE_CUSTOMER_STATUS_CD"),
        (F.lit('HCP')).alias("EVENT_CUSTOMER_TYPE_CD"),
        F.col("valuepaiddt").alias("ValuePaidDt"),
        F.col("event_desc").alias("EVENT_DESC"))

    return df_exp_collate


# pylint: disable=too-many-statements,too-many-locals
def transform_df(df):
    """
    DataFrame transformation to get the desired Structure and fields
    """
    try:
        # Apply expression constant value
        df_exp_constant_value = exp_constant_value(df)

        # Union records
        df_un_expense = un_expense(df_exp_constant_value)

        # Filter out the records having NULL as Value Amount
        df_filtered = df_un_expense.filter(
			F.col("value_amount").isNotNull() & (F.trim(F.col("value_amount")) != ''))

        # Remove duplicates
        df_no_duplicates = df_filtered.dropDuplicates()

        # Apply expression collate
        df_exp_collate = exp_collate(df_no_duplicates)

        # Final transformed dataframe
        final_df = df_exp_collate.select(F.col("COMPANYCD").alias("CompanyCd"),
                        F.col("SRCSYSCD").alias("SrcSysCd"),
                        F.col("EVENT_COUNTRY_MERC__C").alias("CountryCd"),
                        F.col("SRCSYSTYPECD").alias("SrcSysTypeCd"),
                        F.col("MEETING_PARTICIPANT_ID_MERC__C").alias("EventId"),
                        F.col("DATE_OF_EVENT_MERC__C").alias("EventStartDt"),
                        F.col("EVENTTYPECD").alias("EventTypeCd"),
                        F.col("EVENT_DESC").alias("EventDesc"),
                        F.col("EVENT_ID_MERC__C").alias("EventParentId"),
                        F.col("EVENTPARENTCOMPANYCD").alias("EventParentCompanyCD"),
                        F.col("EVENTPARENTSRCSYSCD").alias("EventParentSrcSysCd"),
                        F.col("EVENT_COUNTRY_MERC__C").alias("EventParentCountryCd"),
                        F.col("EVENTSTATUSCD").alias("EventStatusCd"),
                        F.col("OWNER_MASTER_ID_MERC__C").alias("EventCreatedById"),
                        F.col("MEETING_PARTICIPANT_ID_MERC__C").alias("EventValueCustomerId"),
                        F.col("EVENTVALUECUSTOMEREVENTROLE").alias("EventValueCustomerEventRole"),
                        F.col("EVENT_VALUE_CUSTOMER_STATUS_CD").alias("EventValueCustomerStatusCd"),
                        F.col("EVENT_CUSTOMER_TYPE_CD").alias("EventCustomerTypeCd"),
                        F.col("CUSTOMER_ID_GLBL__C").alias("EventCustomerInternalId"),
                        F.col("NAME_GLBL__C").alias("EventCustomerNm"),
                        F.col("VALUE_ID").alias("ValueId"),
                        F.col("VALUE_TYP_CD").alias("ValueTypeCode"),
                        F.col("VALUE_TYP_PRPS_CD").alias("ValuePurposeCd"),
                        F.col("VALUE_PURPOSE_SECONDARY_CD").alias("ValuePurposeSecondaryCd"),
                        F.col("ValuePaidDt").alias("ValuePaidDt"),
                        F.col("VALUE_AMOUNT").alias("ValueAmount"),
                        F.col("CURRENCYISOCODE").alias("ValueCurrencyCD"))

        return final_df

    except Exception as exp:  # pylint: disable=broad-except
        logger.error("Error in the function: %s", str(exp))
        return None


def move_from_raw_to_archieve(bucket_name,directory_path,archive_path):
    """
    Move the file from source to Archive
    """
    try:
        logger.info('init move_from raw to archieve')
        # logger.info(directory_path)
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
            # logger.info(key)
            # Only add keys that are immediate children of the prefix
            if len(key[len(source):].split('/')) >= 2:
                files.append(key)
        # logger.info(files)
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
        logger.info('exit move_from raw to archieve')
        return True
    except Exception as e: # pylint: disable=broad-except
        logger.error("move_from_s3_to_s3 exception :: %s", e)
        return False


def store_flat_file_in_s3(df,bucket_name,folder_name,file_name):
    """
    Uploads a DataFrame as a CSV file to a specified S3 bucket and folder.
    """
    csv_str = df.toPandas().to_csv(sep='\t', index=False)
    # Initialize S3 client
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        delete_response = s3.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': objects_to_delete}
        )
        deleted = delete_response.get('Deleted', [])
        if deleted:
            logger.info("file is deleted")
        else:
            logger.info("file is not deleted")
    # Get formatted datetime string
    # current_datetime = datetime.now()
    # formatted_datetime = current_datetime.strftime("%Y%m%d_%H%M%S")
    # # Define S3 bucket and object key
    # object_key = f'{folder_name}/{file_name}_{formatted_datetime}.tsv'
    object_key = f'{folder_name}/{file_name}.tsv'
    # Upload CSV string to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_str.encode('utf-8'))
    # logger.info("File uploaded to S3 bucket %s with key %s", bucket_name, object_key)

def delete_old_files(bucket_name, prefix=""):
    """
    Delete files in an S3 bucket that are older than 100 days.
 
    :param bucket_name: Name of the S3 bucket.
    :param prefix: (Optional) Prefix to filter files within the bucket.
    """
    # Initialize S3 client
    s3_client = boto3.client("s3")
    # Get the current date and calculate the threshold date
    threshold_date = datetime.now(timezone.utc) - timedelta(days=100)

    try:
        # List all objects in the bucket with the given prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response:
            for obj in response["Contents"]:
                file_key = obj["Key"]
                last_modified = obj["LastModified"]
                # Check if the file is older than 100 days
                if last_modified < threshold_date:
                    # logger.info(f"Deleting {file_key}, Last Modified: {last_modified}")
                    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        else:
            logger.info("No files found in the bucket with the specified prefix.")
    except Exception as e:
        logger.error("Error occurred: %s", e)
        raise e

def get_query_from_s3(file_key):
    """
    This Function is to get query from sql file in s3 bucket
    """
    response = s3_client.get_object(Bucket=rawbucket, Key=file_key)
    sql_script = response['Body'].read().decode('utf-8')

    # Split the SQL script into individual queries
    queries = [query.strip() for query in sql_script.split(';') if query.strip()]
    return queries[0]


def main():
    """
    Executes the main workflow of the application.
    """
    try:
        config_common = {}
        config_common = init_config(config_common)
        # logger.info("Config common values in main: %s", config_common)
        target_folder_cms = args['target_folder_cms']
        target_folder_spnd = args['target_folder_spnd']
        target_file_name = args['target_file_name']
        archive_folder = args['archive_folder']
        bucket_name = rawbucket
        sql_file_location = args['sql_file_location']

        # Move to archive folder
        move_from_raw_to_archieve(bucket_name, target_folder_cms, archive_folder)
        move_from_raw_to_archieve(bucket_name, target_folder_spnd, archive_folder)

        # To get query to a variable from s3 bucket and create dataframe
        query = get_query_from_s3(sql_file_location)
        df = querydb(query)

        # Transform df and drop duplicates
        trans_df = transform_df(df)
        trans_df = trans_df.dropDuplicates()
        target_file_name = target_file_name + datetime.now().strftime('%Y%m%d_%H%M%S')

        # Store in flat file
        # storing flatfile in aods path
        store_flat_file_in_s3(trans_df,bucket_name,target_folder_spnd,target_file_name)

        # storing flatfile in cms path, because we dont have read access in aods
        # with this we can maintain archive with fn : move_from_raw_to_archieve(a,b,c)
        store_flat_file_in_s3(trans_df,bucket_name,target_folder_cms,target_file_name)

        # we retrive in archive only for some days and we delete it
        delete_old_files(bucket_name, prefix=archive_folder)
        delete_old_files(bucket_name, prefix=target_folder_cms)

    except Exception as exp:  # pylint: disable=broad-except
        logger.error("Error in main function: %s", str(exp))
    finally:
        logger.info("gso_ob_act_spnd Glue job completed")


main()
job.commit()
