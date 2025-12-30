"""
Rishabh Jain
Code to perform Transformation and Load data in gso_prod_grp_stg table
"""
import sys
import logging
import json
from datetime import datetime
import boto3
import psycopg2
from botocore.exceptions import ClientError
from psycopg2 import extras
# from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ShortType,
    DecimalType, FloatType, DoubleType, BooleanType, BinaryType, DateType, TimestampType
)
from pyspark.sql import SparkSession

# Initialize Spark session

ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")
args = getResolvedOptions(sys.argv, ['host','db','port','username','password',\
                                     'JOB_NAME','batch_name','region_name','param_name',\
                                     'common_param_name','abc_fetch_param_function',\
                                     'abc_log_stats_func','rds_schema_name','raw_bucket',\
                                     'logging_level','load_metadata_file_path'])
level=args['logging_level']
# Configure the logger
logging.basicConfig(
    level=args['logging_level'],
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.info("Starting AWS Glue job")
host = args["host"]
db = args["db"]
port = args["port"]
username = args["username"]
password = args["password"]
db_conn_dict = {}
db_conn_dict['username'] = username

rdsschemaname = args["rds_schema_name"]
JOB_NAME = args["JOB_NAME"]
batch_name = args["batch_name"]
region_name = args["region_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
rawbucket = args["raw_bucket"]
config_common = {}
url = f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require"
class SecretRetrievalError(Exception):
    """Custom exception for secret retrieval errors."""
try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
    db_conn_dict['password'] = password
except ClientError as e:
  # Handle potential errors during retrieval
    raise SecretRetrievalError(f'Error retrieving secret from Secrets Manager: {e}') from e
s3_client = boto3.client("s3")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

class ConnectDBError(Exception):
    """Custom exception for database connection errors."""
def connect_db():
    """Creates DB connection for the object
    Parameter:
        db_details (dict): Database details
    """
    try:
        conn = psycopg2.connect(database = db,user = username, host= host,password = password,\
                                port = port)
        cur = None
        cur = conn.cursor(
            cursor_factory=extras.RealDictCursor)
        return conn,cur
    except ConnectDBError as error:
        logger.error("Error in connect_db: %s", error)
        return None
class ExecuteQueryError(Exception):
    """Custom exception for query execution errors."""
def execute(conn, cur, query):
    """Execute SQL query.
    Parameters:
        conn : Database connection object
        cur : Cursor object
        query : The query to be processed
    """
    try:
        cur.execute(query)
        conn.commit()
        if query.strip().lower().startswith("select"):
            records = cur.fetchall()
            output = json.loads(json.dumps(records))
            return output
        if query.strip().lower().startswith("update") or query.strip().lower()\
            .startswith("delete"):
            return cur.rowcount
        return "Query executed successfully."
    except psycopg2.Error as e:
        logger.error("PostgreSQL error: %s", e)
        conn.rollback()
        return f"PostgreSQL Error: {e}"
    except ExecuteQueryError as e:
        logger.error("Unknown error: %s", e)
        conn.rollback()
        return f"Unknown Error: {e}"

def init_config(config_common):
    """
    Initialize config and glue job
    """
    try:
        lambda_client = boto3.client("lambda", region_name="us-east-2")
        job_name = 'gso_prod_grp_stg'
        # Step 1: Insert Batch Execution Status
        abc_batch_execution = {"operation_type": "insert", "status": "R", "batch_name": batch_name}
        invoke_response_status = lambda_client.invoke(
            FunctionName="edb_abc_log_batch_status",
            Payload=json.dumps(abc_batch_execution).encode("utf-8")
        )
        batch_execution_status_id = json.loads(invoke_response_status["Payload"].read())
        batch_execution_id = batch_execution_status_id.get('batch_execution_id')
        # Step 2: Insert Process Execution Status
        abc_log_process_status = {
            "operation_type": "insert", 
            "status": "R",
            "batch_execution_id": batch_execution_id,
            "job_name": job_name
        }
        invoke_response_status = lambda_client.invoke(
            FunctionName="edb_abc_log_process_status",
            Payload=json.dumps(abc_log_process_status).encode("utf-8")
        )
        log_process_status_id = json.loads(invoke_response_status["Payload"].read())
        process_execution_id = log_process_status_id.get('process_execution_id')

        # Step 3: Invoke Lambda to fetch config params
        json_payload = {
            "batch_name": batch_name,
            "job_name": job_name
        }
        lambda_client = boto3.client('lambda', region_name=region_name)
        invoke_response = lambda_client.invoke(
        FunctionName=abc_fetch_param_function,
        Payload=json.dumps(json_payload))
        config_obj = json.loads(invoke_response['Payload'].read())
        # Step 4: Update config_common with the fetched data and IDs
        config_common.update(config_obj)
        config_common["job_name"] = job_name
        config_common["batch_name"] = batch_name
        config_common["process_execution_id"] = process_execution_id
        config_common["abc_log_stats_func"] = abc_log_stats_func
        config_common["batch_execution_id"] = batch_execution_id
        return config_common
    except Exception as exep_func:
        logger.error(f"Error in batch management and config initialization: {str(exep_func)}")
        raise exep_func

class QueryDBError(Exception):
    """Custom exception for query errors."""
def querydb(query):
    """Execute the query and return the result as a PySpark DataFrame."""
    try:
        if ";" in query:
            query = query.replace(";", "")
        df = (
            spark.read.format("jdbc")
            .options(
                url=url,
                user=db_conn_dict["username"],
                password=db_conn_dict["password"],
                dbtable=f"({query}) AS union_query",  # Enclose the query and give it an alias
            )
            .load()
        )
        return df
    except QueryDBError as exp:
        logger.error("Exception in sql queries: %s", exp)
        return "No Data"
#Ingesting data by Truncate and Load
def ingest_data(df, tname,truncate,mode = 'append'):
    """This Function ingestion data to postgresdb"""
    try:
        if truncate:
            conn , curr = connect_db()
            query = f"TRUNCATE TABLE {rdsschemaname}.{tname}"
            curr.execute(query)
            conn.commit()
            curr.close()
            conn.close()
        df = df.toDF(*[col.lower() for col in df.columns])
        load_table_name = f'{rdsschemaname}.{tname}'
        connection_properties= {
            "user": db_conn_dict['username'],
            "password": db_conn_dict['password'],
            "driver": "org.postgresql.Driver",
             "batchsize": "100"}
        df.write.jdbc(url=url, table=load_table_name,mode=mode, properties=connection_properties)
    except Exception as e:
        ex=str(e).replace('"','').replace("'","")
        logger.error(ex)
        raise IngestionException(f"ingest_data_in_postgres exception :: {e}" ) from e
class IngestionException(Exception):
    """Custom exception for ingestion errors."""
class LookupFunctionError(Exception):
    """Custom exception for lookup errors."""

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
        logger.error(f"Rollback failed due to Error: {str(ex)}")
        raise ex
#Load History
def loadhistorytable(conn,cursor, tgt_table,stg_table,exec_id):
    """
    Function to load the history table"""
    try:
        updt_query = ("UPDATE <schema>.<tgt_table> a SET is_active= 'N', updated_ts = current_timestamp where prod_grp_external_id_glbl__c in (select a.prod_grp_external_id_glbl__c from <schema>.<tgt_table> a inner join <schema>.<stg_table> b on a.prod_grp_external_id_glbl__c=b.prod_grp_external_id_glbl__c) and a.is_active = 'Y'")

        rfnd_insrt_query= ("INSERT INTO <schema>.<tgt_table> (sfdc_id, product_group_id_glbl__c, src_cd_glbl__c, name, grp_abbr_nm_glbl__c, prod_grp_desc_txt_glbl__c, grp_prps_cd_glbl__c, grp_cnvrsn_unit_cd_glbl__c, start_dt_glbl__c, end_dt_glbl__c, crt_dt_glbl__c, updt_dt_glbl__c, lst_pblsh_dt_glbl__c, cease_pblsh_dt_glbl__c, cntry_cd_glbl__c, dialect_nm_glbl__c, trnsltd_grp_nm_glbl__c, prod_grp_external_id_glbl__c, prod_grp_typ_cd_glbl__c, glbl_parent_product_group__c, is_active, created_ts, updated_ts, updated_by_job_exec_id, created_by_job_exec_id)(select sfdc_id, product_group_id_glbl__c, src_cd_glbl__c, name, grp_abbr_nm_glbl__c, prod_grp_desc_txt_glbl__c, grp_prps_cd_glbl__c, grp_cnvrsn_unit_cd_glbl__c, start_dt_glbl__c, end_dt_glbl__c, crt_dt_glbl__c, updt_dt_glbl__c, lst_pblsh_dt_glbl__c, cease_pblsh_dt_glbl__c, cntry_cd_glbl__c, dialect_nm_glbl__c, trnsltd_grp_nm_glbl__c, prod_grp_external_id_glbl__c, prod_grp_typ_cd_glbl__c, glbl_parent_product_group__c, 'Y'::varchar AS is_active, current_timestamp::timestamp as created_ts, current_timestamp::timestamp as updated_ts,<process_execution_id> as created_by_job_exec_id, <process_execution_id> as updated_by_job_exec_id FROM <schema>.<stg_table>);")
        # Insert into temp
        
        updt_query = updt_query.replace('<schema>',rdsschemaname) \
                                        .replace('<tgt_table>',tgt_table) \
                                        .replace('<stg_table>', stg_table)
        cursor.execute(updt_query)
        conn.commit()
        rfnd_insrt_query = rfnd_insrt_query \
                                    .replace('<process_execution_id>' ,str(exec_id))\
                                    .replace('<schema>',rdsschemaname) \
                                    .replace('<tgt_table>',tgt_table) \
                                    .replace('<stg_table>', stg_table)
        cursor.execute(rfnd_insrt_query)
        conn.commit()
    except ValueError as exp_msg:
        logger.error("Error while writing data ", str(exp_msg))
        rollback_database_table_process(rdsschemaname, tgt_table,
                                        exec_id, cursor, conn)
        raise exp_msg
        #Reading data from db
    except ValueError as ex:
        logger.error("Error while creating cursor", str(ex))
    finally:
        if cursor is not None:
            cursor.close()

def transform_GP(src,df_dict):
    #rtr_split_glbl_cntry_spcfc_records
    # Split the source DataFrame into two different DataFrames based on the condition
    GP = src.filter(src['CNTRY_CD'] == 'ZZ')
    backup_GP = GP
    #exp_global_product_dflt_values
    # Select the PROD_GRP_ID column from GP DataFrame
    GP = GP.withColumn("country_cd_for_trnsltn_out", F.lit("US"))    
    GP = GP.withColumn("dialect_nm_out", F.lit("EN"))
    #lkp_prod_grp_trnsltns_vw1
    GP = GP.withColumnRenamed("PROD_GRP_ID", "prod_grp_prnt_id")\
            .withColumnRenamed("CNTRY_CD", "cntry_cd_in")
    GP = GP.join(df_dict['lkp_prod_grp_trnsltns_vw1'], (GP['PROD_GRP_PRNT_ID'] == df_dict['lkp_prod_grp_trnsltns_vw1']['PROD_GRP_ID']) & (GP['country_cd_for_trnsltn_out'] == df_dict['lkp_prod_grp_trnsltns_vw1']['CNTRY_CD']) & (GP['dialect_nm_out'] == df_dict['lkp_prod_grp_trnsltns_vw1']['DIALECT_NM']), "left").drop("PROD_GRP_ID","DIALECT_NM","CNTRY_CD")
    # GP = GP.drop("PROD_GRP_ID","DIALECT_NM","CNTRY_CD")
    #unioning global products for main union (un_PROD_GRP) - Global_Country_For_Prod_Group_Coutry_not_in_Source
    GP = GP.withColumnRenamed("DIALECT_NM_out", "dialect_nm").withColumnRenamed("cntry_cd_in", "cntry_cd").withColumnRenamed("prod_grp_typ", "prod_grp_typ_cd")
    columns_to_select = [
                        "PROD_GRP_PRNT_ID",
                        "SRC_CD",
                        "GRP_NM",
                        "GRP_ABBR_NM",
                        "PROD_GRP_DESC_TXT",
                        "GRP_PRPS_CD",
                        "GRP_CNVRSN_UNIT_CD",
                        "START_DT",
                        "END_DT",
                        "CRT_DT",
                        "UPDT_DT",
                        "LST_PBLSH_DT",
                        "CEASE_PBLSH_DT",
                        "CNTRY_CD",
                        "DIALECT_NM",
                        "TRNSLTD_GRP_NM",
                        "PROD_GRP_TYP_CD"
                    ]

    # Select the columns from the DataFrame
    Global_Country_For_Prod_Group_Coutry_not_in_Source = GP.select(*columns_to_select)
    return Global_Country_For_Prod_Group_Coutry_not_in_Source


def transform_CSP(src,df_dict):
    #routing the table according to the cntry_cd
    CSP = src.filter(src['CNTRY_CD'] != 'ZZ')
    #lkp_gso_country_dialect
    df = df_dict['lkp_gso_country_dialect']\
        .select(df_dict['lkp_gso_country_dialect']['COUNTRY_CODE'].alias("lookup_country_code"),'dialect')
    CSP = CSP.join(df, CSP['CNTRY_CD'] == df['lookup_country_code'], "left").drop("lookup_country_code")
    #lkp_prod_grp_trnsltns_vw
    df = df_dict['lkp_prod_grp_trnsltns_vw'].\
        select(df_dict["lkp_prod_grp_trnsltns_vw"]['PROD_GRP_ID'].alias("lookup_prod_grp_id"),\
               df_dict["lkp_prod_grp_trnsltns_vw"]['CNTRY_CD'].alias("lookup_cntry_cd"),\
                df_dict["lkp_prod_grp_trnsltns_vw"]['DIALECT_NM'].alias("dialect_nm_pg_trn"),\
                    df_dict["lkp_prod_grp_trnsltns_vw"]['END_DT'].alias("end_dt_pg_trns"),\
                  'TRNSLTD_GRP_NM' )
    CSP = CSP.join(df, (CSP['PROD_GRP_ID'] == df['lookup_prod_grp_id']) & (CSP['CNTRY_CD'] == df['lookup_cntry_cd']) & (CSP['DIALECT'] == df['DIALECT_NM_PG_TRN']), "left").drop("lookup_prod_grp_id","lookup_cntry_cd")
    #renaming columns for easy understanding in exp_set_variable
    CSP = CSP.withColumnRenamed("DIALECT", "dialect_gso") 
    # Create a new column o_END_DT with the condition
    CSP = CSP.withColumn("o_END_DT", F.when(F.col("END_DT_PG_TRNS") < F.col("END_DT"), F.col("END_DT_PG_TRNS")).otherwise(F.col("END_DT")))
    #create column DIALECT_NM_out with the condition IIF(ISNULL(DIALECT_NM_PG_TRN),'EN',DIALECT_NM_PG_TRN)
    CSP = CSP.withColumn("DIALECT_NM_out", F.when(F.isnull(F.col("DIALECT_NM_PG_TRN")), "EN").otherwise(F.col("DIALECT_NM_PG_TRN")))
    CSP = CSP.withColumnRenamed("prod_grp_typ", "prod_grp_typ_cd")
    #storing this in another df so as to pass using filter_non_matching_dialects
    filter_CSP = CSP
    
    #filter_non_matching_dialects column renaming
    # Rename o_END_DT column to END_DT
    filter_CSP = filter_CSP.drop("end_dt")
    filter_CSP = filter_CSP.withColumnRenamed("o_END_DT", "END_DT")\
                           .withColumnRenamed("DIALECT_NM_out", "PG_TRN_DIALECT_NM")\
                           
    filter_CSP = filter_CSP.filter(filter_CSP.PG_TRN_DIALECT_NM == filter_CSP.dialect_gso)
    columns_to_select = [
                        "PROD_GRP_ID",
                        "SRC_CD",
                        "GRP_NM",
                        "GRP_ABBR_NM",
                        "PROD_GRP_DESC_TXT",
                        "GRP_PRPS_CD",
                        "GRP_CNVRSN_UNIT_CD",
                        "START_DT",
                        "END_DT",
                        "CRT_DT",
                        "UPDT_DT",
                        "LST_PBLSH_DT",
                        "CEASE_PBLSH_DT",
                        "CNTRY_CD",
                        "PG_TRN_DIALECT_NM",
                        "TRNSLTD_GRP_NM",
                        "PROD_GRP_TYP_CD"
                    ]
    
    

    # Select the columns from the DataFrame
    filter_CSP = filter_CSP.select(*columns_to_select)
    Multiple_Country = filter_CSP.withColumnRenamed("PROD_GRP_ID", "PROD_GRP_PRNT_ID").withColumnRenamed("PG_TRN_DIALECT_NM", "DIALECT_NM")
    # Show the resulting DataFrame (optional)


    # srt_prod_grp_country_cd
    CSP = CSP.withColumnRenamed("DIALECT_NM_out", "DIALECT_NM")
    CSP = CSP.withColumnRenamed("PROD_GRP_ID", "PROD_GRP_PRNT_ID")
    # List of columns to select
    columns_to_select = [
        "PROD_GRP_PRNT_ID", "SRC_CD", "GRP_NM", "GRP_ABBR_NM", 
        "PROD_GRP_DESC_TXT", "GRP_PRPS_CD", "GRP_CNVRSN_UNIT_CD", 
        "START_DT", "END_DT", "CRT_DT", "UPDT_DT", "LST_PBLSH_DT", 
        "CEASE_PBLSH_DT", "CNTRY_CD", "DIALECT_NM", "TRNSLTD_GRP_NM", 
        "PROD_GRP_TYP_CD"
    ]

    # Select the columns from the CSP DataFrame
    csp_selected_df = CSP.select(*columns_to_select)
    # Sort the DataFrame by PROD_GRP_TYP_CD and PROD_GRP_PRNT_ID columns in ascending order
    csp_selected_df = csp_selected_df.sort(["PROD_GRP_TYP_CD", "PROD_GRP_PRNT_ID"])
    #agg_global_records
    # Perform group by operation and calculate max(end_dt) as end_dt_out
    grouped_df = (csp_selected_df
        .groupBy(["PROD_GRP_PRNT_ID", "PROD_GRP_TYP_CD"])
        .agg(
            F.max("END_DT").alias("END_DT_out"),  # Aggregating END_DT as END_DT_out
            F.first("PROD_GRP_DESC_TXT").alias("PROD_GRP_DESC_TXT"),
            F.first("SRC_CD").alias("SRC_CD"),  # Keeping the first value for other columns
            F.first("GRP_PRPS_CD").alias("GRP_PRPS_CD"),
            F.first("GRP_CNVRSN_UNIT_CD").alias("GRP_CNVRSN_UNIT_CD"),
            F.first("GRP_NM").alias("GRP_NM"),
            F.first("GRP_ABBR_NM").alias("GRP_ABBR_NM"),
            F.first("START_DT").alias("START_DT"),
            F.first("CRT_DT").alias("CRT_DT"),
            F.first("UPDT_DT").alias("UPDT_DT"),
            F.first("LST_PBLSH_DT").alias("LST_PBLSH_DT"),
            F.first("CEASE_PBLSH_DT").alias("CEASE_PBLSH_DT"),
            F.first("CNTRY_CD").alias("CNTRY_CD"),
            F.first("DIALECT_NM").alias("DIALECT_NM"),
            F.first("TRNSLTD_GRP_NM").alias("TRNSLTD_GRP_NM")
        ))

# Add constant values for CNTRY_CD and DIALECT_NM
    final_df = grouped_df.withColumn("CNTRY_CD", F.lit("ZZ")) \
                     .withColumn("DIALECT_NM", F.lit("EN"))

    
    # Show the resulting DataFrame (optional)
    Global_Country_For_Prod_Group_Coutry_in_Source=final_df
    columns_to_select = [
        "PROD_GRP_PRNT_ID", "SRC_CD", "GRP_NM", "GRP_ABBR_NM", 
        "PROD_GRP_DESC_TXT", "GRP_PRPS_CD", "GRP_CNVRSN_UNIT_CD", 
        "START_DT", "END_DT_out", "CRT_DT", "UPDT_DT", "LST_PBLSH_DT", 
        "CEASE_PBLSH_DT", "CNTRY_CD", "DIALECT_NM", "TRNSLTD_GRP_NM", 
        "PROD_GRP_TYP_CD"
    ]
    Global_Country_For_Prod_Group_Coutry_in_Source = Global_Country_For_Prod_Group_Coutry_in_Source.select(*columns_to_select)
    union_df = Global_Country_For_Prod_Group_Coutry_in_Source.union(Multiple_Country)
    return union_df

def union_df(src,df_dict,data,exec_id):
    df1 = transform_GP(src,df_dict)
    df2 = transform_CSP(src,df_dict)

    df = df1.union(df2)
    df = filter_competitor_and_null_pg_global(df,data)
    #aggr_unique _value
    df = (df
        .groupBy(["PROD_GRP_PRNT_ID", "CNTRY_CD"])
        .agg(
            F.max("END_DT").alias("END_DT_out"),  # Aggregating END_DT as END_DT_out
            F.first("PROD_GRP_DESC_TXT").alias("PROD_GRP_DESC_TXT"),
            F.first("PROD_GRP_TYP_CD").alias("PROD_GRP_TYP_CD"),
            F.first("SRC_CD").alias("SRC_CD"),  # Keeping the first value for other columns
            F.first("GRP_PRPS_CD").alias("GRP_PRPS_CD"),
            F.first("GRP_CNVRSN_UNIT_CD").alias("GRP_CNVRSN_UNIT_CD"),
            F.first("GRP_NM").alias("GRP_NM"),
            F.first("GRP_ABBR_NM").alias("GRP_ABBR_NM"),
            F.first("START_DT").alias("START_DT"),
            F.first("CRT_DT").alias("CRT_DT"),
            F.first("UPDT_DT").alias("UPDT_DT"),
            F.first("LST_PBLSH_DT").alias("LST_PBLSH_DT"),
            F.first("CEASE_PBLSH_DT").alias("CEASE_PBLSH_DT"),
            F.first("DIALECT_NM").alias("DIALECT_NM"),
            F.first("TRNSLTD_GRP_NM").alias("TRNSLTD_GRP_NM")
        ))  # Create column end_dt_out with max(END_DT)
    
    # Create a new column PROD_EXTERNAL_ID with the condition PROD_GRP_PRNT_ID||CNTRY_CD
    df = df.withColumn("PROD_EXTERNAL_ID",
                   F.concat(
                       F.col("PROD_GRP_PRNT_ID").cast('int'),
                       F.col("CNTRY_CD")
                   ))
    #lkp_pg_to_pg_vw
    df = df.join(df_dict['lkp_pg_to_pg_vw'], df['PROD_GRP_PRNT_ID'] == df_dict['lkp_pg_to_pg_vw']['PROD_GRP_ID_CHILD'], "left")
    df = df.drop("PROD_GRP_ID_CHILD")
    #rename_for_final_load
    df = df.withColumnRenamed("end_dt_out", "end_dt")
    df = rename_for_final_load(df)
    current_time=datetime.now()
    df=df.withColumn("is_active",F.lit("Y"))
    df=df.withColumn("updated_ts",F.lit(current_time))
    df=df.withColumn("created_ts",F.lit(current_time))
    df=df.withColumn("created_by_job_exec_id",F.lit(exec_id).cast('bigint'))
    df=df.withColumn("updated_by_job_exec_id",F.lit(exec_id).cast('bigint'))
    return df



def filter_competitor_and_null_pg_global(df,data):
    condition = (
    ((F.regexp_replace(F.col("PROD_GRP_TYP_CD"), "'", "") == data['cmpt_pg_typ']) & (F.col("CNTRY_CD") != 'ZZ')) |
    (F.regexp_replace(F.col("PROD_GRP_TYP_CD"), "'", "") == data['lilly']) |
    (F.regexp_replace(F.col("PROD_GRP_TYP_CD"), "'", "") == data['combo']) |
    (F.regexp_replace(F.col("PROD_GRP_TYP_CD"), "'", "") == data['collaborator']) |
    (F.col("PROD_GRP_TYP_CD").isNull() & (F.col("CNTRY_CD") != 'ZZ'))
    )

    # Apply the filter to the DataFrame
    filtered_df = df.filter(condition)

    return filtered_df


def rename_for_final_load(df):
    rename_mapping = {
    "PROD_GRP_PRNT_ID": "PRODUCT_GROUP_ID_GLBL__C",
    "SRC_CD": "SRC_CD_GLBL__C",
    "GRP_NM": "NAME",
    "GRP_ABBR_NM": "GRP_ABBR_NM_GLBL__C",
    "PROD_GRP_DESC_TXT": "PROD_GRP_DESC_TXT_GLBL__C",
    "GRP_PRPS_CD": "GRP_PRPS_CD_GLBL__C",
    "GRP_CNVRSN_UNIT_CD": "GRP_CNVRSN_UNIT_CD_GLBL__C",
    "START_DT": "START_DT_GLBL__C",
    "END_DT": "END_DT_GLBL__C",
    "CRT_DT": "CRT_DT_GLBL__C",
    "UPDT_DT": "UPDT_DT_GLBL__C",
    "LST_PBLSH_DT": "LST_PBLSH_DT_GLBL__C",
    "CEASE_PBLSH_DT": "CEASE_PBLSH_DT_GLBL__C",
    "CNTRY_CD": "CNTRY_CD_GLBL__C",
    "DIALECT_NM": "DIALECT_NM_GLBL__C",
    "TRNSLTD_GRP_NM": "TRNSLTD_GRP_NM_GLBL__C",
    "PROD_EXTERNAL_ID": "PROD_GRP_EXTERNAL_ID_GLBL__C",
    "PROD_GRP_TYP_CD": "PROD_GRP_TYP_CD_GLBL__C",
    "PROD_GRP_ID_PRNT":"GLBL_PARENT_PRODUCT_GROUP__C"
    }

    # Rename columns
    for old_col, new_col in rename_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)

    return df


#Flat File Storage
def store_flat_file_in_s3(curr,query,bucket_name,folder_name,file_name):
    df=querydb(query=query)
    csv_str = df.toPandas().to_csv(index=False)

    # Initialize S3 client
    s3 = boto3.client('s3')
    
    # Define S3 bucket and object key

    object_key = f'{folder_name}/{file_name}.csv'
    
    # Upload CSV string to S3
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_str.encode('utf-8'))


def get_query_from_s3(file_key):
    response = s3_client.get_object(Bucket=rawbucket, Key=file_key)
    sql_script = response['Body'].read().decode('utf-8')
    
    # Split the SQL script into individual queries
    queries = [query.strip() for query in sql_script.split(';') if query.strip()]
    return queries[0]

class MetadataFileError(Exception):
    """Custom exception for metadata file errors."""
def get_load_metadata(s3_path, load_param):
    "This function is used to get current load parameter"
    try:
        response = s3_client.get_object(Bucket=rawbucket, Key=s3_path)
        load_metadata = json.loads(response['Body'].read().decode('utf-8'))
        metadata_load_value = load_metadata["metadata_load_value"]
        return metadata_load_value
    except ClientError as err:
        #logger.error("Error in function get_load_metadata: %s", str(err))
        if err.response['Error']['Code'] == 'NoSuchKey':
            metadata_load_value = load_param
            return metadata_load_value
        raise MetadataFileError(f"Error in function get_load_metadata: {str(err)}") from err
def write_metadata_file(s3_path, load_param):
    "This function is used to write metadata file in s3"
    try:
        data = json.dumps({"metadata_load_value": f"{load_param}"})
        s3_client.put_object(Bucket=rawbucket, Key=s3_path, Body=data)
    except ClientError as err:
        #logger.error("Error in function write_metadata_file: %s", str(err))
        raise MetadataFileError(f"Error in function write_metadata_file: {str(err)}") from err
def list_to_sql_string(lst):
    return ", ".join([f"'{item}'" for item in lst])
def get_dataframes_from_s3(curr,data):
    """
    Get the dataframes from the SQL scripts in S3
    """
    paths = [
        "customer_meeting_services_system/sql/mods/gso_prod_grp_stg/lkp_pg_to_pg_vw.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_grp_stg/lkp_prod_grp_trnsltns_vw1.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_grp_stg/lkp_prod_grp_trnsltns_vw.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_grp_stg/sq_mtm_gh_prod_grp.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_grp_stg/lkp_gso_country_dialect.sql"
        ]
    df_dict ={}
    grp_prps_cd1_str = list_to_sql_string(data["grp_prps_cd1"])
    grp_prps_cd2_str = list_to_sql_string(data["grp_prps_cd2"])
    lly_pg_typ_str = list_to_sql_string(data["lly_pg_typ"])
    prod_load_date_str = f"'{data['prod_load_date']}'"
    cmpt_pg_typ_str = f"'{data['cmpt_pg_typ']}'"
    for sql_file in paths:
        # Extract the key from the file path
        key = sql_file.rsplit("/",maxsplit=1)[-1].replace(".sql", "")
        # Get the query from S3
        query = get_query_from_s3(sql_file)
        if key=="sq_mtm_gh_prod_grp":
            query = query.format(
                grp_prps_cd1=grp_prps_cd1_str, 
                grp_prps_cd2=grp_prps_cd2_str, 
                lly_pg_typ=lly_pg_typ_str, 
                prod_load_date=prod_load_date_str,
                cmpt_pg_typ = cmpt_pg_typ_str
            )
            query = query.replace("SYSDATE",'current_timestamp')
        # Execute the query and store the result in the dictionary  
        df_dict[key] = querydb(query)
    return df_dict
def main():
    try:
        config_common = {}
        data ={}
        config_common = init_config(config_common)
        current_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        load_param = str(config_common['edb_cms_gso_mods_batch_params']['PROD_LOAD_DATE'])
        data['prod_load_date'] = get_load_metadata(args['load_metadata_file_path'], load_param)
        process_execution_id=config_common["process_execution_id"]
        data['grp_prps_cd1'] = config_common['edb_cms_gso_mods_batch_params']['GRP_PRPS_CD1']
        data['grp_prps_cd2'] = config_common['edb_cms_gso_mods_batch_params']['GRP_PRPS_CD2']
        data['lly_pg_typ'] = config_common['edb_cms_gso_mods_batch_params']['LLY_PG_TYP']
        data['cmpt_pg_typ'] = config_common['edb_cms_gso_mods_batch_params']['CMPT_PG_TYP']
        data['lilly'] = config_common['edb_cms_gso_mods_batch_params']['LILLY']
        data['combo'] = config_common['edb_cms_gso_mods_batch_params']['COMBO']
        data['collaborator'] = config_common['edb_cms_gso_mods_batch_params']['COLLABORATOR']
        conn,curr = connect_db()
        t_name='gso_prod_grp_stg'
        hist_tname="gso_prod_grp"
        df_dict = get_dataframes_from_s3(curr,data)
        df = union_df(df_dict['sq_mtm_gh_prod_grp'],df_dict,data,process_execution_id)
        df = df.dropDuplicates()
        df = df.withColumn('product_group_id_glbl__c', \
                                    F.col('product_group_id_glbl__c').cast('int'))
        ingest_data(df,t_name,True)
        loadhistorytable(conn,curr,hist_tname,t_name,process_execution_id)    
        write_metadata_file(args['load_metadata_file_path'], current_date_str)    
    except Exception as exp:
        logger.error("Error in main function", str(exp))
        raise exp
    finally:
        spark.stop()
    

main()
job.commit()
