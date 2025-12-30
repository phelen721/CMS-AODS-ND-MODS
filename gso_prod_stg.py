"""
Abhiram Kalyan Madduru
Code to perform Transformation and Load data in gso_prod_stg table
"""
import sys
import logging
import json
from datetime import datetime
import boto3
import psycopg2
from botocore.exceptions import ClientError
from psycopg2 import extras
# from awsglue.transforms import *co
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, when, lit, col, concat_ws,broadcast
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ShortType,
    DecimalType, FloatType, DoubleType, BooleanType, BinaryType, DateType, TimestampType
)
from pyspark.sql import SparkSession
from functools import reduce
# Initialize Spark session

ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")
args = getResolvedOptions(sys.argv, ['host','db','port','username','password',\
                                     'job_name','batch_name','region_name','param_name',\
                                     'common_param_name','abc_fetch_param_function',\
                                     'abc_log_stats_func','rdsschemaname','rawbucket',\
                                     'logging_level','load_metadata_file_path','tgt_table','hist_table'])
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

rdsschemaname = args["rdsschemaname"]
job_name = args["job_name"]
batch_name = args["batch_name"]
region_name = args["region_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
rawbucket = args["rawbucket"]

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

def ingest_data(df, tname,truncate):
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
        df.write.jdbc(url=url, table=load_table_name,mode='append', properties=connection_properties)
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
        logger.error(ex)
        logger.error(f"Rollback failed due to Error")
        raise ex
#Load History
def loadhistorytable(conn,cursor, tgt_table,stg_table,exec_id):
    """
    Function to load the history table"""
    updt_query = ("UPDATE <schema>.<tgt_table> a SET is_active= 'N', updated_ts = current_timestamp where PRODUCT_EXTERNAL_ID__C in (select a.PRODUCT_EXTERNAL_ID__C from <schema>.<tgt_table> a inner join <schema>.<stg_table> b on a.PRODUCT_EXTERNAL_ID__C=b.PRODUCT_EXTERNAL_ID__C) and a.is_active = 'Y'")

    rfnd_insrt_query= ("INSERT INTO <schema>.<tgt_table>(sfdc_id, alternate_id__c, avg_rx_quantity__c, brnd_cd__c, cease_pblsh_dt__c, class_dvlpmnt_cd__c, collab_ind__c, cnvrsn_fctr__c, crt_dt__c, dflt_cnvrsn_fctr__c, dflt_cnvrsn_uom_cd__c, dfnd_dly_dsg__c, dmnd_dflt_cnvrsn_fctr__c, dmnd_dflt_cnvrsn_uom_cd__c, dmnd_dsg_ptncy_uom_cd__c, dmnd_pkg_cntnt_qty__c, dmnd_pkg_cntnt_uom_cd__c, dmnd_pkg_qty__c, dmnd_pkg_typ_cd__c, dmnd_pkg_uom_cd__c, dmnd_pkg_cntnt_prstn_abr_cd__c, dmnd_ptncy_dsg_qty__c, dmnd_ptncy_qty__c, dmnd_qty_ptncy_uom_cd__c, dmnd_salable_ind__c, dscnt_dscrptn_txt__c, dsgntd_indctr__c, dialect_nm__c, dsg_freq_cd__c, dsg_ptncy_uom_cd__c, dsg_uom_cd__c, edp_fmly_cd__c, edp_pkg_size_cd__c, edp_prod_cd__c, edctnl_mtrl_ctgry_cd__c, edctnl_mtrl_dscnt_pct__c, edctnl_mtrl_id__c, edctnl_mtrl_itm_typ__c, end_dt__c, eqvlncy_fctr__c, fda_pkg_size_cd__c, hcfa_unit_per_pkg__c, item_rgltry_sched_cd__c, lbl_cd__c, lst_lot_exprtn_dt__c, lst_pblsh_dt__c, mtrl_nbr__c, nhi_prc_lst_dt__c, nbr_of_doses__c, oral_admnstrn_cd__c, ordr_frm_flg__c, pkg_cntnt_prstn_abr_cd__c, pkg_cntnt_qty__c, pkg_cntnt_uom_cd__c, pkg_qty__c, pkg_typ_cd__c, pkg_uom_cd__c, ptncy_dsg_qty__c, ptncy_qty__c, prscptn_prod_cd__c, prft_ctr__c, cntry_cd__c, prod_disease_st_flg__c, product_external_id__c, product_group__c, prod_id__c, prod_long_desc__c, prod_nm__c, prod_typ__c, qty_ptncy_uom_cd__c, refrig_cd__c, rpkg_cd__c, salable_ind__c, salt_qty__c, smpl_invntry_var_pct__c, smpl_invntry_var_unit_qty__c, shpmnt_multi__c, src_cd__c, std_dot_cnvrsn_fctr__c, start_dt__c, trmntn_dt__c, thrptc_cd__c, trackable_item__c, trd_nm_id__c, trnsltd_dsply_nm__c, trnsltd_prod_desc_txt__c, unt_typ_cd__c, updt_dt__c, vet_unit_pkg_txt__c, vha_plus_flg__c, altrnt_id_type_cd__c, recordtype, is_active, created_ts, updated_ts, updated_by_job_exec_id, created_by_job_exec_id)(SELECT 	sfdc_id, alternate_id__c, avg_rx_quantity__c, brnd_cd__c, cease_pblsh_dt__c, class_dvlpmnt_cd__c, collab_ind__c, cnvrsn_fctr__c, crt_dt__c, dflt_cnvrsn_fctr__c, dflt_cnvrsn_uom_cd__c, dfnd_dly_dsg__c, dmnd_dflt_cnvrsn_fctr__c, dmnd_dflt_cnvrsn_uom_cd__c, dmnd_dsg_ptncy_uom_cd__c, dmnd_pkg_cntnt_qty__c, dmnd_pkg_cntnt_uom_cd__c, dmnd_pkg_qty__c, dmnd_pkg_typ_cd__c, dmnd_pkg_uom_cd__c, dmnd_pkg_cntnt_prstn_abr_cd__c, dmnd_ptncy_dsg_qty__c, dmnd_ptncy_qty__c, dmnd_qty_ptncy_uom_cd__c, dmnd_salable_ind__c, dscnt_dscrptn_txt__c, dsgntd_indctr__c, dialect_nm__c, dsg_freq_cd__c, dsg_ptncy_uom_cd__c, dsg_uom_cd__c, edp_fmly_cd__c, edp_pkg_size_cd__c, edp_prod_cd__c, edctnl_mtrl_ctgry_cd__c, edctnl_mtrl_dscnt_pct__c, edctnl_mtrl_id__c, edctnl_mtrl_itm_typ__c, end_dt__c, eqvlncy_fctr__c, fda_pkg_size_cd__c, hcfa_unit_per_pkg__c, item_rgltry_sched_cd__c, lbl_cd__c, lst_lot_exprtn_dt__c, lst_pblsh_dt__c, mtrl_nbr__c, nhi_prc_lst_dt__c, nbr_of_doses__c, oral_admnstrn_cd__c, ordr_frm_flg__c, pkg_cntnt_prstn_abr_cd__c, pkg_cntnt_qty__c, pkg_cntnt_uom_cd__c, pkg_qty__c, pkg_typ_cd__c, pkg_uom_cd__c, ptncy_dsg_qty__c, ptncy_qty__c, prscptn_prod_cd__c, prft_ctr__c, cntry_cd__c, prod_disease_st_flg__c, product_external_id__c, product_group__c, prod_id__c, prod_long_desc__c, prod_nm__c, prod_typ__c, qty_ptncy_uom_cd__c, refrig_cd__c, rpkg_cd__c, salable_ind__c, salt_qty__c, smpl_invntry_var_pct__c, smpl_invntry_var_unit_qty__c, shpmnt_multi__c, src_cd__c, std_dot_cnvrsn_fctr__c, start_dt__c, trmntn_dt__c, thrptc_cd__c, trackable_item__c, trd_nm_id__c, trnsltd_dsply_nm__c, trnsltd_prod_desc_txt__c, unt_typ_cd__c, updt_dt__c, vet_unit_pkg_txt__c, vha_plus_flg__c, altrnt_id_type_cd__c, recordtype, 'Y'::varchar AS is_active, current_timestamp::timestamp as created_ts, current_timestamp::timestamp as updated_ts,<process_execution_id> as created_by_job_exec_id, <process_execution_id> as updated_by_job_exec_id FROM <schema>.<stg_table>);")

    try:
        
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

def get_query_from_s3(file_key):
    """
    Get the SQL script from S3
    """
    response = s3_client.get_object(Bucket=rawbucket, Key=file_key)
    sql_script = response['Body'].read().decode('utf-8')
    return sql_script

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
        logger.error("Error in function get_load_metadata: %s", str(err))
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
        logger.error("Error in function write_metadata_file: %s", str(err))
        raise MetadataFileError(f"Error in function write_metadata_file: {str(err)}") from err

    
def rename_cols(df):
    # Create a dictionary mapping original column names to new column names
    rename_mapping = {
        "BRND_CD": "BRND_CD__C",
        "COLLAB_IND": "COLLAB_IND__C",
        "CRT_DT": "CRT_DT__C",
        "DMND_DFLT_CNVRSN_FCTR": "DMND_DFLT_CNVRSN_FCTR__C",
        "DMND_DFLT_CNVRSN_UOM_CD": "DMND_DFLT_CNVRSN_UOM_CD__C",
        "DMND_DSG_PTNCY_UOM_CD": "DMND_DSG_PTNCY_UOM_CD__C",
        "DMND_PKG_CNTNT_PRSNTN_ABBR_CD": "DMND_PKG_CNTNT_PRSTN_ABR_CD__C",
        "DMND_PKG_CNTNT_QTY": "DMND_PKG_CNTNT_QTY__C",
        "DMND_PKG_CNTNT_UOM_CD": "DMND_PKG_CNTNT_UOM_CD__C",
        "DMND_PKG_QTY": "DMND_PKG_QTY__C",
        "DMND_PKG_TYP_CD": "DMND_PKG_TYP_CD__C",
        "DMND_PKG_UOM_CD": "DMND_PKG_UOM_CD__C",
        "DMND_PTNCY_DSG_QTY": "DMND_PTNCY_DSG_QTY__C",
        "DMND_PTNCY_QTY": "DMND_PTNCY_QTY__C",
        "DMND_QTY_PTNCY_UOM_CD": "DMND_QTY_PTNCY_UOM_CD__C",
        "DMND_SALABLE_IND": "DMND_SALABLE_IND__C",
        "DSG_FREQ_CD": "DSG_FREQ_CD__C",
        "EDCTNL_MTRL_ID": "EDCTNL_MTRL_ID__C",
        "EDP_FMLY_CD": "EDP_FMLY_CD__C",
        "EDP_PKG_SIZE_CD": "EDP_PKG_SIZE_CD__C",
        "EDP_PROD_CD": "EDP_PROD_CD__C",
        "FDA_PKG_SIZE_CD": "FDA_PKG_SIZE_CD__C",
        "ITEM_RGLTRY_SCHED_CD": "ITEM_RGLTRY_SCHED_CD__C",
        "LBL_CD": "LBL_CD__C",
        "LST_LOT_EXPRTN_DT": "LST_LOT_EXPRTN_DT__C",
        "MTRL_NBR": "MTRL_NBR__C",
        "NBR_OF_DOSES": "NBR_OF_DOSES__C",
        "PROD_ID": "PROD_ID__C",
        "PROD_NM": "PROD_NM__C",
        "PROD_TYP": "PROD_TYP__C",
        "PRSCPTN_PROD_CD": "PRSCPTN_PROD_CD__C",
        "RPKG_CD": "RPKG_CD__C",
        "SHPMNT_MULTI": "SHPMNT_MULTI__C",
        "SRC_CD": "SRC_CD__C",
        "STD_DOT_CNVRSN_FCTR": "STD_DOT_CNVRSN_FCTR__C",
        "TRD_NM_ID": "TRD_NM_ID__C",
        "UPDT_DT": "UPDT_DT__C",
        "LST_PBLSH_DT": "LST_PBLSH_DT__C",
        "EDCTNL_MTRL_ITM_TYP": "EDCTNL_MTRL_ITM_TYP__C",
        "PROD_DISEASE_ST_FLG": "PROD_DISEASE_ST_FLG__C",
        "REFRIG_CD": "REFRIG_CD__C",
        "TRACKABLE_ITEM": "TRACKABLE_ITEM__C",
        "ALTRNT_ID": "ALTERNATE_ID__C",
        "TRNSLTD_DSPLY_NM": "TRNSLTD_DSPLY_NM__C",
        "TRNSLTD_PROD_DESC_TXT": "TRNSLTD_PROD_DESC_TXT__C",
        "DIALECT_NM": "DIALECT_NM__C",
        "CNTRY_CD": "CNTRY_CD__C",
        "END_DT": "END_DT__C",
        "START_DT": "START_DT__C",
        "CEASE_PBLSH_DT": "CEASE_PBLSH_DT__C",
        "SFDC_ID_PROD_GRP": "PRODUCT_GROUP__C",
        "ALTRNT_ID_TYPE_CD__C": "ALTRNT_ID_TYPE_CD__C",
        "PRODUCT_EXTERNAL_ID": "PRODUCT_EXTERNAL_ID__C",
        "RECORDTYPE": "RECORDTYPE",
        # Add more mappings as necessary
    }
    df_renamed = df.select([F.col(old).alias(new) for old, new in rename_mapping.items()])
    # df_renamed.show()
    return df_renamed
class TransformationError(Exception):
    """Custom exception for transformation errors."""
def transform_df(df_dict,exec_id):
    """
    Function to transform the DataFrame
    """
    try:
        
        df_filtered_mktp= df_dict["mtm_gh_prod_vw_mktp"].filter(df_dict["mtm_gh_prod_vw_mktp"]['alt_cntry_cd'] == df_dict["mtm_gh_prod_vw_mktp"]['stgh_cntry_cd'])
        df_filtered_brnd= df_dict["mtm_gh_prod_vw_brnd"].filter(df_dict["mtm_gh_prod_vw_brnd"]['prod_trns_cntry_cd']==df_dict["mtm_gh_prod_vw_brnd"]['pg_trns_cntry_cd'])
        df_filtered_brnd_gso_product=df_filtered_brnd.join(df_dict['lkp_gso_product_param'],
                                                        ((df_filtered_brnd['PROD_TRNS_CNTRY_CD'] == df_dict['lkp_gso_product_param']['cntry_cd']) & 
                                                         (df_dict['lkp_gso_product_param']['grp_prps_cd_glbl__c'] == df_filtered_brnd['grp_prps_cd'])),
                                                        how='left'
                                                ).select(df_filtered_brnd['*'],df_dict['lkp_gso_product_param']['dialect_nm'].alias('gso_product_dialect_nm'))
        df_filtered_brnd_gso_product=df_filtered_brnd_gso_product.join(df_dict['mtm_gh_prod_grp_trnsltns_vw'],
                                                                        ((df_filtered_brnd_gso_product['PROD_GRP_PRNT_ID'] == df_dict['mtm_gh_prod_grp_trnsltns_vw']['PROD_GRP_ID']) & 
                                                                        (df_dict['mtm_gh_prod_grp_trnsltns_vw']['DIALECT_NM'] == df_filtered_brnd_gso_product['gso_product_dialect_nm']) &
                                                                        (df_dict['mtm_gh_prod_grp_trnsltns_vw']['CNTRY_CD'] == df_filtered_brnd_gso_product['PROD_TRNS_CNTRY_CD'])),
                                                                        how='left'
                                                                    ).select(df_filtered_brnd_gso_product['*'],df_dict['mtm_gh_prod_grp_trnsltns_vw']['DIALECT_NM'].alias('pg_dialect_nm'))
        df_filtered_brnd=df_filtered_brnd_gso_product.filter(df_filtered_brnd_gso_product['pg_dialect_nm']==df_filtered_brnd_gso_product['gso_product_dialect_nm'])
        df_filtered_brnd=df_filtered_brnd.drop('gso_product_dialect_nm','pg_dialect_nm')
        df_filtered_mktp=df_filtered_mktp.drop('stgh_cntry_cd')
        df_filtered_brnd=df_filtered_brnd.drop('pg_trns_cntry_cd')
        df_filtered_mktp=df_filtered_mktp.withColumnRenamed('ALT_CNTRY_CD','cntry_cd')
        df_filtered_brnd=df_filtered_brnd.withColumnRenamed('PROD_TRNS_CNTRY_CD','cntry_cd')
        df= df_dict["mtm_gh_prod_vw_smpl_edu"].union(df_filtered_mktp).union(df_filtered_brnd)
        # df.show()
        df_dict['gso_product_param']=df_dict['gso_product_param'].withColumnRenamed('CNTRY_CD','p_CNTRY_CD')
        df= df.join(df_dict['gso_product_param'],
                    ((df['grp_prps_cd'] == df_dict['gso_product_param']['grp_prps_cd_GLBL__C']) & (df['CNTRY_CD'] == df_dict['gso_product_param']['p_CNTRY_CD'])))
        # df.show()
        df=df.drop('CNTRY_CD','grp_prps_cd_GLBL__C')
        df=df.withColumnRenamed('p_CNTRY_CD','CNTRY_CD')
        
        df = df.withColumn(
                    "o_CEASE_PBLSH_DT", 
                    when(
                        col("CEASE_PBLSH_DT") == F.to_date(lit('12/31/9999'), 'MM/dd/yyyy'), 
                        None
                    ).otherwise(col("CEASE_PBLSH_DT"))
                )
        df=df.drop('CEASE_PBLSH_DT')
        df=df.withColumnRenamed('o_CEASE_PBLSH_DT','CEASE_PBLSH_DT')
        
        lkp_trnslt=df_dict['mtm_gh_prod_trnsltns_vw'].withColumnRenamed('DIALECT_NM','trnsltn_dialect_nm')\
                                                    .withColumnRenamed('CNTRY_CD','trnsltn_cntry_cd')\
                                                    .withColumnRenamed('PROD_ID','trnsltn_prod_id')\
                                                    .withColumnRenamed('CEASE_PBLSH_DT','CEASE_PBLSH_DT_TRN')\
                                                    .withColumnRenamed('END_DT','END_DT_TRN')\
                                                    .withColumnRenamed('START_DT','START_DT_TRN')
        df=df.join(lkp_trnslt,((df['PROD_ID'] == lkp_trnslt['trnsltn_prod_id']) & (df['CNTRY_CD'] == lkp_trnslt['trnsltn_cntry_cd']) & (df['DIALECT_NM'] == lkp_trnslt['trnsltn_dialect_nm'])),how='left')
        df=df.drop('trnsltn_prod_id','trnsltn_cntry_cd','trnsltn_dialect_nm')
        lkp_prod_grp_rplctn=df_dict['gso_prod_grp_rplctn'].withColumnRenamed('SFDC_ID','SFDC_ID_PROD_GRP')
        df=df.join(lkp_prod_grp_rplctn,(df['PROD_GRP_PRNT_ID'] == lkp_prod_grp_rplctn['PROD_GRP_ID_GLBL__C']) & (df['CNTRY_CD'] == lkp_prod_grp_rplctn['CNTRY_CD_GLBL__C']) ,how='left')
        df=df.drop('PROD_GRP_ID_GLBL__C','CNTRY_CD_GLBL__C')
        lkp_mtm_gh_prod_grp_trnsltns_vw=df_dict['lkp_mtm_gh_prod_grp_trnsltns_vw'].withColumnRenamed('DIALECT_NM','grp_trnsltn_dialect_nm')\
                                                    .withColumnRenamed('CNTRY_CD','grp_trnsltn_cntry_cd')\
                                                    .withColumnRenamed('PROD_GRP_ID','grp_trnsltn_prod_grp_id')\
                                                    .withColumnRenamed('CEASE_PBLSH_DT','CEASE_PBLSH_DT_PG_TRN')\
                                                    .withColumnRenamed('END_DT','END_DT_PG_TRN')
        df=df.join(lkp_mtm_gh_prod_grp_trnsltns_vw,((df['PROD_GRP_PRNT_ID'] == lkp_mtm_gh_prod_grp_trnsltns_vw['grp_trnsltn_prod_grp_id']) & (df['CNTRY_CD'] == lkp_mtm_gh_prod_grp_trnsltns_vw['grp_trnsltn_cntry_cd']) & (df['DIALECT_NM'] == lkp_mtm_gh_prod_grp_trnsltns_vw['grp_trnsltn_dialect_nm'])),how='left')
        df=df.drop('grp_trnsltn_prod_grp_id','grp_trnsltn_cntry_cd','grp_trnsltn_dialect_nm')
        lkp_mtm_gh_prod_altrnt_id_vw = df_dict['mtm_gh_prod_altrnt_id_vw'].select(
                                                                                    F.col('CNTRY_CD').alias('altrnt_id_cntry_cd'),
                                                                                    F.col('PROD_ID').alias('altrnt_id_prod_id'),
                                                                                    F.col('CEASE_PBLSH_DT').alias('CEASE_PBLSH_DT_ALT'),
                                                                                    F.col('END_DT').alias('END_DT_ALT'),
                                                                                    F.col('START_DT').alias('START_DT_ALT'),
                                                                                    F.col('ALTRNT_ID'),
                                                                                    F.col('ALTRNT_ID_TYPE_CD')
                                                                                )
        df=df.join(lkp_mtm_gh_prod_altrnt_id_vw,((df['PROD_ID'] == lkp_mtm_gh_prod_altrnt_id_vw['altrnt_id_prod_id']) & (df['CNTRY_CD'] == lkp_mtm_gh_prod_altrnt_id_vw['altrnt_id_cntry_cd']) & (df['ALTRNT_ID_TYPE_CD__C'] == lkp_mtm_gh_prod_altrnt_id_vw['ALTRNT_ID_TYPE_CD'])),how='left')                                                                
        df=df.drop('altrnt_id_prod_id','altrnt_id_cntry_cd','ALTRNT_ID_TYPE_CD__C')
        df=df.withColumnRenamed('ALTRNT_ID_TYPE_CD','ALTRNT_ID_TYPE_CD__C')
        # Using the greatest function
        df = df.withColumn(
            "o_START_DT", 
            F.greatest(col("START_DT"), col("START_DT_TRN"), col("START_DT_ALT"))
        )
        df=df.withColumn(
            "v_CEASE_PBLSH_DT",
            F.when(
                F.isnull(F.col("CEASE_PBLSH_DT")), 
                F.to_date(F.lit('12/31/9999'), 'MM/dd/yyyy')
            ).otherwise(F.col("CEASE_PBLSH_DT"))
        )
        df = df.withColumn(
            "v_CEASE_PBLSH_DT_TRN",
            F.when(
                F.isnull(F.col("CEASE_PBLSH_DT_TRN")), 
                F.to_date(F.lit('12/31/9999'), 'MM/dd/yyyy')
            ).otherwise(F.col("CEASE_PBLSH_DT_TRN"))
        )
        df=df.withColumn(
            "v_CEASE_PBLSH_DT_ALT",
            F.when(
                F.isnull(F.col("CEASE_PBLSH_DT_ALT")), 
                F.to_date(F.lit('12/31/9999'), 'MM/dd/yyyy')
            ).otherwise(F.col("CEASE_PBLSH_DT_ALT"))
        )
        df=df.withColumn(
            "v_CEASE_PBLSH_DT_PROD_GRP_PROD",
            F.when(
                F.isnull(F.col("CEASE_PBLSH_DT_PROD_GRP_PROD")), 
                F.to_date(F.lit('12/31/9999'), 'MM/dd/yyyy')
            ).otherwise(F.col("CEASE_PBLSH_DT_PROD_GRP_PROD"))
        )
        df=df.withColumn(
            "v_CEASE_PBLSH_DT_PG_TRN",
            F.when(
                F.isnull(F.col("CEASE_PBLSH_DT_PG_TRN")), 
                F.to_date(F.lit('12/31/9999'), 'MM/dd/yyyy')
            ).otherwise(F.col("CEASE_PBLSH_DT_PG_TRN"))
        )
        df = df.withColumn(
            "o_CEASE_PBLSH_DT",
            F.when(
                F.col("grp_prps_cd") == 'BRND',
                F.least(
                    F.col("v_CEASE_PBLSH_DT"), 
                    F.col("v_CEASE_PBLSH_DT_TRN"),
                    F.col("v_CEASE_PBLSH_DT_PROD_GRP_PROD"), 
                    F.col("v_CEASE_PBLSH_DT_PG_TRN")
                )
            ).otherwise(
                F.least(
                    F.col("v_CEASE_PBLSH_DT"), 
                    F.col("v_CEASE_PBLSH_DT_TRN"), 
                    F.col("v_CEASE_PBLSH_DT_ALT"), 
                    F.col("v_CEASE_PBLSH_DT_PROD_GRP_PROD")
                )
            )
        )
        df = df.withColumn(
            "o_END_DT",
            F.when(
                F.col("grp_prps_cd") == 'BRND',
                F.least(
                    F.col("END_DT"), 
                    F.col("END_DT_TRN"), 
                    F.col("END_DT_PROD_GRP_PROD"), 
                    F.col("END_DT_PG_TRN")
                )
            ).otherwise(
                F.least(
                    F.col("END_DT"), 
                    F.col("END_DT_TRN"), 
                    F.col("END_DT_ALT"), 
                    F.col("END_DT_PROD_GRP_PROD")
                )
            )
        )
        df=df.withColumn("v_SFDC_ID",col("SFDC_ID_PROD_GRP"))
        df=df.withColumn("v_PROD_ID",col("PROD_ID"))
        df=df.withColumn("v_CNTRY_CD",col("CNTRY_CD"))
        df=df.withColumn("v_ALTRNT_ID__C",col("ALTRNT_ID_TYPE_CD__C"))
        # df.select('v_PROD_ID','PROD_GRP_PRNT_ID','grp_prps_cd','v_CNTRY_CD','v_ALTRNT_ID__C','ALTRNT_ID_TYPE_CD__C').show()
        df = df.withColumn(
            "o_PRODUCT_EXTERNAL_ID", 
            F.concat_ws(
                "",  # Use an empty string as the separator
                F.col("grp_prps_cd"), 
                F.col("PROD_GRP_PRNT_ID").cast('int'), 
                F.col("v_PROD_ID").cast('int'), 
                F.col("v_CNTRY_CD"), 
                F.col("v_ALTRNT_ID__C")
            )
        )
        # df.select('o_PRODUCT_EXTERNAL_ID').show()
        df=df.drop('v_CEASE_PBLSH_DT',\
                    'v_CEASE_PBLSH_DT_TRN',\
                    'v_CEASE_PBLSH_DT_ALT',\
                    'v_CEASE_PBLSH_DT_PROD_GRP_PROD',\
                    'v_CEASE_PBLSH_DT_PG_TRN',\
                    'v_SFDC_ID','v_PROD_ID',\
                    'v_CNTRY_CD',\
                    'v_ALTRNT_ID__C',\
                    'CEASE_PBLSH_DT_ALT',\
                    'CEASE_PBLSH_DT_PG_TRN',\
                    'CEASE_PBLSH_DT_TRN',\
                    'CEASE_PBLSH_DT',\
                    'CEASE_PBLSH_DT_PROD_GRP_PROD',\
                    'END_DT_ALT',\
                    'END_DT_PG_TRN',\
                    'END_DT_TRN',\
                    'END_DT',\
                    'END_DT_PROD_GRP_PROD',\
                    'START_DT_ALT',\
                    'START_DT_TRN',\
                    'START_DT',\
                    'PROD_GRP_PRNT_ID'\
                    'grp_prps_cd'
        )
        
        df=df.withColumnRenamed('o_CEASE_PBLSH_DT','CEASE_PBLSH_DT')\
            .withColumnRenamed('o_END_DT','END_DT')\
            .withColumnRenamed('o_PRODUCT_EXTERNAL_ID','PRODUCT_EXTERNAL_ID')\
            .withColumnRenamed('o_START_DT','START_DT')
        # df.select('PRODUCT_EXTERNAL_ID', 'ALTRNT_ID', 'SFDC_ID_PROD_GRP','PROD_ID').show(truncate=False)
        df=df.join(df_dict['gso_prod_rplctn'],
                   ((df['PROD_ID'] == df_dict['gso_prod_rplctn']['PROD_ID_GLBL__C']) & (df['CNTRY_CD'] == df_dict['gso_prod_rplctn']['CNTRY_CD_GLBL__C']) & (df['PRODUCT_EXTERNAL_ID']==df_dict['gso_prod_rplctn']['PRODUCT_EXTERNAL_ID__C'])),
                    how='left')
        df=df.drop('PROD_ID_GLBL__C','CNTRY_CD_GLBL__C')
        # df.show()
        # Columns you need for grouping
        group_columns = ["product_external_id__c", "PRODUCT_EXTERNAL_ID"]
        # List of other columns for aggregation
        other_columns = [col for col in df.columns if col not in group_columns]
        # Construct the aggregation dictionary
        agg_expr = {
            **{col: F.first(col).alias(col) for col in other_columns}
        }
        # Perform the groupBy and aggregation
        df = df.groupBy(*group_columns).agg(*agg_expr.values())
        # df.show()
        # Assuming df is your DataFrame
        current_date = F.current_date()
        filter_1 = (
            (F.col("product_external_id__c").isNotNull()) |
            (
                (F.col("END_DT") >= current_date) &
                (F.col("CEASE_PBLSH_DT").isNull() | (F.col("CEASE_PBLSH_DT") >= current_date))
            )
        )
        df_filtered = df.filter(filter_1)

        # Filter 2: ALTRNT_ID condition
        filter_2 = F.col("ALTRNT_ID").isNotNull()
        df_filtered = df_filtered.filter(filter_2)
    
        # Filter 3: Either trnsltd_dsply_nm or trnsltd_prod_desc_txt condition
        filter_3 = F.col("trnsltd_dsply_nm").isNotNull() | F.col("trnsltd_prod_desc_txt").isNotNull()
        df_filtered = df_filtered.filter(filter_3)

        # Filter 4: SFDC_ID_PROD_GRP condition
        filter_4 = F.col("SFDC_ID_PROD_GRP").isNotNull()
        df_filtered = df_filtered.filter(filter_4)
        df_filtered=df_filtered.drop('PRODUCT_EXTERNAL_ID__C')
        df=df.drop('PRODUCT_EXTERNAL_ID__C')
        df=rename_cols(df_filtered)
        current_time=datetime.now()
        df=df.withColumn("is_active",lit("Y"))
        df=df.withColumn("updated_ts",lit(current_time))
        df=df.withColumn("created_ts",lit(current_time))
        df=df.withColumn("created_by_job_exec_id",lit(exec_id).cast('bigint'))
        df=df.withColumn("updated_by_job_exec_id",lit(exec_id).cast('bigint'))
        return df
    except (KeyError, ValueError, TypeError) as specific_error:
        logger.error("Specific error occurred: %s", str(specific_error))
        return None
    except TransformationError as exp:
        logger.error("Unexpected error: %s", str(exp))
        return None
    
def get_dataframes_from_s3(curr,active_prod_load_date):
    """
    Get the dataframes from the SQL scripts in S3
    """
    paths = [
        "customer_meeting_services_system/sql/mods/gso_prod_stg/mtm_gh_prod_vw_mktp.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/mtm_gh_prod_vw_smpl_edu.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/mtm_gh_prod_vw_brnd.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/lkp_gso_product_param.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/mtm_gh_prod_grp_trnsltns_vw.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/gso_product_param.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/mtm_gh_prod_trnsltns_vw.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/gso_prod_grp_rplctn.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/lkp_mtm_gh_prod_grp_trnsltns_vw.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/mtm_gh_prod_altrnt_id_vw.sql",
        "customer_meeting_services_system/sql/mods/gso_prod_stg/gso_prod_rplctn.sql"
        ]
    df_dict ={}
    for sql_file in paths:
        # Extract the key from the file path
        key = sql_file.rsplit("/",maxsplit=1)[-1].replace(".sql", "")
        # Get the query from S3
        query = get_query_from_s3(sql_file)
        if key in ("mtm_gh_prod_vw_mktp","mtm_gh_prod_vw_smpl_edu","mtm_gh_prod_vw_brnd"):
            query = query.replace("active_prod_date",f"'{active_prod_load_date}'")
        # Execute the query and store the result in the dictionary
        df_dict[key] = querydb(query)
    return df_dict


def main():
    """
    Main function to execute the ETL process
    """
    try:
        config_common = {}
        config_common = init_config(config_common)
        tgt_table=args["tgt_table"]
        hist_table=args["hist_table"]
        load_metadata_file_path = args["load_metadata_file_path"]
        current_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        load_param = str(config_common['edb_cms_gso_mods_batch_params']['ACTIVE_PROD_LOAD_DATE'])
        prod_load_date = get_load_metadata(load_metadata_file_path, load_param)
        process_execution_id=config_common["process_execution_id"]
        conn,curr = connect_db()
        df_dict = get_dataframes_from_s3(curr,prod_load_date)
        final_df=transform_df(df_dict,process_execution_id)
        final_df = final_df.dropDuplicates()
        ingest_data(final_df,tgt_table,True)
        loadhistorytable(conn,curr,hist_table,tgt_table,process_execution_id)
        write_metadata_file(load_metadata_file_path, current_date_str)
    except Exception as exp:
        logger.error("Error in main function: %s", str(exp))
        raise exp
    finally:
        if conn is not None:
            conn.close()
        if curr is not None:
            curr.close()
        spark.stop()
main()
job.commit()
