"""Glue Script to load GSO_INDCTN_STG table
#<mm/dd/yyyy><author><modification-details>:17-09-2024/Saurav Pournami Nair/v0
"""
import sys
import logging
import json
from datetime import datetime

import boto3
import psycopg2
from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from psycopg2 import extras
from botocore.exceptions import ClientError


s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda", region_name="us-east-2")
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
                           'logging_level',
                           'load_metadata_file_path'])

# Job Arguments
JOB_NAME = args["JOB_NAME"]
host = args["host"]
db = args["db"]
port = args["port"]
username = args["username"]
raw_bucket = args["raw_bucket"]
rds_schema_name = args["rds_schema_name"]
batch_name = args["batch_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
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

logger.info("Starting gso_indctn_stg Glue job")
db_conn_dict = {}
db_conn_dict['username'] = username
try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
    db_conn_dict['password'] = password
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
    """This function is used to get abc details"""
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
    """
    invoke_response = lambda_client.invoke(
        FunctionName=func_name,
        Payload=json.dumps(payload))
    config_obj = json.loads(invoke_response['Payload'].read())
    return config_obj


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
                url=URL,
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


def ingest_data(df, tname,truncate,mode):
    """This Function ingestion data to postgresdb"""
    try:
        load_table_name = f'{rds_schema_name}.{tname}'
        df.write.format("jdbc").options(url=URL, \
                                            user=db_conn_dict['username'],\
                                            dbtable=load_table_name,\
                                            password=db_conn_dict['password'],\
                                            truncate=truncate) \
                                            .mode(mode).save()
        return True
    except Exception as e:
        ex=str(e).replace('"','').replace("'","")
        logger.error(ex)
        raise IngestionException(f"ingest_data_in_postgres exception :: {e}" ) from e


class IngestionException(Exception):
    """Custom exception for ingestion errors."""


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


# pylint: disable=line-too-long
def loadhistorytable(conn,cursor, tgt_table,stg_table,exec_id):
    """
    Function to load the history table"""
    updt_query = "UPDATE <schema>.<tgt_table> a SET is_active= 'N', updated_ts = current_timestamp where indctn_ext_id_glbl__c in (select a.indctn_ext_id_glbl__c from <schema>.<tgt_table> a inner join <schema>.<stg_table> b on a.indctn_ext_id_glbl__c=b.indctn_ext_id_glbl__c) and a.is_active = 'Y'"

    rfnd_insrt_query = "INSERT INTO <schema>.<tgt_table>(sfdc_id, indctn_ext_id_glbl__c, prod_grp_prnt_id_glbl__c, indctn_desc_glbl__c, name__c, indctn_id_glbl__c, indctn_sts_glbl__c, indctn_typ_glbl__c, approval_dt_glbl__c, revoke_dt_glbl__c, start_dt_glbl__c, end_dt_glbl__c, apprvl_sts_glbl__c, country_cd_glbl__c, crt_dt_glbl__c, updt_dt_glbl__c, lst_pblsh_dt_glbl__c, cease_pblsh_dt_glbl__c, dialect_nm_glbl__c, trnsltd_indctn_desc_glbl__c, trnsltd_indctn_sh_desc_glbl__c, prod_grp_prnt_id_glbl_sfdc_id, is_active, created_ts, updated_ts, updated_by_job_exec_id, created_by_job_exec_id)(select sfdc_id, indctn_ext_id_glbl__c, prod_grp_prnt_id_glbl__c, indctn_desc_glbl__c, name__c, indctn_id_glbl__c, indctn_sts_glbl__c, indctn_typ_glbl__c, approval_dt_glbl__c, revoke_dt_glbl__c, start_dt_glbl__c, end_dt_glbl__c, apprvl_sts_glbl__c, country_cd_glbl__c, crt_dt_glbl__c, updt_dt_glbl__c, lst_pblsh_dt_glbl__c, cease_pblsh_dt_glbl__c, dialect_nm_glbl__c, trnsltd_indctn_desc_glbl__c, trnsltd_indctn_sh_desc_glbl__c, prod_grp_prnt_id_glbl_sfdc_id, 'Y'::varchar AS is_active, current_timestamp::timestamp as created_ts, current_timestamp::timestamp as updated_ts,<process_execution_id> as created_by_job_exec_id, <process_execution_id> as updated_by_job_exec_id FROM <schema>.<stg_table>);"
    try:
        # Insert into temp
        updt_query = updt_query.replace('<schema>',rds_schema_name) \
                                        .replace('<tgt_table>',tgt_table) \
                                        .replace('<stg_table>', stg_table)
        cursor.execute(updt_query)
        conn.commit()
        rfnd_insrt_query = rfnd_insrt_query \
                                    .replace('<process_execution_id>' ,str(exec_id))\
                                    .replace('<schema>',rds_schema_name) \
                                    .replace('<tgt_table>',tgt_table) \
                                    .replace('<stg_table>', stg_table)
        cursor.execute(rfnd_insrt_query)
        conn.commit()
    except ValueError as exp_msg:
        logger.error("Error while writing data %s", str(exp_msg))
        rollback_database_table_process(rds_schema_name, tgt_table,
                                        exec_id, cursor, conn)
        raise exp_msg
    finally:
        if cursor is not None:
            cursor.close()


def rename_df(df, exec_id, current_time):
    """ Select and Rename columns """
    try:
        df = df.select((F.col("prod_grp_id").cast('int')).alias("PROD_GRP_PRNT_ID_GLBL__C")
            ,F.col("indctn_desc").alias("INDCTN_DESC_GLBL__C")
            ,F.col("indctn_short_desc").alias("NAME__C")
            ,F.col("indctn_id").alias("INDCTN_ID_GLBL__C")
            ,F.col("indctn_sts").alias("INDCTN_STS_GLBL__C")
            ,F.col("indctn_typ").alias("INDCTN_TYP_GLBL__C")
            ,F.col("apprvl_dt").alias("APPROVAL_DT_GLBL__C")
            ,F.col("revoke_dt").alias("REVOKE_DT_GLBL__C")
            ,F.col("start_dt").alias("START_DT_GLBL__C")
            ,F.col("end_dt").alias("END_DT_GLBL__C")
            ,F.col("apprvl_sts_cd").alias("APPRVL_STS_GLBL__C")
            ,F.col("cntry_cd").alias("COUNTRY_CD_GLBL__C")
            ,F.col("crt_dt").alias("CRT_DT_GLBL__C")
            ,F.col("updt_dt").alias("UPDT_DT_GLBL__C")
            ,F.col("lst_pblsh_dt").alias("LST_PBLSH_DT_GLBL__C")
            ,F.col("cease_pblsh_dt").alias("CEASE_PBLSH_DT_GLBL__C")
            ,F.col("dialect_nm").alias("DIALECT_NM_GLBL__C")
            ,F.col("trnsltd_indctn_desc_txt").alias("TRNSLTD_INDCTN_DESC_GLBL__C")
            ,F.col("trnsltd_indctn_short_desc").alias("TRNSLTD_INDCTN_SH_DESC_GLBL__C")
            ,F.col("indctn_ext_id_glbl__c").alias("INDCTN_EXT_ID_GLBL__C")
            ,F.col("prod_grp_prnt_id_glbl_sfdc_id").alias("PROD_GRP_PRNT_ID_GLBL_SFDC_ID"))

        df = df.withColumn("SFDC_ID", F.lit(None).cast(StringType())) \
                .withColumn("is_active",F.lit("Y")) \
                .withColumn("updated_ts",F.lit(current_time)) \
                .withColumn("created_ts",F.lit(current_time)) \
                .withColumn("created_by_job_exec_id",F.lit(exec_id).cast('bigint')) \
                .withColumn("updated_by_job_exec_id",F.lit(exec_id).cast('bigint'))
        return df
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s",str(exp))
        raise exp


# pylint: disable=line-too-long
def exp_collate_gso_indctn_stg(df, df_prod_grp_rplctn):
    """ Collate all the fields for gso_indctn_stg table """
    try:
        # Rename column
        df = df.withColumnRenamed("dialect", "dialect_nm")

        # Concate columns
        df_intermediate = df.withColumn("indctn_ext_id_glbl__c",
                F.concat((F.col("prod_grp_id").cast('int')), F.concat(F.col("indctn_id"), F.col("cntry_cd")))) \
                .withColumn("prod_cntry_key",
                F.concat((F.col("prod_grp_id").cast('int')), F.col("cntry_cd"))) \
                .withColumn("prod_glbl_key",
                F.concat((F.col("prod_grp_id").cast('int')), F.lit("ZZ")))
        # Lookup to get SFDC Id for PROD_GRP_ID||CNTRY_CD
        df_intermediate = df_intermediate.join(
                df_prod_grp_rplctn.withColumnRenamed("sfdc_id", "v_prod_grp_prnt_id_glbl_sfdc_id_cntry"),
                df_intermediate.prod_cntry_key == df_prod_grp_rplctn.prod_grp_external_id_glbl__c,
                how='left')
        df_intermediate = df_intermediate.drop("prod_grp_external_id_glbl__c")

        # Lookup to get SFDC Id for PROD_GRP_ID||'ZZ'
        df_intermediate = df_intermediate.join(
                df_prod_grp_rplctn.withColumnRenamed("sfdc_id", "v_prod_grp_prnt_id_glbl_sfdc_id_glbl"),
                df_intermediate.prod_glbl_key == df_prod_grp_rplctn.prod_grp_external_id_glbl__c,
                how='left')
        # To create new column to add sfdc id from gso_prod_grp_rplctn
        df_final = df_intermediate.withColumn(
            "prod_grp_prnt_id_glbl_sfdc_id",
            F.when(F.col("v_prod_grp_prnt_id_glbl_sfdc_id_cntry").isNotNull(),
                F.col("v_prod_grp_prnt_id_glbl_sfdc_id_cntry")
            ).otherwise(F.col("v_prod_grp_prnt_id_glbl_sfdc_id_glbl")))


        # To drop not needed colums
        columns_to_drop = ["prod_cntry_key",
                           "prod_glbl_key",
                           "v_prod_grp_prnt_id_glbl_sfdc_id_cntry",
                           "v_prod_grp_prnt_id_glbl_sfdc_id_glbl",
                           "prod_grp_external_id_glbl__c"]
        df_final = df_final.drop(*columns_to_drop)
        return df_final
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s",str(exp))
        raise exp


def filter_gso_indctn_stg_global(df, lilly_value, df_prod_grp_rplctn, current_time, exec_id):
    """ Filter to load only data with country code as Global """
    try:
        # Filter records based on the condition
        df = df.withColumn("prod_grp_typ_in", F.regexp_replace(F.col("prod_grp_typ"), "'", ""))
        df_filtered = df.filter((F.col("prod_grp_typ_in") == lilly_value))

        df_filtered = df_filtered.drop("prod_grp_typ_in")


        # Sort df in ascending order
        df_sorted = df_filtered.orderBy(['prod_grp_id', 'indctn_id'])

        # Group by and transformation
        df_intermediate = df_sorted.groupBy('indctn_id', 'prod_grp_id').agg(
        F.max('indctn_desc').alias('indctn_desc'),
        F.max('indctn_short_desc').alias('indctn_short_desc'),
        F.max('indctn_sts').alias('indctn_sts'),
        F.max('indctn_typ').alias('indctn_typ'),
        F.max('apprvl_dt').alias('apprvl_dt'),
        F.max('revoke_dt').alias('revoke_dt'),
        F.max('start_dt').alias('start_dt'),
        F.max('end_dt').alias('end_dt'),
        F.max('apprvl_sts_cd').alias('apprvl_sts_cd'),
        F.lit('ZZ').alias('cntry_cd'),
        F.max('crt_dt').alias('crt_dt'),
        F.max('updt_dt').alias('updt_dt'),
        F.max('lst_pblsh_dt').alias('lst_pblsh_dt'),
        F.max('cease_pblsh_dt').alias('cease_pblsh_dt'),
        F.lit('EN').alias('dialect_nm'),
        F.max('trnsltd_indctn_desc_txt').alias('trnsltd_indctn_desc_txt'),
        F.max('trnsltd_indctn_short_desc').alias('trnsltd_indctn_short_desc'),
        F.concat((F.col("prod_grp_id").cast('int')),
                F.concat(F.col('indctn_id'),
                F.lit('ZZ'))).alias('indctn_ext_id_glbl__c'))



        # Lookup to get SFDC Id for PROD_GRP_ID||'ZZ'
        df_intermediate = df_intermediate.withColumn("prod_glbl_key",
                                                    F.concat((F.col("prod_grp_id").cast('int')), F.lit("ZZ")))
        df_intermediate = df_intermediate.join(
                df_prod_grp_rplctn.withColumnRenamed("sfdc_id", "prod_grp_prnt_id_glbl_sfdc_id"),
                df_intermediate.prod_glbl_key == df_prod_grp_rplctn.prod_grp_external_id_glbl__c,
                how='left')

        # Drop column
        df_intermediate = df_intermediate.drop("prod_glbl_key")
        df_intermediate = df_intermediate.drop("prod_grp_external_id_glbl__c")


        # Rename columns
        df_final = rename_df(df_intermediate, exec_id, current_time)
        return df_final
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s",str(exp))
        raise exp


def transform_df(df_dict, exec_id, lilly_value): # pylint: disable=too-many-locals
    """ Transformation on the Source and Lookups """
    try:
        current_time = datetime.now()
        df_indctn_stg = df_dict['gso_indctn_stg']
        df_country_dialect = df_dict['gso_country_dialect']
        df_indctn_trnsltns = df_dict['mtm_gh_indctn_trnsltns_vw']
        df_indctn_trnsltns = df_indctn_trnsltns.withColumnRenamed("indctn_id", "indctn_id_trnsltns")
        df_prod_grp_rplctn = df_dict['gso_prod_grp_rplctn']

        # Join gso_country_dialect table
        df_dialect_join = df_indctn_stg.join(df_country_dialect,
                                            df_indctn_stg.cntry_cd==df_country_dialect.country_code,
                                            how='left') \
                                        .select(
                                            *df_indctn_stg.columns,
                                            df_country_dialect.dialect.alias("dialect"))


        # Join mtm_gh_indctn_trnsltns_vw
        df_indctn_trnsltns_join = df_dialect_join.join(df_indctn_trnsltns,
                                (df_dialect_join.indctn_id==df_indctn_trnsltns.indctn_id_trnsltns)
                                & (df_dialect_join.dialect==df_indctn_trnsltns.dialect_nm),
                                how='left') \
                            .select(
                                *df_dialect_join.columns,
                                df_indctn_trnsltns.trnsltd_indctn_desc_txt.alias("trnsltd_indctn_desc_txt"), # pylint: disable=line-too-long
                                df_indctn_trnsltns.trnsltd_indctn_short_desc.alias("trnsltd_indctn_short_desc")) # pylint: disable=line-too-long



        # Filter records where dialect and trnsltd_indctn_desc_txt are None
        df_filter_none = df_indctn_trnsltns_join.filter(F.col("dialect").isNotNull()
                                                    & F.col("trnsltd_indctn_desc_txt").isNotNull())


        # Collate all the fields
        df_collate_indctn_stg = exp_collate_gso_indctn_stg(df_filter_none, df_prod_grp_rplctn)

        # To create window partioned by columns mentioned to group by
        window = Window.partitionBy("prod_grp_id", "indctn_id", "cntry_cd")
        # Add max_end_dt column
        df_collate_indctn_stg = df_collate_indctn_stg.withColumn("max_end_dt",
                                                                F.max("end_dt").over(window))
        # Filter to keep rows with end_dt value equal to max_end_dt value
        df_indctn_stg_final = df_collate_indctn_stg.filter(F.col("end_dt") == F.col("max_end_dt")) \
                                                            .drop("max_end_dt")



        df_indctn_stg_renamed = rename_df(df_indctn_stg_final, exec_id, current_time)


        df_indctn_stg_global = filter_gso_indctn_stg_global(df_collate_indctn_stg,
                                                            lilly_value,
                                                            df_prod_grp_rplctn,
                                                            current_time, exec_id)


        # Union dataframes and remove duplicates
        df_union = df_indctn_stg_renamed.union(df_indctn_stg_global).dropDuplicates()

        return df_union
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s",str(exp))
        raise exp


def get_query_from_s3(file_key):
    """
    This Function is to get query from sql file in s3 bucket
    """
    response = s3_client.get_object(Bucket=raw_bucket, Key=file_key)
    sql_script = response['Body'].read().decode('utf-8')

    # Split the SQL script into individual queries
    queries = [query.strip() for query in sql_script.split(';') if query.strip()]
    return queries[0]

def get_load_metadata(s3_path, load_param):
    "This function is used to get current load parameter"
    try:
        response = s3_client.get_object(Bucket=raw_bucket, Key=s3_path)
        load_metadata = json.loads(response['Body'].read().decode('utf-8'))
        metadata_load_value = load_metadata["metadata_load_value"]
        return metadata_load_value
    except ClientError as err:
        logger.error("Error in function get_load_metadata:: %s",str(err))
        if err.response['Error']['Code'] == 'NoSuchKey':
            metadata_load_value = load_param
            return metadata_load_value
        raise err


def write_metadata_file(s3_path, load_param):
    "This function is used to write metadata file in s3"
    try:
        data = json.dumps({"metadata_load_value": f"{load_param}"})
        s3_client.put_object(Bucket=raw_bucket, Key=s3_path, Body=data)
    except ClientError as err:
        logger.error("Error in function write_metadata_file:: %s",str(err))
        raise err


def main(): # pylint: disable=too-many-locals
    """Main Function"""
    try:
        config_common = {}
        df_dict = {}
        config_common = init_config(config_common)

        # To get variables
        process_execution_id = config_common["process_execution_id"]
        load_metadata_file_path = args["load_metadata_file_path"]
        current_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # To get prod_load_date
        load_param = str(config_common['edb_cms_gso_mods_batch_params']['PROD_LOAD_DATE'])
        prod_load_date = get_load_metadata(load_metadata_file_path, load_param)

        lly_pg_typ = config_common['edb_cms_gso_mods_batch_params']['LLY_PG_TYP']
        lly_pg_typ_str = ",".join(f"'{typ}'" for typ in lly_pg_typ)[1:-1]
        cmpt_pg_typ = str(config_common['edb_cms_gso_mods_batch_params']['CMPT_PG_TYP'])
        lilly_value = str(config_common['edb_cms_gso_mods_batch_params']['LILLY'])
        sql_file_paths = config_common['edb_cms_gso_mods_batch_params']['SQL_FILE_PATHS']

        conn, curr = connect_db() # pylint: disable=unused-variable
        t_name = 'gso_indctn_stg'
        hist_t_name = 'gso_indctn'

        # Iterate over SQL file paths to retrieve and execute queries
        for sql_file in sql_file_paths:
            # Extract the key from the file path
            key = sql_file.rsplit('/', maxsplit=1)[-1].replace(".sql", "")

            # Get the query from S3
            query = get_query_from_s3(sql_file)

            if key == "gso_indctn_stg":
                query = query.replace("$PROD_LOAD_DATE", f"{prod_load_date}").replace("$LLY_PG_TYP",
                                f"{lly_pg_typ_str}").replace("$CMPT_PG_TYP",
                                f"{cmpt_pg_typ}").replace('$LILLY',f"{lilly_value}")
            # Execute the query and store the result in the dictionary
            df_dict[key] = querydb(query)

        df_final = transform_df(df_dict, process_execution_id, lilly_value)

        # To load data in tables
        ingest_data(df_final,t_name,True,'overwrite')
        conn,curr = connect_db()
        loadhistorytable(conn,curr,hist_t_name,t_name,process_execution_id)

        # To write metadata file in s3
        write_metadata_file(load_metadata_file_path, current_date_str)

    except Exception as exp:
        logger.error("Error in main function: %s",str(exp))
        raise exp
    finally:
        logger.info("gso_indctn_stg Glue job completed")
        spark.stop()
        curr.close()
        conn.close()

main()
job.commit()
