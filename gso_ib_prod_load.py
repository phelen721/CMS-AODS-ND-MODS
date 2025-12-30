"""Glue Script to Load Data into Salesforce Object from consent staging table
#<mm/dd/yyyy><author><modification-details>:19-07-2024/Tilak Ram Mahore/v0
"""
import sys
import math
from io import StringIO
import json
import logging
from datetime import datetime
import boto3 # pylint: disable=import-error
import psycopg2 # pylint: disable=import-error
from pyspark.sql.types import ( # pylint: disable=import-error
    StructType, StructField, StringType, IntegerType, LongType, ShortType,
    DecimalType, FloatType, DoubleType, BooleanType, BinaryType, DateType, TimestampType
    )
from pyspark.context import SparkContext # pylint: disable=import-error
from pyspark.sql.functions import when, lit, date_format,\
      col, to_date,substring # pylint: disable=import-error
from awsglue.context import GlueContext # pylint: disable=import-error
from awsglue.utils import getResolvedOptions # pylint: disable=import-error
from awsglue.job import Job # pylint: disable=import-error
from botocore.exceptions import ClientError # pylint: disable=import-error
from psycopg2 import extras # pylint: disable=import-error

ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")

args = getResolvedOptions(sys.argv, ['host','db','port','username','password','job_name',
                                     'batch_name','region_name','param_name','common_param_name',
                                     'abc_fetch_param_function','abc_log_stats_func',
                                     'rds_schema_name','rawbucket',
                                     'sql_file_location','target_folder',
                                     'archive_folder','flow_name','logging_level'])

level=args['logging_level']
# Configure the logger
logging.basicConfig(
    level=args['logging_level'],
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Log job start
logger.info("Starting AWS Glue job")


host = args["host"]
db = args["db"]
port = args["port"]
username = args["username"]
password = args["password"]
rds_schema_name = args["rds_schema_name"]
job_name = args["job_name"]
batch_name = args["batch_name"]
region_name = args["region_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
rawbucket = args["rawbucket"]
sql_file_location= args['sql_file_location']


try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e: # pylint: disable=broad-except
    # Handle potential errors during retrieval
    raise e

s3_client = boto3.client("s3")

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
    except Exception as error: # pylint: disable=broad-except
        logger.error("error in connect_db: %s", error)
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

        if query.strip().lower().startswith("update") or query.strip()\
        .lower().startswith("delete"):
            return cur.rowcount

        return "Query executed successfully."
    except psycopg2.Error as e:
        conn.rollback()
        return f"Error: {e}"
    except Exception as e: # pylint: disable=broad-except
        logger.error("Error: %s", e)
        conn.rollback()
        return f"Unknown Error: {e}"

def abc_batch_management(job_name1):
    """
    This fucntion is used to get Process ID and Execution ID by invoking ABC lambda
    Args:
        job_name1 : job name to get the process id and execution id
    """
    lambda_client= boto3.client("lambda")
    abc_batch_execution = {"operation_type": "insert","status": "R",
                             "batch_name": f"edb_cms_{job_name1}"}
    invoke_response_status = lambda_client.invoke(FunctionName="edb_abc_log_batch_status",
                                                  Payload = json.dumps(abc_batch_execution)
                                                  .encode("utf-8"))
    batch_execution_status_id = json.loads(invoke_response_status["Payload"].read())
    abc_log_process_status = {"operation_type": "insert","status": "R",
                               "batch_execution_id": batch_execution_status_id
                               .get("batch_execution_id"), "job_name": job_name1}

    invoke_response_status = lambda_client.invoke(FunctionName="edb_abc_log_process_status",
                                                  Payload = json.dumps(abc_log_process_status)
                                                  .encode("utf-8") )
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
        config_common : Common configuration object
    """
    try:
        ex_ids=abc_batch_management(job_name)
        batch_execution_id=ex_ids['batch_execution_id']
        process_execution_id=ex_ids['process_execution_id']
        json_payload = {
            "batch_name": args.get("batch_name"),
            "job_name": args.get("job_name")}
        config_dct = invoke_lambda(args['abc_fetch_param_function'], json_payload)
        # Read common config file applicable for all tables
        config_common = config_dct
        config_common["job_name"] = job_name
        config_common["batch_name"] = batch_name
        config_common["process_execution_id"] = process_execution_id
        # config_common["snow_func"] = args['snow_func']
        config_common["abc_log_stats_func"] = abc_log_stats_func
        config_common["batch_execution_id"] = batch_execution_id
        return config_common
    except Exception as exep_func:
        logger.error("Error in initializing config: %s", exep_func)
        raise exep_func


def invoke_lambda(func_name,payload):
    """
    Function to invoke Lambda
    Args:
        func_name : Lambda function name
        payload : Payload to be passed to the Lambda function
    """
    lambda_client = boto3.client('lambda')
    invoke_response = lambda_client.invoke(
        FunctionName=func_name,
        Payload=json.dumps(payload))
    config_obj = json.loads(invoke_response['Payload'].read())
    return config_obj


def querydb(curr, query):
    """Execute sql query
    Parameter:
        conn : Database connection object
        query : The query to be processed
    """
    try:
        conn,curr = connect_db() # pylint: disable=unused-variable
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
            1700: DecimalType(14,6),       # PostgreSQL NUMERIC type
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

        ## Create an empty DataFrame
        empty_df = spark.createDataFrame([], schema)
        return empty_df
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Exceptionin sql queries %s",exp)
        raise exp
    finally:
        curr.close()

def fillnull(df):
    columns_to_update_new =[
      "Brnd_cd__c",
      "Collab_ind__c",
      "Dflt_cnvrsn_fctr__c",
      "Dflt_cnvrsn_uom_cd__c",
      "Dmnd_dsg_ptncy_uom_cd__c",
      "Dmnd_pkg_cntnt_uom_cd__c",
      "Dmnd_pkg_qty__c",
      "Dmnd_pkg_typ_cd__c",
      "Dmnd_pkg_uom_cd__c",
      "Dmnd_pkg_cntnt_prsntn_abbr_cd__c",
      "Dmnd_ptncy_dsg_qty__c",
      "Dmnd_ptncy_qty__c",
      "Dmnd_salable_ind__c",
      "Dsg_ptncy_uom_cd__c",
      "Edctnl_mtrl_ctgry_cd__c",
      "Edctnl_mtrl_id__c",
      "Edctnl_Mtrl_Itm_Typ__c",
      "Fda_pkg_size_cd__c",
      "Item_rgltry_sched_cd__c",
      "Lst_lot_exprtn_dt__c",
      "Nbr_of_doses__c",
      "Pkg_cntnt_prsntn_abbr_cd__c",
      "Pkg_cntnt_qty__c",
      "Pkg_cntnt_uom_cd__c",
      "Pkg_qty__c",
      "Pkg_typ_cd__c",
      "Pkg_uom_cd__c",
      "Ptncy_dsg_qty__c",
      "Ptncy_qty__c",
      "Prscptn_prod_cd__c",
      "Prod_Disease_St_Flg__c",
      "Qty_ptncy_uom_cd__c",
      "Rpkg_cd__c",
      "Salable_ind__c",
      "Shpmnt_multi__c",
      "Std_dot_cnvrsn_fctr__c",
      "Trackable_Item__c"
    ]
    for column in columns_to_update_new:
        df = df.withColumn(column, when(col(column) == '', None).otherwise(col(column)))
        df = df.withColumn(column, when(col(column).isNull(), '#N/A').otherwise(col(column)))
    return df
#Transformation on the Source and Lookups
def transform_df(df):
    """
    DataFrame transformation to get the desired Structure and fields
    Args:
        df (DataFrame): Source DataFrame
    """
    try: # Rename and transform columns
        df = df.withColumn("Name", df["TRNSLTD_DSPLY_NM__C"])
        # Rename columns by reversing the transformations
        df = df.withColumnRenamed("ALTERNATE_ID__C", "Alternate_ID__c") \
            .withColumnRenamed("ALTRNT_ID_TYPE_CD__C", "Altrnt_Id_Type_Cd__c") \
            .withColumnRenamed("AVG_RX_QUANTITY__C", "Avg_rx_quantity__c") \
            .withColumnRenamed("BRND_CD__C", "Brnd_cd__c") \
            .withColumnRenamed("CEASE_PBLSH_DT__C", "Cease_pblsh_dt__c") \
            .withColumnRenamed("CLASS_DVLPMNT_CD__C", "Class_dvlpmnt_cd__c") \
            .withColumnRenamed("CNTRY_CD__C", "Cntry_Cd__c") \
            .withColumnRenamed("CNVRSN_FCTR__C", "Cnvrsn_fctr__c") \
            .withColumnRenamed("COLLAB_IND__C", "Collab_ind__c") \
            .withColumnRenamed("CRT_DT__C", "Crt_dt__c") \
            .withColumnRenamed("DFLT_CNVRSN_FCTR__C", "Dflt_cnvrsn_fctr__c") \
            .withColumnRenamed("DFLT_CNVRSN_UOM_CD__C", "Dflt_cnvrsn_uom_cd__c") \
            .withColumnRenamed("DFND_DLY_DSG__C", "Dfnd_dly_dsg__c") \
            .withColumnRenamed("DIALECT_NM__C", "Dialect_nm__c") \
            .withColumnRenamed("DMND_DFLT_CNVRSN_FCTR__C", "Dmnd_dflt_cnvrsn_fctr__c") \
            .withColumnRenamed("DMND_DFLT_CNVRSN_UOM_CD__C", "Dmnd_dflt_cnvrsn_uom_cd__c") \
            .withColumnRenamed("DMND_DSG_PTNCY_UOM_CD__C", "Dmnd_dsg_ptncy_uom_cd__c") \
            .withColumnRenamed("DMND_PKG_CNTNT_PRSTN_ABR_CD__C",
                                "Dmnd_pkg_cntnt_prsntn_abbr_cd__c") \
            .withColumnRenamed("DMND_PKG_CNTNT_QTY__C", "Dmnd_pkg_cntnt_qty__c") \
			.withColumnRenamed("DMND_PKG_CNTNT_UOM_CD__C", "Dmnd_pkg_cntnt_uom_cd__c") \
            .withColumnRenamed("DMND_PKG_QTY__C", "Dmnd_pkg_qty__c") \
            .withColumnRenamed("DMND_PKG_TYP_CD__C", "Dmnd_pkg_typ_cd__c") \
            .withColumnRenamed("DMND_PKG_UOM_CD__C", "Dmnd_pkg_uom_cd__c") \
            .withColumnRenamed("DMND_PTNCY_DSG_QTY__C", "Dmnd_ptncy_dsg_qty__c") \
            .withColumnRenamed("DMND_PTNCY_QTY__C", "Dmnd_ptncy_qty__c") \
            .withColumnRenamed("DMND_QTY_PTNCY_UOM_CD__C", "Dmnd_qty_ptncy_uom_cd__c") \
            .withColumnRenamed("DMND_SALABLE_IND__C", "Dmnd_salable_ind__c") \
            .withColumnRenamed("DSCNT_DSCRPTN_TXT__C", "Dscnt_dscrptn_txt__c") \
            .withColumnRenamed("DSG_FREQ_CD__C", "Dsg_freq_cd__c") \
            .withColumnRenamed("DSG_PTNCY_UOM_CD__C", "Dsg_ptncy_uom_cd__c") \
            .withColumnRenamed("DSG_UOM_CD__C", "Dsg_uom_cd__c") \
            .withColumnRenamed("DSGNTD_INDCTR__C", "Dsgntd_indctr__c") \
            .withColumnRenamed("EDCTNL_MTRL_ITM_TYP__C", "Edctnl_Mtrl_Itm_Typ__c") \
            .withColumnRenamed("EDCTNL_MTRL_CTGRY_CD__C", "Edctnl_mtrl_ctgry_cd__c") \
            .withColumnRenamed("EDCTNL_MTRL_DSCNT_PCT__C", "Edctnl_mtrl_dscnt_pct__c") \
            .withColumnRenamed("EDCTNL_MTRL_ID__C", "Edctnl_mtrl_id__c") \
            .withColumnRenamed("EDP_FMLY_CD__C", "Edp_fmly_cd__c") \
			.withColumnRenamed("EDP_PKG_SIZE_CD__C", "Edp_pkg_size_cd__c") \
            .withColumnRenamed("EDP_PROD_CD__C", "Edp_prod_cd__c") \
            .withColumnRenamed("EQVLNCY_FCTR__C", "Eqvlncy_fctr__c") \
            .withColumnRenamed("FDA_PKG_SIZE_CD__C", "Fda_pkg_size_cd__c") \
            .withColumnRenamed("HCFA_UNIT_PER_PKG__C", "Hcfa_unit_per_pkg__c") \
            .withColumnRenamed("ITEM_RGLTRY_SCHED_CD__C", "Item_rgltry_sched_cd__c") \
            .withColumnRenamed("LBL_CD__C", "Lbl_cd__c") \
            .withColumnRenamed("LST_LOT_EXPRTN_DT__C", "Lst_lot_exprtn_dt__c") \
            .withColumnRenamed("LST_PBLSH_DT__C", "Lst_pblsh_dt__c") \
            .withColumnRenamed("MTRL_NBR__C", "Mtrl_nbr__c") \
            .withColumnRenamed("NBR_OF_DOSES__C", "Nbr_of_doses__c") \
            .withColumnRenamed("NHI_PRC_LST_DT__C", "Nhi_prc_lst_dt__c") \
            .withColumnRenamed("ORAL_ADMNSTRN_CD__C", "Oral_admnstrn_cd__c") \
            .withColumnRenamed("ORDR_FRM_FLG__C", "Ordr_frm_flg__c") \
            .withColumnRenamed("PKG_CNTNT_PRSTN_ABR_CD__C", "Pkg_cntnt_prsntn_abbr_cd__c") \
            .withColumnRenamed("PKG_CNTNT_QTY__C", "Pkg_cntnt_qty__c") \
            .withColumnRenamed("PKG_CNTNT_UOM_CD__C", "Pkg_cntnt_uom_cd__c") \
			.withColumnRenamed("PKG_QTY__C", "Pkg_qty__c") \
            .withColumnRenamed("PKG_TYP_CD__C", "Pkg_typ_cd__c") \
            .withColumnRenamed("PKG_UOM_CD__C", "Pkg_uom_cd__c") \
            .withColumnRenamed("PROD_DISEASE_ST_FLG__C", "Prod_Disease_St_Flg__c") \
			.withColumnRenamed("PROD_ID__C", "Prod_id__c") \
            .withColumnRenamed("PROD_LONG_DESC__C", "Prod_long_desc__c") \
            .withColumnRenamed("PROD_NM__C", "Prod_nm__c") \
			.withColumnRenamed("PROD_TYP__C", "Prod_typ__c") \
            .withColumnRenamed("PRODUCT_EXTERNAL_ID__C", "Product_External_ID__c") \
            .withColumnRenamed("PRODUCT_GROUP__C", "Product_Group__c") \
            .withColumnRenamed("PRSCPTN_PROD_CD__C", "Prscptn_prod_cd__c") \
            .withColumnRenamed("PTNCY_DSG_QTY__C", "Ptncy_dsg_qty__c") \
            .withColumnRenamed("PTNCY_QTY__C", "Ptncy_qty__c") \
            .withColumnRenamed("QTY_PTNCY_UOM_CD__C", "Qty_ptncy_uom_cd__c") \
            .withColumnRenamed("RECORDTYPE", "RecordTypeId") \
            .withColumnRenamed("REFRIG_CD__C", "Refrig_Cd__c") \
            .withColumnRenamed("RPKG_CD__C", "Rpkg_cd__c") \
            .withColumnRenamed("SALABLE_IND__C", "Salable_ind__c") \
            .withColumnRenamed("SALT_QTY__C", "Salt_qty__c") \
            .withColumnRenamed("SHPMNT_MULTI__C", "Shpmnt_multi__c") \
            .withColumnRenamed("SMPL_INVNTRY_VAR_PCT__C", "Smpl_invntry_var_pct__c") \
            .withColumnRenamed("SMPL_INVNTRY_VAR_UNIT_QTY__C", "Smpl_invntry_var_unit_qty__c") \
            .withColumnRenamed("SRC_CD__C", "Src_cd__c") \
			.withColumnRenamed("START_DT__C", "Start_dt__c") \
            .withColumnRenamed("STD_DOT_CNVRSN_FCTR__C", "Std_dot_cnvrsn_fctr__c") \
            .withColumnRenamed("THRPTC_CD__C", "Thrptc_cd__c") \
            .withColumnRenamed("TRACKABLE_ITEM__C", "Trackable_Item__c") \
            .withColumnRenamed("TRD_NM_ID__C", "Trd_nm_id__c") \
            .withColumnRenamed("TRMNTN_DT__C", "Trmntn_dt__c") \
            .withColumnRenamed("TRNSLTD_DSPLY_NM__C", "Trnsltd_Dsply_Nm__c") \
            .withColumnRenamed("TRNSLTD_PROD_DESC_TXT__C", "Trnsltd_Prod_Desc_Txt__c") \
            .withColumnRenamed("UNT_TYP_CD__C", "Unt_typ_cd__c") \
            .withColumnRenamed("UPDT_DT__C", "Updt_dt__c") \
            .withColumnRenamed("VET_UNIT_PKG_TXT__C", "Vet_unit_pkg_txt__c") \
            .withColumnRenamed("VHA_PLUS_FLG__C", "Vha_plus_flg__c") \
            .withColumnRenamed("END_DT__C", "end_dt__c") \
            .withColumnRenamed("PRFT_CTR__C", "prft_ctr__c")


		# Assuming your DataFrame is named df and the column containing
        # the date is 'your_date_column'
        df = df.withColumn(
            "Cease_pblsh_dt__c",
            date_format(col("Cease_pblsh_dt__c"), "yyyy-MM-dd")
        )
        df = df.withColumn(
            "Crt_dt__c",
            date_format(col("Crt_dt__c"), "yyyy-MM-dd")
            )
        df = df.withColumn(
            "Lst_lot_exprtn_dt__c",
            date_format(col("Lst_lot_exprtn_dt__c"), "yyyy-MM-dd")
            )
        df = df.withColumn(
            "Lst_pblsh_dt__c",
            date_format(col("Lst_pblsh_dt__c"), "yyyy-MM-dd")
            )
        df = df.withColumn(
            "Nhi_prc_lst_dt__c",
            date_format(col("Nhi_prc_lst_dt__c"), "yyyy-MM-dd")
            )
        df = df.withColumn(
            "Start_dt__c",
            date_format(col("Start_dt__c"), "yyyy-MM-dd")
        )
        df = df.withColumn(
            "Trmntn_dt__c",
            date_format(col("Trmntn_dt__c"), "yyyy-MM-dd")
        )
        df = df.withColumn(
            "Updt_dt__c",
            date_format(col("Updt_dt__c"), "yyyy-MM-dd")
        )
        df = df.withColumn(
            "end_dt__c",
            date_format(col("end_dt__c"), "yyyy-MM-dd")
        )

            # Transformations
        df = df.withColumn("Cease_pblsh_dt__c",
                    when(
                        df["Cease_pblsh_dt__c"] >= to_date(lit("01/01/3999"), "MM/dd/yyyy"),
                        to_date(lit("12/31/4000"), "MM/dd/yyyy")
                    ).otherwise(df["Cease_pblsh_dt__c"])
                ).withColumn(
                    "Lst_lot_exprtn_dt__c", 
                    when(
                        df["Lst_lot_exprtn_dt__c"] >= to_date(lit("01/01/3999"), "MM/dd/yyyy"),
                        to_date(lit("12/31/4000"), "MM/dd/yyyy")
                    ).otherwise(df["Lst_lot_exprtn_dt__c"])
				).withColumn(
                    "Trmntn_dt__c", 
                    when(
                        df["Trmntn_dt__c"] >= to_date(lit("01/01/3999"), "MM/dd/yyyy"),
                        to_date(lit("12/31/4000"), "MM/dd/yyyy")
                    ).otherwise(df["Trmntn_dt__c"])
				).withColumn(
                    "end_dt__c", 
                    when(
                        df["end_dt__c"] >= to_date(lit("01/01/3999"), "MM/dd/yyyy"),
                        to_date(lit("12/31/4000"), "MM/dd/yyyy")
                    ).otherwise(df["end_dt__c"])
				)
        
        df=fillnull(df)
        df = df.withColumn("Name", substring("Name", 1, 80))
        return df


    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s", exp)
        return None


def move_from_raw_to_archive(bucket_name, directory_path, archive_path):
    """
    Move multiple files from source to archive directory in S3.
    Args:
        bucket_name (str): The name of the S3 bucket.
        directory_path (str): The source directory path.
        archive_path (str): The archive directory path.
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
    """
    To invoke appflow
    Args:
        flow_name : Name of the flow
    """
    # Initialize the AppFlow client
    appflow_client = boto3.client('appflow')

    # Trigger AppFlow execution
    response = appflow_client.start_flow(
        flowName=f'{flow_name}'
    )

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
    '''Main Function'''
    try:
        target_folder=args['target_folder']
        archive_folder=args['archive_folder']
        flow_name=args['flow_name']
        conn, curr = connect_db() # pylint: disable=unused-variable
        bucket_name=rawbucket
        move_from_raw_to_archive(bucket_name,target_folder,archive_folder)
        # get query to a variable from s3 bucket
        query=get_query_from_s3(sql_file_location)
        df=querydb(curr,query)
        df=transform_df(df)
        store_flat_file_in_s3(df,bucket_name,target_folder)
        #Invoke Appflow
        invoke_appflow_del(flow_name)
    except Exception as exp:
        logger.error("Error in main function: %s", exp)
        raise exp
    finally:
        curr.close()
        conn.close()
        spark.stop()
main()
job.commit()
