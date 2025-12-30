"""Glue Script to Load Data into Salesforce Object from consent staging table
#<mm/dd/yyyy><author><modification-details>:19-07-2024/Tilak Ram Mahore/v0
"""
import sys
import json
import math
from io import StringIO
import logging
from datetime import datetime
import boto3 # pylint: disable=import-error
import psycopg2 # pylint: disable=import-error
from pyspark.sql.types import ( # pylint: disable=import-error
    StructType, StructField, StringType, IntegerType, LongType, ShortType,
    DecimalType, FloatType, DoubleType, BooleanType, BinaryType, DateType, TimestampType
    )
from pyspark.context import SparkContext # pylint: disable=import-error
from pyspark.sql.functions import when, lit, date_format, col, to_date,to_utc_timestamp,substring # pylint: disable=import-error
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

# Setting up the arguments
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

# Get the secret from AWS Secrets Manager
try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    password = get_secret_value_response["SecretString"]
except ClientError as e:
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

        if query.strip().lower().startswith("update") or query.strip().lower()\
            .startswith("delete"):
            return cur.rowcount

        return "Query executed successfully."
    except psycopg2.Error as e:
        logger.info("Error: %s", e)
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
        job_name1: Job name to get the process ID for
    """
    lambda_client= boto3.client("lambda")
    abc_batch_execution = {"operation_type": "insert","status": "R",
                             "batch_name": f"{batch_name}"}
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
        config_common: Common configuration object 
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
        logger.error("Error in initializing config: %s", exep_func)
        raise exep_func


def invoke_lambda(func_name,payload):
    """
    Function to invoke Lambda
    Args:
        func_name: Lambda function name
        payload: Payload to pass to the Lambda function
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

        ## Create an empty DataFrame
        empty_df = spark.createDataFrame([], schema)
        return empty_df
    except Exception as exp: # pylint: disable=broad-except
        logger.error("Exceptionin sql queries %s",exp)
        raise exp
    finally:
        curr.close()


#Transformation on the Source and Lookups
def transform_df(df):
    """
    DataFrame transformation to get the desired Structure and fields
    Args:
        df : DataFrame to transform
    """
    try:
        df = df.withColumn("Name1", df["NAME"])
        df = df.withColumn("LST_PBLSH_DT_GLBL__C", date_format(to_utc_timestamp(col("LST_PBLSH_DT_GLBL__C"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        df = df.withColumn("CRT_DT_GLBL__C", date_format(to_utc_timestamp(col("CRT_DT_GLBL__C"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        df = df.withColumn("UPDT_DT_GLBL__C", date_format(to_utc_timestamp(col("UPDT_DT_GLBL__C"), "UTC"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        #Rename columns by reversing the transformations
        df = df.withColumnRenamed("CEASE_PBLSH_DT_GLBL__C", "Cease_Pblsh_Dt_GLBL__c") \
            .withColumnRenamed("CNTRY_CD_GLBL__C", "Cntry_Cd_GLBL__c") \
            .withColumnRenamed("CRT_DT_GLBL__C", "Crt_Dt_GLBL__c") \
            .withColumnRenamed("DIALECT_NM_GLBL__C", "Dialect_Nm_GLBL__c") \
            .withColumnRenamed("GLBL_PARENT_PRODUCT_GROUP__C", "GLBL_Parent_Product_Group__c") \
            .withColumnRenamed("GRP_ABBR_NM_GLBL__C", "Grp_Abbr_Nm_GLBL__c") \
            .withColumnRenamed("GRP_CNVRSN_UNIT_CD_GLBL__C", "Grp_Cnvrsn_Unit_Cd_GLBL__c") \
            .withColumnRenamed("GRP_PRPS_CD_GLBL__C", "Grp_Prps_Cd_GLBL__c") \
            .withColumnRenamed("LST_PBLSH_DT_GLBL__C", "Lst_Pblsh_Dt_GLBL__c") \
            .withColumnRenamed("PROD_GRP_DESC_TXT_GLBL__C", "Prod_Grp_Desc_Txt_GLBL__c") \
            .withColumnRenamed("PROD_GRP_TYP_CD_GLBL__C", "Prod_Grp_Typ_Cd_GLBL__c") \
            .withColumnRenamed("PROD_GRP_EXTERNAL_ID_GLBL__C",
                               "Product_Group_External_ID_GLBL__c") \
            .withColumnRenamed("PRODUCT_GROUP_ID_GLBL__C", "Product_Group_Id_GLBL__c") \
            .withColumnRenamed("SRC_CD_GLBL__C", "Src_Cd_GLBL__c") \
            .withColumnRenamed("START_DT_GLBL__C", "Start_Dt_GLBL__c") \
            .withColumnRenamed("TRNSLTD_GRP_NM_GLBL__C", "Trnsltd_Grp_Nm_GLBL__c") \
            .withColumnRenamed("NAME", "Untrnsltd_Grp_Nm_GLBL__c") \
            .withColumnRenamed("UPDT_DT_GLBL__C", "Updt_Dt_GLBL__c")


		# Assuming your DataFrame is named df and the column containing the date is
        # 'your_date_column'
        df = df.withColumn(
            "Cease_Pblsh_Dt_GLBL__c",
            date_format(col("Cease_Pblsh_Dt_GLBL__c"), "yyyy-MM-dd")
        )
        df = df.withColumn(
            "End_Dt_GLBL__c",
            date_format(col("End_Dt_GLBL__c"), "yyyy-MM-dd")
        )


            # Transformations
        df = df.withColumn("Cease_Pblsh_Dt_GLBL__c",
                    when(
                        df["Cease_Pblsh_Dt_GLBL__c"] >= to_date(lit("01/01/3999"), "MM/dd/yyyy"),
                        to_date(lit("12/31/4000"), "MM/dd/yyyy")
                    ).otherwise(df["Cease_Pblsh_Dt_GLBL__c"])
                ).withColumn(
                    "End_Dt_GLBL__c", 
                    when(
                        df["End_Dt_GLBL__c"] >= to_date(lit("01/01/3999"), "MM/dd/yyyy"),
                        to_date(lit("12/31/4000"), "MM/dd/yyyy")
                    ).otherwise(df["End_Dt_GLBL__c"])
				).withColumn(
				"Name",
				when(
					(col("Cntry_Cd_GLBL__c") == 'ZZ') | col("Trnsltd_Grp_Nm_GLBL__c").isNull(),
					col("Name1")
				).otherwise(col("Trnsltd_Grp_Nm_GLBL__c"))
			)
        df = fillnull(df)
        df = df.withColumn("Name", substring("Name", 1, 80))
        return df


    except Exception as exp: # pylint: disable=broad-except
        logger.error("Error in the function: %s", exp)
        return None

def fillnull(df):
    columns_to_update = [
        "Cease_Pblsh_Dt_GLBL__c",
        "GLBL_Parent_Product_Group__c",
        "Grp_Abbr_Nm_GLBL__c",
        "Grp_Cnvrsn_Unit_Cd_GLBL__c",
        "Prod_Grp_Desc_Txt_GLBL__c",
        "Prod_Grp_Typ_Cd_GLBL__c",
        "Src_Cd_GLBL__c",
        "Trnsltd_Grp_Nm_GLBL__c"
    ]
    for column in columns_to_update:
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
    """
    To invoke appflow
    Args:
        flow_name : Appflow name to trigger
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
        file_key : file key to get the query
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
        spark.stop()
main()
job.commit()
