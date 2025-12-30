"""
Abhiram Kalyan Madduru
Code to load data from ECDP to MODS(CMS AURORA)
"""
import sys
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from psycopg2 import extras
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySparkJob") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.network.timeout", "600s") \
    .config("spark.shuffle.io.retryWait", "60s") \
    .config("spark.shuffle.io.maxRetries", "10") \
    .config("spark.shuffle.file.buffer", "512k") \
    .config("spark.reducer.maxReqsInFlight", "1") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .getOrCreate()
ses = boto3.client('ses')
secret_manager = boto3.client("secretsmanager")
args = getResolvedOptions(sys.argv, ['cms_aurora_host','cms_aurora_db','cms_aurora_port',
                                     'username','password',\
                                     'job_name','batch_name','region_name','param_name',\
                                     'common_param_name','abc_fetch_param_function',\
                                     'abc_log_stats_func','rdsschemaname','rawbucket',\
                                     'logging_level', \
                                     'ecdp_host','ecdp_db','ecdp_port','ecdp_username','ecdp_secret','ecdp_schema_name'])
level=args['logging_level']
# Configure the logger
logging.basicConfig(
    level=args['logging_level'],
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Setting up the arguments
logger = logging.getLogger(__name__)
logger.info("Starting AWS Glue job")
cms_aurora_host = args["cms_aurora_host"]
cms_aurora_db = args["cms_aurora_db"]
cms_aurora_port = args["cms_aurora_port"]
usename_db = args["username"]
db_conn_dict = {}
db_conn_dict['username_aurora'] = usename_db
ecdp_host = args["ecdp_host"]
ecdp_db = args["ecdp_db"]
ecdp_port = args["ecdp_port"]
ecdp_username = args["ecdp_username"]
ecdp_schema= args["ecdp_schema_name"]
db_conn_dict['username_ecdp'] = ecdp_username
rdsschemaname = args["rdsschemaname"]
job_name = args["job_name"]
batch_name = args["batch_name"]
region_name = args["region_name"]
param_name = args["param_name"]
common_param_name = args["common_param_name"]
abc_fetch_param_function = args["abc_fetch_param_function"]
abc_log_stats_func = args["abc_log_stats_func"]
rawbucket = args["rawbucket"]
# refinedbucket = args["refinedbucket"]
config_common = {}
aurora_url = f"jdbc:postgresql://{cms_aurora_host}:{cms_aurora_port}/{cms_aurora_db}?sslmode=require"
ecdp_url = f"jdbc:postgresql://{ecdp_host}:{ecdp_port}/{ecdp_db}?sslmode=require"
class SecretRetrievalError(Exception):
    """Custom exception for secret retrieval errors."""
    
# Retrieve the RDS password from Secrets Manager
try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["password"])
    rds_password = get_secret_value_response["SecretString"]
    db_conn_dict['password_aurora'] = rds_password
except ClientError as e:
  # Handle potential errors during retrieval
    raise SecretRetrievalError(f'Error retrieving secret from Secrets Manager: {e}') from e

# Retrieve the ECDP password from Secrets Manager
try:
    get_secret_value_response = secret_manager.get_secret_value(SecretId=args["ecdp_secret"])
    ecdp_password = get_secret_value_response["SecretString"]
    db_conn_dict['password_ecdp'] = ecdp_password
except ClientError as e:
  # Handle potential errors during retrieval
    raise SecretRetrievalError(f'Error retrieving secret from Secrets Manager: {e}') from e
s3_client = boto3.client("s3")

glueContext = GlueContext(spark)
job = Job(glueContext)

class QueryDBError(Exception):
    """Custom exception for query errors."""
def querydb(table_name):
    """Execute the query and return the result as a PySpark DataFrame.
    Args:
    table_name : table name to query
    """
    load_table_name = f'{ecdp_schema}.{table_name}'
    try:
        df = (
            spark.read.format("jdbc")
            .options(
                url=ecdp_url,
                user=db_conn_dict["username_ecdp"],
                password=db_conn_dict["password_ecdp"],
                dbtable=load_table_name,
            )
            .load()
        )
        return df
    except QueryDBError as exp:
        logger.error("Exception in sql queries: %s", exp)
        return "No Data"
#Ingesting data by Truncate and Load
def ingest_data(df, tname,truncate,mode):
    """This Function ingestion data to postgresdb
        Args:
        df : Dataframe
        tname : table name
        truncate : boolean value to truncate the table
        mode : mode of ingestion
        """
    try:
        load_table_name = f'{rdsschemaname}.{tname}'
        df.write.format("jdbc").options(url=aurora_url, \
                                            user=db_conn_dict['username_aurora'],\
                                            dbtable=load_table_name,\
                                            password=db_conn_dict['password_aurora'],\
                                            truncate=truncate) \
                                            .mode(mode).save()
        return True
    except Exception as e:
        ex=str(e).replace('"','').replace("'","")
        logger.error(ex)
        raise IngestionException(f"ingest_data_in_postgres exception :: {e}" ) from e
#Ingesting data to incremental load
class IngestionException(Exception):
    """Custom exception for ingestion errors."""
def rollback_database_table_process(schema, tb_name, process_id, cursor, conn):
    """
    Rollback from Refined Db if load is unsuccessful
    Args:
    schema : Schema name of the table
    tb_name : Table name to rollback
    process_id : Process id of the job execution
    cursor : Cursor object 
    conn : Connection object
    """
    try:
        delete_query = f"DELETE FROM {schema}.{tb_name} \
        WHERE created_by_job_exec_id = {process_id}"
        cursor.execute(delete_query)
        conn.commit()
    except KeyError as ex:
        print(ex)
        print(f"Rollback failed due to Error")
        raise ex

def main():
    """
    Main function to load data from ECDP to MODS
    """
    try:
        # Load the data from ECDP
        table_list=['mtm_gh_indctn_trnsltns_vw',\
                    'mtm_gh_indication_vw',\
                    'mtm_gh_pg_to_ind_vw',\
                    'mtm_gh_pg_to_pg_vw',\
                    'mtm_gh_prod_altrnt_id_vw',\
                    'mtm_gh_prod_grp_altrnt_id_vw',\
                    'mtm_gh_prod_grp_trnsltns_vw',\
                    'mtm_gh_prod_grp_typ_vw',\
                    'mtm_gh_prod_grp_vw',\
                    'mtm_gh_prod_trnsltns_vw',\
                    'mtm_gh_prod_vw',\
                    'mtm_gh_prod_grp_to_prod_vw',\
                    'mtm_cntry'
                    ]
        for table in table_list:
            df = querydb(table)
            if df == "No Data":
                logger.error("No data found in ECDP")
                raise Exception("No data found in ECDP")
            # Ingest the data into MODS(CMS AURORA)
            ingest_data(df,table, "true", "overwrite")
    except Exception as e:
        logger.error("Error loading data from ECDP to MODS: %s", e)
        raise e
    finally:
        spark.stop()
main()
job.commit()


