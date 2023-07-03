import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import logging
import boto3
import json
from botocore.exceptions import ClientError
import re
glue_client = boto3.client("glue")
athena = boto3.client('athena')

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                          'partition_key','sql_location','out_bucket_name','sql_bucket_name','sql_folder_name','jdbc_driver','secret_name'])

##

print ("The job name is: ", args['JOB_NAME'])
print ("The partition key is: ", args['partition_key'])
partition_key = args['partition_key']
#partition_key = 'GX01'
table_dictionary = {}

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# move these values to config variables
base_s3_path = "s3a://"+args['out_bucket_name']
bucket_name = args['out_bucket_name']
sql_bucket_name = args['sql_bucket_name']
sql_key = args['sql_folder_name']+"/"+args['sql_location']
partition_prefix = "/src_sys_loc_"
mapping_file_location = ""
secret_name = args['secret_name']

#s3_client
s3_client = boto3.client('s3')

#returns s3 path for the file.    
def get_partition_path(partition_folder):
    print("base_s3_path",base_s3_path + "/" + "grm_draas/" + f"{partition_folder.lower()}" + f"{partition_prefix.lower()}" + "cd=" + f"{partition_key}")
    return base_s3_path + "/" + "grm_draas/" + f"{partition_folder.lower()}" + f"{partition_prefix.lower()}" + "cd=" + f"{partition_key}"
    # s3://dr-statemgr-247272907227-data/grm_draas/membr_cvrg/

#Insert data into existing partition 
def insert_into_existing_partition(key, spark_df, full_partition_path):
    print (f"Appending to partition...{full_partition_path}")
    spark_df.write.mode('append').parquet(f"{full_partition_path}")
    print (f"Done writing to patition...row count of newly written records: {spark_df.count()}")
# Delete existing folder, if exists. 
def delete_existing_partition_files(folder):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    #bucket.objects.filter(Prefix=folder + f"{partition_prefix.lower()}" + "cd=" + f"{partition_key}").delete()
    bucket.objects.filter(Prefix="grm_draas/" + f"{folder}" + f"{partition_prefix.lower()}" + "cd=" + f"{partition_key}").delete()
    response = s3.list_objects(Bucket=bucket, Prefix="grm_draas/" + f"{folder}" + f"{partition_prefix.lower()}" + "cd=" + f"{partition_key}")
    if 'Contents' in response:
        print("Contents")
    else:
        print("grm_draas/" + f"{folder}" + f"{partition_prefix.lower()}" + "cd=" + f"{partition_key}"+ "does not exist in the bucket ")
        
    
    print("Deleted...." + "grm_draas/" + f"{folder}" + f"{partition_prefix.lower()}" + "cd=" + f"{partition_key}")

#Fetch connection string from the secrets manager
def get_secret():
    secret_name = args['secret_name']
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name)
        conn_secret = json.loads(get_secret_value_response['SecretString'])
        return str(conn_secret[partition_key])
    except ClientError as e:
        raise e


def build_param_data():
#     files = s3_client.list_objects_v2(Bucket = bucket_name, Prefix = "parameters/") 
    data = s3_client.get_object(Bucket=sql_bucket_name, Key=sql_key)
    select_qry = ''
    table_count = 1

    if (data != None):

        for line in data['Body'].read().splitlines():
            each_line = line.decode("utf-8").strip()
            if (each_line[:2] not in ["--",";"]):
                if (each_line[:8] == "TRUNCATE"):
                    print(each_line)
                    value_info = re.search('TABLE(.*);',each_line)
                    value = value_info.group(1).split(".")[1].strip()
                    key = "TRUNCATE " + str(value)
                    table_dictionary[key] = value
                elif (each_line[:6] == "INSERT"):
                    key_info = re.search('TABLE(.*)PARTITION',each_line)
                    key = key_info.group(1).split(".")[1].strip()
                else:
                    select_qry += each_line + ' '
            elif (each_line[:1] == ";"):
                if (key in table_dictionary):
                    print(key)
                    temp_key = "existingPartition" + "-"+ str(table_count) + "+" + key
                    table_dictionary[temp_key] = select_qry
                    table_count += 1
                else:
                    table_dictionary[key] = select_qry
                select_qry = ''
        return table_dictionary
    

def athena_partitions(key, partition_key, full_partition_path):
    print("key: ", key, "partition_key: ", partition_key, "full_partition_path: ", full_partition_path)
    query = f"ALTER TABLE grm_draas.{key} ADD IF NOT EXISTS PARTITION (src_sys_loc_cd = '{partition_key}');"
    print("query----",query)
    athena.start_query_execution(
        QueryString = query,
        QueryExecutionContext={
            'Database': "grm_draas"
        },
        ResultConfiguration={
            'OutputLocation': 's3://'+full_partition_path.split('//')[1]+ '/'
            }
            )


def load_data(partition_key):
    for key, value in table_dictionary.items():
        print ("key in load_data function: ",key)
        if ("TRUNCATE" not in key):
            pushdown_query =  f"{value}"
            print ("pushdown_query ",pushdown_query)
            spark_df = sparkSession.read.format("jdbc").option("url",f'{secret}').option("query",pushdown_query).option("driver",args['jdbc_driver']).load()
            print("spark dataframe count(): ", spark_df.count())
            if spark_df.count()>0:
                spark_df.createOrReplaceTempView("data")
                #print("-------dataframe-----",spark_df)
                res = sparkSession.sql("select * from data limit 10")
                res.show()
                print (f"done creating partition {key}...row count: {spark_df.count()}")
                if (key.startswith("existingPartition")):
                    print(f"{key}" + " exists...")
                    full_partition_path = get_partition_path(key.split('+')[1])
                    print("full_partition_path: ", full_partition_path)
                    insert_into_existing_partition(key, spark_df, full_partition_path)
                
                else:
                    print(f"{key}" + " does not exist...")
                    full_partition_path = get_partition_path(key)
                    print(full_partition_path)
                    print("-------dataframe-----",spark_df)
                    spark_df.write.parquet(full_partition_path)
                    athena_partitions(key, partition_key, full_partition_path)
            elif spark_df.count() == 0 and key in ["membr_cvrg","membr_info","subscrbr_membr_info"]:
                sys.exit(f"Error message: Count is zero f{value}")
                
        elif ("TRUNCATE" in key):
            print("--------partition value -------",value)
            delete_existing_partition_files(value)
        

        

print("****Starting*****")
sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
sparkSession = gc.spark_session

#Get Connection string from Secrets Manager
secret = get_secret()

#if (partition_key in ['GXO1','GXQ0']):
if partition_key:
    table_dictionary = build_param_data()
    load_data(partition_key)
    print(table_dictionary)