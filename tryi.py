import os
import boto3
import glob
import datetime
import psycopg2
import psycopg2.extras

import pandas as pd
import numpy as np
from io import BytesIO

s3_client = boto3.client('s3')

BATCH_SERVICE_TABLE = "ge_batch_service_" + os.environ['STAGE']

STATUS_VALIDATION_INPROGRESS = "VALIDATION_INPROGRESS"
STATUS_AVAILABLE = "AVAILABLE"
STATUS_INVALID = "FILE_INVALID"
TMP_DIR = "/tmp"


def get_db_con():
    
    """
    create database connection and cursor
    """
    dbconuri = os.environ.get('DB_DATA_GETEMAIL').split("//")[1]
    user = dbconuri.split(":")[0]
    pwd = dbconuri.split(":")[1].split("@")[0]
    host = dbconuri.split(":")[1].split("@")[1]
    port = int(dbconuri.split("/")[0].split(":")[-1])
    dbname = dbconuri.split("/")[-1]

    con = psycopg2.connect(database=dbname, user=user, password=pwd, host=host, port=port)
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    con.autocommit = True
    return con, cur

def insert_inprogress(cur, data):
    print("BATCH_SERVICE_TABLE:", BATCH_SERVICE_TABLE)
    qry = f"""INSERT INTO {BATCH_SERVICE_TABLE} 
            (
                s3_path, 
                filename, 
                total_records, 
                status, 
                created_at, 
                updated_at,
                comment
            ) 
            VALUES
            (
                '{data['s3_path']}',  
                '{data['filename']}',  
                {data['total_records']},  
                '{data['status']}',  
                '{data['created_at']}',  
                '{data['updated_at']}', 
                '{data['comment']}'
            );
        """
    cur.execute(qry)
    cur.execute('SELECT LASTVAL()')
    lastid = cur.fetchone()['lastval']
    return lastid
    
def update_status(cur, id, status, total_records, comment):
    qry = f"""UPDATE {BATCH_SERVICE_TABLE} 
            SET status = '{status}',
            total_records = {total_records},
            comment =  '{comment}',
            updated_at = '{datetime.datetime.now()}'
            WHERE id = {id}
    """
    cur.execute(qry)
    return cur.rowcount

def verify_columns(df):
    columns = ['population_id', 'contact_id', 'firstname', 'lastname', 'companyname', 'domain']
    return all(item.lower() in df.columns for item in columns)

def get_invalid_records(df_s3_data):
    df = df_s3_data[ 
        (
            df_s3_data['contact_id'].isna() |
            df_s3_data['firstname'].isna() |
            df_s3_data['lastname'].isna()
        )
        |
        (df_s3_data['contact_id'] == '-')
        |
        (df_s3_data['firstname'] == '-')
        |
        (df_s3_data['lastname'] == '-')
        |
        (df_s3_data['companyname'].isna() & df_s3_data['domain'].isna())
    ]

    return df.shape[0]

def get_file_extn(s3_file_path):
    return s3_file_path.split(".")[-1]

def get_file_name(s3_file_path):
    return s3_file_path.split("/")[-1]

def remove_tmp_files(path = TMP_DIR):
    files = glob.glob(f'{TMP_DIR}/*')
    for file in files:
        os.remove(file)
    print(f'tmp file in dir:{TMP_DIR} have been deleted.')

    s3_client = boto3.client('s3')
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"] #"dnb-info"
    # s3_file_path = 'batch-algo/requests/UrgentPriority8InternationalFile_18_20221123.csv' #"batch-algo/requests/sample.csv"
    s3_file_path = event["Records"][0]["s3"]["object"]["key"] #"batch-algo/requests/sample.csv"
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_path)
    
    con, cur = get_db_con()
    now = datetime.datetime.now()
    data = {}
    data['s3_path'] = f's3://{bucket_name}/{"/".join(s3_file_path.split("/")[:-1])}/'
    data['filename'] = s3_file_path.split("/")[-1]
    data['total_records'] = 0
    data['status'] = STATUS_VALIDATION_INPROGRESS
    data['created_at'] = now
    data['updated_at'] = now
    data['comment'] = ""
    print("insert_data: ", data)
    
    try:
        # id = insert_inprogress(cur, data)
        print('created_new_id: ', id)
    except Exception as err:
        print("insert_inprogress error: ",err)
        con.close()
        return {'error': err}
    
    remove_tmp_files()
    if id:
        rows, status, comment = 0, STATUS_AVAILABLE, ''
        file_name = get_file_name(s3_file_path)
        if get_file_extn(s3_file_path) == "csv":
            try:
                try:
                    df_s3_data = pd.read_csv(BytesIO(resp['Body'].read()))
                except Exception as err:
                    # print('Error', err)
                    df_s3_data = pd.read_csv(BytesIO(resp['Body'].read()))
                    print("success") 

                df_s3_data.to_csv(f'{TMP_DIR}/{file_name}', encoding='utf-8', index=False)
                df_s3_data = pd.read_csv(f'{TMP_DIR}/{file_name}')
                df_s3_data.columns = df_s3_data.columns.str.lower()
                if not verify_columns(df_s3_data):
                    status, comment = STATUS_INVALID, 'invalid header'
                else:
                    df_s3_data = df_s3_data.replace("\\N", np.nan)
                    rows, columns = df_s3_data.shape
                    invalid_records = get_invalid_records(df_s3_data)
                    if invalid_records:
                        comment = f'invalid_records:{invalid_records}'
            except Exception as err:
                status, comment = STATUS_INVALID, f'ERROR: {err}'
                    
        else:
            status, comment = STATUS_INVALID, 'file extension is not .csv'
    
        try:
            rc = update_status(cur, id, status , rows, comment)
        except Exception as err:
            print("update_status:error: ", err)
    
    con.close()
    remove_tmp_files()
    print("lambda_handler: CONNECTION WAS CLOSED SUCCESSFULLY!")

    return {
        "statusCode": 200,
    }