import logging
import os
import pandas as pd
import json
import azure.functions as func
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL


from urllib.parse import quote_plus
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

#logger.setLevel(logging.INFO)
session_manifest_file = 'session.json'

# get environment variables
connect_string = os.environ["celldeathstorageaccount_STORAGE"]
db_server = os.environ["db_server"]
db_database = os.environ["db_database"]
db_username = os.environ["db_username"]
db_password = os.environ["db_password"]
db_driver = os.environ["db_driver"]

connection_string = f"DRIVER={db_driver};SERVER={db_server};DATABASE={db_database};UID={db_username};PWD={db_password}"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

engine = sqlalchemy.create_engine(connection_url)

#conn = engine.connect()


metadata = MetaData()


session_tbl = Table('session', metadata,
        Column('session_id', String(20), primary_key=True),
        Column('session_date', String(50)),
        Column('donor_id', String(50)),
        Column('organ_id', String(50)),
        Column('biopsy_id', String(50)),
        Column('is_current', String(5))
     )

def is_exist_row_session(session_id):
    # sw = select([session_tbl]).where(session_tbl.c.session_id == session_id)
    # conn = engine.connect()
    # sw_result = conn.execute(sw) 
    # rowcount = len(sw_result._saved_cursor._result.rows)   
    session_mkr = sessionmaker(bind=engine)
    session = session_mkr()
    f_rows = session.query(session_tbl).filter(session_tbl.c.session_id==session_id).all()
    if len(f_rows) > 0:
        return True
    else:
        session.query(session_tbl).filter(session_tbl.c.session_id != session_id).update({'is_current':'False'},synchronize_session=False)
        session.commit()
        return False

def exists(container_nm, blob_nm):
    blob = BlobClient.from_connection_string(conn_str=connect_string,
                                             container_name=container_nm,
                                             blob_name=blob_nm)
    return blob.exists()

def get_container_client(container_nm):
    blob_service_client = BlobServiceClient.from_connection_string(connect_string)
    return blob_service_client.get_container_client(container_nm)

def read_json(container_nm, blob_nm):
    container_client = get_container_client(container_nm)
    blob_client = container_client.get_blob_client(blob_nm)
    return blob_client.download_blob().readall()  # read blob content as string

def get_blob_count(blob_list):
    blobs = []
    for blob in blob_list:
        blobs.append(blob.name)
    return len(blobs)


def main(myblob: func.InputStream, msg: func.Out[str]):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes \n")

    file_name = myblob.name.split("/")[1]
    container_name = myblob.name.split("/")[0]


    if file_name.lower() == session_manifest_file or exists(container_name, session_manifest_file) :
        logging.info(f"  Found session.json, reading \n")
        session_json = read_json(container_name, session_manifest_file)
        session = json.loads(session_json)
        session_id = session['session_id']
        session_date = session['session_date']
        donor_id = session['donor']
        section_data = pd.json_normalize(data=session['organ'], record_path=['biopsy', 'section', 'tiff_files'],
                                         meta=['organ_id', ['biopsy', 'biopsy_id'],
                                               ['biopsy', 'section', 'section_id']])
        logging.info(f"Session Id: {session_id}, Session Date: {session_date}, Donor id: {donor_id}")

        session_table_data = [{'session_id':session_id, 
                                'session_date':session_date, 
                                'donor_id': donor_id,
                                'is_current':'True'}]

        expected_file_count = section_data.shape[0]
        files_in_blob =  get_blob_count(get_container_client(container_name).list_blobs())
        logging.info(f"no of files expected {expected_file_count} vs number of files present: {files_in_blob}")

        # all files have been uploaded and  if session doesn't already exist
        if files_in_blob >= expected_file_count and (not is_exist_row_session(session_id)):
            table_df = pd.DataFrame(session_table_data)
            #set all is_current to False

            #update sql database with session details
            table_df.to_sql('session', engine, if_exists='append', index=False)
            msg.set(session_id)
            return

        else: #all files not uploaded
            ret_msg = "nothing to do"
            logging.info(ret_msg)
            return
    else:
         logging.info(f"  Waiting for session.json... \n")
         return


