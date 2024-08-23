"""
Template Component main class.
"""
import logging
from datetime import datetime
import requests
import sys
import json
import os 
import logging
import requests
import xml.etree.ElementTree as ET
from keboola.component.exceptions import UserException
from keboola.component.base import ComponentBase
import json 
import xmltodict
from datetime import datetime, timezone, timedelta
import time 
from io import StringIO
from requests_oauthlib import OAuth1Session
from botocore.exceptions import NoCredentialsError
import boto3
import pandas as pd

current_time=datetime.now(timezone.utc)

# configuration variables
KEBOOLA_TOKEN = 'keboola_token'
BUCKET_ID = 'bucket_id'
TABLE_NAME = 'table_name'
NS_CLIENT_KEY = 'ns_client_key'
NS_CLIENT_SECRET = 'ns_client_secret'
NS_RESOURCE_OWNER_KEY = 'ns_resource_owner_key'
NS_RESOURCE_OWNER_SECRET ='ns_resource_owner_secret'
NS_REALM ='ns_realm'
NS_RESTLET_URL='ns_restlet_url'
NS_SEARCH_ID = 'ns_SearchID'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEBOOLA_TOKEN,BUCKET_ID,TABLE_NAME,NS_CLIENT_KEY, NS_CLIENT_SECRET,NS_RESOURCE_OWNER_KEY, NS_RESOURCE_OWNER_SECRET,NS_REALM,NS_RESTLET_URL,NS_SEARCH_ID]
REQUIRED_IMAGE_PARS = []



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


def create_keboola_table_direct(df, keboola_token, bucket_id, table_name):
    # Convert the DataFrame to a CSV string
    csv_data = df.to_csv(index=False)
    
    # Use StringIO to create a file-like object for the CSV
    csv_file = StringIO(csv_data)
    
    # Step 1: Prepare the file upload with federationToken
    prepare_file_url = 'https://connection.keboola.com/v2/storage/files/prepare'
    
    headers = {
        'X-StorageApi-Token': keboola_token,
        'Content-Type': 'application/json'
    }
    
    # Prepare the file upload
    prepare_payload = {
        "name": f"{table_name}.csv",
        "sizeBytes": len(csv_data),
        "isEncrypted": False,
        "isPublic": False,
        "isSliced": False,
        "federationToken": True,
        "tags": []
    }
    
    prepare_response = requests.post(prepare_file_url, headers=headers, data=json.dumps(prepare_payload))
    
    if prepare_response.status_code != 200:
        logging.error(f'Failed to prepare file upload: {prepare_response.text}')
        return None
    
    # Get the necessary parameters from the response
    prepare_result = prepare_response.json()
    file_id = prepare_result['id']
    upload_params = prepare_result['uploadParams']
    
    # Step 2: Use boto3 to upload the CSV file to S3
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=upload_params['credentials']['AccessKeyId'],
            aws_secret_access_key=upload_params['credentials']['SecretAccessKey'],
            aws_session_token=upload_params['credentials']['SessionToken'],
            region_name='us-east-1'
        )
        
        s3_client.put_object(
            Bucket=upload_params['bucket'],
            Key=upload_params['key'],
            Body=csv_file.getvalue(),
            ACL=upload_params['acl']
        )
        
        logging.info(f'CSV file uploaded successfully with file ID: {file_id}')
        
    except NoCredentialsError:
        logging.error("Credentials not available")
        return True



def get_keboola_tables(keboola_token, bucket_id, include=""):
    headers = {'X-StorageApi-Token': keboola_token}
    url = f'https://connection.keboola.com/v2/storage/buckets/{bucket_id}/tables'
    params = {'include': include} if include else {}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        tables = response.json()
        table_names = [table['name'] for table in tables]
        logging.info(f'List of tables: {table_names}')
        return table_names
    else:
        logging.error(f'Failed to retrieve tables: {response.text}')
        return True



def truncate_keboola_table(keboola_token, keboola_table_id):
    values = json.dumps({"allowTruncate": True})
    headers = {
        'Content-Type': 'application/json',
        'X-StorageApi-Token': keboola_token
    }
    url = f'https://connection.keboola.com/v2/storage/tables/{keboola_table_id}/rows'
    response = requests.request("DELETE", url, headers=headers, data=values)
    if response.status_code == 202:
        logging.info('Table truncated successfully!')
    else:
        logging.error(f'Failed to truncate table: {response.text}')
    return True
    
def load_to_keboola(df, keboola_token, keboola_table_id, chunk_size=2500):
    truncate_keboola_table(keboola_token, keboola_table_id)
    url = f'https://connection.keboola.com/v2/storage/tables/{keboola_table_id}/import/import?incremental=1'
    headers = {'X-StorageApi-Token': keboola_token}
    
    for start in range(0, len(df), chunk_size):
        end = start + chunk_size
        chunk = df.iloc[start:end]
        csv_data = chunk.to_csv(index=False)
        csv_file = StringIO(csv_data)
        files = {'data': ('data.csv', csv_file, 'text/csv')}
        response = requests.post(url, headers=headers, files=files)
        
        if response.status_code == 202:
            response_data = response.json()
            total_rows = response_data.get("totalRowsCount", 0)
            warnings = response_data.get("warnings", [])
            
            # Checking for warnings
            if warnings:
                logging.info(f'Data chunk {start // chunk_size + 1} uploaded with warnings: {warnings}')
            else:
                logging.info(f'Data chunk {start // chunk_size + 1} uploaded successfully! Total rows: {total_rows}')
        
        elif response.status_code == 200:
            # Sometimes 200 may also be returned if processing completes successfully
            response_data = response.json()
            total_rows = response_data.get("totalRowsCount", 0)
            logging.info(f'Data chunk {start // chunk_size + 1} uploaded successfully! Total rows: {total_rows}')
        
        else:
            logging.error(f'Failed to upload data chunk {start // chunk_size + 1}: {response.text}')
        return True
    
def fetch_netsuite_data(cleint_key, client_secret, resource_owner_key, resource_owner_secret, realm, searchID,restlet_url):
    session = OAuth1Session(
        client_key=cleint_key,
        client_secret=client_secret,
        resource_owner_key=resource_owner_key,
        resource_owner_secret=resource_owner_secret,
        signature_type='auth_header',
        signature_method='HMAC-SHA256',
        realm=realm
    )
    payload = {"searchID": searchID}
    response = session.post(restlet_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    return response


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self) -> None:

        # Define the required parameters
        """Runs the component.
        Validates the configuration parameters and triggers a Boomi job.
        """

       # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)

        
        client_key = self.configuration.parameters.get(NS_CLIENT_KEY)
        table_name = self.configuration.parameters.get(TABLE_NAME)
        client_secret = self.configuration.parameters.get(NS_CLIENT_SECRET)
        resource_owner_key = self.configuration.parameters.get(NS_RESOURCE_OWNER_KEY)
        resource_owner_secret = self.configuration.parameters.get(NS_RESOURCE_OWNER_SECRET)
        realm = self.configuration.parameters.get(NS_REALM)
        searchID = self.configuration.parameters.get(NS_SEARCH_ID)
        restlet_url=self.configuration.parameters.get(NS_RESTLET_URL)
        bucket_id = self.configuration.parameters.get(BUCKET_ID)
        keboola_token = self.configuration.parameters.get(KEBOOLA_TOKEN)
        keboola_table_id = f"{self.configuration.parameters.get(BUCKET_ID)}.{self.configuration.parameters.get(TABLE_NAME)}"
        logging.info(f"keboola_table_id: {keboola_table_id}")
        
        # Fetch data from Netsuite

        # Fetch data from NetSuite
        response = fetch_netsuite_data(
            client_key=client_key,
            client_secret=client_secret,
            resource_owner_key =resource_owner_key,
            resource_owner_secret =resource_owner_secret,
            realm =realm, 
            searchID = searchID,
            restlet_url= restlet_url)

        try:
            data = response.json()
            results = data.get('results', [])
            formatted_data = [
                {
                    "recordType": record.get("recordType"),
                    "id": record.get("id"),
                    "entityid": record["values"].get("entityid"),
                    "name": record["values"].get("altname"),
                    "note": record["values"].get("userNotes.note"),
                    "notetype": record["values"].get("userNotes.notetype"),
                    "notedate": record["values"].get("userNotes.notedate"),
                    "author": record["values"].get("userNotes.author", [{}])[0].get("text", "")
                } for record in results
            ]
            df = pd.DataFrame(formatted_data)
            logging.info(f"Total records fetched from netsuite: {len(formatted_data)}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return
        

        # Upload data to Keboola
        if df is not None:
            table_names = get_keboola_tables(keboola_token, bucket_id)
            if table_name in table_names:
                load_to_keboola(df, keboola_token = keboola_token, keboola_table_id=keboola_table_id, chunk_size=2500)
            else:
                create_keboola_table_direct(df, keboola_token = keboola_token, bucket_id = bucket_id, table_name=table_name)

"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        #logging.info("Component started")
        comp = Component()
    
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception("User configuration error: %s", exc)
        exit(1)
    except Exception as exc:
        logging.exception("Unexpected error: %s", exc)
        exit(2)
