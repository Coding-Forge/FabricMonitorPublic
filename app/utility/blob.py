import os
import json
import logging
from typing import Dict, Any, Coroutine
from dotenv import load_dotenv, dotenv_values
from datetime import datetime, timedelta

from azure.storage.blob import BlobClient
from azure.identity import ClientSecretCredential

logging.basicConfig(filename='myapp.log', level=logging.INFO)


class Blob_File_Management:

    def __init__(self):
        self.app_settings = dotenv_values(".env")
        self.storage_url = self.app_settings.get("storage_url")
        self.container_name = self.app_settings.get("container_name")
        self.sp = json.loads(self.app_settings['ServicePrincipal'])
        self.credentials = ClientSecretCredential(
            client_id=self.sp['AppId'],
            client_secret=self.sp['AppSecret'],
            tenant_id=self.sp['TenantId']
        )

    def read_from_file(self, blob_name):
        pass

    def write_to_file(self, blob_name, content):

        blob_client = BlobClient(
            account_url=self.storage_url, 
            container_name=self.container_name, 
            blob_name=blob_name, 
            credential=self.credentials
        )

        print(f"I got here: {content}")

        #with open('local_file.txt', 'rb') as local_file:
        blob_client.upload_blob(data=content)

        




        #blob_name = 'your-folder/sample-blob.txt'
        
        