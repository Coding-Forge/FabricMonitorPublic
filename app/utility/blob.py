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
        self.container_name = self.app_settings.get("StorageAccountContainerName")
        self.sp = json.loads(self.app_settings['ServicePrincipal'])
        self.credentials = ClientSecretCredential(
            client_id=self.sp['AppId'],
            client_secret=self.sp['AppSecret'],
            tenant_id=self.sp['TenantId']
        )


    def read_from_file(self, blob_name):
        pass

    async def write_to_file(self, blob_name, content):
        """
        param blob_name is a string of the path where you want to save your file
        param content is a byte variable that can be uploaded to blob storage
        """

        conn_str = self.app_settings.get("StorageAccountConnStr")

        if conn_str is None:
            blob_client = BlobClient(
                account_url=self.storage_url, 
                container_name=self.container_name, 
                blob_name=blob_name, 
                credential=self.credentials,
                
            )

        else:
            blob_client = BlobClient.from_connection_string(
                conn_str=conn_str,
                container_name=self.container_name,
                blob_name=blob_name,
            )            

        blob_client.upload_blob(data=content, overwrite=True)

        