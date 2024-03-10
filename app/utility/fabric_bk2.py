import os
import json

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import FileSystemClient, DataLakeDirectoryClient
from dotenv import dotenv_values

class File_Table_Management:

    def __init__(self):
        self.settings = dotenv_values(".env")
        self.sp = json.loads(self.settings['ServicePrincipal'])
        self.tenant_id = self.sp['TenantId']
        self.client_id = self.sp['AppId']
        self.client_secret = self.sp['AppSecret']
        self.workspace_name = self.settings['WorkspaceName']
        #self.fsc = self.get_file_system_client()
        
    def __await__(self):
        # Call ls the constructor and returns the instance
        return self.get_file_system_client(
            client_id=self.client_id, 
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
            workspace_name=self.workspace_name).__await__()

    

    async def get_file_system_client(self) -> FileSystemClient:
        try:

            cred = ClientSecretCredential(tenant_id=self.tenant_id,
                                        client_id=self.client_id,
                                        client_secret=self.client_secret)

            file_system_client = FileSystemClient(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                file_system_name=self.workspace_name,
                credential=cred)
            return file_system_client

        except Exception as e:
            print(f"what is the error {e}")

    async def create_file_system_client(self, service_client, file_system_name: str) -> FileSystemClient:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        return file_system_client

    async def create_directory_client(self,  path: str) -> DataLakeDirectoryClient:
        file_system_client = await self.get_file_system_client()
        directory_client = file_system_client.get_directory_client(path)
        return await directory_client

    async def list_directory_contents(self, file_system_client: FileSystemClient, directory_name: str):
        paths = file_system_client.get_paths(path=directory_name)
        for path in paths:
            print(path.name + '\n')

    async def create_directory(self,  directory_name: str) -> DataLakeDirectoryClient:
        file_system_client = self.get_file_system_client()
        directory_client = file_system_client.create_directory(directory_name)

        return directory_client            

    async def upload_file_to_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
        file_client = directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="rb") as data:
            await file_client.upload_data(data, overwrite=True)

    async def download_file_from_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
        file_client = await directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="wb") as local_file:
            download = await file_client.download_file()
            local_file.write(download.readall())
            local_file.close()


    async def write_json_to_file(self, directory_client: DataLakeDirectoryClient, file_name, json_data):
        """
        param directory_client needs to be a client to a directory for uploading data
        param file_name is the name that will be saved to
        param json_data needs to be a json_byte content
        """
        try:
            file_client = await directory_client.get_file_client(file_name)
        except Exception as e:
            print("treat an error as if the file does not exist")
            directory_client.create_file(file_name)

        
        file_client.upload_data(json_data, overwrite=True)