import os
import json
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import FileSystemClient, DataLakeDirectoryClient

class File_Table_Management:

    def __init__(self, tenant_id, client_id, client_secret, workspace_name):
        self.fsc = self.get_file_system_client(
            client_id=client_id, 
            client_secret=client_secret, 
            tenant_id=tenant_id, 
            workspace_name=workspace_name)

    def get_file_system_client(self, client_id, client_secret, tenant_id, workspace_name) -> FileSystemClient:
        cred = ClientSecretCredential(tenant_id=tenant_id,
                                    client_id=client_id,
                                    client_secret=client_secret)

        file_system_client = FileSystemClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            file_system_name=workspace_name,
            credential=cred)

        return file_system_client

    def create_file_system_client(self, service_client, file_system_name: str) -> FileSystemClient:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        return file_system_client

    def create_directory_client(self, file_system_client: FileSystemClient, path: str) -> DataLakeDirectoryClient:
        directory_client = file_system_client.get_directory_client(path)
        return directory_client

    def list_directory_contents(self, file_system_client: FileSystemClient, directory_name: str):
        paths = file_system_client.get_paths(path=directory_name)
        for path in paths:
            print(path.name + '\n')

    def create_directory(self, file_system_client: FileSystemClient, directory_name: str) -> DataLakeDirectoryClient:
        directory_client = file_system_client.create_directory(directory_name)

        return directory_client            

    def upload_file_to_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
        file_client = directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="rb") as data:
            file_client.upload_data(data, overwrite=True)

    def download_file_from_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
        file_client = directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="wb") as local_file:
            download = file_client.download_file()
            local_file.write(download.readall())
            local_file.close()


    def write_json_to_file(self, directory_client: DataLakeDirectoryClient, file_name: str, json_data: dict):
        try:
            file_client = directory_client.get_file_client(file_name)
        except Exception as e:
            print("treat an error as if the file does not exist")
            directory_client.create_file(file_name)

        json_string = json.dumps(json_data)
        json_bytes = json_string.encode('utf-8')
        
        file_client.upload_data(json_bytes, overwrite=True)