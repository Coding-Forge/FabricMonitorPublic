import json
from app.utility.fabric import File_Table_Management
from app.utility.blob import Blob_File_Management
from dotenv import dotenv_values

class File_Management(File_Table_Management, Blob_File_Management):

    def __init__(self):
        self.settings = dotenv_values(".env")
        self.bfm = Blob_File_Management()
        self.ftm = File_Table_Management()

        if self.settings['container_name']:
            self.storage_location = "blob"
        else:
            self.storage_location = "lakehouse"
            

    async def save(self, path:str, file_name:str, content:dict):
        """
        param path is the location to where the content will be saved
        param file_name is the name of the file to be saved
        param content is a dictionary that is converted to bytes and saved as the path and file name
        """

        json_string = json.dumps(content)
        json_bytes = json_string.encode('utf-8')

        if self.storage_location == "blob":
            await self.bfm.write_to_file(blob_name=f"{path}/{file_name}", content=json_bytes)
        else:
            #TODO: create a directory
            #TODO: upload/stream to location

            path = f"{self.settings['LakehouseName']}.Lakehouse/Files/{path}"            
            dc = await self.ftm.create_directory(directory_name=path)
            await self.ftm.write_json_to_file(directory_client=dc, file_name=file_name, json_data=json_bytes)

