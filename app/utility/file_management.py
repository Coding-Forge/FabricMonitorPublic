import json
from app.utility.fabric import File_Table_Management
from app.utility.blob import Blob_File_Management
from app.utility.local import Local_File_Management

from dotenv import dotenv_values

class File_Management(File_Table_Management, Blob_File_Management, Local_File_Management):

    def __init__(self):
        self.settings = dotenv_values(".env")
        self.bfm = Blob_File_Management()
        self.ftm = File_Table_Management()
        self.lfm = Local_File_Management(self.settings['OutputPath'])


        if self.settings['container_name']:
            self.storage_location = "blob"
        elif self.settings["OutputPath"]:
            self.storage_location = "local"
        else:
            self.storage_location = "lakehouse"
            

    async def save(self, path:str, file_name:str, content):
        """
        param: path is the location to where the content will be saved
        param: file_name is the name of the file to be saved
        param: content is a dictionary that is converted to bytes and saved as the path and file name
        """
        
        try:

            if isinstance(content, dict) or isinstance(content, list):
                content = json.dumps(content)
                content = content.encode('utf-8')
            else:
                content = content.encode('utf-8')

        #save file to storage
            if self.storage_location == "blob":

                path = f"{path}{file_name}"
                #print(path)
                await self.bfm.write_to_file(blob_name=path, content=content)
            elif self.storage_location == "local":
                await self.lfm.save(path=path, file_name=file_name, content=content)    
            elif self.storage_location == "lakehouse":
                #TODO: create a directory
                #TODO: upload/stream to location

                path = f"{self.settings['LakehouseName']}.Lakehouse/Files/{path}"   

                path = path.replace("//","/")
                
                await self.ftm.write_json_to_file(path=path, file_name=file_name, json_data=content)
            else:
                print("No storage location found")
                exit()

        except Exception as e:
            print(f"Error: {e}")


