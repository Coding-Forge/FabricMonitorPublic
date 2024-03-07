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

    def save(self, path:str, file_name:str, content):
        if self.storage_location == "blob":
            self.bfm.write_to_file(blob_name=f"{path}/{file_name}", content=content)
        else:
            self.ftm.write_json_to_file()

