import os
import logging
from dotenv import dotenv_values

logging.basicConfig(filename='myapp.log', level=logging.INFO)

class Local_File_Management:

    def __init__(self, root_path:str=None):
        self.app_settings = dotenv_values("app/.env")
        self.root_path = root_path

    async def save(self, path:str, file_name:str, content):
        """
        param: path is the location to where the content will be saved
        param: file_name is the name of the file to be saved
        param: content is a dictionary that is converted to bytes and saved as the path and file name
        """
        
        try:

            path = f"{self.root_path}/{path}"
            cwd = os.getcwd()
            os.makedirs(path, exist_ok=True)
            os.chdir(path)

            path = f"{path}{file_name}"
            with open(file_name, "wb") as file:
                file.write(content)

            # change the directory back to the original
            os.chdir(cwd)
        except Exception as e:
            print(f"Error: {e}")
            exit()