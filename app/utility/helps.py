import os
import json
import requests
import logging
import msal
import aiohttp

from datetime import datetime, timedelta
from adal import AuthenticationContext
from ..utility.fabric import File_Table_Management

logging.basicConfig(filename='myapp.log', level=logging.INFO)

class Bob:

    def __init__(self):
        with open('config.json', 'r') as file:
            f = file.read()
        
        if isinstance(f, str):
            self.app_settings = json.loads(f)
        else:
            self.app_settings = json.dumps(f)

    async def create_path(self, path):
        """
        Create a path if it does not exist
        path: str
        """
        folder_path = os.path.dirname(path)
        os.makedirs(folder_path, exist_ok=True)
        return path

    def get_context(self):
        """
        Get the access token for the Power BI API
        """
        sp = self.app_settings['ServicePrincipal']
        tenant_id = sp['TenantId']
        client_id = sp['AppId']
        client_secret = sp['AppSecret']

        # Create a ConfidentialClientApplication object
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}"
        )
        scopes = ["https://analysis.windows.net/powerbi/api/.default"]
        
        # Acquire a token using client credentials
        try:
            result = app.acquire_token_for_client(scopes=scopes)
            if "access_token" in result:
                access_token = result["access_token"]
                # Use the access token to make API calls to Power BI
                headers = {'Authorization': f'Bearer {access_token}'}

                # TODO: Add your Power BI API calls here

            else:
                # If silent token acquisition fails, fallback to interactive authentication
                result = app.acquire_token_for_client(scopes=scopes)

                if "access_token" in result:
                    # TODO: Add your Power BI API calls here
                    access_token = result["access_token"]
                    # Use the access token to make API calls to Power BI
                    headers = {'Authorization': f'Bearer {access_token}'}

                else:
                    print(result.get("error_description", "Authentication failed."))

            return headers
        
        except Exception as ex:
            print(ex)


    def convert_dt_str(self, date_time):
        """
        Convert a datetime object to a string
        date_time: datetime object
        """
        format = "%Y-%m-%dT%H:%M:%SZ"
        datetime_str = datetime.strptime(date_time, format)
    
        return datetime_str

    async def get_state(self, path, file_name="state.json"):
        """
        takes a path arguement as string 
        takes a file_name as a string (optional parameter)
        returns a json file that has information about the last run date
        and scan dates
        """

        sp = self.app_settings['ServicePrincipal']

        FF = File_Table_Management(
            tenant_id=sp['TenantId'],
            client_id=sp['AppId'],
            client_secret=sp['AppSecret'],
            workspace_name=self.app_settings['WorkspaceName']
        )

        fsc = FF.fsc

        paths = fsc.get_paths(path=path)
        found = False
        for p in paths:
            if file_name in p.name:
                found = True
                break
        
        # create a directory client
        dc = FF.create_directory_client(fsc, path=path)
        
        if found:
            try:
                FF.download_file_from_directory(directory_client=dc, local_path="./",file_name=file_name)

                with open(file_name, 'r') as file:
                    f = file.read()

                return f
                                                
            except Exception as e:
                print("An exception occurred while reading the file:", str(e))
        else:
            cfg = dict()
            if "catalog" in path:
                cfg["lastRun"] = (datetime.now() + timedelta(days=-30)).strftime('%Y-%m-%dT%H:%M:%SZ')
                cfg["lastFullScan"] = (datetime.now() + timedelta(days=-30)).strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                cfg["lastRun"] = (datetime.now() + timedelta(days=-30)).strftime('%Y-%m-%dT%H:%M:%SZ')

            try:
                with open(file_name, 'w') as file:
                    file.write(json.dumps(cfg))

                folder = FF.create_directory(file_system_client=FF.fsc, directory_name=path)    
                FF.upload_file_to_directory(directory_client=folder,local_path= "./", file_name=file_name)                

                return json.dumps(cfg)
            except Exception as e:
                print("An exception occurred while creating file:", str(e))

    logging.info('Started')

    async def invokeAPI(self, rest_api, headers=None, json=None):
        """
        Invoke a REST API
        url: str
        headers: dict
        body: dict
        """
        api_root = "https://api.powerbi.com/v1.0/myorg/"

        #async with aiohttp.ClientSession() as session:
        #    while not work_queue.empty():
        #        url = await work_queue.get()
        #        print(f"Task {name} getting URL: {url}")
        #        timer.start()
        #        async with session.get(url) as response:
        #            await response.text()
        #        timer.stop()
        url = api_root + rest_api

        if json:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=json) as response:
                    return await response.text()
        else:
            if not headers:
                url = rest_api
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        return await response.text()
            else:
                url = api_root + rest_api
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        return await response.text()

  

    def get_settings(self):
        """
        Get the settings from the config.json file
        """
        return self.app_settings
    

    logging.info('Finished')
