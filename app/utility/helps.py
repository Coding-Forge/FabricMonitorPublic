import os
import json
import logging
import msal
import aiohttp
from typing import Dict, Any, Coroutine
from dotenv import load_dotenv, dotenv_values
from datetime import datetime, timedelta
from app.utility.fabric import File_Table_Management
from datetime import datetime

logging.basicConfig(filename='myapp.log', level=logging.INFO)


class Bob:

    def __init__(self):
        self.app_settings = dotenv_values(".env")

    async def create_path(self, path):
        """
        Create a path if it does not exist
        path: str
        """
        folder_path = os.path.dirname(path)
        os.makedirs(folder_path, exist_ok=True)
        return path

    def get_context(self, graph=False, tenant=False):
        """
        Get the access token for the Power BI API
        """
        try:
            sp = json.loads(self.app_settings['ServicePrincipal'])
            tenant_id = sp['TenantId']
            client_id = sp['AppId']
            client_secret = sp['AppSecret']

        except Exception as e:
            print("An exception occurred while reading the file:", str(e))

        if graph:
            authority = f"https://login.microsoftonline.com/{tenant_id}"
            scope = "https://graph.microsoft.com/.default"
        elif tenant:
            authority = f"https://login.microsoftonline.com/{tenant_id}"
            scope = "https://api.fabric.microsoft.com/.default"  
        else:
            #authority = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token?api-version=1.0"
            authority = f"https://login.microsoftonline.com/{tenant_id}"
            scope = "https://analysis.windows.net/powerbi/api/.default"
            

        # Create a ConfidentialClientApplication object
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority
        )

        scopes = [scope]
        
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

        print(f"What is the date time {date_time}")

        if isinstance(date_time, datetime):
            date_time = date_time.strftime(format)
            
        datetime_str = datetime.strptime(date_time, format)
    
        return datetime_str

    async def get_state(self, path, file_name="state.json"):
        """
        takes a path arguement as string 
        takes a file_name as a string (optional parameter)
        returns a json file that has information about the last run date
        and scan dates
        """
        sp = json.loads(self.app_settings['ServicePrincipal'])
        FF = File_Table_Management()
        fsc = FF.fsc #await FF.get_file_system_client()
        try:
            paths = fsc.get_paths(path=path)
            found = False
            for p in paths:
                if file_name in p.name:
                    found = True
                    break
        except TypeError as te:
            print(f"this is an error {te}")

        # create a directory client
        dc = FF.create_directory_client(path=path)
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

                folder = await FF.create_directory(file_system_client=FF.fsc, directory_name=path)    
                await FF.upload_file_to_directory(directory_client=folder,local_path= "./", file_name=file_name)                
       
                return await json.dumps(cfg)
            except Exception as e:
                print("An exception occurred while creating file:", str(e))

    logging.info('Started')

    async def save_state(self, path, file_name="state.json"):
        """
        takes a path arguement as string 
        takes a file_name as a string (optional parameter)
        saves a json file that has information about the last run
        """
        sp = json.loads(self.app_settings['ServicePrincipal'])
        FF = File_Table_Management()
            
        fsc = FF.fsc

        cfg = dict()
        # TODO: Figure out if this is a scan or fullscan
        if "catalog" in path:
            cfg["lastRun"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            cfg["lastFullScan"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            cfg["lastRun"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            with open(file_name, 'w') as file:
                file.write(json.dumps(cfg))

            folder = FF.create_directory(file_system_client=FF.fsc, directory_name=path)    
            FF.upload_file_to_directory(directory_client=folder,local_path= "./", file_name=file_name)                

            return json.dumps(cfg)
        except Exception as e:
            print(f"Save file exception - {e}")
        

    async def invokeAPI(self, rest_api, headers=None, json=None)-> Coroutine[Dict[str,Any], None, None]:
        """
        Invoke a REST API
        url: str
        headers: dict
        body: dict
        """
        api_root = "https://api.powerbi.com/v1.0/myorg/"

        url = api_root + rest_api

        ## The conintuation Token redirects to your organization for Power BI instead of accessing
        ## the REST API the originated the call. Therefore, we need to intercept and call the 
        ## using the continuation URI
        if "continuationToken" in rest_api:
            async with aiohttp.ClientSession() as session:
                async with session.get(url=rest_api, headers=headers) as response:
                    return await response.json()

        if json:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=json) as response:
                    return await response.json()
        else:
            if not headers:
                url = rest_api
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        return await response.json()
            else:
                url = api_root + rest_api
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        return await response.json()

  

    def get_settings(self):
        """
        Get the settings from the config.json file
        """
        return self.app_settings
    

    logging.info('Finished')