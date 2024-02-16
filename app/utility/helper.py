import os
import json
import requests
import logging

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



    # create a path if it does not exist on the local machine
    def create_path(path):
        """
        Create a path if it does not exist
        path: str
        """
        folder_path = os.path.dirname(path)
        os.makedirs(folder_path, exist_ok=True)
        return path

    # get access to the power bi apis and return the necessary header
    def get_context(self):
        """
        Get the access token for the Power BI API
        """
        sp = self.app_settings['ServicePrincipal']
        tenant_id = sp['TenantId']
        client_id = sp['AppId']
        client_secret = sp['AppSecret']


        # Define the authority URL
        authority_url = f"https://login.microsoftonline.com/{tenant_id}"

        print(f"Authority URL: {authority_url}")

        # Define the resource URL
        resource_url = "https://analysis.windows.net/powerbi/api"

        # Create an instance of the AuthenticationContext
        context = AuthenticationContext(authority_url)

        # Acquire an access token using the client credentials
        token = context.acquire_token_with_client_credentials(resource_url, client_id, client_secret)

        # Check if the token acquisition was successful
        if 'accessToken' in token:
            access_token = token['accessToken']
            
            # Make a GET request to the API with the access token
            headers = {'Authorization': f'Bearer {access_token}'}

        return headers

    def convert_dt_str(self, date_time):
        """
        Convert a datetime object to a string
        date_time: datetime object
        """
        format = "%Y-%m-%dT%H:%M:%SZ"
        datetime_str = datetime.strptime(date_time, format)
    
        return datetime_str

    def get_state(self, path, file_name="state.json"):
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

    def invokeAPI(self, rest_api, headers=None, json=None):
        """
        Invoke a REST API
        url: str
        headers: dict
        body: dict
        """
        api_root = "https://api.powerbi.com/v1.0/myorg/"
        if json:
            url = api_root + rest_api
            response = requests.post(url, headers=headers, json=json)
        else:
            if not headers:
                url = rest_api
                response = requests.get(url)
            else:
                url = api_root + rest_api
                response = requests.get(url, headers=headers)

        if response.status_code in [200, 202]:
            return response.json()
        else:
            print(f"ERROR: {response.status_code} - {response.text}")   
            logging.info(f"ERROR: {response.status_code} - {response.text}")
    
    logging.info('Finished')

    def get_settings(self):
        """
        Get the settings from the config.json file
        """
        return self.app_settings
    