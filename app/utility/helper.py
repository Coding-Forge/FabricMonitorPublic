import os
import json
import requests
from datetime import datetime, timedelta
from adal import AuthenticationContext
from fabric import File_Table_Management

class Bob:
    def __init__(self, name):
        with open('../../config.json', 'r') as file:
            self.config = file.readlines()

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
        tenant_id = self.config['tenant_id']
        client_id = self.config['client_id']
        client_secret = self.config['client_secret']


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

    def get_state(path, file_name="state.json"):
        """
        takes a path arguement as string 
        takes a file_name as a string (optional parameter)
        returns a json file that has information about the last run date
        and scan dates
        """
        FF = File_Table_Management()
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

                with open(file_name, "r") as file:
                    config = json.loads(file.read())

                return config
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


    def invokeAPI(self, rest_api, headers):
        """
        Invoke a REST API
        url: str
        headers: dict
        """
        api_root = "https://api.powerbi.com/v1.0/myorg/"
        headers = self.get_context()
        
        url = api_root + rest_api
        response = requests.get(url, headers=headers)
        if response.status_code in [200, 202]:
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}")   