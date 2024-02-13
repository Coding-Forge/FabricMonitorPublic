# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "e3dc10b9-e97c-474b-b401-9881fb6a324f",
# META       "default_lakehouse_name": "FabricLake",
# META       "default_lakehouse_workspace_id": "c3975a5d-0560-40b2-b67a-a83a14bfc992",
# META       "known_lakehouses": [
# META         {
# META           "id": "e3dc10b9-e97c-474b-b401-9881fb6a324f"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d2db216c-8493-48e9-b118-a56c27142ed7",
# META       "workspaceId": "c3975a5d-0560-40b2-b67a-a83a14bfc992"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Get Secrets whether from Key Vault or config.json

# MARKDOWN ********************

# ## Python packages to be used in the notebook

# CELL ********************

import os
import requests
import pandas as pd
import json

from datetime import datetime
from datetime import timedelta
from adal import AuthenticationContext
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import FileSystemClient, DataLakeDirectoryClient


# MARKDOWN ********************

# ### Utility functions
# 
# Until I can figure out where to put my utility or have a class this section will do  
# 
# #### Using File System Client  
# 
# To work with files on lakehouse or warehouse see examples
# 
# using service principals with fabric lakehouse and warehouse 
# 
# [Fabric Service Principals](https://debruyn.dev/2023/how-to-use-service-principal-authentication-to-access-microsoft-fabrics-onelake/)
# 
# ```
#     paths = file_system_client.get_paths(path=f"/{lakehouse name}.Lakehouse/Files/")
#     for p in paths:
#         print(p.name)
# 
# 
#     paths = file_system_client.get_paths(path=f"/{warehouse name}.Warehouse/Tables/")
#     for p in paths:
#         print(p.name)
# ```

# CELL ********************

# makes the entire path of a string
def create_path(path):
    folder_path = os.path.dirname(path)
    os.makedirs(folder_path, exist_ok=True)
    return path

# get access to the power bi apis and return the necessary header
def get_context():
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

def convert(date_time):
    format = "%Y-%m-%dT%H:%M:%SZ"
    datetime_str = datetime.strptime(date_time, format)
 
    return datetime_str

## Lakehouse File functions

def get_file_system_client(tenant_id, client_id, client_secret, workspace_name) -> FileSystemClient:
    cred = ClientSecretCredential(tenant_id=tenant_id,
                                client_id=client_id,
                                client_secret=client_secret)

    file_system_client = FileSystemClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        file_system_name=workspace_name,
        credential=cred)

    return file_system_client

def create_file_system_client(service_client, file_system_name: str) -> FileSystemClient:
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    return file_system_client

def create_directory_client(file_system_client: FileSystemClient, path: str) -> DataLakeDirectoryClient:
    directory_client = file_system_client.get_directory_client(path)
    return directory_client

def list_directory_contents(file_system_client: FileSystemClient, directory_name: str):
    paths = file_system_client.get_paths(path=directory_name)
    for path in paths:
        print(path.name + '\n')

def upload_file_to_directory(directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)

def download_file_from_directory(directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="wb") as local_file:
        download = file_client.download_file()
        local_file.write(download.readall())
        local_file.close()

# MARKDOWN ********************

# ## Get Secrets from Key Vault

# CELL ********************

# TODO: set up key vault and get secrets

client_id = "e15fadf9-bc05-4ade-af9f-d79a918bbacd"
client_secret = "gCa8Q~w9zuEnkl9SBCo7OXcwriGBA7C26nGTIdzs"
tenant_id = "0b69ab40-1bc7-4666-9f20-691ba105a907"
workspace_name = "FabricMonitor"
outputPath = "./Data"

# MARKDOWN ********************

# # Authentication Credentials
# 
# Read the values from a config file or connect to Azure Key Vault to pull out the secrets

# CELL ********************

outputBatchCount = 5000
api_root = "https://api.powerbi.com/v1.0/myorg/"

# MARKDOWN ********************

# ## Get Modified Activities since last run date
# 
# Get the lastRun date that is written in the state file. this date will be used to get workspaces that have been modified after the recorded date
# 
# ```
# activity = dict()
# activity["lastRun"]= (datetime.now() + timedelta(days=-15)).strftime('%Y-%m-%dT%H:%M:%SZ')
# 
# 
# with open('settings.json', 'w') as file:
#     file.write(json.dumps(activity))
# ```

# CELL ********************

# TODO: check if state.json exists, if not one can make it and add the current date


fsc = get_file_system_client(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, workspace_name=workspace_name)

dc = create_directory_client(fsc, path="FabricLake.Lakehouse/Files/activity/")
upload_file_to_directory(directory_client=dc,local_path= "./", file_name="settings.json")

# CELL ********************

download_file_from_directory(directory_client=dc, local_path="./",file_name="settings.json")

with open("settings.json", "r") as file:
    config = json.loads(file.read())

lastRun = config.get("lastRun")

lastRun_tm = convert(lastRun)
pivotDate = lastRun_tm + timedelta(days=-30)


# MARKDOWN ********************

# ### Get Activity for a Time period
# 
# using the admin/activityevents api we can call for all the activities or pass in a start and end date with an optional filter  
# The format of the call will be as follows
#  
# documentation: [admin/activityevents](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/get-activity-events)
# 
# ```
# GET https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='2019-08-13T07:55:00.000Z'&endDateTime='2019-08-13T08:55:00.000Z'  
# 
# GET https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='2019-08-13T07:55:00.000Z'&endDateTime='2019-08-13T08:55:00.000Z'&$filter=Activity eq 'viewreport' and UserId eq 'john@contoso.com'
# ```

# CELL ********************

headers = get_context()

def get_activity():
    while (pivotDate<lastRun):
        audits = list()
        pageIndex = 1
        flagNoActivity = True

        # keep the start and end time within a 24 hour period by adding 24 hours and removing 1 second 
        nextDate = (pivotDate + timedelta(hours=24)) + timedelta(seconds=-1)
        api_url = f"admin/activityevents?startDateTime='{pivotDate.strftime('%Y-%m-%dT%H:%M:%SZ')}'&endDateTime='{nextDate.strftime('%Y-%m-%dT%H:%M:%SZ')}'"
        api = api_root + api_url

        continuationUri=""

        # python does not have a do while so this is the best way 
        # just need to break out of the loop when a condition is met
        while(True):

            if continuationUri:
                response = requests.get(url=result.get("continuationUri"))
            else:
                response = requests.get(url=api, headers=headers)            

            # check the https response code for 200
            if response.status_code==200:
                result = response.json()

                # this is common to both parts of the if statement
                if result.get("activityEventEntities"):
                    audits.append(result.get("activityEventEntities"))
            
                if result.get("continuaionURi"):
                    continuationUri = result.get("continuationUri")

                # create the folder structure for the output path
                outputPath = create_path(f"{config.get('OutputPath')}/activity/{pivotDate.strftime('%Y')}/{pivotDate.strftime('%m')}/")

                # do a for loop until all json arrays in audits are read and written to storage
                for audit in audits:

                    if pageIndex == 1:
                        outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}.json"
                    else:
                        outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}_{pageIndex}.json"

                    # TODO: convert audits to json
                    with open(outputFilePath, "w") as file:
                        file.write(json.dumps(audit))


                    flagNoActivity = False
                    pageIndex +=1 
                    audits = ""

                # get out of the inner while loop
                break

            else:
                print(f"Error: {response.status_code} - {response.text}")
                break

        pivotDate += timedelta(days=1)

# CELL ********************

def get_state():
    with open("state.json", "r") as file:
        json_data = file.read()

    # Parse JSON data
    data = json.loads(json_data)
    return data

# MARKDOWN ********************

# ## Get Catalog information

# CELL ********************

# TODO: see if state.json exists on the blob storage 
if os.path.exists("state.json"):
    state = get_state()
else:
    state = {
        "LastRun": "2024-02-01T00:00:00",
        "LastFullScan": "2024-01-15T00:00:00"
    }

    with open("state.json",'w') as file:
        file.write(json.dumps(state))

print(state)

# TODO: see if catalog last run is present

# TODO: make sure the path exists for the data to be written to {scans, snapshots}

catalog_types = ["scan","snapshots"]

today = datetime.now()

scanOutputPath = create_path("scan/{}/{}/".format(today.strftime('%Y'), today.strftime('%m')))
snapshotOutputPath = create_path("snapshots/{}/{}/".format(today.strftime('%Y'), today.strftime('%m')))


# TODO: authenticate

# TODO: get all the scans for apps

# TODO: scan workspaces that were modified
# TODO: determine if fullscan is required

pd = "abfs[s]://<workspace>@onelake.dfs.fabric.microsoft.com/<item>.<itemtype>/<path>/<fileName>"
pp = "https://FabricMonitor@onelake.dfs.fabric.microsoft.com/FabricLake.Lakehouse/Files/catalog"



response = requests.put(pp, data=json.dumps(state),headers=headers)
if response.status_code == 200:
    print("success")
else:
    print(f"failed with status code {response.status_code}")


# CELL ********************

#api_url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='2019-08-13T07:55:00'&endDateTime='2019-08-13T08:55:00'"
api_url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='2024-01-15T00:00:00'&endDateTime='2024-01-15T23:59:00'"
response = requests.get(api_url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Convert the JSON response to a pandas DataFrame
    data = response.json()
    
else:
    # Handle the error case
    print(f"Error: {response.status_code} - {response.text}")

# CELL ********************

api_url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified"
response = requests.get(api_url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Convert the JSON response to a pandas DataFrame
    data = response.json()
    for workspace in data:
        print(workspace.get("id"))
    
else:
    # Handle the error case
    print(f"Error: {response.status_code} - {response.text}")

# CELL ********************

target_folder_path = "/FabricLake.Lakehouse/Files/activity/"

# Create the target folder if it doesn't exist
file_system_client.create_directory(target_folder_path)

# Get the DataLakeDirectoryClient for the target folder
directory_client = file_system_client.get_directory_client(target_folder_path)
