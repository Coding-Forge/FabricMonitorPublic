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

# CELL ********************

config = {
  "OutputPath": "./Data",
  "StorageAccountContainerRootPath": "",
  "StorageAccountConnStr": "DefaultEndpointsProtocol=https;AccountName=powerbimonitor91ed;AccountKey=KIFypgGgseWdcevH6RfBxo6cbmf9h1aGBqJTp1z9Rd7vPHL0DXWojFlIDu6dOlfCbBg53xWL4wgv+AStISnOAg==;EndpointSuffix=core.windows.net",
  "StorageAccountContainerName": "pbimonitor",
  "FullScanAfterDays": "30",
  "CatalogGetInfoParameters": "lineage=true&datasourceDetails=true&getArtifactUsers=true&datasetSchema=true&datasetExpressions=true",
  "CatalogGetModifiedParameters": "excludePersonalWorkspaces=false&excludeInActiveWorkspaces=true",
  "ServicePrincipal": {
    "AppId": "e15fadf9-bc05-4ade-af9f-d79a918bbacd",
    "AppSecret": "gCa8Q~w9zuEnkl9SBCo7OXcwriGBA7C26nGTIdzs",
    "TenantId": "0b69ab40-1bc7-4666-9f20-691ba105a907",
    "Environment": "Public"
  },
  "Subscription_ID": "4465cf7c-8bde-41f8-aa38-938da8ac30a9",
  "Subscription_Name": "ME-MngEnvMCAP084084-brcampb-1",
  "Function_App_Plan_Name": "coding-forge-app-plan",
  "Resource_Group_Name": "Coding-Forge",
  "LastRun": "2024-01-08",
  "GraphExtractGroups": "false",
  "GraphPaginateCount": 10000
}

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


# MARKDOWN ********************

# ### Utility functions
# 
# Until I can figure out where to put my utility or have a class this section will do


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

# MARKDOWN ********************

# # Authentication Credentials
# 
# Read the values from a config file or connect to Azure Key Vault to pull out the secrets

# CELL ********************

sp = config.get("ServicePrincipal")
client_id = sp.get("AppId")
client_secret = sp.get("AppSecret")
tenant_id = sp.get("TenantId")
outputBatchCount = 5000
outputPath = config.get("OutputPath")
api_root = "https://api.powerbi.com/v1.0/myorg/"

# MARKDOWN ********************

# ## Get Modified Workspaces  
# 
# Get the lastRun date that is written in the state file. this date will be used to get workspaces that have been modified after the recorded date
# 


# CELL ********************

# TODO: Fix this to use the lastRun date stored in the state.json file

lastRun = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
pivotDate = lastRun + timedelta(days=-30)

#lastRun.strftime("%Y-%m-%dT%H:%M:%SZ")
lastRun.strftime("%Y-%m-%d") + "T00:00:00"
lastRun

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

from azure.identity import DefaultAzureCredential

# Create a credential object
credential = DefaultAzureCredential()

# Use the credential to authenticate to Azure
# Replace 'Storage' with the desired resource type
token = credential.get_token("https://storage.azure.com")

# Access the token value
access_token = token.token

# Now you can use the access_token as needed
# For example, you can print it
print(access_token)

os.listdir()


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

# Define the authority URL
authority_url = f"https://login.microsoftonline.com/{tenant_id}"

print(f"Authority URL: {authority_url}")

# Define the resource URL
resource_url = "https://analysis.windows.net/powerbi/api"

# Define the API endpoint to get the list of workspaces that have been modified
api_url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified"
# Create an instance of the AuthenticationContext
context = AuthenticationContext(authority_url)

# Acquire an access token using the client credentials
token = context.acquire_token_with_client_credentials(resource_url, client_id, client_secret)
print(f"Token: {token}")
# Check if the token acquisition was successful
if 'accessToken' in token:
    access_token = token['accessToken']
    
    # Make a GET request to the API with the access token
    headers = {'Authorization': f'Bearer {access_token}'}
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
else:
    # Handle the authentication error
    print(f"Authentication failed: {token.get('error')}")


# CELL ********************

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


# CELL ********************

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import FileSystemClient

cred = ClientSecretCredential(tenant_id=tenant_id,
                              client_id=client_id,
                              client_secret=client_secret)

file_system_client = FileSystemClient(
    account_url="https://onelake.dfs.fabric.microsoft.com",
    file_system_name="FabricMonitor",
    credential=cred)
paths = file_system_client.get_paths(path="/FabricLake.Lakehouse/Files/")
for p in paths:
    print(p.name)

# CELL ********************

file_system_client

# CELL ********************

target_folder_path = "/FabricLake.Lakehouse/Files/activity/"

# Create the target folder if it doesn't exist
file_system_client.create_directory(target_folder_path)

# Get the DataLakeDirectoryClient for the target folder
directory_client = file_system_client.get_directory_client(target_folder_path)

# CELL ********************

create_path("activity/event/")

# CELL ********************

with open("activity/event/config.json", "w") as file:
    file.write(json.dumps(config))

# CELL ********************

# Upload the local file to the target folder

with open(local_file_path, "rb") as file:
    file_contents = file.read()
    file_client = directory_client.create_file(file_name)
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))

print("File uploaded successfully.")


# CELL ********************




# CELL ********************

def upload_file_to_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)
