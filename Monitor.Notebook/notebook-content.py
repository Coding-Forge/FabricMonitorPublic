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


# CELL ********************

UTC_FORMAT="%Y-%m-%dT%H:%M:%SZ"

# MARKDOWN ********************

# ### Utility functions
# 
# Until I can figure out where to put my utility or have a class this section will do  
# 
# > To work with files on lakehouse or warehouse see examples  
# > using service principals with fabric lakehouse and warehouse  
# > [Fabric Service Principals](https://debruyn.dev/2023/how-to-use-service-principal-authentication-to-access-microsoft-fabrics-onelake/)  
# > [Microsoft Docs](https://github.com/MicrosoftDocs/fabric-docs/blob/main/docs/onelake/onelake-access-python.md)  
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

class FabricFiles:

    def __init__(self, tenant_id, client_id, client_secret, workspace_name):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.workspace_name = workspace_name
        self.fsc = self.get_file_system_client(client_id, client_secret, tenant_id, workspace_name)
            

    def get_file_system_client(self, client_id, client_secret, tenant_id, workspace_name) -> FileSystemClient:
        cred = ClientSecretCredential(tenant_id=tenant_id,
                                    client_id=client_id,
                                    client_secret=client_secret)

        file_system_client = FileSystemClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            file_system_name=workspace_name,
            credential=cred)

        return file_system_client

    def create_file_system_client(self, service_client, file_system_name: str) -> FileSystemClient:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        return file_system_client

    def create_directory_client(self, file_system_client: FileSystemClient, path: str) -> DataLakeDirectoryClient:
        directory_client = file_system_client.get_directory_client(path)
        return directory_client

    def list_directory_contents(self, file_system_client: FileSystemClient, directory_name: str):
        paths = file_system_client.get_paths(path=directory_name)
        for path in paths:
            print(path.name + '\n')

    def create_directory(self, file_system_client: FileSystemClient, directory_name: str) -> DataLakeDirectoryClient:
        directory_client = file_system_client.create_directory(directory_name)

        return directory_client            

    def upload_file_to_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
        file_client = directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="rb") as data:
            file_client.upload_data(data, overwrite=True)

    def download_file_from_directory(self, directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
        file_client = directory_client.get_file_client(file_name)

        with open(file=os.path.join(local_path, file_name), mode="wb") as local_file:
            download = file_client.download_file()
            local_file.write(download.readall())
            local_file.close()

    

## data retreival from lakehouse

def get_state(path, file_name="state.json"):
    """
    takes a path arguement as string 
    takes a file_name as a string (optional parameter)
    returns a json file that has information about the last run date
    and scan dates
    """
    FF = FabricFiles(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        workspace_name=workspace_name
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


# MARKDOWN ********************

# ## Get Secrets from Key Vault

# CELL ********************

# TODO: set up key vault and get secrets

client_id = "e15fadf9-bc05-4ade-af9f-d79a918bbacd"
client_secret = "gCa8Q~w9zuEnkl9SBCo7OXcwriGBA7C26nGTIdzs"
tenant_id = "0b69ab40-1bc7-4666-9f20-691ba105a907"
workspace_name = "FabricMonitor"
lakehouse_name = "FabricLake"
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

# get the information from state.json
config = get_state(f"/{lakehouse_name}.Lakehouse/Files/activity/")
if isinstance(config, str):
    lastRun = json.loads(config).get("lastRun")
else:
    lastRun = config.get("lastRun")

lastRun_tm = convert(lastRun)
pivotDate = lastRun_tm + timedelta(days=-30)


# CELL ********************

config

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

# MARKDOWN ********************

# ### Get Configuration data for Catalog Scan
# 
# > We need to get the values for lastRun and lastFullScan  
# > If the json file does not exist then we need to create it  
# >and save it in the appropriate folder
# 
# > This is the catalog folder

# CELL ********************

# TODO: see if state.json exists on the blob storage 

config = get_state(f"/{lakehouse_name}.Lakehouse/Files/catalog/")
if isinstance(config, str):
    LastRun = json.loads(config).get("lastRun")
    LastFullScan = json.loads(config).get("lastFullScan")
else:
    LastRun = config.get("lastRun")
    LastFullScan = config.get("lastFullScan")

lastRun_tm = convert(LastRun)
lastFullScan_tm = convert(LastFullScan)

pivotScan = lastRun_tm + timedelta(days=-30)
pivotFullScan = lastFullScan_tm + timedelta(days=-30)

# MARKDOWN ********************

# ### Get Catalog Apps Information
# 
# [Get admin/apps](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/apps-get-apps-as-admin) 


# CELL ********************

# TODO: make sure the path exists for the data to be written to {scans, snapshots}

catalog_types = ["scan","snapshots"]

today = datetime.now()

scans = f"{lakehouse_name}.Lakehouse.Files/catalog/scan/{today.strftime('%Y')}/{today.strftime('%m')}"
snapshots =f"snapshots/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
#snapshots =f"{lakehouse_name}.Lakehouse.Files/catalog/snapshots/{today.strftime('%Y')}/{today.strftime('%m')}"

FF = FabricFiles(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
    workspace_name=workspace_name
)


# CELL ********************

headers = get_context()

snapshotFiles = list()

# TODO: get all the scans for apps
filePath = f"{snapshots}/apps.json"
snapshotFiles.append(filePath)


rest_api = "admin/apps?$top=5000&$skip=0"

url = api_root + rest_api
print("What is the url {}".format(url))
response = requests.get(url=url, headers=headers)
   

# check to see if the filepath already exists
if response.status_code == 200:
    result = response.json()

    found=False

    if not os.path.exists(filePath):
        #scanOutputPath = create_path()
        # you need to add the trailing / to make sure the last part is made a folder
        snapshotOutputPath = create_path(f"{snapshots}")


        with open(filePath, 'w') as file:
            file.write(json.dumps(result))

        lakehouse_dir = f"{lakehouse_name}.Lakehouse/Files/catalog/{snapshots}"
        print(f"What if the folder path: {lakehouse_dir}")

        # TODO: this works up to here in that it creates a local file that will then be uploaded to Files
        try:
            FF.list_directory_contents(file_system_client=FF.fsc, directory_name=lakehouse_dir)

            paths = FF.fsc.get_paths(lakehouse_dir)
            for path in paths:
                if "app.json" in path:
                    found = True
                else:
                    found = False

# TODO: create a function that will check if a file exists in the specified directory
            if not found:
                try:
                    FF.upload_file_to_directory(directory_client=folder,local_path=snapshots, file_name="apps.json")    
                except Exception as e:
                    print(e)

        except Exception as e:
            if "BadRequest" in str(e) or "The specified path does not exist" in str(e):
                print("Directory could not be found on Lakehouse")
                #dc = FF.create_directory_client(file_system_client=FF.fsc, path=lakehouse_dir)
                folder = FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_dir)
                try:
                    FF.upload_file_to_directory(directory_client=folder,local_path=snapshots, file_name="apps.json")    
                except Exception as e:
                    print(e)
            else:
                print("something else", str(e))
else:
    print(f"Did not get data", str(response.status_code))


# MARKDOWN ********************

# ## Get Catalog Scan
# 
# use the API to get scans or full scans  
# modify the following two variables `getInfoDetails` and `getModifiedWorkspaceParams` to alter the data being recorded

# CELL ********************

getInfoDetails = "lineage=true&datasourceDetails=true&getArtifactUsers=true&datasetSchema=false&datasetExpressions=false"
getModifiedWorkspacesParams = "excludePersonalWorkspaces=False&excludeInActiveWorkspaces=False"
FullScanAfterDays = 30
reset  = True

# CELL ********************

from dateutil.parser import parse

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False

# MARKDOWN ********************

# #### Figure out if we need to run a full scan
# 
# check to see if the `lastScan` and `lastFullScan` differ by 30 days or more.  
# If the remainder is greater than 30 then set `modifiedLastScan` to the minimum allowed  
# date of `Now() -30 days`. Also check to make sure the `lastScan` is not older than 30  
# days from the current date
# 
# **NOTE:** The date for `modifiedLastRun` must be in ISO-8601 format. _DO NOT DEVIATE_  `datetime.now().isoformat()` does not work as it leaves off the milliseconds
# ```
# 2024-01-15T13:31:44.0000000Z
# ```


# CELL ********************

fullScan = False

if is_date(LastRun) and is_date(LastFullScan):
    daysSinceLastFullScan =  lastRun_tm - lastFullScan_tm

if daysSinceLastFullScan.days > FullScanAfterDays:
        fullScan = True
else:
    print(f"Days to next full scan {FullScanAfterDays - daysSinceLastFullScan.days}")

if not fullScan:
    if (datetime.now() - lastRun_tm).days > 30:
        print("Cannot scan past 30 days. Will update lastrun to minimum allowable days")
        modifiedLastRun = (datetime.now() + timedelta(days=-30)).isoformat()
    else:
        modifiedLastRun = lastRun_tm.isoformat()+".0000000Z"
        

parameters = f"modifiedSince={modifiedLastRun}&" + getModifiedWorkspacesParams 

# MARKDOWN ********************

# ### Get Modified Workspaces since last scan  
# 
# Supporting Documentation: [Modified Workspaces](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-modified-workspaces)
# 
# ```
# GET https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?modifiedSince={modifiedSince}&excludePersonalWorkspaces={excludePersonalWorkspaces}&excludeInActiveWorkspaces={excludeInActiveWorkspaces}  
# 
# GET https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?modifiedSince=2020-10-02T05:51:30.0000000Z&excludePersonalWorkspaces=True&excludeInActiveWorkspaces=True
# ```

# CELL ********************

rest_api = "admin/workspaces/modified?"
url = api_root + rest_api + parameters

#api_url="https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?modifiedSince=2024-01-15T13:31:44.0000000Z&excludePersonalWorkspaces=false&excludeInActiveWorkspaces=true"
#response = requests.get(api_url, headers=headers)
response = requests.get(url, headers=headers)

throttleErrorSleepSeconds = 3700
scanStatusSleepSeconds = 5
getInfoOuterBatchCount = 1500
getInfoInnerBatchCount = 100       

workspaces = list()

# Check if the request was successful
if response.status_code == 200:
    # Convert the JSON response to a pandas DataFrame
    data = response.json()
    for workspace in data:
        workspaces.append(workspace.get("id"))
   
else:
    # Handle the error case
    print(f"Error: {response.status_code} - {response.text}")

# MARKDOWN ********************

# ## Get Workspace Info
# 
# Supporting document: [admin/workspace-info](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info)
# 
# > API URL with method
# ```
# POST https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True
# ```
# >Request body
# 
# ```
# {
#   "workspaces": [
#     "97d03602-4873-4760-b37e-1563ef5358e3",
#     "67b7e93a-3fb3-493c-9e41-2c5051008f24"
#   ]
# }
# ```

# CELL ********************

# check to see if any of the workspaces have been modified
# the list will have all the workspace ids that have been modified within the specified date range

rest_api ="admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True"
url = api_root + rest_api

if workspaces:
    body = {
        "workspaces":workspaces
    }

    response = requests.post(url=url, headers=headers, json=body)    
    if response.status_code==202:
        result = response.json()




# CELL ********************

result

# CELL ********************

import time

def waitOn429Error(script:str, sleepSeconds:int=3601, tentatives:int=1):
    try:
        
        #Invoke-Command -ScriptBlock $script
        exec(script)

    except Exception as e:
        ex = str(e)

        errors = [
            "*Error reading JObject from JsonReader*",
            "*Response status code does not indicate success: *",
            "*429 (Too Many Requests)*",
            "*You have exceeded the amount of requests allowed*"
        ]    
        errorText = ex
        ## If code errors at this location it is likely due to a 429 error. The PowerShell comandlets do not handle 429 errors with the appropriate message. This code will cover the known errors codes.
        if errorText in errors:
            tentatives -= 1

            if tentatives < 0:
               throw "[Wait-On429Error] Max Tentatives reached!"    
            else:
                time.sleep(sleepSeconds)
                waitOn429Error(script=script, sleepSeconds=sleepSeconds, tentatives=tentatives)            
        else:
            throw

# MARKDOWN ********************

# $modifiedRequestUrl = "admin/workspaces/modified?$getModifiedWorkspacesParams"
# 
#     Write-Host "Reset: $reset"
#     Write-Host "Since: $($state.Catalog.LastRun)"
#     Write-Host "FullScan: $fullScan"
#     Write-Host "Last FullScan: $($state.Catalog.LastFullScan)"
#     Write-Host "FullScanAfterDays: $($config.FullScanAfterDays)"
#     Write-Host "GetModified parameters '$getModifiedWorkspacesParams'"
#     Write-Host "GetInfo parameters '$getInfoDetails'"
#     
#     # Get Modified Workspaces since last scan (Max 30 per hour)
#     
#     $workspacesModified = Invoke-PowerBIRestMethod -Url $modifiedRequestUrl -Method Get | ConvertFrom-Json
# 
#     if (!$workspacesModified -or $workspacesModified.Count -eq 0) {
#         Write-Host "No workspaces modified"
#     }
#     else {
#         Write-Host "Modified workspaces: $($workspacesModified.Count)"    
# 
#         $throttleErrorSleepSeconds = 3700
#         $scanStatusSleepSeconds = 5
#         $getInfoOuterBatchCount = 1500
#         $getInfoInnerBatchCount = 100        
# 
#         Write-Host "Throttle Handling Variables: getInfoOuterBatchCount: $getInfoOuterBatchCount;  getInfoInnerBatchCount: $getInfoInnerBatchCount; throttleErrorSleepSeconds: $throttleErrorSleepSeconds"
#         # postworkspaceinfo only allows 16 parallel requests, Get-ArrayInBatches allows to create a two level batch strategy. It should support initial load without throttling on tenants with ~50000 workspaces
#         # Call Get-ArrayInBatches from the Utils.psm1 module and execute the content of the script block 
#         Get-ArrayInBatches -array $workspacesModified -label "GetInfo Global Batch" -batchCount $getInfoOuterBatchCount -script {
#     

# CELL ********************

#api_url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified"

api_url="https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?modifiedSince=2024-01-15T13:31:44.0000000Z&excludePersonalWorkspaces=false&excludeInActiveWorkspaces=true"
response = requests.get(api_url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Convert the JSON response to a pandas DataFrame
    data = response.json()

    # begin the looping proces of workspace scan using the id
    for workspace_id in data:
        print(workspace_id)


        Get-ArrayInBatches -array $workspacesModified -label "GetInfo Global Batch" -batchCount $getInfoOuterBatchCount -script {
            param($workspacesModifiedOuterBatch, $i)
                                            
            $script:workspacesScanRequests = @()

            # Call GetInfo in batches of 100 (MAX 500 requests per hour)
            Get-ArrayInBatches -array $workspacesModifiedOuterBatch -label "GetInfo Local Batch" -batchCount $getInfoInnerBatchCount -script {
                param($workspacesBatch, $x)
                
                Wait-On429Error -tentatives 1 -sleepSeconds $throttleErrorSleepSeconds -script {
                    
                    $bodyStr = @{"workspaces" = @($workspacesBatch.Id) } | ConvertTo-Json
        
                    # $script: scope to reference the outerscope variable

                    $getInfoResult = @(Invoke-PowerBIRestMethod -Url "admin/workspaces/getInfo?$getInfoDetails" -Body $bodyStr -method Post | ConvertFrom-Json)

                    $script:workspacesScanRequests += $getInfoResult

                }
            }                








else:
    # Handle the error case
    print(f"Error: {response.status_code} - {response.text}")

# CELL ********************

target_folder_path = "/FabricLake.Lakehouse/Files/activity/"

# Create the target folder if it doesn't exist
file_system_client.create_directory(target_folder_path)

# Get the DataLakeDirectoryClient for the target folder
directory_client = file_system_client.get_directory_client(target_folder_path)

# MARKDOWN ********************

# ### UTILITY
# 
# function Get-ArrayInBatches
# {
#     [cmdletbinding()]
#     param
#     (        
#         [array]$array
#         ,
#         [int]$batchCount
#         ,
#         [ScriptBlock]$script
#         ,
#         [string]$label = "Get-ArrayInBatches"
#     )
# 
#     $skip = 0
#     
#     $i = 0
# 
#     do
#     {   
#         $batchItems = @($array | Select-Object -First $batchCount -Skip $skip)
# 
#         if ($batchItems)
#         {
#             Write-Host "[$label] Batch: $($skip + $batchCount) / $($array.Count)"
#             
#             Invoke-Command -ScriptBlock $script -ArgumentList @($batchItems, $i)
# 
#             $skip += $batchCount
#         }
#         
#         $i++
#         
#     }
#     while($batchItems.Count -ne 0 -and $batchItems.Count -ge $batchCount)   
# }
# 
# function Wait-On429Error
# {
#     [cmdletbinding()]
#     param
#     (        
#         [ScriptBlock]$script
#         ,
#         [int]$sleepSeconds = 3601
#         ,
#         [int]$tentatives = 1
#     )
#  
#     try {
#         
#         Invoke-Command -ScriptBlock $script
# 
#     }
#     catch {
# 
#         $ex = $_.Exception
#         
#         $errorText = $ex.ToString()
#         ## If code errors at this location it is likely due to a 429 error. The PowerShell comandlets do not handle 429 errors with the appropriate message. This code will cover the known errors codes.
#         if ($errorText -like "*Error reading JObject from JsonReader*" -or ($errorText -like "*429 (Too Many Requests)*" -or $errorText -like "*Response status code does not indicate success: *" -or $errorText -like "*You have exceeded the amount of requests allowed*")) {
# 
#             Write-Host "'429 (Too Many Requests)' Error - Sleeping for $sleepSeconds seconds before trying again" -ForegroundColor Yellow
#             Write-Host "Printing Error for Logs: '$($errorText)'"
#             $tentatives = $tentatives - 1
# 
#             if ($tentatives -lt 0)
#             {            
#                throw "[Wait-On429Error] Max Tentatives reached!"    
#             }
#             else
#             {
#                 Start-Sleep -Seconds $sleepSeconds
#                 
#                 Wait-On429Error -script $script -sleepSeconds $sleepSeconds -tentatives $tentatives            
#             }
#         }
#         else {
#             throw  
#         }         
#     }
# }

# MARKDOWN ********************

#     #region Workspace Scans: 1 - Get Modified; 2 - Start Scan for modified; 3 - Wait for scan finish; 4 - Get Results
#     $modifiedRequestUrl = "admin/workspaces/modified?$getModifiedWorkspacesParams"
#     $workspacesModified = Invoke-PowerBIRestMethod -Url $modifiedRequestUrl -Method Get | ConvertFrom-Json
# 
#     if (!$workspacesModified -or $workspacesModified.Count -eq 0) {
#         Write-Host "No workspaces modified"
#     }
#     else {
#         Write-Host "Modified workspaces: $($workspacesModified.Count)"    
# 
#         $throttleErrorSleepSeconds = 3700
#         $scanStatusSleepSeconds = 5
#         $getInfoOuterBatchCount = 1500
#         $getInfoInnerBatchCount = 100        
# 
#         Write-Host "Throttle Handling Variables: getInfoOuterBatchCount: $getInfoOuterBatchCount;  getInfoInnerBatchCount: $getInfoInnerBatchCount; throttleErrorSleepSeconds: $throttleErrorSleepSeconds"
#         # postworkspaceinfo only allows 16 parallel requests, Get-ArrayInBatches allows to create a two level batch strategy. It should support initial load without throttling on tenants with ~50000 workspaces
#         # Call Get-ArrayInBatches from the Utils.psm1 module and execute the content of the script block 
#         Get-ArrayInBatches -array $workspacesModified -label "GetInfo Global Batch" -batchCount $getInfoOuterBatchCount -script {
#             param($workspacesModifiedOuterBatch, $i)
#                                             
#             $script:workspacesScanRequests = @()
# 
#             # Call GetInfo in batches of 100 (MAX 500 requests per hour)
#             Get-ArrayInBatches -array $workspacesModifiedOuterBatch -label "GetInfo Local Batch" -batchCount $getInfoInnerBatchCount -script {
#                 param($workspacesBatch, $x)
#                 
#                 Wait-On429Error -tentatives 1 -sleepSeconds $throttleErrorSleepSeconds -script {
#                     
#                     $bodyStr = @{"workspaces" = @($workspacesBatch.Id) } | ConvertTo-Json
#         
#                     # $script: scope to reference the outerscope variable
# 
#                     $getInfoResult = @(Invoke-PowerBIRestMethod -Url "admin/workspaces/getInfo?$getInfoDetails" -Body $bodyStr -method Post | ConvertFrom-Json)
# 
#                     $script:workspacesScanRequests += $getInfoResult
# 
#                 }
#             }                
# 
#             # Wait for Scan to execute - https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspaceinfo_getscanstatus (10,000 requests per hour)
#             # Get successful scans
#             while (@($workspacesScanRequests | Where-Object status -in @("Running", "NotStarted"))) {
#                 Write-Host "Waiting for scan results, sleeping for $scanStatusSleepSeconds seconds..."
#         
#                 Start-Sleep -Seconds $scanStatusSleepSeconds
#         
#                 foreach ($workspaceScanRequest in $workspacesScanRequests) {    
#                     try {
#                         $scanStatus = Invoke-PowerBIRestMethod -Url "admin/workspaces/scanStatus/$($workspaceScanRequest.id)" -method Get | ConvertFrom-Json
#                         Write-Host "Scan '$($scanStatus.id)' : '$($scanStatus.status)'"
#                         $workspaceScanRequest.status = $scanStatus.status
#                     }
#                     catch [System.OutOfMemoryException] {
#                         # Handle the OutOfMemoryException here
#                         Write-Host "An OutOfMemoryException occurred: $_"
#                     }
#                 }
#             }
#         
#             # Get Scan results (500 requests per hour) - https://docs.microsoft.com/en-us/rest/api/power-bi/admin/workspaceinfo_getscanresult    
#             
#             foreach ($workspaceScanRequest in $workspacesScanRequests) {   
#                 Wait-On429Error -tentatives 1 -sleepSeconds $throttleErrorSleepSeconds -script {
#                     
#                     try {
#                         Write-Host "Getting scan result for '$($workspaceScanRequest.id)'"
#                         $scanResult = Invoke-PowerBIRestMethod -Url "admin/workspaces/scanResult/$($workspaceScanRequest.id)" -method Get | ConvertFrom-Json
#                         Write-Host "Scan Result'$($scanStatus.id)' : '$($scanResult.workspaces.Count)'"
#                     }
#                     catch [System.OutOfMemoryException] {
#                         Write-Host "ConvertFrom-Json failed with out of memory exception"
#                         <#Do this if a terminating exception happens#>
#                     }
#             
#                     $fullScanSuffix = ""
# 
#                     if ($fullScan) {              
#                         $fullScanSuffix = ".fullscan"      
#                     }
#                     
#                     $outputFilePath = "$scansOutputPath\$($workspaceScanRequest.id)$fullScanSuffix.json"
#             
#                     $scanResult | Add-Member –MemberType NoteProperty –Name "scanCreatedDateTime"  –Value $workspaceScanRequest.createdDateTime -Force
#             
#                     try{
#                         Write-Host "Writing to '$outputFilePath'"
#                         ConvertTo-Json $scanResult -Depth 10 -Compress | Out-File $outputFilePath -force
# 
#                     } catch [System.OutOfMemoryException] {
#                         Write-Host "An OutOfMemoryException occurred: $_"
#                         Write-Host "ConvertTo-Json failed for '$outputFilePath'"
#                     }
# 
#                     # Save to Blob
# 
#                     if ($config.StorageAccountConnStr -and (Test-Path $outputFilePath)) {
# 
#                         Write-Host "Writing to Blob Storage"
#                         
#                         $storageRootPath = "$($config.StorageAccountContainerRootPath)/catalog"
#             
#                         Add-FileToBlobStorage -storageAccountConnStr $config.StorageAccountConnStr -storageContainerName $config.StorageAccountContainerName -storageRootPath $storageRootPath -filePath $outputFilePath -rootFolderPath $outputPath     
# 
#                         Remove-Item $outputFilePath -Force
#                     }
#                 }
#             }
#         }                        
#     }
# 
#     #endregion
# 
#     # Save State
# 
#     Write-Host "Saving state"
# 
#     New-Item -Path (Split-Path $stateFilePath -Parent) -ItemType Directory -Force -ErrorAction SilentlyContinue | Out-Null
# 
#     $state.Catalog.LastRun = [datetime]::UtcNow.Date.ToString("o")
#     
#     if ($fullScan) {        
#         $state.Catalog.LastFullScan = [datetime]::UtcNow.Date.ToString("o")     
#     }
# 
#     ConvertTo-Json $state | Out-File $stateFilePath -force -Encoding utf8
# }
# finally {
#     $stopwatch.Stop()
# 
#     Write-Host "Ellapsed: $($stopwatch.Elapsed.TotalSeconds)s"
# }
