import os
import json
import time
import asyncio
from codetiming import Timer

from datetime import datetime, timedelta
from ..utility.helps import Bob
#from ..utility.fab2 import File_Table_Management
from app.utility.file_management import File_Management

####### CATALOG PRECONFIGURATION #######
catalog_types = ["scan","snapshots"]
today = datetime.now()
getInfoDetails = "lineage=true&datasourceDetails=true&getArtifactUsers=true&datasetSchema=false&datasetExpressions=false"
getModifiedWorkspacesParams = "excludePersonalWorkspaces=False&excludeInActiveWorkspaces=False"
FullScanAfterDays = 30
reset  = True

throttleErrorSleepSeconds = 3700
scanStatusSleepSeconds = 5
getInfoOuterBatchCount = 1500
getInfoInnerBatchCount = 100
runsInParallel = 16       

####### CATALOG PRECONFIGURATION #######
##################### INTIALIZE THE CONFIGURATION #####################

bob = Bob()
settings = bob.get_settings()

# get POWER BI context
headers = bob.get_context()

lakehouse_catalog = f"{settings['LakehouseName']}.Lakehouse.Files/catalog/"

sp = json.loads(settings['ServicePrincipal'])

##################### INTIALIZE THE CONFIGURATION #####################



async def get_workspace_info(workspace_groups):
    workspaceScanResults = []
    
    body = {
        "workspaces":workspace_groups
    }

    #print(f"Scanning workspaces: {body}")

    rest_api = "admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True"
    result = await bob.invokeAPI(rest_api=rest_api, headers=headers, json=body) 

    if "ERROR" in result:
        print(f"Error: {result}")
    else:
        workspaceScanResults.append(result)

        for workspaceScanResult in workspaceScanResults:

            while(workspaceScanResult.get("status") in ["Running", "NotStarted"]):
            
                #print(f"Waiting for scan results, sleeping for {scanStatusSleepSeconds} seconds...")
                #time.sleep(scanStatusSleepSeconds)

                rest_api = f"admin/workspaces/scanStatus/{workspaceScanResult.get('id')}"
                result = await bob.invokeAPI(rest_api=rest_api, headers=headers)

                if "ERROR" in result:
                    print(f"Error: {result}")
                else:
                    workspaceScanResult["status"] = result.get("status")

            if "Succeeded" in workspaceScanResult["status"]:
                id = workspaceScanResult.get("id")

                rest_api = f"admin/workspaces/scanResult/{id}"
                scanResult = await bob.invokeAPI(rest_api=rest_api, headers=headers)

                # TODO: create a better check on whether scan results were returned or error thrown
                if "ERRORs" in scanResult:
                    print(f"Error: Did not get scan results for workspace {id}")
                else:

                    today = datetime.utcnow()
                    fm = File_Management()
                    path = f"catalog/scans/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
                    #dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=path)
                    try:
                        await fm.save(path=path, file_name="scanResults.json", content=scanResult)
                        #await FF.write_json_to_file(directory_client=dc, file_name="scanResults.json", json_data=scanResult)
                    except TypeError as e:
                        print(f"Please fix the async to handle the Error: {e} -- is this the issue")


async def get_workspace_info_wrapper(subgroup):
    await get_workspace_info(workspace_groups=subgroup)


async def main():
    state = await bob.get_state(f"/{settings['LakehouseName']}.Lakehouse/Files/catalog/")
    if isinstance(state, str):
        LastRun = json.loads(state).get("lastRun")
        LastFullScan = json.loads(state).get("lastFullScan")
    else:
        LastRun = state.get("lastRun")
        LastFullScan = state.get("lastFullScan")

    lastRun_tm = bob.convert_dt_str(LastRun)
    lastFullScan_tm = bob.convert_dt_str(LastFullScan)

    pivotScan = lastRun_tm + timedelta(days=-30)
    pivotFullScan = lastFullScan_tm + timedelta(days=-30)    


    rest_api = "admin/workspaces/modified?"
    result = await bob.invokeAPI(rest_api=rest_api, headers=headers)


    workspaces = list()

    # Check if the request was successful
    if "ERROR" not in result:
        # Convert the JSON response to a pandas DataFrame
        for workspace in result:
            workspaces.append(workspace.get("id"))
    else:
        # Handle the error case
        print(f"Error: {result}")
        # TODO: do I quit from the application or retry
        exit(0)

    # The first thing is to get all the workspaces that have been modified
    # Split into groups of 500
    # scroll through the list of workspaces and get the scan results for each workspace
        # and split each group as evenly as possible into 16 groups
        # and then run the scan results for each group of 500 workspaces
    # Split workspaceScanResults into groups of 500
    # The list is now a list of lists of up to 500 workspaces each that is partitioned into 16 subgroups
    # Split each group into 16 subgroups as evenly as possible
    # Each of the 16 subgroups is then run in parallel
    # Access the groups by using subgroups[][] and then run the scan results for each group of 500 workspaces

    groups_of_500 = [workspaces[i:i+500] for i in range(0, len(workspaces), 500)]

    subgroups = []
    for group in groups_of_500:
        items_per_subgroup = len(group) // runsInParallel
        remainder = len(group) % runsInParallel
        start_index = 0
        subgroup = []
        for i in range(runsInParallel):
            end_index = start_index + items_per_subgroup
            if i < remainder:
                end_index += 1
            subgroup.append(group[start_index:end_index])
            start_index = end_index
        subgroups.append(subgroup)

    work_queue = asyncio.Queue()

    for groups in subgroups:
        for subgroup in groups:
            await work_queue.put(subgroup)

    while not work_queue.empty():
        subgroup = await work_queue.get()
        try:
            await get_workspace_info_wrapper(subgroup=subgroup)
        
        # Try to catch any 429 errors
        except Exception as e:
            await asyncio.sleep(10)



if __name__ == "__main__":
    asyncio.run(main())