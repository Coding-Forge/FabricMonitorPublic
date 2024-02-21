import os
import json
import time
import asyncio

from datetime import datetime, timedelta
from ..utility.helps import Bob
from ..utility.fab2 import File_Table_Management

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

####### CATALOG PRECONFIGURATION #######



async def main():
##################### INTIALIZE THE CONFIGURATION #####################
    bob = Bob()
    settings = bob.get_settings()
    
    # get POWER BI context
    headers = bob.get_context()

    lakehouse_catalog = f"{settings['LakehouseName']}.Lakehouse.Files/catalog/"
    
    FF = File_Table_Management(
        tenant_id=settings['ServicePrincipal']['TenantId'],
        client_id=settings['ServicePrincipal']['AppId'],
        client_secret=settings['ServicePrincipal']['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )

##################### INTIALIZE THE CONFIGURATION #####################

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


    rest_api ="admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True"

    if workspaces:

        body = {
            "workspaces":workspaces
        }

        workspaceScanResults = []

        result = await bob.invokeAPI(rest_api=rest_api, headers=headers, json=body) 

        if "ERROR" in result:
            print(f"Error: {result}")
        else:
            workspaceScanResults.append(result)

            for workspaceScanResult in workspaceScanResults:

                while(workspaceScanResult.get("status") in ["Running", "NotStarted"]):
                
                    print(f"Waiting for scan results, sleeping for {scanStatusSleepSeconds} seconds...")

                    time.sleep(scanStatusSleepSeconds)

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

                        path = f"{settings['LakehouseName']}.Lakehouse/Files/catalog/scans/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
                        dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=path)
                        try:
                            FF.write_json_to_file(directory_client=dc, file_name="scanResults.json", json_data=scanResult)
                        except TypeError as e:
                            print(f"Please fix the async to handle the Error: {e} -- is this the issue")


if __name__ == "__main__":
    asyncio.run(main())