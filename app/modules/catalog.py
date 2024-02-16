import os
import json
import time

from datetime import datetime, timedelta
from ..utility.helper import Bob
from ..utility.fabric import File_Table_Management

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



def main():
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

    state = bob.get_state(f"/{settings['LakehouseName']}.Lakehouse/Files/catalog/")
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
    result = bob.invokeAPI(rest_api=rest_api, headers=headers)


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

        result = bob.invokeAPI(rest_api=rest_api, headers=headers, json=body) 

        if "ERROR" in result:
            print(f"Error: {result}")
        else:   
            workspaceScanResults.append(result)

            for workspaceScanResult in workspaceScanResults:

                while(workspaceScanResult.get("status") in ["Running", "NotStarted"]):
                
                    print(f"Waiting for scan results, sleeping for {scanStatusSleepSeconds} seconds...")

                    time.sleep(scanStatusSleepSeconds)

                    rest_api = f"admin/workspaces/scanStatus/{workspaceScanResult.get('id')}"

                    result = bob.invokeAPI(rest_api=rest_api, headers=headers)
                    if "ERROR" in result:
                        print(f"Error: {result}")
                    else:
                        workspaceScanResult["status"] = result.get("status")

                if "Succeeded" in workspaceScanResult["status"]:
                    id = workspaceScanResult.get("id")

                    rest_api = f"admin/workspaces/scanResult/{id}"

                    scanResult = bob.invokeAPI(rest_api=rest_api, headers=headers)
                    if "ERROR" in scanResult:
                        print(f"Error: {scanResult}")
                    else:
                        with open("scanResults.json",'w') as file:
                            file.write(json.dumps(scanResult))

                        today = datetime.utcnow()

                        path = f"{settings['LakehouseName']}.Lakehouse/Files/catalog/scans/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
                        dc = FF.create_directory(file_system_client=FF.fsc, directory_name=path)
                        FF.upload_file_to_directory(directory_client=dc, local_path=".", file_name="scanResults.json")
                    



if __name__ == "__main__":
    main()