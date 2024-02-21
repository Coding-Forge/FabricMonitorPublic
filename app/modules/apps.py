import os
import json
import logging
import asyncio
import time

from ..utility.fab2 import File_Table_Management
from ..utility.helps import Bob
from datetime import datetime, timedelta

####### CATALOG PRECONFIGURATION #######
catalog_types = ["scan","snapshots"]
today = datetime.now()
getInfoDetails = "lineage=true&datasourceDetails=true&getArtifactUsers=true&datasetSchema=false&datasetExpressions=false"
getModifiedWorkspacesParams = "excludePersonalWorkspaces=False&excludeInActiveWorkspaces=False"
FullScanAfterDays = 30
reset  = True
####### CATALOG PRECONFIGURATION #######

logging.basicConfig(filename='myapp.log', level=logging.INFO)


async def main():
    logging.info('Started')
##################### INTIALIZE THE CONFIGURATION #####################
    bob = Bob()
    # get POWER BI context and settings -- this call must be synchronous
        
    settings = bob.get_settings()
    headers = bob.get_context()
    print(headers)
    
    FF = File_Table_Management(
        tenant_id=settings['ServicePrincipal']['TenantId'],
        client_id=settings['ServicePrincipal']['AppId'],
        client_secret=settings['ServicePrincipal']['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )

    lakehouse_catalog = f"{settings['LakehouseName']}.Lakehouse/Files/catalog/"

##################### INTIALIZE THE CONFIGURATION #####################

    try:
        state = await bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/catalog/")
    except Exception as e:
        print(f"Error: {e}")

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

# create a file structure for the api results
    scans = f"scan/{today.strftime('%Y')}/{today.strftime('%m')}/"
    snapshots =f"snapshots/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"

    snapshotFiles = list()

    # TODO: get all the scans for apps
    filePath = f"{snapshots}/apps.json"
    snapshotFiles.append(filePath)
    logging.info(f"Headers: {headers}")  

    rest_api = "admin/apps?$top=5000&$skip=0"
    try:
        result = await bob.invokeAPI(rest_api=rest_api, headers=headers)
    except Exception as e:
        print(f"Error: {e} - {result}")
    
    print(f"Result: {result}")
    # check to see if the filepath already exists
    if "ERROR" not in result:

        ## check if file already exists
        lakehouse_dir = f"{lakehouse_catalog}{snapshots}"

        print(f"Checking if {lakehouse_dir} exists")

        try:
            paths = FF.fsc.get_paths(lakehouse_dir)
            for path in paths:
                print(f"Path: {path.name}")
                if "app.json" in path.name:
                    exit(0)

        except Exception as e:
            print(f"Error: {e} - continue with executing code")    

        dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_dir)

        try:
            FF.write_json_to_file(directory_client=dc, file_name="apps.json", json_data=result)
        except TypeError as e:
            print(f"Please fix the async to handle the Error: {e} -- is this the issue")
        
    else:
        print(f"Did not get data", result)
    
    logging.info('Finished')


if __name__ == "__main__":
    asyncio.run(main() )


