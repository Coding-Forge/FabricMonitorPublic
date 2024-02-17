import os
import json
import logging
import asyncio

from ..utility.fabric import File_Table_Management
from ..utility.helper import Bob
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
    settings = bob.get_settings()
    
    FF = File_Table_Management(
        tenant_id=settings['ServicePrincipal']['TenantId'],
        client_id=settings['ServicePrincipal']['AppId'],
        client_secret=settings['ServicePrincipal']['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )

    # get POWER BI context
    headers = bob.get_context()

    lakehouse_catalog = f"{settings['LakehouseName']}.Lakehouse.Files/catalog/"

##################### INTIALIZE THE CONFIGURATION #####################

    state = bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/catalog/")
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
    
    result = bob.invokeAPI(rest_api=rest_api, headers=headers)
    logging.info(f"Result: {result}")       

    # check to see if the filepath already exists
    if "ERROR" in result:

        found=False

        if not os.path.exists(filePath):
            #scanOutputPath = create_path()
            # you need to add the trailing / to make sure the last part is made a folder
            snapshotOutputPath = bob.create_path(f"{snapshots}")


            with open(filePath, 'w') as file:
                file.write(json.dumps(result))

            lakehouse_dir = f"{lakehouse_catalog}/{snapshots}"

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
        print(f"Did not get data", result)
    
    logging.info('Finished')


if __name__ == "__main__":
    main() 