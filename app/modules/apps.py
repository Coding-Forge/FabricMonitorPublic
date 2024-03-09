import os
import json
import logging
import asyncio
import time

#from ..utility.fab2 import File_Table_Management
from ..utility.helps import Bob

from app.utility.file_management import File_Management
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
    
    sp = json.loads(settings['ServicePrincipal'])
    

    lakehouse_catalog = f"catalog/"

##################### INTIALIZE THE CONFIGURATION #####################

    try:
        state = bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/catalog/")
    except Exception as e:
        print(f"Error: {e}")

    if isinstance(state, str):
        LastRun = json.loads(state).get("catalog").get("lastRun")
        LastFullScan = json.loads(state).get("catalog").get("lastFullScan")
    else:
        LastRun = state.get("catalog").get("lastRun")
        LastFullScan = state.get("catalog").get("lastFullScan")

    if LastRun is None:
        LastRun = datetime.now()

    if LastFullScan is None:
        LastFullScan = datetime.now()        

    lastRun_tm = bob.convert_dt_str(LastRun)
    lastFullScan_tm = bob.convert_dt_str(LastFullScan)

    pivotScan = lastRun_tm + timedelta(days=-30)
    pivotFullScan = lastFullScan_tm + timedelta(days=-30)    

# create a file structure for the api results
    #scans = f"scan/{today.strftime('%Y')}/{today.strftime('%m')}"
    snapshots =f"snapshots/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"

    snapshotFiles = list()

    # TODO: get all the scans for apps
    filePath = f"{snapshots}apps.json"
    snapshotFiles.append(filePath)
    logging.info(f"Headers: {headers}")  

    rest_api = "admin/apps?$top=5000&$skip=0"
    try:
        result = await bob.invokeAPI(rest_api=rest_api, headers=headers)
    except Exception as e:
        print(f"Error: {e} - {result}")
    
    # check to see if the filepath already exists
    if "ERROR" not in result:

        ## check if file already exists
        path = f"{lakehouse_catalog}{snapshots}"

        #print(f"Checking if {lakehouse_dir} exists")

        #try:
        #    paths = FF.fsc.get_paths(lakehouse_dir)
        #    for path in paths:
        #        #print(f"Path: {path.name}")
        #        if "app.json" in path.name:
        #            raise RuntimeError('app.json already exists')
        #except Exception as e:
        #    print(f"Error: {e} - continue with executing code")    

        #dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_dir)

        try:
            fm = File_Management()

            #only grab the value section from result
            info=result.get("value")
            await fm.save(path=path, file_name="apps.json", content=info)

            #await FF.write_json_to_file(directory_client=dc, file_name="apps.json", json_data=result)
        except TypeError as e:
            print(f"Please fix the async to handle the Error: {e} -- is this the issue")
        
    else:
        print(f"Did not get data", result)
    
    logging.info('Finished')


if __name__ == "__main__":
    asyncio.run(main() )


