import os
import json
import logging
import asyncio
import time
import requests

#from ..utility.fab2 import File_Table_Management
from app.utility.file_management import File_Management
from ..utility.helps import Bob
from datetime import datetime, timedelta

####### Refresh History PRECONFIGURATION #######
today = datetime.now()
####### CATALOG PRECONFIGURATION #######

logging.basicConfig(filename='myapp.log', level=logging.INFO)

async def main():
    logging.info('Started')
##################### INTIALIZE THE CONFIGURATION #####################
    bob = Bob()
    # get POWER BI context and settings -- this call must be synchronous
    settings = bob.get_settings()
    headers = bob.get_context(tenant=True)
    #headers['Content-Type'] = 'application/json'

    sp = json.loads(settings['ServicePrincipal'])

    today = datetime.now()

    lakehouse_dir = f"datasetrefresh/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    file_name = "workspaces.datasets.refreshes.json"

##################### INTIALIZE THE CONFIGURATION #####################

#    state = await bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/catalog/")
 
    # rest_api = "admin/capacities/refreshables"
    # GET https://api.powerbi.com/v1.0/myorg/admin/groups?$expand=datasets
    # htpps://api.powerbi.com/v1.0/myorg/admin/groups?$expand=datasets
    rest_api = "admin/groups?$expand=datasets&$top=5000"

    fm = File_Management()

    # get a list of workspaces with datasets that have are refreshable
    result = await bob.invokeAPI(rest_api=rest_api, headers=headers)
    
    if "ERROR" in result:
        print(f"Error: {result}")
    else:

        # GET https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes

        resfresh_history = list()

        for item in result['value']:
            for dataset in item['datasets']:
                if dataset['isRefreshable']==True:
                    rest_api = f"groups/{item['id']}/datasets/{dataset['id']}/refreshes"
                    try:
                        refreshes = await bob.invokeAPI(rest_api=rest_api, headers=headers)
                        for refresh in refreshes['value']:
                            if len(refresh)>0:
                                resfresh_history.append(refresh)
                        
                        await fm.save(path=lakehouse_dir, file_name=file_name,content=resfresh_history)
                    except Exception as e:
                        pass

        #dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_dir)
        #await FF.write_json_to_file(directory_client=dc, file_name=file_name, json_data=result)

# TODO: Fix error that comes from return application/json
# doesn't kill the job but does throw an error

if __name__ == "__main__":
    asyncio.run(main())

