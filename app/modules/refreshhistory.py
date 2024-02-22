import os
import json
import logging
import asyncio
import time
import requests

from ..utility.fab2 import File_Table_Management
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

    FF = File_Table_Management(
        tenant_id=settings['ServicePrincipal']['TenantId'],
        client_id=settings['ServicePrincipal']['AppId'],
        client_secret=settings['ServicePrincipal']['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )

    today = datetime.now()

    lakehouse_dir = f"{settings['LakehouseName']}.Lakehouse/Files/datasetrefresh/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    file_name = "workspaces.datasets.refreshes.json"

##################### INTIALIZE THE CONFIGURATION #####################

#    state = await bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/catalog/")
 
    rest_api = "admin/capacities/refreshables"


    result = await bob.invokeAPI(rest_api=rest_api, headers=headers)
    if "ERROR" in result:
        print(f"Error: {result}")
    else:
        dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_dir)
        FF.write_json_to_file(directory_client=dc, file_name=file_name, json_data=result)

# TODO: Fix error that comes from return application/json
# doesn't kill the job but does throw an error

if __name__ == "__main__":
    asyncio.run(main())

