import os
import json
import logging
import asyncio
import time
import requests

from ..utility.fab2 import File_Table_Management
from ..utility.helps import Bob
from datetime import datetime, timedelta

####### CATALOG PRECONFIGURATION #######
catalog_types = ["scan","snapshots"]
today = datetime.now()
reset  = True
####### CATALOG PRECONFIGURATION #######

logging.basicConfig(filename='myapp.log', level=logging.INFO)

async def main():
    logging.info('Started')
##################### INTIALIZE THE CONFIGURATION #####################
    bob = Bob()
    # get POWER BI context and settings -- this call must be synchronous
    settings = bob.get_settings()
    headers = bob.get_context(tenant=True)
    headers['Content-Type'] = 'application/json'

    sp = json.loads(settings['ServicePrincipal'])
    FF = File_Table_Management(
        tenant_id=sp['TenantId'],
        client_id=sp['AppId'],
        client_secret=sp['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )

    today = datetime.now()

    lakehouse_dir = f"{settings['LakehouseName']}.Lakehouse/Files/tenant/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"

    tenantUrl = "https://api.fabric.microsoft.com/v1/admin/tenantsettings"
    apiResource = "https://api.fabric.microsoft.com/"
    TenantFilePath = "$($outputPath)\tenant-settings.json"

##################### INTIALIZE THE CONFIGURATION #####################

#    state = await bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/catalog/")
 
    url = "https://api.fabric.microsoft.com/v1/admin/tenantsettings"
    

    response = requests.get(url=url, headers=headers)
    if response.status_code == 200:
        result = response.json()

        dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_dir)
        await FF.write_json_to_file(directory_client=dc, file_name="tenant-settings.json", json_data=result)

# TODO: Fix error that comes from return application/json
# doesn't kill the job but does throw an error

if __name__ == "__main__":
    asyncio.run(main())

