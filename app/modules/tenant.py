import os
import json
import logging
import asyncio
import time
import requests

from ..utility.helps import Bob
from app.utility.file_management import File_Management
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

    today = datetime.now()

    lakehouse_dir = f"tenant/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"

    tenantUrl = "https://api.fabric.microsoft.com/v1/admin/tenantsettings"
    apiResource = "https://api.fabric.microsoft.com/"
    TenantFilePath = "$($outputPath)\tenant-settings.json"

##################### INTIALIZE THE CONFIGURATION #####################

    url = "https://api.fabric.microsoft.com/v1/admin/tenantsettings"
    
    fm = File_Management()

    response = requests.get(url=url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        await fm.save(path=lakehouse_dir, file_name="tenant-settings.json", content=result)
#        dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_dir)
#        await FF.write_json_to_file(directory_client=dc, file_name="tenant-settings.json", json_data=result)

# TODO: Fix error that comes from return application/json
# doesn't kill the job but does throw an error

if __name__ == "__main__":
    asyncio.run(main())

