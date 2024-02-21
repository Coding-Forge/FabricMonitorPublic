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
    
    FF = File_Table_Management(
        tenant_id=settings['ServicePrincipal']['TenantId'],
        client_id=settings['ServicePrincipal']['AppId'],
        client_secret=settings['ServicePrincipal']['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )
    api_root = "https://api.powerbi.com/v1.0/myorg/"
    rest_api = "admin/apps?$top=5000&$skip=0"

    response = requests.get(url=api_root+rest_api, headers=headers)
    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error: {response.status_code}")

if __name__ == "__main__":
    asyncio.run(main())