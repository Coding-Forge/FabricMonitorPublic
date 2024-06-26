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
    headers = bob.get_context(graph=True)
    headers['Content-Type'] = 'application/json'

    sp = json.loads(settings['ServicePrincipal'])

    today = datetime.now()

    lakehouse_catalog = f"graph/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
    Groups = bool(settings.get("GraphExtractGroups"))
    
##################### INTIALIZE THE CONFIGURATION #####################

    graphUrl = "https://graph.microsoft.com/beta"
    outputPath = f"{lakehouse_catalog}"
    graphCalls = [
        {'users':
            { 
                'GraphURL': f'{graphUrl}/users?$select=id,displayName,assignedLicenses,UserPrincipalName',
                'FilePath': 'users.json'
            }
        },
        {"subscribedSkus":
            {
            'GraphURL': f'{graphUrl}/subscribedSkus?$select=id,capabilityStatus,consumedUnits,prepaidUnits,skuid,skupartnumber,prepaidUnits',
            'FilePath': 'subscribedskus.json'
            }
        }
    ]

    if Groups:
        graphCalls.append(
            {"groups":
                {
                    'GraphURL': f'{graphUrl}/groups?$filter=securityEnabled eq true&$select=id,displayName',
                    'FilePath': 'groups.json'
                }
            }
        )

    fm = File_Management()

    for call in graphCalls:
        for key, value in call.items():
            #print(f"Getting {key} from {value['GraphURL']} and file path {value['FilePath']}")


            response = requests.get(value['GraphURL'], headers=headers)
            if response.status_code == 200:
                result = response.json()

                info = result.get("value")

                await fm.save(path=lakehouse_catalog, file_name=value['FilePath'], content=info)

                #dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehouse_catalog)
                #await FF.write_json_to_file(directory_client=dc, file_name=value['FilePath'], json_data=result)

# TODO: Fix error that comes from return application/json
# doesn't kill the job but does throw an error

if __name__ == "__main__":
    asyncio.run(main())
