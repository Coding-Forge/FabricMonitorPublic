import json
import os
import logging

from datetime import datetime, timedelta
from ..utility.helper import Bob
from ..utility.fabric import File_Table_Management
logging.basicConfig(filename='myapp.log', level=logging.INFO)

def main():
    logging.info('Started')

    ##### INTIALIZE THE CONFIGURATION #####
    bob = Bob()
    settings = bob.get_settings()
    headers = bob.get_context()

    FF = File_Table_Management(
        tenant_id=settings['ServicePrincipal']['TenantId'],
        client_id=settings['ServicePrincipal']['AppId'],
        client_secret=settings['ServicePrincipal']['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )

    ##### INTIALIZE THE CONFIGURATION #####
    
    config = bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/activity/")
    if isinstance(config, str):
        lastRun = json.loads(config).get("lastRun")
    else:
        lastRun = config.get("lastRun")

    # if lastRun is recorded then proceed from there
    lastRun_tm = bob.convert_dt_str(lastRun)
    pivotDate = lastRun_tm.replace(hour=0, minute=0, second=0, microsecond=0)
    # Your code here

    def push_family():
        family = {
            "name": "John",
            "age": 30,
            "city": "New York"
        }

        path = "FabricLake.Lakehouse/Files/activity/2024/02/"
        dc = FF.create_directory_client(FF.fsc, path=path)
        FF.write_json_to_file(dc, "family.json", family)



    push_family()




if __name__ == "__main__":
    main()

