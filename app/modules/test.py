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


    family = {
        "name": "John",
        "age": 30,
        "city": "New York"
    }

    path = "FabricLake.Lakehouse/Files/activity/2024/02/"
    #dc = FF.create_directory_client(FF.fsc, "activity")
    dc = FF.create_directory(file_system_client=FF.fsc, directory_name=path)
    FF.write_json_to_file(dc, path, family)


if __name__ == "__main__":
    main()
