import asyncio
import json
import logging
import os
import time

from datetime import datetime, timedelta
from ..utility.helps import Bob
from ..utility.fab2 import File_Table_Management
logging.basicConfig(filename='myapp.log', level=logging.INFO)

async def record_audits(DirectoryClient, FF:File_Table_Management, audit, pivotDate, pageIndex, outputPath):
    if pageIndex == 1:
        outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}.json"
        lakehouseFile = f"{pivotDate.strftime('%Y%m%d')}.json"
    else:
        outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}_{pageIndex}.json"
        lakehouseFile = f"{pivotDate.strftime('%Y%m%d')}_{pageIndex}.json"

    ### This can now be streamed using the write_json_to_file method
    # TODO: convert audits to json
    #with open(outputFilePath, "w") as file:
    #    file.write(json.dumps(audit))
    FF.write_json_to_file(directory_client=DirectoryClient, file_name=lakehouseFile, json_data=audit)
    #FF.upload_file_to_directory(directory_client=dc, local_path=outputPath, file_name=lakehouseFile)

    flagNoActivity = False

    pageIndex +=1 
    audits = ""


async def main():
    logging.info('Started')

    ##### INTIALIZE THE CONFIGURATION #####
    bob = Bob()
    settings = bob.get_settings()
    headers =  bob.get_context()

    sp = json.loads(settings['ServicePrincipal'])
    FF = File_Table_Management(
        tenant_id=sp['TenantId'],
        client_id=sp['AppId'],
        client_secret=sp['AppSecret'],
        workspace_name=settings['WorkspaceName']
    )

    ##### INTIALIZE THE CONFIGURATION #####
    config = await bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/activity/")



    await bob.save_state(path=f"{settings['LakehouseName']}.Lakehouse/Files/activity/")



if __name__ == "__main__":
    asyncio.run(main())