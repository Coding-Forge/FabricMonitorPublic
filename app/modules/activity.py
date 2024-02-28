import asyncio
import json
import logging
import random


from datetime import datetime, timedelta
from ..utility.helps import Bob
from ..utility.fab2 import File_Table_Management
logging.basicConfig(filename='myapp.log', level=logging.INFO)

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

async def record_audits(DirectoryClient, FF:File_Table_Management, audit, pivotDate, pageIndex=1):


    #pageIndex = str(random.randint(100, 10000)).zfill(5)
    pageIndex = str(pageIndex).zfill(5)
    lakehouseFile = f"{pivotDate.strftime('%Y%m%d')}_{pageIndex}.json"
    FF.write_json_to_file(directory_client=DirectoryClient, file_name=lakehouseFile, json_data=audit)

    #if pageIndex == 1:
    #    outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}.json"
    #    lakehouseFile = f"{pivotDate.strftime('%Y%m%d')}.json"
    #else:
    #    outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}_{pageIndex}.json"
    #    lakehouseFile = f"{pivotDate.strftime('%Y%m%d')}_{pageIndex}.json"

    ### This can now be streamed using the write_json_to_file method
    # TODO: convert audits to json
    #with open(outputFilePath, "w") as file:
    #    file.write(json.dumps(audit))
    #FF.upload_file_to_directory(directory_client=dc, local_path=outputPath, file_name=lakehouseFile)

    flagNoActivity = False


async def activity_events(url=None, headers=None, pivotDate=None, pageIndex=1):
    audits = list()

    result = await bob.invokeAPI(rest_api=url, headers=headers)

    # check the https response code for 200
    if "ERROR" in result:
        logging.error(f"Error: {result}")
        innerLoop = False
    else:
        # this is common to both parts of the if statement
        if result.get("activityEventEntities"):
            audits.append(result.get("activityEventEntities"))

        # create the folder structure for the output path
        localPath = f"{settings.get('OutputPath')}/activity/{pivotDate.strftime('%Y')}/{pivotDate.strftime('%m')}/"
        lakehousePath = f"{settings['LakehouseName']}.Lakehouse/Files/activity/{pivotDate.strftime('%Y')}/{pivotDate.strftime('%m')}/"

        # create the folder structure for the output path                       
        #outputPath = bob.create_path(localPath)
        outputPath = localPath

        dc = await FF.create_directory(file_system_client=FF.fsc, directory_name=lakehousePath)

        # do a for loop until all json arrays in audits are read and written to storage
        for audit in audits:
            await record_audits(dc, FF, audit, pivotDate, pageIndex=pageIndex)

        try:
            if result.get("continuationUri"):
                continuationUri = result.get("continuationUri")

                if "continuationToken" in continuationUri:
                    head = headers
                    head['Content-Type'] = 'application/json'

                    # result = requests.get(url=continuationUri, headers=head)
                    # if result.status_code == 200:
                    #     print(f"what is the result {result}")
                    pageIndex+=1
                    await activity_events(url=continuationUri, headers=head, pivotDate=pivotDate, pageIndex=pageIndex)
        except Exception as e:
            print(f"Error: {e}")



async def main():
    logging.info('Started')

    config = await bob.get_state(f"{settings['LakehouseName']}.Lakehouse/Files/activity/")


    if isinstance(config, str):
        lastRun = json.loads(config).get("lastRun")
    else:
        lastRun = config.get("lastRun")

    # if lastRun is recorded then proceed from there
    lastRun_tm = bob.convert_dt_str(lastRun)
    pivotDate = lastRun_tm.replace(hour=0, minute=0, second=0, microsecond=0)
    # Your code here

    async def get_activity(pivotDate=pivotDate):
        while (pivotDate<datetime.now()):
            audits = list()
            pageIndex = 1
            flagNoActivity = True

            # keep the start and end time within a 24 hour period by adding 24 hours and removing 1 second 
            nextDate = (pivotDate + timedelta(hours=24)) + timedelta(seconds=-1)
            rest_api = f"admin/activityevents?startDateTime='{pivotDate.strftime('%Y-%m-%dT%H:%M:%SZ')}'&endDateTime='{nextDate.strftime('%Y-%m-%dT%H:%M:%SZ')}'"

            logging.info(f"Rest API: {rest_api}")
            continuationUri=""

            result = None
            innerLoop = True
            # python does not have a do while so this is the best way 
            # just need to break out of the loop when a condition is met

            await activity_events(url=rest_api, headers=headers, pivotDate=pivotDate)                        

            pivotDate += timedelta(days=1)


        #await get_activity(pivotDate=pivotDate)   
    await get_activity()


if __name__ == "__main__":
    asyncio.run(main())
