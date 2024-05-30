import asyncio
import json
import logging
import random
import time

from datetime import datetime, timedelta
from ..utility.helps import Bob
from app.utility.file_management import File_Management
logging.basicConfig(filename='myapp.log', level=logging.INFO)

##### INTIALIZE THE CONFIGURATION #####
bob = Bob()
settings = bob.get_settings()
headers =  bob.get_context()

fm = File_Management()


##### INTIALIZE THE CONFIGURATION #####

async def get_workspaces(url:str, pageCount:int):
    response = await bob.invokeAPI(url, headers=headers)
    pivotDate = datetime.now()

    workspaces = response.get("workspaces")

    index = str(pageCount).zfill(5)

    Path = f"workspaces/{pivotDate.strftime('%Y')}/{pivotDate.strftime('%m')}/"
    await fm.save(path=Path, file_name=f"{datetime.now().strftime('%Y%m%d')}_{index}.workspaces.json",content=workspaces)

    try:
        continuationUri = response.get("continuationUri")
    except Exception as e:
        pass

    if continuationUri:
        if "continuationToken" in continuationUri:
            pageCount = pageCount + 1
            await get_workspaces(url=continuationUri, pageCount=pageCount)



async def main():
    logging.info('Started')

    url = "https://api.fabric.microsoft.com/v1/admin/workspaces"

    await get_workspaces(url=url,pageCount=1)



if __name__ == "__main__":
    asyncio.run(main())
