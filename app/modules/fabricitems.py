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

async def main():
    logging.info('Started')
    
    try:
        config = await fm.read(file_name="state.yaml")
    except Exception as e:
        print(f"Error: {e}")
        return



    # if isinstance(config, str):
    #     lastRun = json.loads(config).get("activity").get("lastRun")
    # else:
    #     lastRun = config.get("activity").get("lastRun")

    # if lastRun is recorded then proceed from there
    # lastRun_tm = bob.convert_dt_str(lastRun)
    # pivotDate = lastRun_tm.replace(hour=0, minute=0, second=0, microsecond=0)
    # Your code here


    today = datetime.now()
    async def get_data(url,pageIndex=1):
        pageIndex = str(pageIndex).zfill(5)
        response = await bob.invokeAPI(url, headers=headers)
        Path = f"items/{today.strftime('%Y')}/{today.strftime('%m')}/{today.strftime('%d')}/"
        await fm.save(path=Path, file_name=f"fabricitems_{pageIndex}.json",content=response.get("itemEntities"))

        try:
            continuationUri = response.get("continuationUri")
            if continuationUri:
                await get_data(continuationUri)
        except Exception as e:
            logging.info("No continuation uri")
            return

    url = "https://api.fabric.microsoft.com/v1/admin/items"
    await get_data(url)

    


if __name__ == "__main__":
    asyncio.run(main())
