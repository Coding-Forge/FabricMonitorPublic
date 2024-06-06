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

    if isinstance(config, str):
        lastRun = json.loads(config).get("activity").get("lastRun")
    else:
        lastRun = config.get("activity").get("lastRun")

    # if lastRun is recorded then proceed from there
    lastRun_tm = bob.convert_dt_str(lastRun)
    pivotDate = lastRun_tm.replace(hour=0, minute=0, second=0, microsecond=0)
    # Your code here

    url = "https://api.fabric.microsoft.com/v1/admin/domains"

    response = await bob.invokeAPI(url, headers=headers)
    print(response)

    domain_workspaces=[]

    for domain in response.get("domains"):
        domainId = domain['id']

        response = await bob.invokeAPI(f"https://api.fabric.microsoft.com/v1/admin/domains/{domainId}/workspaces", headers=headers)
        print(response)
        for value in response.get("value"):
            if len(value) != 0:
                domain["workspace"] = value.get("displayName")
                domain["workspaceId"] = value.get("id")
                domain_workspaces.append(domain)

        domain_workspaces.append(domain)

    Path = f"domains/{pivotDate.strftime('%Y')}/{pivotDate.strftime('%m')}/"
    await fm.save(path=Path, file_name="domains.json",content=domain_workspaces)






if __name__ == "__main__":
    asyncio.run(main())
