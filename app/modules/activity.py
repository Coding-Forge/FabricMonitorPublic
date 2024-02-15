import json
import requests
from datetime import datetime, timedelta
from utility.helper import Bob

api_root = "https://api.powerbi.com/v1.0/myorg/"


def main():
    ##### INTIALIZE THE CONFIGURATION #####
    with open('../../config.json', 'r') as file:
        settings = file.readlines()

    bob = Bob()

    headers = bob.get_context()
    ##### INTIALIZE THE CONFIGURATION #####
    
    config = bob.get_state(f"/{settings["LakehouseName"]}.Lakehouse/Files/activity/")
    if isinstance(config, str):
        lastRun = json.loads(config).get("lastRun")
    else:
        lastRun = config.get("lastRun")

    lastRun_tm = bob.convert_dt_str(lastRun)
    pivotDate = lastRun_tm + timedelta(days=-30)
    # Your code here

    def get_activity():
        while (pivotDate<lastRun):
            audits = list()
            pageIndex = 1
            flagNoActivity = True

            # keep the start and end time within a 24 hour period by adding 24 hours and removing 1 second 
            nextDate = (pivotDate + timedelta(hours=24)) + timedelta(seconds=-1)
            rest_api = f"admin/activityevents?startDateTime='{pivotDate.strftime('%Y-%m-%dT%H:%M:%SZ')}'&endDateTime='{nextDate.strftime('%Y-%m-%dT%H:%M:%SZ')}'"

            continuationUri=""

            # python does not have a do while so this is the best way 
            # just need to break out of the loop when a condition is met
            while(True):

                if continuationUri:
                    result = bob.invokeAPI(continuationUri, headers=headers)
                else:
                    result = bob.invokeAPI(rest_api=rest_api, headers=headers)


                # check the https response code for 200
                if "ERROR" in result:

                    # this is common to both parts of the if statement
                    if result.get("activityEventEntities"):
                        audits.append(result.get("activityEventEntities"))
                
                    if result.get("continuaionURi"):
                        continuationUri = result.get("continuationUri")

                    # create the folder structure for the output path
                    outputPath = bob.create_path(f"{config.get('OutputPath')}/activity/{pivotDate.strftime('%Y')}/{pivotDate.strftime('%m')}/")

                    # do a for loop until all json arrays in audits are read and written to storage
                    for audit in audits:

                        if pageIndex == 1:
                            outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}.json"
                        else:
                            outputFilePath = f"{outputPath}/{pivotDate.strftime('%Y%m%d')}_{pageIndex}.json"

                        # TODO: convert audits to json
                        with open(outputFilePath, "w") as file:
                            file.write(json.dumps(audit))


                        flagNoActivity = False
                        pageIndex +=1 
                        audits = ""

                    # get out of the inner while loop
                    break

                else:
                    print(result)
                    break

            pivotDate += timedelta(days=1)








if __name__ == "__main__":
    main()

