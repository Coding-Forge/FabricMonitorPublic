import json
from datetime import datetime, timedelta
from utility.helper import Bob

def main():

    with open('../../config.json', 'r') as file:
        settings = file.readlines()

    bob = Bob()
    config = bob.get_state(f"/{settings["LakehouseName"]}.Lakehouse/Files/activity/")
    if isinstance(config, str):
        lastRun = json.loads(config).get("lastRun")
    else:
        lastRun = config.get("lastRun")

    lastRun_tm = bob.convert_dt_str(lastRun)
    pivotDate = lastRun_tm + timedelta(days=-30)
    # Your code here

if __name__ == "__main__":
    main()

