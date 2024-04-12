import asyncio
import json
import yaml
import sys

from codetiming import Timer
from croniter import croniter

from app.modules.activity import main as Activity
from app.modules.apps import main as Apps
from app.modules.catalog import main as Catalog
from app.modules.graph import main as Graph
from app.modules.tenant import main as Tenant
from app.modules.refreshhistory import main as RefreshHistory
from app.modules.refreshables import main as Refreshables
from app.modules.gateway import main as Gateway


from datetime import datetime, timedelta
from app.utility.helps import Bob
from app.utility.file_management import File_Management


async def tasker(name, work_queue):
    while not work_queue.empty():
        module = await work_queue.get()
        print(f"Task {name} running {module.__name__}")
        await module()
        work_queue.task_done()

async def task(name, work_queue):
    timer = Timer(text=f"Task {name} elapsed time: {{:.1f}}")
    while not work_queue.empty():
        module = await work_queue.get()
        
        print(f"Task {name} running {module.__name__}")
        timer.start()
        await module()
        timer.stop()


def is_function_due(cron_syntax, last_run):
    last_run_datetime = last_run

    cron = croniter(cron_syntax, last_run_datetime)
    next_run_datetime = cron.get_next(datetime)
    
    print(f"What is the next run date value {next_run_datetime} and what is the current datetime {datetime.now()}")

    if next_run_datetime.strftime("%Y-%m-%d") <= datetime.now().strftime("%Y-%m-%d"):
        return True
    else:
        return False



async def main():
    # Your code here
    bob = Bob()

    settings = bob.get_settings()
    args = sys.argv[1:]
    # get the state.yaml file that include information about the last run
    current_state = bob.get_state("local")

    # get the modules selected in the configuration for the application
    modules = settings.get("ApplicationModules").replace(" ","").split(",")
    # print(f"what are the modules {modules}")
    run_jobs = []


    work_queue = asyncio.Queue()

    for module in modules:
        cron = settings.get(f"{module}_cron")
        run = current_state.get(f"{module.lower()}").get("lastRun")

        last_run = bob.convert_dt_str(run)    

        if is_function_due(cron,last_run):
            run_jobs.append(module)

    print(run_jobs)

    classes = [globals()[module] for module in run_jobs]

    for module in classes:
        await work_queue.put(module)
    

    # tasks = await create_module_tasks(dynamic_modules)

    # Run tasks concurrently using asyncio.gather()
    # results = await asyncio.gather(*tasks)


    with Timer(text="\nTotal elapsed time: {:.1f}"):
        await asyncio.gather(
            asyncio.create_task(task("Activity", work_queue)),
            asyncio.create_task(task("Apps", work_queue)),
            asyncio.create_task(task("Catalog", work_queue)),
            # asyncio.create_task(task("Graph", work_queue)),
            # asyncio.create_task(task("Tenant", work_queue)),
            # asyncio.create_task(task("Gateway", work_queue)),
            # asyncio.create_task(task("Refresh History", work_queue))
        )


    # this has all the information needed to modify the state.yaml file
    # update the state.yaml file with the last run information
    for job in run_jobs:
        current_state[job.lower()]["lastRun"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    try:
        bob.save_state(path=".", data=current_state)
    except Exception as e:
        print(f"Bob Error: {e}")
    try:
        fm = File_Management()
        await fm.save("", "state.yaml", current_state)
    except Exception as e:
        print(f"fm Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())

