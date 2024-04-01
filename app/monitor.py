import asyncio
import json
import yaml

from codetiming import Timer
from app.modules.activity import main as Activity
from app.modules.apps import main as Apps
from app.modules.catalog import main as Catalog
from app.modules.graph import main as Graph
from app.modules.tenant import main as Tenant
from app.modules.refreshhistory import main as RefreshHistory
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


async def main():
    # Your code here
    bob = Bob()
    settings = bob.get_settings()

    # get the state.yaml file that include information about the last run
    current_state = bob.get_state("local")

    # get the modules selected in the configuration for the application
    modules = settings.get("ApplicationModules").replace(" ","").split(",")
    classes = [globals()[module] for module in modules]

    work_queue = asyncio.Queue()

    for module in classes:
        await work_queue.put(module)

    with Timer(text="\nTotal elapsed time: {:.1f}"):
        await asyncio.gather(
            asyncio.create_task(task("Activity", work_queue)),
            asyncio.create_task(task("Apps", work_queue)),
            asyncio.create_task(task("Catalog", work_queue)),
            asyncio.create_task(task("Graph", work_queue)),
            asyncio.create_task(task("Tenant", work_queue)),
            asyncio.create_task(task("Gateway", work_queue)),
            asyncio.create_task(task("Refresh History", work_queue))
        )

    # this has all the information needed to modify the state.yaml file


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

