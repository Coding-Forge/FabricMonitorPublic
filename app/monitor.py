import asyncio
from codetiming import Timer

from app.modules.activity import main as Activity
from app.modules.apps import main as Apps
from app.modules.catalog import main as Catalog
from app.modules.graph import main as Graph
from app.modules.tenant import main as Tenant
from app.modules.refreshhistory import main as RefreshHistory

from app.utility.helps import Bob
from app.utility.fab2 import File_Table_Management

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
    #bob = Bob()
    #settings = bob.get_settings()
    #
    ## get POWER BI context
    #headers = bob.get_context()
#
    #lakehouse_catalog = f"{settings['LakehouseName']}.Lakehouse.Files/catalog/"
    #
    #FF = File_Table_Management(
    #    tenant_id=settings['ServicePrincipal']['TenantId'],
    #    client_id=settings['ServicePrincipal']['AppId'],
    #    client_secret=settings['ServicePrincipal']['AppSecret'],
    #    workspace_name=settings['WorkspaceName']
    #)

    #asyncio.run(async_main())

    work_queue = asyncio.Queue()
    modules = [Activity, Apps, Catalog, Graph, Tenant, RefreshHistory]

    for module in modules:
        await work_queue.put(module)

    with Timer(text="\nTotal elapsed time: {:.1f}"):
        await asyncio.gather(
            asyncio.create_task(task("One", work_queue)),
            asyncio.create_task(task("Two", work_queue)),
            asyncio.create_task(task("Three", work_queue)),
            asyncio.create_task(task("Four", work_queue)),
            asyncio.create_task(task("Five", work_queue))
        )

if __name__ == "__main__":
    asyncio.run(main())

