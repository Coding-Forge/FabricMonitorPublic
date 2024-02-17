import asyncio
from codetiming import Timer

from app.modules.activity import main as Activity
from app.modules.apps import main as Apps

async def task(name, work_queue):
    while not work_queue.empty():
        module = await work_queue.get()
        print(f"Task {name} running {module.__name__}")
        await module()
        work_queue.task_done()


async def async_main():
    await Activity()
    print("running activity")
    await Apps()
    print("running apps")

async def main():
    # Your code here

    #asyncio.run(async_main())

    work_queue = asyncio.Queue()
    modules = [Activity, Apps]

    for module in modules:
        await work_queue.put(module)

    with Timer(text="\nTotal elapsed time: {:.1f}"):
        await asyncio.gather(
            asyncio.create_task(task("One", work_queue)),
            asyncio.create_task(task("Two", work_queue)),
        )

if __name__ == "__main__":
    asyncio.run(main())

