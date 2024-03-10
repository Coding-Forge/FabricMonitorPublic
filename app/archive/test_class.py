from app.utility.file_management import File_Management
import asyncio
import json

async def main():

    test = '[{"name":"brandon","age":"54","height":"74","unit":"inches"},{"name":"chizu","age":"52","height":"68","unit":"inches"},{"name":"will","age":"21","height":"73","unit":"inches"},{"name":"kait","age":"20","height":"71","unit":"inches"}]'

    test_d = json.loads(test)

    test_s = json.dumps(test_d, indent=4, sort_keys=True)

    test_b = test_s.encode('utf-8')

    fm = File_Management()

    await fm.save(path='test/2024/03/09', file_name='whole3_family.json', content=test_b)



if __name__ == "__main__":
    asyncio.run(main())
