import os

import motor.motor_asyncio
from dotenv import load_dotenv

load_dotenv()

client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGO_URI"])
data = client.po.get_collection('data')
no_data = client.po.get_collection('no_data')


async def clear_data():
    await data.remove()


async def insert_data(key, d):
    await data.insert_one({'symbol': key, 'data': d})


async def update_data(key, d):
    await data.replace_one({'symbol': key}, {'symbol': key, 'data': d})


async def insert_no_data(nd):
    if await no_data.count_documents({'symbol': nd}) > 0:
        print('already added')
        return
    await no_data.insert_one({'symbol': nd})


async def fetch_data():
    data_as_list = await find_all(data.find(None))
    data_as_dict = dict()
    for d in data_as_list:
        data_as_dict[d['symbol']] = d['data']
    return data_as_dict


def fetch_no_data(query):
    return find_all(no_data.find(query))


async def find_all(cursor):
    cursor = cursor.allow_disk_use(True)
    results = []
    async for result in cursor:
        results.append(result)
    return results
