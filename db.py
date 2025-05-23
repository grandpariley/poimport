import os

import motor.motor_asyncio
from dotenv import load_dotenv

load_dotenv()

client = motor.motor_asyncio.AsyncIOMotorClient(os.environ["MONGO_URI"])
data = client.po.get_collection('data')
no_data = client.po.get_collection('no_data')


async def clear_data():
    await data.delete_many({})


async def insert_data(key, d):
    await data.insert_one({'symbol': key, 'data': d})


async def update_data(key, d):
    await data.replace_one({'symbol': key}, {'symbol': key, 'data': d})


async def insert_no_data(nd):
    if await no_data.count_documents({'symbol': nd}) > 0:
        print('already added')
        return
    await no_data.insert_one({'symbol': nd})


async def fetch_data(symbol=None):
    if symbol is None:
        data_as_list = await find_all(data.find({}))
    else:
        data_as_list = await find_all(data.find({'symbol': symbol}))
    data_as_dict = dict()
    for d in data_as_list:
        data_as_dict[d['symbol']] = d['data']
    return data_as_dict if symbol is None else data_as_dict[symbol]


async def fetch_no_data():
    return await find_all(no_data.find({}))


async def find_all(cursor):
    cursor = cursor.allow_disk_use(True)
    results = []
    async for result in cursor:
        results.append(result)
    return results


async def count():
    return await data.count_documents({})


async def symbols():
    s = []
    cursor = data.find({})
    async for result in cursor:
        s.append(result['symbol'])
    return s
