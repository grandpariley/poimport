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


def fetch_data(symbol):
    return client.get_io_loop().run_until_complete(_fetch_data(symbol))


async def _fetch_data(symbol):
    data_as_list = await find_all(data.find({'symbol': symbol}))
    data_as_dict = dict()
    for d in data_as_list:
        data_as_dict[d['symbol']] = d['data']
    return data_as_dict[symbol]


async def fetch_no_data():
    return await find_all(no_data.find(None))


async def find_all(cursor):
    cursor = cursor.allow_disk_use(True)
    results = []
    async for result in cursor:
        results.append(result)
    return results


def count():
    return client.get_io_loop().run_until_complete(_count())


async def _count():
    return await data.count_documents(None)


def symbols():
    return client.get_io_loop().run_until_complete(_symbols())


async def _symbols():
    s = []
    cursor = data.find(None)
    async for result in cursor:
        print(result)
        s.append(result['symbol'])
    return s
