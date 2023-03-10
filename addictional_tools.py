import asyncio
import json
import os
import shutil

import aiohttp
import pymongo
import wget
from zipfile import ZipFile
import requests
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
}


def drop_collection_from_db(data_base, collection):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    db = client[data_base]
    db.drop_collection(collection)


# def get_collection_from_db(data_base, collection):
#     client = pymongo.MongoClient('mongodb://localhost:27017')
#     db = client[data_base]
#     return db[collection]


def get_collection_from_db(data_base, collection, client):
    db = client[data_base]
    return db[collection]


def get_all_data_from_collection(collection):
    col = get_collection_from_db('db', collection)
    results = list(col.find())
    return results


def download_file(url, file_name):
    wget.download(url, file_name)


def download_file_requests(url, file_name):
    try:
        response = requests.get(url, headers=headers)
        open(file_name, "wb").write(response.content)
    except:
        time.sleep(60)
        print('problem')
        download_file_requests(url, file_name)
    print('File is downloaded')


def extract_zip_file(file_path, destination_path):
    with ZipFile(file_path, 'r') as zObject:
        zObject.extractall(
            path=destination_path)
    print('File is extracted')


def get_json_from_request(url):
    return json.loads((requests.get(url, headers=headers)).text)


def delete_file(path_to_file):
    if os.path.exists(path_to_file):
        os.remove(path_to_file)
    else:
        print("The file does not exist")


def delete_directory(path_to_directory):
    if os.path.exists(path_to_directory):
        shutil.rmtree(path_to_directory)
    else:
        print("Directory does not exist")


def get_request_data(url, verify=False):
    response = requests.get(url, verify=verify)
    if response.status_code == 200:
        return response
    else:
        time.sleep(10)
        get_request_data(url, verify=False)


def create_directory(path_to_dir, name):
    mypath = f'{path_to_dir}/{name}'
    if not os.path.isdir(mypath):
        os.makedirs(mypath)


async def get_page(session, url):
    while True:
        try:
            async with session.get(url, headers=headers) as r:
                # print(r.status)
                if r.status == 200:
                    return await r.json()
        except asyncio.exceptions.TimeoutError:
            print('asyncio.exceptions.TimeoutError')


async def get_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(get_page(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def get_all_data_urls(urls, limit=5000):
    timeout = aiohttp.ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=limit)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        data = await get_all(session, urls)
        return data
