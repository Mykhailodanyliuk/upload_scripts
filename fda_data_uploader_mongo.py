import asyncio
import json
import time, datetime
import jellyfish
import pymongo
import wget

import addictional_tools
import pytz
import requests
from zipfile import ZipFile
import glob
import os
import shutil

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Accept": "*/*",
    "Accept-Language": "uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br"
}


def get_collection_from_db(data_base, collection):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    db = client[data_base]
    return db[collection]


# keys_dict = {'fda_drug_ndc': 'product_ndc', 'fda_device_event': 'report_number', 'fda_device': 'k_number',
#              'fda_device_enforcement': 'recall_number', 'fda_device_recall': 'cfres_id', 'device_classification'
#              :'regulation_number', 'fda_device_covid19':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':}
#
# def upload_fda_data(file, collection, data_key):
#     data_collection = get_collection_from_db('db', collection)
#     existed_records = set([x.get(data_key) for x in data_collection.find({}, {data_key: 1, '_id': 0})])
#     with open(file, 'r', encoding='utf-8') as opened_file:
#         json_data = json.load(opened_file)
#         results = json_data.get('results')
#         for result in results:
#             if result.get(data_key) not in existed_records:
#                 data_collection.insert_one(result)

# keys_dict = {'fda_drug_ndc': 'product_ndc', 'fda_device_event': 'report_number', 'fda_device': 'k_number',
#              'fda_device_enforcement': 'recall_number', 'fda_device_recall': 'cfres_id', 'device_classification'
#              :'regulation_number', 'fda_device_covid19':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':, 'fda_':}

def upload_data_to_db(file, collection):
    data_collection = get_collection_from_db('db', collection)
    with open(file, 'r', encoding='utf-8') as opened_file:
        json_data = json.load(opened_file)
        results = json_data.get('results')
        for result in results[:500]:
            result['upload_at'] = datetime.datetime.now()
            data_collection.insert_one(result)


def get_fda_list_new_zip_files():
    fda_all_zip = get_collection_from_db('db', 'fda_files')
    all_files_json = addictional_tools.get_json_from_request('https://api.fda.gov/download.json')
    files_list = []
    for category in all_files_json.get('results').keys():
        for subcategory in all_files_json.get('results').get(category).keys():
            if subcategory != 'drugsfda':
                for partition in all_files_json.get('results').get(category).get(subcategory).get('partitions')[:1]:
                    file_link = partition.get('file')
                    if not fda_all_zip.find_one({'zip_name': file_link}):
                        files_list.append({'category': category, 'subcategory': subcategory, 'file_link': file_link})
    return files_list


def upload_fda_data(file_dict):
    update_collection = get_collection_from_db('db', 'update_collection')
    fda_all_zip = get_collection_from_db('db', 'fda_files')
    file_link = file_dict.get('file_link')
    category = file_dict.get('category')
    subcategory = file_dict.get('subcategory')
    collection = get_collection_from_db('db', f'fda_{category}_{subcategory}')
    last_len_records = collection.count_documents({})
    path_to_data_directory = 'G:/Programming/workProject/downloads'
    zip_file_path = file_link[::-1]
    zip_file_path = f"{path_to_data_directory}/{zip_file_path[:zip_file_path.find('/')][::-1]}"
    directory = "downloads"
    parent_dir = "G:/Programming/workProject"
    path = os.path.join(parent_dir, directory)
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
    print(file_link)
    wget.download(file_link, zip_file_path)
    with ZipFile(zip_file_path, 'r') as zip:
        zip.extractall(path=path_to_data_directory)
    file_path = zip_file_path.replace('.zip', '')
    upload_data_to_db(file_path, f'fda_{category}_{subcategory}')
    if os.path.exists(path_to_data_directory):
        shutil.rmtree(path_to_data_directory)
    fda_all_zip.insert_one({'zip_name': file_link})

    total_records = collection.count_documents({})
    update_query = {'name': f'fda_{category}_{subcategory}', 'new_records': total_records - last_len_records,
                    'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': f'fda_{category}_{subcategory}'}):
        update_collection.update_one({'name': f'fda_{category}_{subcategory}'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)


if __name__ == '__main__':
    for zip_file in get_fda_list_new_zip_files():
        upload_fda_data(zip_file)
