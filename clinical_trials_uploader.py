import asyncio
import json
import time, datetime
import jellyfish
import pymongo
import parsing_tools
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


def drop_collection_from_db(data_base, collection):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    db = client[data_base]
    db.drop_collection(collection)


def get_collection_from_db(data_base, collection):
    client = pymongo.MongoClient('mongodb://localhost:27017')
    db = client[data_base]
    return db[collection]


def get_all_data_from_collection(collection):
    col = get_collection_from_db('db', collection)
    results = list(col.find())
    return results


def download_file(url, file_name):
    try:
        response = requests.get(url, headers=headers)
        open(file_name, "wb").write(response.content)
    except:
        time.sleep(60)
        print('problem')
        download_file(url, file_name)
    print('File is downloaded')


def extract_zip_file(file_path, destination_path):
    with ZipFile(file_path, 'r') as zObject:
        zObject.extractall(
            path=destination_path)
    print('File is extracted')


def upload_clinical_trials():
    clinical_trials_collection = get_collection_from_db('db', 'clinical_trials1')
    organizations_collection = get_collection_from_db('db', 'clinical_trials_organizations1')
    update_collection = get_collection_from_db('db', 'update_collection1')
    path_to_zip = '/home/dev/AllAPIJSON.zip'
    path_to_data_directory = '/home/devAllAPIJSON'
    download_file('https://ClinicalTrials.gov/AllAPIJSON.zip', '/home/dev/AllAPIJSON.zip')
    existed_nct = clinical_trials_collection.distinct('nct_id')
    with ZipFile(path_to_zip, 'r') as zip:
        zip_files = zip.namelist()
        zip_files.remove('Contents.txt')
        zip_files = [file for file in zip_files if file[-16:-5] not in existed_nct]
        for file in zip_files[:2000]:
            zip.extract(file, path=path_to_data_directory, pwd=None)
            with open(f'{path_to_data_directory}/{file}', 'r' ,encoding='utf-8') as json_file:
                data = json.load(json_file)
                organization = data.get('FullStudy').get('Study').get('ProtocolSection').get(
                    'IdentificationModule').get('Organization').get('OrgFullName')
                nct_id = data.get('FullStudy').get('Study').get('ProtocolSection').get('IdentificationModule').get(
                    'NCTId')
                upload_at = datetime.datetime.now()
                try:
                    clinical_trials_collection.insert_one(
                        {'organization': organization, 'nct_id': nct_id, 'upload_at': upload_at, 'data': data})
                except pymongo.errors.DuplicateKeyError:
                    continue

    if os.path.exists(path_to_zip):
        os.remove(path_to_zip)
    else:
        print("The file does not exist")
    shutil.rmtree(path_to_data_directory)

    organizations = clinical_trials_collection.distinct(key='organization')
    last_len_records = len(organizations)
    for organization in list(organizations):
        list_organization_trials = [trial.get('nct_id') for trial in list(clinical_trials_collection.find(
            {'organization': organization}))]
        if organizations_collection.find_one({'name':organization}) is None:
            organizations_collection.insert_one({'organization':organization, 'nct_ids':list_organization_trials})
        else:
            organizations_collection.update_one({'organization':organization}, {'$set': {'nct_ids': list_organization_trials}})

    total_records = organizations_collection.count_documents({})
    update_query = {'name': 'clinical_trials', 'new_records': total_records - last_len_records, 'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'clinical_trials'}):
        update_collection.update_one({'name': 'clinical_trials'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)
if __name__ == '__main__':
    upload_clinical_trials()
