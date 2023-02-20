import json
import datetime
import time

import pymongo
from zipfile import ZipFile
import os
import shutil

from addictional_tools import *

def upload_clinical_trials():
    clinical_trials_collection = get_collection_from_db('db', 'clinical_trials')
    organizations_collection = get_collection_from_db('db', 'clinical_trials_organizations')
    update_collection = get_collection_from_db('db', 'update_collection')
    current_directory = os.getcwd()
    directory_name = 'downloads'
    path_to_directory = f'{current_directory}/{directory_name}'
    delete_directory(path_to_directory)
    create_directory(current_directory, directory_name)
    path_to_zip = f'{path_to_directory}/AllAPIJSON.zip'
    download_file('https://ClinicalTrials.gov/AllAPIJSON.zip', path_to_zip)
    existed_nct = [x.get('nct_id') for x in clinical_trials_collection.find({}, {'nct_id': 1, '_id':0})]
    with ZipFile(path_to_zip, 'r') as zip:
        zip_files = zip.namelist()
        zip_files.remove('Contents.txt')
        zip_files = [file for file in zip_files if file[-16:-5] not in existed_nct]
        for file in zip_files:
            zip.extract(file, path=path_to_directory, pwd=None)
            with open(f'{path_to_directory}/{file}', 'r', encoding='utf-8') as json_file:
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
    delete_directory(path_to_directory)


    organizations = clinical_trials_collection.distinct(key='organization')
    last_len_records = len(organizations)
    for organization in list(organizations):
        list_organization_trials = [trial.get('nct_id') for trial in list(clinical_trials_collection.find(
            {'organization': organization}))]
        if organizations_collection.find_one({'name': organization}) is None:
            organizations_collection.insert_one({'organization': organization, 'nct_ids': list_organization_trials})
        else:
            organizations_collection.update_one({'organization': organization},
                                                {'$set': {'nct_ids': list_organization_trials}})

    total_records = organizations_collection.count_documents({})
    update_query = {'name': 'clinical_trials', 'new_records': total_records - last_len_records,
                    'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'clinical_trials'}):
        update_collection.update_one({'name': 'clinical_trials'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)


if __name__ == '__main__':
    while True:
        time.sleep(900)
        upload_clinical_trials()
        time.sleep(14400)
