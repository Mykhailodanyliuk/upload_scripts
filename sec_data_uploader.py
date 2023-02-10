import json
import time, datetime
import pymongo
import requests
from zipfile import ZipFile
import os
import shutil

import addictional_tools


def upload_sec_tickers_data():
    update_collection = addictional_tools.get_collection_from_db('db', 'update_collection')
    sec_tickers_data_collection = addictional_tools.get_collection_from_db('db', 'sec_data_tickers')
    last_len_records = sec_tickers_data_collection.count_documents({})
    while True:
        try:
            loc_json = json.loads((requests.get('https://www.sec.gov/files/company_tickers.json')).text)
            break
        except:
            time.sleep(60)
            pass
    for company in loc_json:
        cik_str = str(loc_json[company].get('cik_str')).zfill(10)
        ticker = loc_json[company].get('ticker')
        title = loc_json[company].get('title')
        if sec_tickers_data_collection.find_one({'cik_str': str(loc_json[company].get('cik_str')).zfill(10)}) is None:
            sec_tickers_data_collection.insert_one({'cik_str': cik_str, 'tickers': [ticker], 'title': title, 'upload_at': datetime.datetime.now()})
        else:
            tickers_db = sec_tickers_data_collection.find_one(
                {'cik_str': str(loc_json[company].get('cik_str')).zfill(10)}).get('tickers')
            if ticker not in tickers_db:
                tickers_db.append(ticker)
                print(tickers_db)
            update_query = {'tickers': tickers_db}
            sec_tickers_data_collection.update_one({'cik_str': str(loc_json[company].get('cik_str')).zfill(10)},
                                                   {"$set": update_query})

    total_records = sec_tickers_data_collection.count_documents({})
    update_query = {'name': 'sec_tickers', 'new_records': total_records - last_len_records,
                    'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'sec_tickers'}):
        update_collection.update_one({'name': 'sec_tickers'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)


def upload_sec_fillings_data():
    sec_data_collection = addictional_tools.get_collection_from_db('db', 'sec_data')
    update_collection = addictional_tools.get_collection_from_db('db', 'update_collection')
    path_to_zip = '/home/dev/submissions.zip'
    path_to_data_directory = '/home/dev/submissions'
    addictional_tools.download_file_requests('https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip',
                                    path_to_zip)
    last_len_records = sec_data_collection.count_documents({})
    existed_ciks = [x.get('cik') for x in sec_data_collection.find({}, {'cik': 1, '_id':0})]
    with ZipFile(path_to_zip, 'r') as zip:
        zip_files = zip.namelist()
        zip_files = [file[3:13] for file in zip_files if len(file) == 18]
        zip_files = [f'CIK{file}.json' for file in list(set(zip_files).difference(existed_ciks))]
        for file in zip_files:
            zip.extract(file, path=path_to_data_directory, pwd=None)
            with open(f'{path_to_data_directory}/{file}', 'r') as json_file:
                new_sec_company_data = json.load(json_file)
                try:
                    sec_data_collection.insert_one(
                        {'cik': new_sec_company_data.get('cik').zfill(10), 'ein': new_sec_company_data.get('ein'),
                         'sic': new_sec_company_data.get('sic'), 'name': new_sec_company_data.get('name'),
                         'upload_date': datetime.datetime.now(), 'data': new_sec_company_data,
                         'tickers': new_sec_company_data.get('tickers')})
                except pymongo.errors.DuplicateKeyError:
                    continue
    if os.path.exists(path_to_zip):
        os.remove(path_to_zip)
    else:
        print("The file does not exist")
    if os.path.exists(path_to_data_directory):
        shutil.rmtree(path_to_data_directory)
    else:
        print("Directory does not exist")
    total_records = sec_data_collection.count_documents({})
    update_query = {'name': 'sec', 'new_records': total_records - last_len_records, 'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'sec'}):
        update_collection.update_one({'name': 'sec'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)
    npi_collection = addictional_tools.get_collection_from_db('db','npi')
    if update_collection.find_one({'name': 'npi'}):
        update_collection.update_one({'name': 'npi'}, {"$set": {'name': 'npi', 'new_records': 0, 'total_records': npi_collection.count_documents({}),
                    'update_date': datetime.datetime.now()}})
    else:
        update_collection.insert_one({'name': 'npi', 'new_records': 0, 'total_records': npi_collection.count_documents({}),
                    'update_date': datetime.datetime.now()})


if __name__ == '__main__':
    while True:
        time.sleep(1800)
        upload_sec_tickers_data()
        upload_sec_fillings_data()
        time.sleep(14400)
