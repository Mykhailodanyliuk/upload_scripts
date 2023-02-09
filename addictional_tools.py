import pymongo
import wget
from zipfile import ZipFile


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
    wget.download(url,file_name)


def extract_zip_file(file_path, destination_path):
    with ZipFile(file_path, 'r') as zObject:
        zObject.extractall(
            path=destination_path)
    print('File is extracted')