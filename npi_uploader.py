import asyncio
import datetime
import jellyfish
import pymongo

from addictional_tools import get_collection_from_db, get_all_data_urls


def write_all_data_parser3():
    update_collection = get_collection_from_db('db', 'update_collection', client)
    npi_data = get_collection_from_db('db', 'npi_data', client)
    sec_tickers_collection = get_collection_from_db('db', 'sec_data_tickers', client)
    last_len_records = npi_data.estimated_document_count()

    ciks = [c_json.get('cik_str').lstrip('0') for c_json in sec_tickers_collection.find({})]
    ciks = list(set(ciks))
    links1 = [
        f'https://api.orb-intelligence.com/3/search/?api_key=c66c5dad-395c-4ec6-afdf-7b78eb94166a&limit=10&cik={cik}'
        for cik in ciks]
    fisrt_data1 = asyncio.run(get_all_data_urls(links1, 2))
    second_links = []
    unsearched_ciks = []
    for data1 in fisrt_data1:
        if data1.get('results_count') == 1:
            second_links.append(data1.get('results')[0].get('fetch_url'))
        else:
            unsearched_ciks.append(data1.get('request_fields').get('cik'))
    link_name_list = []

    for unsearched_cik in unsearched_ciks:
        searched_company = sec_tickers_collection.find_one({'cik_str': unsearched_cik.zfill(10)})
        for ticker in searched_company.get("tickers"):
            link_name_list.append([
                f'https://api.orb-intelligence.com/3/search/?api_key=c66c5dad-395c-4ec6-afdf-7b78eb94166a&limit=10&ticker={ticker.replace("-", "")}',
                searched_company.get("title")])

    links2 = [block[0] for block in link_name_list]
    fisrt_data2 = asyncio.run(get_all_data_urls(links2, 2))
    for index, data in enumerate(fisrt_data2):
        for results_data in data.get('results'):
            if jellyfish.jaro_winkler_similarity(results_data.get('name').lower(),
                                                 link_name_list[index][1].lower()) > 0.85:
                second_links.append(results_data.get('fetch_url'))
                break
    second_data = asyncio.run(get_all_data_urls(second_links, 2))
    cik_npi_list = [[data.get('cik'), data.get('npis')] for data in second_data if data.get('npis') != []]
    for cik, npi in cik_npi_list:
        print(cik)
        npi_update_query = {'cik': cik, 'npi': npi, 'upload_at': datetime.datetime.now()}
        if npi_data.find_one({'cik': cik}):
            npi_data.update_one({'cik': cik}, {"$set": npi_update_query})
        else:
            npi_data.insert_one(npi_update_query)

    total_records = npi_data.estimated_document_count()
    update_query = {'name': 'npi_data', 'new_records': total_records - last_len_records,
                    'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'npi_data'}):
        update_collection.update_one({'name': 'npi_data'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)


if __name__ == '__main__':
    client = pymongo.MongoClient('mongodb://localhost:27017')
    write_all_data_parser3()
    client.stop()
