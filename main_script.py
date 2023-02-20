import time

import uspto_data_uploader,fda_data_uploader_mongo,sec_data_uploader,hhs_data_uploader,clinical_trials_uploader
if __name__ == '__main__':
    while True:
        try:
            uspto_data_uploader.upload_all_uspto_zips()
        except Exception as e:
            print("uspto_data_uploader problem")
            print(e)
        try:
            sec_data_uploader.upload_sec_tickers_data()
            sec_data_uploader.upload_sec_fillings_data()
        except Exception as e:
            print("uspto_data_uploader problem")
            print(e)
        try:
            hhs_data_uploader.upload_hhs_data()
        except Exception as e:
            print("uspto_data_uploader problem")
            print(e)
        try:
            for zip_file in fda_data_uploader_mongo.get_fda_list_new_zip_files():
                fda_data_uploader_mongo.upload_fda_data(zip_file)
        except Exception as e:
            print("uspto_data_uploader problem")
            print(e)
        try:
            clinical_trials_uploader.upload_clinical_trials()
        except Exception as e:
            print("uspto_data_uploader problem")
            print(e)
