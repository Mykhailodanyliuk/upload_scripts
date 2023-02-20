import time
import traceback

import uspto_data_uploader,fda_data_uploader_mongo,sec_data_uploader,hhs_data_uploader,clinical_trials_uploader
if __name__ == '__main__':
    while True:
        try:
            uspto_data_uploader.upload_all_uspto_zips()
        except Exception as e:
            print("uspto_data_uploader problem")
            print(traceback.format_exc())
        try:
            sec_data_uploader.upload_sec_tickers_data()
        except Exception as e:
            print("sec_tickers problem")
            print(traceback.format_exc())
        try:
            sec_data_uploader.upload_sec_fillings_data()
        except Exception as e:
            print("sec_fillings problem")
            print(traceback.format_exc())
        try:
            hhs_data_uploader.upload_hhs_data()
        except Exception as e:
            print("hhs_data_uploader problem")
            print(traceback.format_exc())
        try:
            for zip_file in fda_data_uploader_mongo.get_fda_list_new_zip_files():
                fda_data_uploader_mongo.upload_fda_data(zip_file)
        except Exception as e:
            print("fda_data_uploader_mongo problem")
            print(traceback.format_exc())
        try:
            clinical_trials_uploader.upload_clinical_trials()
        except Exception as e:
            print("clinical_trials_uploader problem")
            print(traceback.format_exc())
