import os
from src.code.upload_postgres import load_all_tables
from src.code.upload_s3 import convert_and_upload

if __name__ == "__main__":

    # load postgres
    load_all_tables()
    print('Upload to Postgres complete')

    # load s3
    convert_and_upload(os.environ['SPORTS_STATISTICS_HOST_URI'],
                       os.environ['SPORTS_STATISTICS_CFB_DATASET_RELATIVE_PATH'],
                       os.environ['S3_RAW_BUCKET_NAME'])
    print('Upload to S3 complete')

