import os
from upload_postgres import load_all_tables
from upload_s3 import convert_and_upload
from dotenv import load_dotenv

if __name__ == "__main__":
    # Load environment variables from .env file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    load_dotenv(os.path.abspath('../../.env'))

    # load s3
    convert_and_upload(os.environ['SPORTS_STATISTICS_HOST_URI'],
                       os.environ['SPORTS_STATISTICS_CFB_DATASET_FULL_PATH'],
                       os.environ['S3_RAW_BUCKET_NAME'])
    print('Upload to S3 complete')

    # load postgres
    load_all_tables()
    print('Upload to Postgres complete')



