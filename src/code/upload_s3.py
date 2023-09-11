"""
 A function that uploads an extracts, transforms and uploads files to S3
"""
import io
import boto3
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os


def convert_and_upload(host_name, url, bucket_name):
    """
    This function extracts all the .CSVs files at the given url
    and does necessary transformations before converting to parquet
    and loading the file into the appropriate S3 bucket
    :param bucket_name: The name of the S3 bucket to upload the files to
    :param host_name: The host server (i.e. https://google.com)
    :param url: The URL path from where we should extract CSV files
    :return: nothing
    """

    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')
    host_folder = host_name[8:]

    # Find all the anchor tags in the HTML
    alink_tags = soup.find_all('a', href=True)

    # get environment variables
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.abspath('../../.env'))

    print(os.path.abspath('../../.env'))

    # Access environment variables using os.environ
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # Set up the S3 client, will create the bucket if it doesn't exist
    s3_client = boto3.client('s3',
                             region_name='us-east-1',
                             aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key)
    s3_client.create_bucket(Bucket=bucket_name)

    # Iterate through the anchor tags but only do work on .CSVs
    for alink_tag in alink_tags:

        link = alink_tag.get('href')

        if link[-4:] == '.csv':
            # combine the host URL with the relative link path to get the full CSV path
            csv = host_name + link

            df = pd.read_csv(csv)

            link_array = link.split('/')

            file_csv = link_array[len(link_array) - 1]
            conf = link_array[len(link_array) - 2].replace('-', '')
            year = link_array[len(link_array) - 3]

            sub_folder = df.iloc[0].at['Stat Type'].lower().replace(' ', '') + '/'

            df['conf_syscol'] = conf
            df['year_syscol'] = year

            file_name = file_csv[0:-4] + '-' + conf + '-' + year + ".parquet"

            # we want to make sure all columns in the dataframe are of type 'string'
            df = df.apply(lambda x: x.astype(str))

            # convert data frame to parquet and store in bucket
            parquet_data = df.to_parquet()

            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(name=bucket_name)
            bucket.upload_fileobj(io.BytesIO(parquet_data), host_folder + sub_folder + file_name)


if __name__ == "__main__":

    # get environment variables
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.abspath('../../.env'))

    convert_and_upload(os.environ['SPORTS_STATISTICS_HOST_URI'],
                       os.environ['SPORTS_STATISTICS_CFB_DATASET_FULL_PATH'],
                       os.environ['S3_RAW_BUCKET_NAME'])

    print('Upload to S3 complete')
