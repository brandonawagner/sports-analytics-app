"""
 A Lambda function that uploads a extracts, transforms and uploads files to S3
"""
import io
import boto3
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup


def convert_and_upload(host_name, url, bucket_name):
    """
    This function extracts all the .CSVs files at the given url
    and does necessary transformations before converting to parquet
    and loading the file into the appropriate S3 bucket
    :param bucket_name: The name of the S3 bucket to upload the files to
    :param host_name: The host server (i.e. https://google.com)
    :param url: The full URL path where we should extract CSV files
    :return: nothing
    """

    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, 'html.parser')

    # Find all the anchor tags in the HTML
    alink_tags = soup.find_all('a', href=True)

    # Set up the S3 client
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)

    # Iterate through the anchor tags but only do work on .CSVs
    for alink_tag in alink_tags:

        link = alink_tag.get('href')

        if link[-4:] == '.csv':
            # combine the host URL with the relative link path to get the full CSV path
            csv = host_name + link

            df = pd.read_csv(csv)

            # TODO manipulate the dataframe to add the year and conference
            parquet_data = df.to_parquet(engine='fastparquet')

            lt_array = link.split('/')

            file_csv = lt_array[len(lt_array) - 1]
            conf = lt_array[len(lt_array) - 2].replace('-', '')
            year = lt_array[len(lt_array) - 3]

            file_name = file_csv[0:-4] + '-' + conf + '-' + year + ".parquet"

            s = boto3.resource('s3')
            b = s.Bucket(name=bucket_name)
            b.upload_fileobj(io.BytesIO(parquet_data), file_name)


def lambda_handler(event, context):
    bk = 'bw-sports-analytics-data'
    h = 'https://sports-statistics.com'
    u = h + '/sports-data/college-football-stats-datasets-csv-database/'
    convert_and_upload(h, u, bk)
    return {
        'message': 'upload complete'
    }


if __name__ == "__main__":
    buc = 'bw-sports-analytics-data'
    host = 'https://sports-statistics.com'
    uri = host + '/sports-data/college-football-stats-datasets-csv-database/'
    convert_and_upload(host, uri, buc)
