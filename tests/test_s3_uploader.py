import os
import boto3
import pytest
import zipfile
import argparse
from moto import mock_aws

import s3_uploader


@pytest.fixture
def s3_setup():
    with mock_aws():
        conn = boto3.client('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='my_s3_bucket')
        yield conn


def test_download_zip_from_url():
    zip_path = 'test.zip'
    s3_uploader.download_zip_from_url('https://getsamplefiles.com/download/zip/sample-1.zip', zip_path)
    assert os.path.exists(zip_path)
    os.remove(zip_path)


def test_extract_zip_files():
    zip_path = 'test.zip'
    folder_name = 'test_folder'

    # Creation of a zip file for testing
    os.mkdir(folder_name)
    with zipfile.ZipFile(zip_path, 'w') as zip_file:
        zip_file.writestr('test.txt', 'This is a test file')

    # Test extraction
    assert s3_uploader.extract_zip_files(folder_name, zip_path) == 0
    assert os.path.exists(os.path.join(folder_name, 'test.txt'))

    os.remove(zip_path)
    os.remove(os.path.join(folder_name, 'test.txt'))
    os.rmdir(folder_name)


def test_get_all_files_from_folder():
    folder_name = 'test_folder'
    os.mkdir(folder_name)
    file_path = os.path.join(folder_name, 'test.txt')
    with open(file_path, 'w') as f:
        f.write('test')

    files = s3_uploader.get_all_files_from_folder(folder_name)
    assert len(files) == 1
    assert files == ['test.txt']

    os.remove(file_path)
    os.rmdir(folder_name)


def test_upload_file_to_s3(s3_setup):
    bucket_name = 'my_s3_bucket'
    folder_name = 'test_folder'
    file_path = 'test.txt'
    os.mkdir(folder_name)
    with open(os.path.join(folder_name, file_path), 'w') as f:
        f.write('test')

    s3_client = boto3.client('s3')

    s3_uploader.upload_file_to_s3(s3_client, bucket_name, folder_name, file_path)

    s3_objects = s3_client.list_objects_v2(Bucket=bucket_name)
    assert 'Contents' in s3_objects
    assert len(s3_objects['Contents']) == 1
    assert s3_objects['Contents'][0]['Key'] == 'test.txt'

    os.remove(os.path.join(folder_name, file_path))
    os.rmdir(folder_name)
