import os
import boto3
import logging
import zipfile
import requests
import argparse
import concurrent.futures


def download_zip_from_url(url: str, zip_path: str):
    """Using stream parameter to deal with slow connection or large files"""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    logging.debug(f'Response ok, started downloading {zip_path} from {url}')
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(1024):
            f.write(chunk)
    logging.info(f'Downloaded zip archive: {zip_path}')
    return 0


def extract_zip_files(folder_name: str, zip_path: str) -> int:
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_files:
            zip_files.extractall(path=folder_name)
        return 0
    except Exception as e:
        logging.error(f'Error extracting files from ZIP archive: {e}')
        logging.debug(f'Removing {zip_path}')
        os.remove(zip_path)
        return 1


def get_all_files_from_folder(folder_name: str) -> list:
    all_files = []
    for root, dirs, files in os.walk(folder_name):
        for file in files:
            logging.debug(f'File detected: {file}')
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, folder_name)
            all_files.append(relative_path)
    return all_files


def upload_file_to_s3(s3_client, bucket_name, folder_name, file_path):
    try:
        key = os.path.basename(file_path)
        s3_client.upload_file(f'{folder_name}/{file_path}', bucket_name, key)
        logging.info(f'Uploaded file to S3: {key}')
        return 0
    except Exception as e:
        logging.error(f'Error uploading file {file_path}: {e}')
        return 1


def parse_arguments():
    parser = argparse.ArgumentParser(description='Upload files from a zip archive of given url to S3 with concurrency')
    parser.add_argument('zip_url', help='Url of the zip archive to download')
    parser.add_argument('bucket_name', help='Name of the S3 bucket to upload files to')
    parser.add_argument('--concurrency', type=int, default=10, help='Amount of concurrent uploads, default: 10')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    return parser.parse_args()


def main():
    args = parse_arguments()

    log_format = '%(levelname)s: %(message)s'
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format=log_format)

    # Download and extract zip archive in temp folder
    file_name = os.path.basename(args.zip_url)
    folder_name = file_name.split('.')[0]
    zip_path = f'{folder_name}/{file_name}'
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
    logging.debug(f'Started downloading')
    download_zip_from_url(args.zip_url, zip_path)
    logging.debug(f'Started extracting')
    extract_zip_files(folder_name, zip_path)

    # S3 client configuration
    logging.debug(f'S3 client configuration')
    session = boto3.Session()
    s3_client = session.client('s3')

    # Remove downloaded zip file
    logging.debug(f'Removing downloaded zip file: {zip_path}')
    os.remove(zip_path)

    # Get all files paths from nested directory in folder_name/file_name format
    all_files = get_all_files_from_folder(folder_name)

    # Upload files concurrently
    logging.debug(f'Started uploading files concurrently')
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        executor.map(lambda file_path: upload_file_to_s3(s3_client, args.bucket_name, folder_name, file_path), all_files)


if __name__ == '__main__':
    main()
