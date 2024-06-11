from setuptools import setup, find_packages

setup(
    name='cli_s3_uploader',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'boto3',
        'logging',
        'zipfile',
        'requests',
        'argparse',
        'concurrent.futures',
    ],
    entry_points={
        'console_scripts': [
            'cli_s3_uploader=cli_s3_uploader.s3_uploader:main',
        ],
    },
)
