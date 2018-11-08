import os

import boto3
import botocore
from netCDF4 import Dataset

# Helpful when working with large netCDF files in S3
# Use in place of list of paths to files
class S3BasicIterator(object):
    def __init__(self, bucket, files, path):
        self.bucket = bucket
        self.files = files
        self.path = os.path.abspath(path)
        self.i = 0
        self.s3 = boto3.resource('s3')

    def __iter__(self):
        return self

    def __next__(self):
        if self.i < len(self.files):
            # Remove previous file
            if self.i > 0:
                previous_file_path = os.path.join(self.path, self.files[self.i-1])
                os.remove(previous_file_path)

            download_path = os.path.join(self.path, self.files[self.i])
            download_folder = os.path.dirname(download_path)
            # Make sure folder exists
            if not os.path.exists(download_folder):
                os.makedirs(download_folder)
            # Download file if necessary
            if not os.path.exists(download_path):
                self.s3.Bucket(self.bucket).download_file(self.files[self.i], download_path)
            self.i += 1
            return Dataset(download_path)
        else:
            raise StopIteration
