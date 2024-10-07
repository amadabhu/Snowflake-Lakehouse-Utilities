import boto3
import os
import sys
import threading


# Public Access Key
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
print(AWS_ACCESS_KEY)
# Secret Access Key
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
print(AWS_SECRET_KEY)
# Name of the target S3 bucket
BUCKET_NAME = 'insert_your_bucket_here'
# Name of the directory within the bucket the file will be uploaded (can be blank)
TARGET_DIR = ''
# Local filepath for the file to be uploaded
UPLOAD_FILEPATH = r'Insert_your_path_here'


class S3ProgressPercentage(object):
    def __init__(self, filename, is_download=False):
        self._filename = filename
        self._size = 0 if is_download else float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._is_download = is_download

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            if self._is_download:
                sys.stdout.write("\r%s  %s MB" % (self._filename, round(self._seen_so_far/(1024*1024), 2)))
            else:
                percentage = (self._seen_so_far / self._size) * 100
                sys.stdout.write(
                    "\r%s  %s / %s MB (%.2f%%)" % (
                        self._filename, round(self._seen_so_far/(1024*1024), 2), round(self._size/(1024*1024), 2),
                        percentage))
            sys.stdout.flush()


def upload_files(path):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    s3 = session.client('s3')

    result = s3.get_bucket_acl(Bucket=BUCKET_NAME)
    print(result)

    if TARGET_DIR is not None and TARGET_DIR.strip() != '':
        bucket_filepath = '{}\\{}'.format(TARGET_DIR.strip().strip('\\'), UPLOAD_FILEPATH.split('\\')[-1])
    else:
        bucket_filepath = UPLOAD_FILEPATH.split('\\')[-1].strip()

    for subdir, dirs, files in os.walk(UPLOAD_FILEPATH):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                bucket_filepath1 = bucket_filepath + '\\' + os.path.basename(full_path)
                s3.upload_file(full_path, BUCKET_NAME, bucket_filepath1,
                               Callback=S3ProgressPercentage(full_path))


if __name__ == "__main__":

    upload_files(UPLOAD_FILEPATH)


