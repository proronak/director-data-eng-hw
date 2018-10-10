import boto3
import os
import sys

clnt = boto3.client('s3')
# dest='hinge-hw'

# fldr='hinge-homework/director-data-engineering/ratings'

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print ('usage: upload_files_to_s3.py dest_bucket_name folder_from_where_files_will_be_uploaded')
        sys.exit(1)
    dest=sys.argv[1]
    fldr=sys.argv[2]

    for root, dirs, files in os.walk(fldr):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, fldr)

            # print(local_path)
            # print(relative_path)

            clnt.upload_file(local_path, dest, relative_path)
