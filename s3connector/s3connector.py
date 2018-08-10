import boto3
import json
import botocore
import os
from botocore import exceptions

# loads config file
with open('config.json') as config:
    config = json.load(config)

directory = os.path.dirname(os.path.realpath(__file__)) + '/dropzone/'

# The s3 connector class. It implements s3 functionality like up- and download, checking key-existence etc.
class s3conn:

    # Constructor of s3 instance, iniializes connections.
    def __init__(self):
        self.s3 = boto3.client('s3', region_name='eu-west-1', endpoint_url=config['endpoint_url'],
                               aws_access_key_id=config['aws_access_key_id'],
                               aws_secret_access_key=config['aws_secret_access_key'])
        self.s3u = boto3.resource('s3', region_name='eu-west-1', endpoint_url=config['endpoint_url'],
                                  aws_access_key_id=config['aws_access_key_id'],
                                  aws_secret_access_key=config['aws_secret_access_key'])

    # Uploads a file to s3
    # Parameters:   filename = name of the local file
    #               mybucket = bucket name
    #               id = keyname in s3
    def upload(self, filename, mybucket,  id):
        self.s3u.meta.client.upload_file(filename, mybucket, id)

    # Uploads a fileobject to s3
    # Parameters:   filename = name of the local file
    #               mybucket = bucket name
    #               id = keyname in s3
    def uploadobj(self, filename, mybucket, id):
        self.s3.upload_fileobj(filename, mybucket, id)

    # Downloads a file from s3
    # Parameters:   filename = name of the local file
    #               mybucket = bucket name
    #               id = keyname in s3
    def download(self, mybucket, id, filename):
        self.s3u.Object(mybucket, id).download_file(filename)

    def download_all_files(self):
        try:
            list = self.s3.list_objects(Bucket='yargendropzone')['Contents']
        except KeyError:
            return
        counter = 0
        filepaths = []

        for key in list:
            key_name = key['Key']
            print key_name

            if not os.path.exists(directory):
                os.makedirs(directory)

            key_name_def = directory + key_name.replace("/", "_")
            print key_name_def
            self.download('yargendropzone', key_name, key_name_def)
            filepaths.append(key_name_def)

        return filepaths

    # Creates bucket
    # Parameter:    name = name of bucket to create
    def create_bucket(self, name):
        self.s3.create_bucket(Bucket=name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})

    # Listes all buckets in s3 space
    def list_buckets(self):
        bla = self.s3u.buckets.all()
        list = []
        for b in bla:
            list.append(b.name)

        return list

    # Returns a bucket object
    # Parameter:    name = name of bucket
    def nr_of_keys_bucket(self, name):
        bucket = self.s3u.Bucket(name)
        size = sum(1 for _ in bucket.objects.all())
        return size

    # Checks if key exists in a certain s3 bucket
    # Parameter:    bucketname = name of bucket in which the search will be performed
    #               keyname = name of the key for which will be searched
    def key_exists(self, bucketname, keyname):

        try:
            self.s3u.Object(bucketname, keyname).load()
        except exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise
        else:
            exists = True

        return exists


    def delete_keys(self):
        try:
            try:
                list = self.s3.list_objects(Bucket='yargendropzone')['Contents']
            except KeyError:
                print "Nothing to delete..."
                return

            for key in list:
                key_name = key['Key']
                self.s3u.Object('yargendropzone', key_name).delete()
                print "[!] Removing key {0} in s3 dropzone.".format(key_name)

        except Exception:
            print "[!] Couldn't delete key in s3 storage."

if __name__ == "__main__":
    s = s3conn()
    list = s.list_buckets()
    s.delete_keys()
