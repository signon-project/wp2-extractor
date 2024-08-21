# Copyright 2021-2023 FINCONS GROUP AG within the Horizon 2020
# European project SignON under grant agreement no. 101017255.

# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 

#     http://www.apache.org/licenses/LICENSE-2.0 

# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

import argparse

import boto3
from botocore.config import Config
import botocore

from os import makedirs, path, remove

import pandas as pd
import numpy as np

from zipfile import ZipFile

import yaml

PROPERTIES = ["userid", "age", "gender", "hearingstatus", "annotationlanguage", "languagetype", "messagetype", "register", "sourcelanguage", "filetype"]

PROP_TO_CONFIG = {
    "userid": "user_id",
    "age": "age_group",
    "annotationlanguage": "annotation_language",
    "gender": "gender",
    "languagetype": "language_type",
    "messagetype": "message_type",
    "register": "register",
    "sourcelanguage": "source_language",
    "hearingstatus": "iam",
    "destination": "destination",
    "username": "username",
    "password": "password",
    "filetype": "file_type"
}

def parse_arguments():
    parser = argparse.ArgumentParser(description="Arguments for extracting contribution files from Minio", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-id", "--user-id", help="hash-phone-number from which download the files")
    parser.add_argument("-a", "--age-group", help="Metadata Age")
    parser.add_argument("-al", "--annotation-language", help="Metadata Annotation Language")
    parser.add_argument("-g", "--gender", help="Metadata Gender")
    parser.add_argument("-lt", "--language-type", help="Metadata Language Type")
    parser.add_argument("-mt", "--message-type", help="Metadata Message Type")
    parser.add_argument("-r", "--register", help="Metadata Register")
    parser.add_argument("-sl", "--source-language", help="Metadata Source Language")
    parser.add_argument("-iam", "--iam", help="Metadata Hearing Status")
    parser.add_argument("-ft", "--file-type", help="Metadata for File Type")
    parser.add_argument("-dest", "--destination", help="Destination folder", default="minioDownload")
    parser.add_argument("-usr", "--username", help="Username for minio access")
    parser.add_argument("-pwd", "--password", help="Password for minio access")
    parser.add_argument("-ms", "--minio-structure", help="Flag to define if download file should be grouped by user or not (minio-like)", action='store_true')
    parser.add_argument("-zip", "--zip", help="Flag to define if zip files should be maintained as zip files and do not unzip them after download", action='store_true')
    parser.add_argument("-ow", "--overwrite", help="Flag to define whether to overwrite or not a file that has been already downloaded with the same name", action='store_true')
    parser.add_argument("-csv", "--csv", help="Flag to define whether to retrieve a .csv file for the objects downloaded", action="store_true")
    args = parser.parse_args()
    config = vars(args)
    return config

def filtering(config_args, response, PROP_TO_CONFIG, PROPERTIES):
    for info in PROPERTIES:
        if config_args[PROP_TO_CONFIG[info]] != None and response['Metadata'][info] != config_args[PROP_TO_CONFIG[info]]:
            return False
    return True

def helper_makePath(dir):
    if not path.exists(dir):
        makedirs(dir)


def makePath(isMinioStructure, isZip, users, destination_folder, objects_to_download):
    if isMinioStructure:
        for user in users:
            if isZip:
                helper_makePath(destination_folder + '/' + user)
            else:
                for obj in objects_to_download:
                    user_zip_name = obj[:obj.rfind('.')]
                    helper_makePath(destination_folder + '/' + user_zip_name)
    else:
        if isZip:
            helper_makePath(destination_folder)
        else:
            for obj in objects_to_download:
                tmp = obj[obj.find('/')+1:]
                zip_name = tmp[:tmp.rfind('.')]
                helper_makePath(destination_folder + '/' + zip_name)


def downloadContributions(objects_to_download, isMinioStructure, bucket_name, s3_resource, destination_folder, isZip, isOverwrite, users):
    if len(objects_to_download) > 0: print("|-|\tDownloading Contributions...\n")

    makePath(isMinioStructure, isZip, users, destination_folder, objects_to_download)

    cnt = 0

    if not isMinioStructure and not isZip and not isOverwrite:
        for obj in objects_to_download:
            tmp = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + tmp
            s3_resource.Bucket(bucket_name).download_file(obj, dest)
            cnt += 1
            print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t"+tmp)
            with ZipFile(dest, 'r') as zipObj:
                for obj_in_zip in zipObj.namelist():
                    if obj_in_zip[len(obj_in_zip)-1] == '/':
                        helper_makePath(dest[:dest.rfind('.')] + '/' + obj_in_zip)
                    else:
                        if not path.exists(dest[:dest.rfind('.')] + '/' + obj_in_zip):
                            print("\t\t"+obj_in_zip)
                            zipObj.extract(obj_in_zip, dest[:dest.rfind('.')])
                        else:
                            print("\t|X|\t"+obj_in_zip)
            remove(dest)
        print("\n\n\t|X| --> File Already Exists")

    if not isMinioStructure and not isZip and isOverwrite:
        for obj in objects_to_download:
            tmp = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + tmp
            s3_resource.Bucket(bucket_name).download_file(obj, dest)
            cnt += 1
            print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t"+tmp)
            with ZipFile(dest, 'r') as zipObj:
                for obj_in_zip in zipObj.namelist():
                    if obj_in_zip[len(obj_in_zip)-1] == '/':
                        helper_makePath(dest[:dest.rfind('.')] + '/' + obj_in_zip)
                    else:
                        if not path.exists(dest[:dest.rfind('.')] + '/' + obj_in_zip):
                            print("\t\t"+obj_in_zip)
                        else:
                            print("\t|X|\t"+obj_in_zip)
                        zipObj.extract(obj_in_zip, dest[:dest.rfind('.')])
            remove(dest)
        print("\n\n\t|X| --> File Overwritten")

    if not isMinioStructure and isZip and not isOverwrite:
        for obj in objects_to_download:
            obj_name = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + obj_name
            if not path.exists(dest):
                s3_resource.Bucket(bucket_name).download_file(obj, dest)
                cnt += 1
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t\t"+obj_name)
            else:
                cnt += 1
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t|X|\t"+obj_name)
        print("\n\n\t|X| --> File Already Exists")

    if not isMinioStructure and isZip and isOverwrite:
        for obj in objects_to_download:
            obj_name = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + obj_name
            cnt += 1
            if not path.exists(dest):
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t\t"+obj_name)
            else:
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t|X|\t"+obj_name)
            s3_resource.Bucket(bucket_name).download_file(obj, dest)
        print("\n\n\t|X| --> File Overwritten")

    if isMinioStructure and not isZip and not isOverwrite:
        for obj in objects_to_download:
            tmp = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + obj
            s3_resource.Bucket(bucket_name).download_file(obj, dest)
            cnt += 1
            print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t"+tmp)
            with ZipFile(dest, 'r') as zipObj:
                for obj_in_zip in zipObj.namelist():
                    if obj_in_zip[len(obj_in_zip)-1] == '/':
                        helper_makePath(dest[:dest.rfind('.')] + '/' + obj_in_zip)
                    else:
                        if not path.exists(dest[:dest.rfind('.')] + '/' + obj_in_zip):
                            print("\t\t"+obj_in_zip)
                            zipObj.extract(obj_in_zip, dest[:dest.rfind('.')])
                        else:
                            print("\t|X|\t"+obj_in_zip)
            remove(dest)
        print("\n\n\t|X| --> File Already Exists")

    if isMinioStructure and not isZip and isOverwrite:
        for obj in objects_to_download:
            tmp = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + obj
            s3_resource.Bucket(bucket_name).download_file(obj, dest)
            cnt += 1
            print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t"+tmp)
            with ZipFile(dest, 'r') as zipObj:
                for obj_in_zip in zipObj.namelist():
                    if obj_in_zip[len(obj_in_zip)-1] == '/':
                        helper_makePath(dest[:dest.rfind('.')] + '/' + obj_in_zip)
                    else:
                        if not path.exists(dest[:dest.rfind('.')] + '/' + obj_in_zip):
                            print("\t\t"+obj_in_zip)
                        else:
                            print("\t|X|\t"+obj_in_zip)
                        zipObj.extract(obj_in_zip, dest[:dest.rfind('.')])
            remove(dest)
        print("\n\n\t|X| --> File Overwritten")


    if isMinioStructure and isZip and not isOverwrite:
        for obj in objects_to_download:
            obj_name = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + obj
            if not path.exists(dest):
                s3_resource.Bucket(bucket_name).download_file(obj, dest)
                cnt += 1
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t\t"+obj_name)
            else:
                cnt += 1
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t|X|\t"+obj_name)
        print("\n\n\t|X| --> File Already Exists")


    if isMinioStructure and isZip and isOverwrite:
        for obj in objects_to_download:
            obj_name = obj[obj.find('/')+1:]
            dest = destination_folder + '/' + obj
            cnt += 1
            if not path.exists(dest):
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t\t"+obj_name)
            else:
                print("|"+str(cnt)+"/"+str(len(objects_to_download))+"|\t|X|\t"+obj_name)
            s3_resource.Bucket(bucket_name).download_file(obj, dest)
        print("\n\n\t|X| --> File Overwritten")

    print('-'*115)


def main():
    configFile = 'config.yml'
    with open(configFile, 'rb') as f:
        conf = yaml.safe_load(f.read())

    config_args = parse_arguments()

    if config_args['username'] == None:
        print("Please define username using argument -usr <username> to access minio")
        return -1
    else:
        username = config_args['username']

    if config_args['password'] == None:
        print("Please define password using argument -pwd <password> to access minio")
        return -1
    else:
        password = config_args['password']

    s3_resource = boto3.resource('s3',
                                endpoint_url=conf['minio']['endpoint'],
                                aws_access_key_id=config_args['username'],
                                aws_secret_access_key=config_args['password']
                                )

    s3_client = boto3.client("s3",
                            endpoint_url=conf['minio']['endpoint'],
                            aws_access_key_id=config_args['username'],
                            aws_secret_access_key=config_args['password']
                            )
    print('-'*115)
    print("\t\t\t\tAccess on Minio Successful!\n")
    print("|#|\tUsername:\t\t\t" + username)
    print('-'*115)

    isZip = False
    if config_args['zip'] != None:
        if config_args['zip'] == True:
            isZip = True
    print("|#|\tZip Mode:\t\t\t"+str(isZip))

    isMinioStructure = False
    if config_args['minio_structure'] != None:
        if config_args['minio_structure'] == True:
            isMinioStructure = True
    print("|#|\tMinio Structure Mode:\t\t"+str(isMinioStructure))

    isOverwrite = False
    if config_args['overwrite'] != None:
        if config_args['overwrite'] == True:
            isOverwrite = True
    print("|#|\tOverwrite Mode:\t\t\t"+str(isOverwrite))

    isCSV = False
    if config_args['csv'] != None:
        if config_args['csv'] == True:
            isCSV = True
    print("|#|\tRetrieve Filtered CSV Mode:\t"+str(isCSV))

    destination_folder = config_args['destination']
    print("|#|\tDestination Folder:\t\t"+str(path.abspath(destination_folder)))
    print('-'*115)

    objects_to_download = set()
    user_list = set()
    isAll = True

    for prop in PROPERTIES:
        if config_args[PROP_TO_CONFIG[prop]] != None:
            isAll = False

    if isAll:
        object_list = list(s3_resource.Bucket(conf['minio']['bucketName']).objects.all())
        for item in object_list:
            response = s3_client.head_object(Bucket=conf['minio']['bucketName'], Key=item.key)
            objects_to_download.add(item.key)
            user = item.key[:item.key.find('/')]
            user_list.add(user)
    else:
        if config_args['user_id'] != None:
            object_list = list(s3_resource.Bucket(conf['minio']['bucketName']).objects.filter(Prefix=config_args['user_id']))
        else:
            object_list = list(s3_resource.Bucket(conf['minio']['bucketName']).objects.all())

        isPropertyFilter = False
        for info in PROPERTIES:
            if config_args[PROP_TO_CONFIG[info]] != None:
                isPropertyFilter = True

        for item in object_list:
            response = s3_client.head_object(Bucket=conf['minio']['bucketName'], Key=item.key)
            if isPropertyFilter:
                if response['Metadata'] != {}:
                    if filtering(config_args, response, PROP_TO_CONFIG, PROPERTIES):
                        objects_to_download.add(item.key)
            else:
                objects_to_download.add(item.key)

            user = item.key[:item.key.find('/')]
            user_list.add(user)

    downloadContributions(objects_to_download, isMinioStructure, conf['minio']['bucketName'], s3_resource, destination_folder, isZip, isOverwrite, user_list)

    if isCSV:
        columns = ['filename'] + PROPERTIES
        df = pd.DataFrame(columns=columns, index=np.arange(len(objects_to_download)))
        row = 0
        for item in objects_to_download:
            response = s3_client.head_object(Bucket=conf['minio']['bucketName'], Key=item)
            user = item[:item.find('/')]
            obj = item[item.find('/')+1:]
            for key in response['Metadata'].keys():
                df.at[row, key] = response['Metadata'][key]
            df.at[row, 'userid'] = user
            df.at[row, 'filename'] = obj
            row += 1
        df.to_csv("filtered_objects.csv")
        objects_to_download_tmp = set()
        object_list_tmp = list(s3_resource.Bucket(conf['minio']['bucketName']).objects.all())
        columns = ['filename'] + PROPERTIES
        df = pd.DataFrame(columns=columns, index=np.arange(len(object_list_tmp)))
        row = 0
        for item in object_list_tmp:
            response = s3_client.head_object(Bucket=conf['minio']['bucketName'], Key=item.key)
            objects_to_download_tmp.add(item.key)
            user = item.key[:item.key.find('/')]
            obj = item.key[item.key.find('/')+1:]
            for key in response['Metadata'].keys():
                df.at[row, key] = response['Metadata'][key]
            df.at[row, 'userid'] = user
            df.at[row, 'filename'] = obj
            row += 1

        df.to_csv("total_objects.csv")
        print("\t\tCorrectly retrieved statistics file for objects stored in Minio\n")
        print("|#|\t"+str(path.abspath("total_objects.csv")))
        print('-'*115)

main()