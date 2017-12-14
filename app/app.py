# -*- coding: utf-8 -*-

"""Monitor folder and upload media files to AWS S3

It will get monitored folder from environment or from command line
    monitored_folder/ : media files
    monitored_folder/done/ : all the files succesfully uploaded
    monitored_folder/error/: orphaned JSON files
    monitored_folder/logs/ : log files
    monitored_folder/config/config.json :  config file

"""


from __future__ import print_function
import sys
from os import path, getenv, listdir, rename
import time
import json
import logging
import logging.config
import threading
import StringIO
import boto3
import botocore
import random

CONFIG_FOLDER = "config"
CONFIG_FILENAME = "config.json"

ERROR_FOLDER = "error"
DONE_FOLDER = "done"

MONITORING_DELAY_DEFAULT = 5

FILE_MOVE_RETRIES = 3
FILE_MOVE_DELAY = 5

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = path.getsize(filename)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            log = logging.getLogger(__name__)
            self._seen_so_far += bytes_amount
            percentage = (float(self._seen_so_far) / float(self._size)) * 100

            log.debug("Upload %s:  %s / %s  (%.2f%%)", self._filename, self._seen_so_far, self._size, percentage)

            # sys.stdout.write(
                # "\\r%s  %s / %s  (%.2f%%)" % (
                    # self._filename, self._seen_so_far, self._size,
                    # percentage))
            # sys.stdout.flush()


def safe_move(source, destination):
    """Move a source file to destination.
    Check and catch all errors.

    Args:
        source: source file
        destination: destination to move file to

    Returns:
        N/A

    """
    log = logging.getLogger(__name__)

    retries_left = FILE_MOVE_RETRIES
    while retries_left:
        retries_left -= 1

        try:
            rename(source, destination)
        except Exception as error:
            log.error("Error moving file %s to %s: %s", source, destination, error)
            return

        # check if file really moved
        if path.isfile(source):
            log.error("File %s NOT moved to %s, despite no error reported, retrying", source, destination, error)
            time.sleep(FILE_MOVE_DELAY)
        else:
            return

    log.error("File %s NOT moved to %s, despite no error reported, after %s retries", source, destination, error, FILE_MOVE_RETRIES)
    return


def process_json_files(json_file_list, monitored_folder, config):
    """Process  a list of files names.

    Args:
        json_file_list: list of absolute file names
        monitored_folder: folder to monitor

    Returns:
        N/A

    """
    log = logging.getLogger(__name__)

    try:
        s3 = boto3.resource(
            's3',
            region_name=config["region"],
            aws_access_key_id=config["aws_key"],
            aws_secret_access_key=config["aws_secret"],
        )
        bucket = s3.Bucket(config["bucket"])
    except Exception as error:
        log.error("Unable connect to S3. Found error: %s", error)
        return

    # process all files
    for json_file_name in json_file_list:
        log.debug("Process file %s", json_file_name)

        # read json file
        try:
            json_file_path = path.abspath(path.join(monitored_folder, json_file_name))
            with open(json_file_path) as data_file:
                json_file = json.load(data_file)
        except Exception as error:
            log.error("Unable to read JSON file %s. Found error: %s", json_file_path, error)
            continue
        log.debug("Read file %s", json_file_name)

        # prepare metadata
        try:
            metadata = {}
            config_metadata = config["metadata"]
            for metadata_key, metadata_name in config_metadata.iteritems():
                if json_file.get(metadata_name, None):
                    metadata[metadata_key] = json_file[metadata_name]
        except Exception as error:
            log.error("Unable to prepare metadata for JSON file %s. Found error: %s", json_file_path, error)
        log.debug("Prepared metadata %s", metadata)

        # prepare media file name/path
        media_file_name = json_file["filename"] + json_file["extension"]
        media_file_path = path.abspath(path.join(monitored_folder, media_file_name))

        # check if media file exists
        if not path.isfile(media_file_path):
            log.error("Media file %s not found.", media_file_path)
            safe_move(json_file_path, path.abspath(path.join(monitored_folder, ERROR_FOLDER, json_file_name)))
            continue
        log.debug("Checked media file %s exists", media_file_path)

        # prepare upload arguments
        upload_kwargs = {}
        try:
            upload_kwargs["ACL"] = config["s3-acl"]
            # on AWS "/" are not allowed in bucket name, any path must be in key name
            if config.get("bucket-path", None) is not None:
                bucket_key = config["bucket-path"]+media_file_name
            else:
                bucket_key = media_file_name
            upload_kwargs["Metadata"] = metadata
        except KeyError as error:
            # ignore missing fields
            pass
        log.debug("Prepared upload arguments %s", upload_kwargs)

        # check if media file exist in bucket
        log.debug("Check if media file %s exist in bucket", media_file_name)
        media_exists_in_bucket = True
        try:
            s3.Object(config["bucket"], bucket_key).load()
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == "404":
                # does not exist, all ok
                log.debug("Media file %s does not exists in bucket %s", bucket_key, config["bucket"])
                media_exists_in_bucket = False
                pass
            else:
                # something went wrong with S3,return to future retry
                log.error("Error checking if media file %s exists in S3. Found error code %s, message: %s.",
                        media_file_path,
                        error.response['Error']['Code'],
                        error.response['Error']['Message'])
                return

        if media_exists_in_bucket:
            # key_exists, move to error
            log.error("Media file %s already exists.", media_file_name)
            safe_move(json_file_path, path.abspath(path.join(monitored_folder, ERROR_FOLDER, json_file_name)))
            safe_move(media_file_path, path.abspath(path.join(monitored_folder, ERROR_FOLDER, media_file_name)))
            continue

        # upload media to s3
        try:
            # http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.upload_file
            # Similar behavior as S3Transfer's upload_file() method, except that parameters are capitalized.
            s3.meta.client.upload_file(
                media_file_path,
                config["bucket"],
                bucket_key,
                Callback=ProgressPercentage(media_file_path),
                ExtraArgs=upload_kwargs,
            )
            log.debug("Media file %s uploaded successfully to key %s.", media_file_path, bucket_key)
        except botocore.exceptions.ClientError as error:
            # something went wrong with S3,return to future retry
            log.error("Error uploading media file %s to S3. Found error code %s, message: %s.",
                media_file_path,
                error.response['Error']['Code'],
                error.response['Error']['Message'])
            return
        except boto3.exceptions.S3UploadFailedError as error:
            log.error("Unable to upload file %s. Found error: %s", media_file_path, error)
            return
        log.debug("Uploaded media file %s", media_file_path)

        # clean-up
        safe_move(json_file_path, path.abspath(path.join(monitored_folder, DONE_FOLDER, json_file_name)))
        safe_move(media_file_path, path.abspath(path.join(monitored_folder, DONE_FOLDER, media_file_name)))
        log.debug("Clean-up done")


def mononitor_folder(monitored_folder, config):
    """Monitor folder.

    Args:
        monitored_folder: folder to monitor
        config: config dictionary

    Returns:
        N/A

    """
    log = logging.getLogger(__name__)

    monitored_folder = path.abspath(monitored_folder)
    log.debug("Monitoring %s", monitored_folder)
    monitoring_delay = config.get("monitoring-delay", MONITORING_DELAY_DEFAULT)
    while True:
        # get file list
        try:
            file_list = [name for name in listdir(monitored_folder) if name.endswith("." + config["watch-extension"])]
        except Exception as error:
            log.error("Unable to get JSON file list. Will retry in %s seconds. Found error: %s", monitoring_delay, error)
            time.sleep(monitoring_delay)
            continue
        random.shuffle(file_list)
        log.debug("Got file list %s", file_list)
        process_json_files(file_list, monitored_folder, config)
        time.sleep(monitoring_delay)


################################################################################


def main():
    """Main script routine

    Perform initializations:
        - read config file
        - init logging
        - start monitoring loop
    """

    # get monitored folder from enviroment variable
    if getenv('MONITORED_FOLDER', None) is None:
        print("Environment variable MONITORED_FOLDER is not defined", file=sys.stderr)
        return
    monitored_folder = getenv('MONITORED_FOLDER')

    # read config file
    try:
        config_path = path.abspath(path.join(monitored_folder, CONFIG_FOLDER, CONFIG_FILENAME))
        with open(config_path) as data_file:
            config = json.load(data_file)
    except:
        print("Unable to read config file %s" % CONFIG_FILENAME, file=sys.stderr)
        raise

    # initialize logging
    try:
        logging_config = config["log-config"]

        # set up logging handlers filenames to absolute
        for handler in [ "file_all", "file_error" ]:
            log_file_name = logging_config["handlers"][handler]["filename"]
            log_file_path = path.abspath(path.join(monitored_folder, log_file_name))
            logging_config["handlers"][handler]["filename"] = log_file_path

        logging.config.dictConfig(logging_config)
        log = logging.getLogger(__name__)
        log.info("Logging successfully initialized")
    except:
        print("Unable to initialize logging", file=sys.stderr)
        raise

    # check AWS credentials:
    try:
        log.debug("Checking AWS credentials")
        s3 = boto3.resource(
            's3',
            region_name=config["region"],
            aws_access_key_id=config["aws_key"],
            aws_secret_access_key=config["aws_secret"],
        )
        bucket = s3.Bucket(config["bucket"])
        data = StringIO.StringIO("Test content")
        bucket.upload_fileobj(data, 'credentials_test_object')
        response = bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': 'credentials_test_object',
                    },
                ],
                'Quiet': True
            },
        )
        log.debug("AWS credentials ok")
    except Exception as error:
        log.error("Unable connect to S3. Found error: %s", error)
        return


    # start monitoring folder
    mononitor_folder(monitored_folder, config)


if __name__ == '__main__':
    main()


