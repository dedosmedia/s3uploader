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
import boto3
import botocore

CONFIG_FOLDER = "config"
CONFIG_FILENAME = "config.json"

ERROR_FOLDER = "error"
DONE_FOLDER = "done"

MONITORING_DELAY = 5


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
        log.error("Unable connect to S3. Found error: %s", json_file_path, error)
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

        # prepare upload arguments
        upload_kwargs = {}
        try:
            upload_kwargs["ACL"] = config["s3-acl"]
            # on AWS "/" are not allowed in bucket name, any path must be in key name
            if config.get("bucket-path", None) is not None:
                upload_kwargs["Key"] = config["bucket-path"]+media_file_name
            else:
                upload_kwargs["Key"] = media_file_name
            upload_kwargs["Metadata"] = metadata
        except KeyError as error:
            # ignore missing fields
            pass
        log.debug("Prepared upload arguments %s", upload_kwargs)

        # check if media file exist in bucket
        log.debug("Check if media file %s exist in bucket", media_file_name)
        try:
            s3.Object(config["bucket"], upload_kwargs["Key"]).load()
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == "404":
                # does not exist, all ok
                log.debug("Media file %s does not exists", media_file_name)
                pass
            else:
                # something went wrong with S3,return to future retry
                log.error("Error checking if media file %s exists in S3. Found error code %s, message: %s.",
                        media_file,
                        error.response['Error']['Code'],
                        error.response['Error']['Message'])
                return
        else:
            # key_exists, move to error
            log.error("Media file %s already exists.", media_file_name)
            rename(json_file_path, path.abspath(path.join(monitored_folder, ERROR_FOLDER, json_file_name)))
            rename(media_file_path, path.abspath(path.join(monitored_folder, ERROR_FOLDER, media_file_name)))
            continue

        # open media file for upload
        try:
            media_file = open(media_file_path, "rb")
            upload_kwargs["Body"] = media_file
        except Exception as error:
            log.error("Unable to open media file for JSON file %s. Found error: %s", json_file_path, error)
            rename(json_file_path, path.abspath(path.join(monitored_folder, ERROR_FOLDER, json_file_name)))
            continue
        log.debug("Open media file %s", media_file_path)

        # upload media to s3
        try:
            key = bucket.put_object(**upload_kwargs)
            log.debug("Put object: %s", key.__dict__)
        except botocore.exceptions.ClientError as error:
            # something went wrong with S3,return to future retry
            log.error("Error uploading media file %s to S3. Found error code %s, message: %s.",
                media_file_path,
                error.response['Error']['Code'],
                error.response['Error']['Message'])
            media_file.close()
            return

        log.debug("Uploaded media file %s", media_file_path)
        # clean-up
        media_file.close()
        rename(json_file_path, path.abspath(path.join(monitored_folder, DONE_FOLDER, json_file_name)))
        rename(media_file_path, path.abspath(path.join(monitored_folder, DONE_FOLDER, media_file_name)))
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
    while True:
        # get file list
        try:
            file_list = [name for name in listdir(monitored_folder) if name.endswith("." + config["watch-extension"])]
        except Exception as error:
            log.error("Unable to get JSON file list. Will retry in %s seconds. Found error: %s", MONITORING_DELAY, error)
            time.sleep(MONITORING_DELAY)
            continue

        log.debug("Got file list %s", file_list)
        process_json_files(file_list, monitored_folder, config)
        time.sleep(MONITORING_DELAY)


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

        # set up logging folder
        log_file_name = logging_config["handlers"]["file"]["filename"]
        log_file_path = path.abspath(path.join(monitored_folder, log_file_name))
        logging_config["handlers"]["file"]["filename"] = log_file_path

        logging.config.dictConfig(logging_config)
        log = logging.getLogger(__name__)
        log.info("Logging succesfully initialized")
    except:
        print("Unable to initialize logging", file=sys.stderr)
        raise

    # start monitoring folder
    mononitor_folder(monitored_folder, config)


if __name__ == '__main__':
    main()


