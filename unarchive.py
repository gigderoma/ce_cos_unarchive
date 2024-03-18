#!/usr/bin/env python3

"""A python script to restore archived object in Object Storage.
"""

import re
import os
import sys
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.exceptions import ClientError
import ibm_s3transfer.manager
from tqdm import tqdm
from colorama import Fore, Back, Style
from ibm_cos_sdk_config.resource_configuration_v1 import ResourceConfigurationV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
import math


# Constants for IBM COS values
COS_ENDPOINT = "https://s3{0}.{1}.cloud-object-storage.appdomain.cloud"
COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
BAR_FORMAT = "{l_bar}{bar} | {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
LOG_RE_PATTERN = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3} - (?:INFO|WARNING) - Object '(.*?)'.*\(index=(\d+?)\).*"


class Status:
    error = 0
    restored = 0
    already_restored = 0

    def __init__(self, error, restored, already_restored):
        self.error = error
        self.restored = restored
        self.already_restored = already_restored

    def increment_error(self, increment):
        self.error += increment

    def increment_restored(self, increment):
        self.restored += increment

    def increment_already_restored(self, increment):
        self.already_restored += increment

    def __str__(self):
        return f'[Errors: {self.error}, Already restored: {self.already_restored}, Restored: {self.restored}]'

def get_object_count(bucket_name, api_key):

    authenticator = IAMAuthenticator(apikey=api_key)
    rc_client = ResourceConfigurationV1(authenticator=authenticator)
    config = rc_client.get_bucket_config(bucket_name)
    logging.info("Number of objects into the bucket: {0}".format(config.result.get("object_count")))
    return(config.result.get("object_count"))


def get_page_count(bucket_name, max_keys, prefix, start_after):

    logging.info("Retrieving bucket pages from: '{0}'".format(bucket_name))
    try:

        if not cos_cli:
            raise Exception("cos_cli is not defined.")

        print("Calculating the number of pages...", end=" ", flush=True)
        paginator = cos_cli.get_paginator('list_objects_v2')
        pages = None
        if not prefix:
            pages = paginator.paginate(Bucket=bucket_name, MaxKeys=max_keys, StartAfter=start_after)
        else:
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys, StartAfter=start_after)

        count = sum(1 for _ in pages)
        logging.info("Page count: {0}".format(count))
        print(Fore.GREEN +str(count), flush=True)
        print(Style.RESET_ALL)
        return count

    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
        logging.error("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket pages: {0}".format(e))
        logging.error("Unable to retrieve bucket pages: {0}".format(e))


def get_object_list(bucket_name, max_keys, prefix, start_after):
    logging.info("Retrieving bucket contents from: '{0}'".format(bucket_name))
    try:

        if not cos_cli:
            raise Exception("cos_cli is not defined.")

        more_results = True
        next_token = ""
        object_list = list()

        while more_results:
            if not prefix:
                response = cos_cli.list_objects_v2(
                    Bucket=bucket_name, MaxKeys=max_keys, ContinuationToken=next_token, StartAfter=start_after
                )
            else:
                response = cos_cli.list_objects_v2(
                    Bucket=bucket_name,
                    MaxKeys=max_keys,
                    ContinuationToken=next_token,
                    Prefix=prefix,
                    StartAfter=start_after
                )
            files = response["Contents"]
            archived_objects = list(
                filter(
                    lambda obj: (
                        obj["StorageClass"] == "GLACIER"
                        or obj["StorageClass"] == "ACCELERATED"
                    ),
                    files,
                )
            )
            object_list += archived_objects
            if response["IsTruncated"]:
                next_token = response["NextContinuationToken"]
            else:
                more_results = False
                next_token = ""

        return list([object["Key"] for object in object_list])

    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
        logging.error("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))
        logging.error("Unable to retrieve bucket contents: {0}".format(e))

def restore_objects(bucket_name: str, max_keys: int , prefix: str, threads: int, days: int, start_after: str, start_index: int, object_count: int):
    if not start_after:
        logging.info(f"Restoring '{bucket_name}' bucket objects ({object_count if object_count >= 0 else ''}) with prefix '{prefix if prefix else ''}' for {days} days using {threads} threads")
    else:
        logging.info(f"Restoring '{bucket_name}' bucket objects ({object_count if object_count >= 0 else ''}) with prefix '{prefix if prefix else ''}' starting from object '{start_after}' (index={start_index:010}) for {days} days using {threads} threads")

    try:

        if not cos_cli:
            raise Exception("cos_cli is not defined.")

        page_index = 1
        status = Status(0, 0 ,0)
        paginator = cos_cli.get_paginator('list_objects_v2')
        pages = None
        if not prefix:
            pages = paginator.paginate(Bucket=bucket_name, MaxKeys=max_keys, StartAfter=start_after)
            if not start_after:
                page_count = math.ceil(object_count / max_keys)
            else:
                page_count = math.ceil((object_count - start_index) / max_keys)
        else:
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys, StartAfter=start_after)
            page_count = get_page_count(bucket_name=bucket_name, max_keys=max_keys, prefix=prefix, start_after=start_after)

        with tqdm(total=page_count, desc="Total", unit="page") as total_pb:
            index = 0 if start_index == -1 else (start_index + 1)
            for page in pages:
                objects = page["Contents"]
                archived_objects = list([object["Key"] for object in
                    filter(
                        lambda obj: (
                            obj["StorageClass"] == "GLACIER"
                            or obj["StorageClass"] == "ACCELERATED"
                        ),
                        objects,
                    )]
                )

                executor = ThreadPoolExecutor(max_workers=threads)
                tasks = []
                # elaborate one page objects
                with tqdm(total=len(archived_objects), desc="Page {0}".format(page_index), unit='obj', leave=False) as progress_bar:
                    for object in archived_objects:
                        task = executor.submit(restore_object, bucket_name, object, days, index)
                        tasks.append(task)
                        index+=1

                    for completed_task in as_completed(tasks):
                        result = completed_task.result()
                        status.increment_error(result.error)
                        status.increment_already_restored(result.already_restored)
                        status.increment_restored(result.restored)
                        progress_bar.update(1)

                page_index+=1
                total_pb.update(1)

        return status

    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
        logging.error("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))
        logging.error("Unable to retrieve bucket contents: {0}".format(e))

def restore_object(bucket_name: str, object_key: str, days: int, index: int):
    try:
        object_head = cos_cli.head_object(Bucket=bucket_name, Key=object_key)
        if object_head:
            restore_status = object_head.get("Restore")
            if restore_status:
                if 'ongoing-request="false"' in restore_status:
                    logging.warning(f"Object '{object_key}' (index={index:010}) has been already restored.")
                else:
                    logging.warning(
                        f"Object '{object_key}' (index={index:010}) restoration is still in progress."
                    )
                return Status(0, 0, 1)
            else:
                cos_cli.restore_object(
                    Bucket=bucket_name, Key=object_key, RestoreRequest={"Days": days}
                )
                logging.info(f"Object '{object_key}' (index={index:010}) has been restored.")
                return Status(0, 1, 0)

        else:
            logging.error(f"Object '{object_key}' (index={index:010}) not found.")

        return Status(1, 0, 0)
    except Exception as e:
        logging.error(f"Failed to restore object '{object_key}' (index={index}): {e}")
        return Status(1, 0, 0)

def get_last_n_lines(file_name, N):
    # Create an empty list to keep the track of last N lines
    list_of_lines = []
    # Open file for reading in binary mode
    with open(file_name, 'rb') as read_obj:
        # Move the cursor to the end of the file
        read_obj.seek(0, os.SEEK_END)
        # Create a buffer to keep the last read line
        buffer = bytearray()
        # Get the current position of pointer i.e eof
        pointer_location = read_obj.tell()
        # Loop till pointer reaches the top of the file
        while pointer_location >= 0:
            # Move the file pointer to the location pointed by pointer_location
            read_obj.seek(pointer_location)
            # Shift pointer location by -1
            pointer_location = pointer_location -1
            # read that byte / character
            new_byte = read_obj.read(1)
            # If the read byte is new line character then it means one line is read
            if new_byte == b'\n':
                # Save the line in list of lines
                list_of_lines.append(buffer.decode()[::-1])
                # If the size of list reaches N, then return the reversed list
                if len(list_of_lines) == N:
                    return list(reversed(list_of_lines))
                # Reinitialize the byte array to save next line
                buffer = bytearray()
            else:
                # If last read character is not eol then add it in buffer
                buffer.extend(new_byte)

        # As file is read completely, if there is still data in buffer, then its first line.
        if len(buffer) > 0:
            list_of_lines.append(buffer.decode()[::-1])

    # return the reversed list
    return list_of_lines

def get_last_object(file_name, N, pattern) -> tuple[str, int]:
    for line in reversed(get_last_n_lines("unarchive.log", 100)):
        match = re.search(pattern, line)
        if match:
            return match.group(1), int(match.group(2))

    return None

def main(arguments):

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apikey", help="API Key string", required=True)
    parser.add_argument("-b", "--bucket", help="The name of the bucket", required=True)
    parser.add_argument(
        "--prefix",
        help="Limits the response to keys that begin with the specified prefix. Page number calculation uses a slower method.",
    )
    parser.add_argument(
        "--resume",
        help="Continue a restore after the last object in the log.",
        action="store_true"
    )
    parser.add_argument(
        "--log",
        dest="log_file",
        help="Logfile. Default is {0}.log".format(os.path.splitext(os.path.basename(__file__))[0]),
        default="{0}.log".format(os.path.splitext(os.path.basename(__file__))[0])
    )
    parser.add_argument("--crn", help="COS service id (CRN)", required=True)
    parser.add_argument(
        "-r", "--region", default="eu-de", help="COS Region. Default region is eu-de"
    )
    parser.add_argument(
        "--endpoint-type", default="public", choices=["private", "public", "direct"], help="COS Endpoint Type. Default is public. It could be public, private or direct", dest="endpoint"
    )
    parser.add_argument(
        "-d",
        "--days",
        type=int,
        help="Specified the lifetime of the temporarily restored object",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--size",
        type=int,
        default=1000,
        help="Restricts the number of objects to retrieve. Default and maximum is 1000",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=10,
        help="Number of parallel threads. Must be less than SIZE. Default is 10",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logs"
    )

    args = parser.parse_args(arguments)

    if(args.threads > args.size):
        parser.error("Threads must be less or equal than size")

    last_object=""
    last_index=-1
    if(args.resume):
        last_object, last_index=get_last_object(args.log_file, 100, LOG_RE_PATTERN)
        if not last_object:
            print(Fore.RED + "Resume is not possible. Unable to retrieve last elaborated object from the log.")
            print(Style.RESET_ALL)
            exit(1)

    logging.basicConfig(
        filename=args.log_file,
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG if args.verbose == True else logging.INFO,
    )

    endpoint_type=("." + args.endpoint) if args.endpoint != "public" else ""

    # Create client connection
    global cos_cli
    print("Login to IBM Cloud Object Storage...", end=" ")
    logging.info("Login to IBM Cloud Object Storage using {0} endpoint ('{1}')".format(args.endpoint, COS_ENDPOINT.format(endpoint_type, args.region)))
    cos_cli = ibm_boto3.client(
        "s3",
        ibm_api_key_id=args.apikey,
        ibm_service_instance_id=args.crn,
        ibm_auth_endpoint=COS_AUTH_ENDPOINT,
        config=Config(signature_version="oauth", max_pool_connections=args.threads),
        endpoint_url=COS_ENDPOINT.format(endpoint_type, args.region),
    )
    print(Fore.GREEN + "OK")
    print(Style.RESET_ALL)

    object_count = -1
    if not args.prefix:
        object_count = get_object_count(bucket_name=args.bucket, api_key=args.apikey)
        if object_count < 1:
            print(Fore.RED + "Unable to retrieve total objects.")
            print(Style.RESET_ALL)
            exit(1)

    status = restore_objects(bucket_name=args.bucket, max_keys=args.size, prefix=args.prefix, threads=args.threads, days=args.days, start_after=last_object, start_index=last_index, object_count=object_count)
    print(f"All restore are completed, status: {status}")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
