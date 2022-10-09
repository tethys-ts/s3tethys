#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  8 11:02:38 2022

@author: mike
"""
import io
import os
import pandas as pd
import zstandard as zstd
import copy
import boto3
import botocore
from time import sleep
from typing import Optional, List, Any, Union
import tethys_data_models as tdm
import pathlib
from pydantic import HttpUrl
import shutil
import gzip
# import tethys_smart_open as smart_open
import smart_open
import smart_open.http as so_http
from botocore import exceptions as bc_exceptions
import concurrent.futures

# import utils
from s3tethys import utils

so_http.DEFAULT_BUFFER_SIZE = 524288

###################################################
### Parameters

public_remote_key = 'https://b2.tethys-ts.xyz/file/tethysts/tethys/public_remotes_v4.json.zst'

s3_url_base = 's3://{bucket}/{key}'

##################################################
### Functions



def s3_client(connection_config: dict, max_pool_connections: int = 30):
    """
    Function to establish a client connection with an S3 account. This can use the legacy connect (signature_version s3) and the curent version.

    Parameters
    ----------
    connection_config : dict
        A dictionary of the connection info necessary to establish an S3 connection. It should contain service_name, endpoint_url, aws_access_key_id, and aws_secret_access_key. connection_config can also be a URL to a public S3 bucket.
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.

    Returns
    -------
    S3 client object
    """
    ## Validate config
    _ = tdm.base.ConnectionConfig(**connection_config)

    s3_config = copy.deepcopy(connection_config)

    if 'config' in s3_config:
        config0 = s3_config.pop('config')
        config0.update({'max_pool_connections': max_pool_connections, 'retries': {'mode': 'standard'}, 'read_timeout': 120})
        config1 = boto3.session.Config(**config0)

        s3_config1 = s3_config.copy()
        s3_config1.update({'config': config1})

        s3 = boto3.client(**s3_config1)
    else:
        s3_config.update({'config': botocore.config.Config(max_pool_connections=max_pool_connections, retries={'mode': 'standard'}, read_timeout=120)})
        s3 = boto3.client(**s3_config)

    return s3


def get_object_s3(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, connection_config: dict = None, public_url: HttpUrl=None, version_id=None, range_start: int=None, range_end: int=None, chunk_size=524288):
    """
    General function to get an object from an S3 bucket. One of s3, connection_config, or public_url must be used.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    s3 : botocore.client.BaseClient
        An S3 client object created via the s3_client function.
    connection_config : dict
        A dictionary of the connection info necessary to establish an S3 connection. It should contain service_name, s3, endpoint_url, aws_access_key_id, and aws_secret_access_key.
    public_url : str
        A URL to a public S3 bucket. This is generally only used for Backblaze object storage.
    bucket : str
        The bucket name.

    Returns
    -------
    bytes
        bytes object of the S3 object.
    """
    transport_params = {'buffer_size': chunk_size}

    if isinstance(version_id, str):
        transport_params['version_id'] = version_id

    ## Headers
    headers = {}
    # Range
    range_dict = {}

    if range_start is not None:
        range_dict['start'] = str(range_start)
    else:
        range_dict['start'] = ''

    if range_end is not None:
        range_dict['end'] = str(range_end)
    else:
        range_dict['end'] = ''

    ## Get the object
    if isinstance(public_url, str) and (version_id is None):
        url = utils.create_public_s3_url(public_url, bucket, obj_key)

        if range_dict:
            range1 = 'bytes={start}-{end}'.format(**range_dict)
            headers['Range'] = range1

        file_obj = smart_open.open(url, 'rb', headers=headers, transport_params=transport_params, compression='disable')

    elif isinstance(s3, botocore.client.BaseClient) or isinstance(connection_config, dict):
        if range_dict:
            range1 = 'bytes={start}-{end}'.format(**range_dict)
            transport_params.update({'client_kwargs': {'S3.Client.get_object': {'Range': range1}}})

        if s3 is None:
            _ = tdm.base.ConnectionConfig(**connection_config)

            s3 = s3_client(connection_config)

        s3_url = s3_url_base.format(bucket=bucket, key=obj_key)
        transport_params['client'] = s3

        file_obj = smart_open.open(s3_url, 'rb', transport_params=transport_params, compression='disable')

    else:
        raise TypeError('One of s3, connection_config, or public_url needs to be correctly defined.')

    return file_obj


def stream_to_file(file_obj, file_path, chunk_size=524288):
    """

    """
    file_path1 = pathlib.Path(file_path)
    file_path1.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path1, 'wb') as f:
        chunk = file_obj.read(chunk_size)
        while chunk:
            f.write(chunk)
            chunk = file_obj.read(chunk_size)


def decompress_stream_to_file(file_obj, file_path, chunk_size=524288):
    """

    """
    file_path1 = pathlib.Path(file_path)
    file_path1.parent.mkdir(parents=True, exist_ok=True)

    if file_path1.suffix == '.zst':
        file_path2 = file_path1.stem
        dctx = zstd.ZstdDecompressor()

        with open(file_path2, 'wb') as f:
            dctx.copy_stream(file_obj, f, read_size=chunk_size, write_size=chunk_size)

    elif file_path1.suffix == '.gz':
        file_path2 = file_path1.stem

        with gzip.open(file_obj, 'rb') as s_file, open(file_path2, 'wb') as d_file:
            shutil.copyfileobj(s_file, d_file, chunk_size)

    else:
        file_path2 = file_path1
        stream_to_file(file_obj, file_path2, chunk_size)

    return str(file_path2)


def decompress_stream_to_object(file_obj, compression, chunk_size=524288):
    """

    """
    b1 = io.BytesIO()

    if compression == 'zstd':
        dctx = zstd.ZstdDecompressor()

        with open(b1, 'wb') as f:
            dctx.copy_stream(file_obj, f, read_size=chunk_size, write_size=chunk_size)

    elif compression == '.gz':

        with gzip.open(file_obj, 'rb') as s_file, open(b1, 'wb') as d_file:
            shutil.copyfileobj(s_file, d_file, chunk_size)

    else:
        with open(b1, 'wb') as f:
            chunk = file_obj.read(chunk_size)
            while chunk:
                f.write(chunk)
                chunk = file_obj.read(chunk_size)

    return b1


def put_object_s3(s3, bucket, obj_key, file_obj, metadata=None, content_type=None, retries=3):
    """
    Function to upload data to an S3 bucket.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    file_obj : io.BytesIO
        The file object to be uploaded.
    metadata : dict or None
        A dict of the metadata that should be saved along with the object.
    content_type : str
        The http content type to associate the object with.

    Returns
    -------
    None
    """
    max_size = 1024*1024*5
    s3_url = s3_url_base.format(bucket=bucket, key=obj_key)

    transport_params = {'client': s3}

    extras = {}
    if isinstance(metadata, dict):
        extras.update({'Metadata': metadata})
    if isinstance(content_type, str):
        extras.update({'ContentType': content_type})

    obj_size = utils.determine_file_obj_size(file_obj)

    if obj_size > max_size:
        transport_params['client_kwargs'] = {'S3.Client.create_multipart_upload': extras}
        transport_params['multipart_upload'] = True
    else:
        transport_params['client_kwargs'] = {'S3.Client.put_object': extras}
        transport_params['multipart_upload'] = False

    counter = retries
    while counter > 0:
        try:
            with smart_open.open(s3_url, 'wb', transport_params=transport_params, compression='disable') as f:
                f.write(file_obj)

            # obj2 = s3.put_object(Bucket=bucket, Key=key, Body=obj, Metadata=metadata, ContentType=content_type)
            break
        except bc_exceptions.ConnectionClosedError as err:
            print(err)
            counter = counter - 1
            if counter == 0:
                raise err
            print('...trying again...')
            sleep(3)
        # except bc_exceptions.ConnectTimeoutError as err:
        #     print(err)
        #     counter = counter - 1
        #     if counter == 0:
        #         raise err
        #     print('...trying again...')
        #     sleep(3)


def put_file_s3(s3, bucket, obj_key, file_path, metadata=None, content_type=None, retries=3):
    """
    Function to upload data to an S3 bucket.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    file_path : str or pathlib.Path
        The path to the file to be uploaded.
    metadata : dict or None
        A dict of the metadata that should be saved along with the object.
    content_type : str
        The http content type to associate the object with.

    Returns
    -------
    None
    """
    with open(file_path, 'rb') as f:
        put_object_s3(s3, bucket, obj_key, f, metadata, content_type, retries=retries)



def copy_object_s3(s3, source_bucket, dest_bucket, source_key, dest_key, retries=3):
    """
    Copies an object in an S3 database to another location in an S3 database. They must have the same fundemental connection_config. All metadata is copied to the new object.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object
    """
    source_dict = {'Bucket': source_bucket, 'Key': source_key}

    counter = retries
    while counter > 0:
        try:
            resp = s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=source_dict, MetadataDirective='COPY')
            break
        except bc_exceptions.ConnectionClosedError as err:
            print(err)
            counter = counter - 1
            if counter == 0:
                raise err
            print('...trying again...')
            sleep(3)
        # except bc_exceptions.ConnectTimeoutError as err:
        #     print(err)
        #     counter = counter - 1
        #     if counter == 0:
        #         raise err
        #     print('...trying again...')
        #     sleep(3)

    return resp


def multi_copy_object_s3(s3, source_bucket, dest_bucket, source_dest_keys, retries=5, threads=30):
    """
    Same as the copy_object_s3 except with multi threading. The input source_dest_keys must be a list of dictionaries with keys named source_key and dest_key.
    """
    keys = copy.deepcopy(source_dest_keys)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        for key in keys:
            key['s3'] = s3
            key['source_bucket'] = source_bucket
            key['dest_bucket'] = dest_bucket
            f = executor.submit(copy_object_s3, **key)
            futures.append(f)
        runs = concurrent.futures.wait(futures)

    resp_list = [r.result() for r in runs[0]]

    return resp_list


def list_objects_s3(s3, bucket, prefix, start_after='', delimiter='', continuation_token='', date_format=None):
    """
    Wrapper S3 function around the list_objects_v2 base function with a Pandas DataFrame output.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
        Limits the response to keys that begin with the specified prefix.
    start_after : str
        The S3 key to start after.
    delimiter : str
        A delimiter is a character you use to group keys.
    continuation_token : str
        ContinuationToken indicates to S3 that the list is being continued on this bucket with a token.

    Returns
    -------
    DataFrame
    """
    if s3._endpoint.host == 'https://vault.revera.co.nz':
        js = []
        while True:
            js1 = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=start_after, Delimiter=delimiter)

            if 'Contents' in js1:
                js.extend(js1['Contents'])
                if 'NextMarker' in js1:
                    start_after = js1['NextMarker']
                else:
                    break
            else:
                break

    else:
        js = []
        while True:
            js1 = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, StartAfter=start_after, Delimiter=delimiter, ContinuationToken=continuation_token)

            if 'Contents' in js1:
                js.extend(js1['Contents'])
                if 'NextContinuationToken' in js1:
                    continuation_token = js1['NextContinuationToken']
                else:
                    break
            else:
                break

    if js:
        f_df1 = pd.DataFrame(js)[['Key', 'LastModified', 'ETag', 'Size']].copy()
        if isinstance(date_format, str):
            f_df1['KeyDate'] = pd.to_datetime(f_df1.Key.apply(lambda x: utils.path_date_parser(x, date_format)), utc=True, errors='coerce').dt.tz_localize(None)
        f_df1['ETag'] = f_df1['ETag'].str.replace('"', '')
        f_df1['LastModified'] = pd.to_datetime(f_df1['LastModified']).dt.tz_localize(None)
    else:
        if isinstance(date_format, str):
            f_df1 = pd.DataFrame(columns=['Key', 'LastModified', 'ETag', 'Size', 'KeyDate'])
        else:
            f_df1 = pd.DataFrame(columns=['Key', 'LastModified', 'ETag', 'Size'])

    return f_df1


def list_object_versions_s3(s3, bucket, prefix, start_after='', delimiter=None, date_format=None):
    """
    Wrapper S3 function around the list_object_versions base function with a Pandas DataFrame output.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    prefix : str
        Limits the response to keys that begin with the specified prefix.
    start_after : str
        The S3 key to start at.
    delimiter : str or None
        A delimiter is a character you use to group keys.

    Returns
    -------
    DataFrame
    """
    js = []
    while True:
        if isinstance(delimiter, str):
            js1 = s3_client.list_object_versions(Bucket=bucket, Prefix=prefix, KeyMarker=start_after, Delimiter=delimiter)
        else:
            js1 = s3_client.list_object_versions(Bucket=bucket, Prefix=prefix, KeyMarker=start_after)

        if 'Versions' in js1:
            js.extend(js1['Versions'])
            if 'NextKeyMarker' in js1:
                start_after = js1['NextKeyMarker']
            else:
                break
        else:
            break

    if js:
        f_df1 = pd.DataFrame(js)[['Key', 'VersionId', 'IsLatest', 'LastModified', 'ETag', 'Size']].copy()
        if isinstance(date_format, str):
            f_df1['KeyDate'] = pd.to_datetime(f_df1.Key.apply(lambda x: utils.path_date_parser(x, date_format)), utc=True, errors='coerce').dt.tz_localize(None)
        f_df1['ETag'] = f_df1['ETag'].str.replace('"', '')
        f_df1['LastModified'] = pd.to_datetime(f_df1['LastModified']).dt.tz_localize(None)
    else:
        if isinstance(date_format, str):
            f_df1 = pd.DataFrame(columns=['Key', 'VersionId', 'IsLatest', 'LastModified', 'ETag', 'Size', 'KeyDate'])
        else:
            f_df1 = pd.DataFrame(columns=['Key', 'VersionId', 'IsLatest', 'LastModified', 'ETag', 'Size'])

    return f_df1






































