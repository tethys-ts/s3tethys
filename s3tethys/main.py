#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  8 11:02:38 2022

@author: mike
"""
import pandas as pd
import io
import os
import zstandard as zstd
import copy
import boto3
import botocore
from time import sleep
from typing import Optional, List, Any, Union
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
AnyPath = Union[str, pathlib.Path]

###################################################
### Parameters

public_remote_key = 'https://b2.tethys-ts.xyz/file/tethysts/tethys/public_remotes_v4.json.zst'

s3_url_base = 's3://{bucket}/{key}'

multipart_size = 2**28

##################################################
### S3 Client


def s3_client(connection_config: utils.ConnectionConfig, max_pool_connections: int = 30, max_attempts: int = 3, retry_mode: str='adaptive', read_timeout: int=120):
    """
    Function to establish a client connection with an S3 account. This can use the legacy connect (signature_version s3) and the current version.

    Parameters
    ----------
    connection_config : dict
        A dictionary of the connection info necessary to establish an S3 connection. It should contain service_name, endpoint_url, aws_access_key_id, and aws_secret_access_key.
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of max attempts passed to the "retries" option in the S3 config.
    retry_mode: str
        The retry mode passed to the "retries" option in the S3 config.
    read_timeout: int
        The read timeout in seconds passed to the "retries" option in the S3 config.

    Returns
    -------
    S3 client object
    """
    ## Validate config
    _ = utils.ConnectionConfig(**connection_config)

    s3_config = copy.deepcopy(connection_config)

    if 'config' in s3_config:
        config0 = s3_config.pop('config')
        config0.update({'max_pool_connections': max_pool_connections, 'retries': {'mode': retry_mode, 'max_attempts': max_attempts}, 'read_timeout': read_timeout})
        config1 = boto3.session.Config(**config0)

        s3_config1 = s3_config.copy()
        s3_config1.update({'config': config1})

        s3 = boto3.client(**s3_config1)
    else:
        s3_config.update({'config': botocore.config.Config(max_pool_connections=max_pool_connections, retries={'mode': retry_mode, 'max_attempts': max_attempts}, read_timeout=read_timeout)})
        s3 = boto3.client(**s3_config)

    return s3


###############################################
### Functions that take or return file/stream objects


def get_object_s3(obj_key: str, bucket: str, s3: botocore.client.BaseClient = None, connection_config: dict = None, public_url: HttpUrl=None, version_id: str=None, range_start: int=None, range_end: int=None, chunk_size: int=524288, retries: int=3, read_timeout: int=120):
    """
    General function to get an object from an S3 bucket. One of s3, connection_config, or public_url must be used. This function will return a file object of the object in the S3 (or url) location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    obj_key : str
        The object key in the S3 bucket.
    bucket : str
        The bucket name.
    s3 : botocore.client.BaseClient
        An S3 client object created via the s3_client function.
    connection_config : dict
        A dictionary of the connection info necessary to establish an S3 connection. It should contain service_name, s3, endpoint_url, aws_access_key_id, and aws_secret_access_key.
    public_url : http str
        A URL to a public S3 bucket.
    version_id : str
        The S3 version id associated with the object.
    range_start: int
        The byte range start for the file.
    range_end: int
        The byte range end for the file.
    chunk_size: int
        The amount of bytes to download as once.
    retries: int
        The number of url request retries to perform before failing. This shouldn't be necessary given the max_attempts parameter in the s3_client function...but I'll keep it around until it's clearly not needed.
    read_timeout: int
        The read timeout for the url request. This only applies if the public_url is set which consequently uses the url_to_stream function. The read_timeout for normal S3 requests are set in the s3_client function.

    Returns
    -------
    file object
        file object of the S3 object.
    """
    transport_params = {'buffer_size': chunk_size}

    if isinstance(version_id, str):
        transport_params['version_id'] = version_id

    # Range
    if (range_start is not None) or (range_end is not None):
        range_dict = {}
        if range_start is not None:
            range_dict['start'] = str(range_start)
        else:
            range_dict['start'] = ''

        if range_end is not None:
            range_dict['end'] = str(range_end)
        else:
            range_dict['end'] = ''

        range1 = 'bytes={start}-{end}'.format(**range_dict)
    else:
        range1 = None

    ## Get the object
    if isinstance(public_url, str) and (version_id is None):
        url = utils.create_public_s3_url(public_url, bucket, obj_key)

        file_obj = url_to_stream(url, range_start, range_end, chunk_size, retries, read_timeout)

    elif isinstance(s3, botocore.client.BaseClient) or isinstance(connection_config, dict):
        if range1 is not None:
            transport_params.update({'client_kwargs': {'S3.Client.get_object': {'Range': range1}}})

        if s3 is None:
            _ = utils.ConnectionConfig(**connection_config)

            s3 = s3_client(connection_config)

        s3_url = s3_url_base.format(bucket=bucket, key=obj_key)
        transport_params['client'] = s3

        try:
            file_obj = smart_open.open(s3_url, 'rb', transport_params=transport_params, compression='disable')
        except Exception as err:
            # print('smart_open could not open url with the following error:')
            # print(err)
            file_obj = None

    else:
        raise TypeError('One of s3, connection_config, or public_url needs to be correctly defined.')

    return file_obj


def url_to_stream(url: HttpUrl, range_start: int=None, range_end: int=None, chunk_size: int=524288, retries: int=3, read_timeout: int=120):
    """
    Function to create a file object from a file stored via http(s). This function will return a file object of the object in the url location. This file object does not contain any data until data is read from it, which ensures large files are not completely read into memory.

    Parameters
    ----------
    url: http str
        The http url to the file.
    range_start: int
        The byte range start for the file.
    range_end: int
        The byte range end for the file.
    chunk_size: int
        The amount of bytes to download as once.
    retries: int
        The number of url request retries to perform before failing.
    read_timeout: int
        The read timeout for the url request.

    Returns
    -------
    file object
        file object of the S3 object.
    """
    transport_params = {'buffer_size': chunk_size, 'timeout': read_timeout}

    # Range
    if (range_start is not None) or (range_end is not None):
        range_dict = {}
        if range_start is not None:
            range_dict['start'] = str(range_start)
        else:
            range_dict['start'] = ''

        if range_end is not None:
            range_dict['end'] = str(range_end)
        else:
            range_dict['end'] = ''

        range1 = 'bytes={start}-{end}'.format(**range_dict)
        transport_params['headers'] = {'Range': range1}
    else:
        range1 = None

    ## Get the object
    counter = retries
    while True:
        try:
            file_obj = smart_open.open(url, 'rb', transport_params=transport_params, compression='disable')
            break
        except Exception as err:
            counter = counter - 1
            if counter > 0:
                sleep(3)
            else:
                # print('smart_open could not open url with the following error:')
                # print(err)
                file_obj = None
                break

    return file_obj


def zstd_stream_reader(stream, buffer_size: int=512000):
    """

    """
    if hasattr(stream, '_buffer_size'):
        buffer_size = stream._buffer_size
    dctx = zstd.ZstdDecompressor()
    reader = dctx.stream_reader(stream, read_size=buffer_size)
    # reader._buffer_size = buffer_size

    return reader


def zstd_stream_writer(stream, buffer_size: int=512000):
    """

    """
    if hasattr(stream, '_buffer_size'):
        buffer_size = stream._buffer_size
    dctx = zstd.ZstdCompressor(1)
    writer = dctx.stream_writer(stream, write_size=buffer_size)
    writer._buffer_size = buffer_size

    return writer


def stream_to_file(file_obj: io.BufferedIOBase, file_path: AnyPath, chunk_size: int=524288):
    """
    Convert a file object (stream) to a local file on disk. No decompression will occur. This function will read chunks of the file_obj and iteratively write those chunks to a file. Consequently, little memory is needed when saving a large file.

    Parameters
    ----------
    file_obj : io.BytesIO or io.BufferedIOBase
        The file object to be saved to file.
    file_path: str or pathlib.Path
        The output file path. If you want the function to do decompression, make sure to include the appropriate compression extension.
    chunk_size: int
        The amount of bytes to use in memory for processing the object.

    Returns
    -------
    str path
    """
    file_path1 = pathlib.Path(file_path)
    file_path1.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path1, 'wb') as f:
        chunk = file_obj.read(chunk_size)
        while chunk:
            f.write(chunk)
            chunk = file_obj.read(chunk_size)


def decompress_stream_to_file(file_obj: io.BufferedIOBase, file_path: AnyPath, chunk_size: int=524288):
    """
    Decompress a file object (stream) to a local file on disk. Decompression will occur if the file_path has an extension of .zst or .gz, otherwise no decompression will occur. If decompression occurs, then the extension will be removed from the file_path. This function will read chunks of the file_obj and iteratively write those chunks to a file. Consequently, little memory is needed when saving a large file.

    Parameters
    ----------
    file_obj : io.BytesIO or io.BufferedIOBase
        The file object to be saved to file.
    file_path: str or pathlib.Path
        The output file path. If you want the function to do decompression, make sure to include the appropriate compression extension.
    chunk_size: int
        The amount of bytes to use in memory for processing the object.

    Returns
    -------
    str path
    """
    file_path1 = pathlib.Path(file_path)
    file_path1.parent.mkdir(parents=True, exist_ok=True)

    if file_path1.suffix == '.zst':
        file_path2 = file_path1.parent.joinpath(file_path1.stem)
        dctx = zstd.ZstdDecompressor()

        with open(file_path2, 'wb') as f:
            dctx.copy_stream(file_obj, f, read_size=chunk_size, write_size=chunk_size)

    elif file_path1.suffix == '.gz':
        file_path2 = file_path1.parent.joinpath(file_path1.stem)

        with gzip.open(file_obj, 'rb') as s_file, open(file_path2, 'wb') as d_file:
            shutil.copyfileobj(s_file, d_file, chunk_size)

    else:
        file_path2 = file_path1
        stream_to_file(file_obj, file_path2, chunk_size)

    return str(file_path2)


def decompress_stream_to_object(file_obj: io.BufferedIOBase, compression: str=None, chunk_size: int=524288):
    """
    Decompress a file object (stream) to another file object. Decompression will occur only for compression options of zstd or gzip, otherwise no decompression will occur. This function will read the entire file_obj to another file object, but it will do it in chunks. Consequently, the entire file_obj will need to fit into memory.

    Parameters
    ----------
    file_obj : io.BytesIO or io.BufferedIOBase
        The file object to be saved to file.
    compression: str
        The compression types. Options are zstd and gzip.
    chunk_size: int
        The amount of bytes to use in memory for processing the object.

    Returns
    -------
    file object
    """
    b1 = io.BytesIO()

    if compression.lower() == 'zstd':
        dctx = zstd.ZstdDecompressor()

        dctx.copy_stream(file_obj, b1, read_size=chunk_size, write_size=chunk_size)

    elif compression.lower() == 'gzip':

        with gzip.open(file_obj, 'rb') as s_file:
            shutil.copyfileobj(s_file, b1, chunk_size)

    else:
        with open(b1, 'wb') as f:
            chunk = file_obj.read(chunk_size)
            while chunk:
                f.write(chunk)
                chunk = file_obj.read(chunk_size)

    b1.seek(0)

    return b1


def put_object_s3(s3: botocore.client.BaseClient, bucket: str, obj_key: str, file_obj: io.BufferedIOBase, metadata: dict=None, content_type: str=None, object_legal_hold: bool=False, chunk_size: int=524288, retries: int=3):
    """
    Function to upload data to an S3 bucket. This function will iteratively write the input file_obj in chunks ensuring that little memory is needed writing the object.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    file_obj : io.BytesIO or io.BufferedIOBase
        The file object to be uploaded.
    metadata : dict or None
        A dict of the metadata that should be saved along with the object.
    content_type : str
        The http content type to associate the object with.
    object_legal_hold : bool
        Should the object be uploaded with a legal hold?
    chunk_size: int
        The amount of bytes to use in memory for processing the object.
    retries: int
        The number of url request retries to perform before failing. This shouldn't be necessary given the max_attempts parameter in the s3_client function...but I'll keep it around until it's clearly not needed.

    Returns
    -------
    None
    """
    s3_url = s3_url_base.format(bucket=bucket, key=obj_key)

    transport_params = {'client': s3}

    extras = {}
    if isinstance(metadata, dict):
        extras.update({'Metadata': metadata})
    if isinstance(content_type, str):
        extras.update({'ContentType': content_type})
    if object_legal_hold:
        extras.update({'ObjectLockLegalHoldStatus': 'ON'})

    obj_size = utils.determine_file_obj_size(file_obj)

    if obj_size > multipart_size:
        transport_params['client_kwargs'] = {'S3.Client.create_multipart_upload': extras}
        transport_params['multipart_upload'] = True
    else:
        transport_params['client_kwargs'] = {'S3.Client.put_object': extras}
        transport_params['multipart_upload'] = False

    counter = retries
    while True:
        try:
            with smart_open.open(s3_url, 'wb', transport_params=transport_params, compression='disable') as f:
                chunk = file_obj.read(chunk_size)
                while chunk:
                    f.write(chunk)
                    chunk = file_obj.read(chunk_size)
            break
        except bc_exceptions.ConnectionClosedError as err:
            print(err)
            counter = counter - 1
            if counter == 0:
                raise err
            print('put_object failed...trying again...')
            sleep(3)
        # except bc_exceptions.ConnectTimeoutError as err:
        #     print(err)
        #     counter = counter - 1
        #     if counter == 0:
        #         raise err
        #     print('...trying again...')
        #     sleep(3)


def put_file_s3(s3: botocore.client.BaseClient, bucket: str, obj_key: str, file_path: AnyPath, metadata: dict=None, content_type: str=None, retries: int=3):
    """
    Function to upload a local file to an S3 bucket. This function will iteratively write the input file in chunks ensuring that little memory is needed writing the object.

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
    retries: int
        The number of url request retries to perform before failing. This shouldn't be necessary given the max_attempts parameter in the s3_client function...but I'll keep it around until it's clearly not needed.

    Returns
    -------
    None
    """
    with open(file_path, 'rb') as f:
        put_object_s3(s3, bucket, obj_key, f, metadata, content_type, retries=retries)


#####################################
### Other S3 operations


def copy_object_s3(s3: botocore.client.BaseClient, source_bucket: str, dest_bucket: str, source_key: str, dest_key: str, retries: int=3):
    """
    Copies an object in an S3 database to another location in an S3 database. They must have the same fundemental connection_config. All metadata is copied to the new object.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy_object

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    source_bucket : str
        The S3 source bucket.
    dest_bucket : str
        The S3 destination bucket.
    source_key : str
        The key name for the source object.
    dest_key : str
        The key name for the destination object.
    retries: int
        The number of url request retries to perform before failing. This shouldn't be necessary given the max_attempts parameter in the s3_client function...but I'll keep it around until it's clearly not needed.

    Returns
    -------
    S3 response
    """
    source_dict = {'Bucket': source_bucket, 'Key': source_key}

    counter = retries
    while True:
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


def multi_copy_object_s3(s3: botocore.client.BaseClient, source_bucket: str, dest_bucket: str, source_dest_keys: list, retries: int=5, threads: int=30):
    """
    Same as the copy_object_s3 except with multi threading. The input source_dest_keys must be a list of dictionaries with keys named source_key and dest_key.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    source_bucket : str
        The S3 source bucket.
    dest_bucket : str
        The S3 destination bucket.
    source_dest_keys : list of dict
        A list of dictionaries with keys named source_key and dest_key. Similar to the copy_obect_s3 function.
    retries: int
        The number of url request retries to perform before failing. This shouldn't be necessary given the max_attempts parameter in the s3_client function...but I'll keep it around until it's clearly not needed.
    threads: int
        The number of simultaneous copy_object_s3 runs.

    Returns
    -------
    list of S3 responses
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


def list_objects(s3: botocore.client.BaseClient, bucket: str, prefix: str=None, start_after: str=None, delimiter: str=None, max_keys: int=None, continuation_token: str=None):
    """
    Wrapper S3 function around the list_objects_v2 base function.

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
    date_format : str
        If the object key has a date in it, pass a date format string to parse and add a column called KeyDate.

    Returns
    -------
    dict
    """
    params = utils.build_params(bucket, start_after=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

    if continuation_token is not None:
        params['ContinuationToken'] = continuation_token

    js = []
    while True:
        js1 = s3.list_objects_v2(**params)

        if 'Contents' in js1:
            js.extend(js1['Contents'])
            if 'NextContinuationToken' in js1:
                continuation_token = js1['NextContinuationToken']
            else:
                break
        else:
            break

    return js

    # if js:
    #     f_df1 = pd.DataFrame(js)[['Key', 'LastModified', 'ETag', 'Size']].copy()
    #     if isinstance(date_format, str):
    #         f_df1['KeyDate'] = pd.to_datetime(f_df1.Key.apply(lambda x: utils.path_date_parser(x, date_format)), utc=True, errors='coerce').dt.tz_localize(None)
    #     f_df1['ETag'] = f_df1['ETag'].str.replace('"', '')
    #     f_df1['LastModified'] = pd.to_datetime(f_df1['LastModified']).dt.tz_localize(None)
    # else:
    #     if isinstance(date_format, str):
    #         f_df1 = pd.DataFrame(columns=['Key', 'LastModified', 'ETag', 'Size', 'KeyDate'])
    #     else:
    #         f_df1 = pd.DataFrame(columns=['Key', 'LastModified', 'ETag', 'Size'])

    # return f_df1


def list_object_versions(s3: botocore.client.BaseClient, bucket: str, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, delete_markers: bool=False):
    """
    Wrapper S3 function around the list_object_versions base function.

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
    delimiter : str or None
        A delimiter is a character you use to group keys.
    date_format : str
        If the object key has a date in it, pass a date format string to parse and add a column called KeyDate.

    Returns
    -------
    dict
    """
    params = utils.build_params(bucket, key_marker=start_after, prefix=prefix, delimiter=delimiter, max_keys=max_keys)

    js = []
    dm = []
    while True:
        js1 = s3.list_object_versions(**params)

        if 'Versions' in js1:
            js.extend(js1['Versions'])
            if 'DeleteMarkers' in js1:
                dm.extend(js1['DeleteMarkers'])
            if 'NextKeyMarker' in js1:
                params['KeyMarker'] = js1['NextKeyMarker']
            else:
                break
        else:
            break

    if delete_markers:
        return js, dm
    else:
        return js

    # if js:
    #     f_df1 = pd.DataFrame(js)[['Key', 'VersionId', 'IsLatest', 'LastModified', 'ETag', 'Size']].copy()
    #     if isinstance(date_format, str):
    #         f_df1['KeyDate'] = pd.to_datetime(f_df1.Key.apply(lambda x: utils.path_date_parser(x, date_format)), utc=True, errors='coerce').dt.tz_localize(None)
    #     f_df1['ETag'] = f_df1['ETag'].str.replace('"', '')
    #     f_df1['LastModified'] = pd.to_datetime(f_df1['LastModified']).dt.tz_localize(None)
    # else:
    #     if isinstance(date_format, str):
    #         f_df1 = pd.DataFrame(columns=['Key', 'VersionId', 'IsLatest', 'LastModified', 'ETag', 'Size', 'KeyDate'])
    #     else:
    #         f_df1 = pd.DataFrame(columns=['Key', 'VersionId', 'IsLatest', 'LastModified', 'ETag', 'Size'])

    # return f_df1


def list_objects_s3(s3: botocore.client.BaseClient, bucket: str, prefix: str, start_after: str='', delimiter: str='', continuation_token: str='', date_format: str=None):
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
    date_format : str
        If the object key has a date in it, pass a date format string to parse and add a column called KeyDate.

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


def list_object_versions_s3(s3: botocore.client.BaseClient, bucket: str, prefix: str, start_after: str='', delimiter: str=None, date_format: str=None):
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
    date_format : str
        If the object key has a date in it, pass a date format string to parse and add a column called KeyDate.

    Returns
    -------
    DataFrame
    """
    js = []
    while True:
        if isinstance(delimiter, str):
            js1 = s3.list_object_versions(Bucket=bucket, Prefix=prefix, KeyMarker=start_after, Delimiter=delimiter)
        else:
            js1 = s3.list_object_versions(Bucket=bucket, Prefix=prefix, KeyMarker=start_after)

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


def delete_objects_s3(s3: botocore.client.BaseClient, bucket: str, obj_keys: List[dict]):
    """
    obj_keys must be a list of dictionaries. The dicts must have the keys named Key and VersionId derived from the list_object_versions function. This function will automatically separate the list into 1000 count list chunks (required by the delete_objects request).

    Returns
    -------
    None
    """
    for keys in utils.chunks(obj_keys, 1000):
        _ = s3.delete_objects(Bucket=bucket, Delete={'Objects': keys, 'Quiet': True})



def put_object_legal_hold_s3(s3: botocore.client.BaseClient, bucket: str, obj_key: str, lock: bool=False):
    """
    Function to put or remove a legal hold on an object.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.
    lock : bool
        Should a lock be added to the object?

    Returns
    -------
    boto3 response
    """
    if lock:
        hold = {'Status': 'ON'}
    else:
        hold = {'Status': 'OFF'}

    resp = s3.put_object_legal_hold(Bucket=bucket, Key=obj_key, LegalHold=hold)

    return resp


def put_object_lock_configuration_s3(s3: botocore.client.BaseClient, bucket: str, lock: bool=False):
    """
    Function to enable or disable object locks for a bucket.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    lock : bool
        Should a lock be enabled for the bucket?

    Returns
    -------
    boto3 response
    """
    if lock:
        hold = {'ObjectLockEnabled': 'Enable'}
    else:
        hold = {'ObjectLockEnabled': 'Disable'}

    resp = s3.put_object_lock_configuration(Bucket=bucket, ObjectLockConfiguration=hold)

    return resp


def get_object_legal_hold_s3(s3: botocore.client.BaseClient, bucket: str, obj_key: str):
    """
    Function to get the staus of a legal hold of an object.

    Parameters
    ----------
    s3 : boto3.client
        A boto3 client object
    bucket : str
        The S3 bucket.
    obj_key : str
        The key name for the uploaded object.

    Returns
    -------
    bool
    """
    try:
        resp = s3.get_object_legal_hold(Bucket=bucket, Key=obj_key)
    except botocore.exceptions.ClientError:
        return False

    status = resp['LegalHold']['Status']

    if status == 'ON':
        return True
    else:
        return False

































