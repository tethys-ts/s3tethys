#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  8 11:02:46 2022

@author: mike
"""
import io
import os
import pandas as pd
from pydantic import BaseModel, HttpUrl

#######################################################
### Parameters

b2_public_key_pattern = '{base_url}/{bucket}/{obj_key}'
contabo_public_key_pattern = '{base_url}:{bucket}/{obj_key}'


#######################################################
### Functions


class ConnectionConfig(BaseModel):
    service_name: str
    endpoint_url: HttpUrl
    aws_access_key_id: str
    aws_secret_access_key: str


def create_public_s3_url(base_url, bucket, obj_key):
    """
    This should be updated as more S3 providers are added!
    """
    if 'contabo' in base_url:
        key = contabo_public_key_pattern.format(base_url=base_url.rstrip('/'), bucket=bucket, obj_key=obj_key)
    else:
        key = b2_public_key_pattern.format(base_url=base_url.rstrip('/'), bucket=bucket, obj_key=obj_key)

    return key


def path_date_parser(path, date_format=None):
    """

    """
    name1 = os.path.split(path)[1]
    date1 = pd.to_datetime(name1, format=date_format, exact=False, infer_datetime_format=True)

    return date1


def determine_file_obj_size(file_obj):
    """

    """
    pos = file_obj.tell()
    file_obj.seek(0, io.SEEK_END)
    size = file_obj.tell()
    file_obj.seek(pos)

    return size


# def attach_prefix(prefix, key):
#     """

#     """
#     if key == '':
#         new_key = prefix
#     elif not prefix.startswith('/'):
#         new_key = prefix + '/' + prefix


def build_params(bucket: str, obj_key: str=None, start_after: str=None, prefix: str=None, delimiter: str=None, max_keys: int=None, key_marker: str=None):
    """

    """
    params = {'Bucket': bucket}
    if start_after:
        params['StartAfter'] = start_after
    if obj_key:
        params['Key'] = obj_key
    if prefix:
        params['Prefix'] = prefix
    if delimiter:
        params['Delimiter'] = delimiter
    if max_keys:
        params['MaxKeys'] = max_keys
    if key_marker:
        params['KeyMarker'] = key_marker

    return params


def chunks(lst, n_items):
    """
    Yield successive n-sized chunks from lst.
    """
    lst_len = len(lst)
    n = lst_len//n_items

    pos = 0
    for i in range(0, n):
        yield lst[pos:pos + n_items]
        pos += n_items

    remainder = lst_len%n_items
    if remainder > 0:
        yield lst[pos:pos + remainder]



































