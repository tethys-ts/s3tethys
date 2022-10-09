#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  8 11:02:46 2022

@author: mike
"""
import io
import os
import pandas as pd

#######################################################
### Parameters

b2_public_key_pattern = '{base_url}/{bucket}/{obj_key}'
contabo_public_key_pattern = '{base_url}:{bucket}/{obj_key}'


#######################################################
### Functions


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









































