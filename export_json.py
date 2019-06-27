#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ETL :
from __future__ import print_function

import sys
import pymongo
import json
import time
import gzip
import traceback


def default(obj):
    """Default JSON serializer."""
    import calendar, datetime

    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
        millis = int(
            time.mktime(obj.timetuple())
        )
        return millis
    return str(obj)

WRITE_MAX = 10000

db_table_name = sys.argv[1]
uid_column_name = sys.argv[2]
event_time = sys.argv[3]
event_type = sys.argv[4]
event_name = sys.argv[5]
output_json_filename = sys.argv[6]
project_name = sys.argv[7]
use_time_in_id = sys.argv[8]

isgzipped = False
if output_json_filename[-2:] == "gz":
    isgzipped = True

mclient = pymongo.MongoClient("mongodb://10.15.101.79:27017/")
mdb = mclient['core']
mcol = mdb[db_table_name]

str_buff_arr = []
if isgzipped == True:
    f = gzip.open(output_json_filename, "w")
else:
    f = open(output_json_filename, "w")
allcount = 0
try:
    count = 0
    allinfo = mcol.find()
    for user in allinfo:
        count += 1
        new_user = dict()
        for key, value in user.items():
            new_user["{}_{}".format(db_table_name, key)] = value
        if not user:
            break
        dict_user = dict()
        dict_user["distinct_id"] = user[uid_column_name]
        if (use_time_in_id == '1'):
            dict_user["time"] = int(1000 * time.mktime(time.strptime(user[uid_column_name][-19:], "%Y-%m-%dT%H:%M:%S")))
        elif (event_time is None or event_time == ''):
            dict_user["time"] = 1558761194000
        else:
            dict_user["time"] = int(1000 * time.mktime(user[event_time].timetuple()))
        dict_user["type"] = event_type
        dict_user["time_free"] = 1
        if event_type == "track":
            dict_user["event"] = event_name
        dict_user["properties"] = new_user
        dict_user["project"] = project_name
        str_buff_arr.append(json.dumps(dict_user, default=default) + "\r\n")
        if count == WRITE_MAX:
            f.write(''.join(str_buff_arr))
            str_buff_arr = []
            count = 0
    else:
        f.write(''.join(str_buff_arr))
except Exception as e:
    traceback.print_exc()
