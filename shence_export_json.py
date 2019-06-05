#!/usr/bin/python3
# -*- coding: utf-8 -*-
# ETL :
from __future__ import print_function

import re, os, sys, time, datetime, calendar
import pymongo
import json
from bson import json_util
import codecs
import time

from colorclass import Color, set_dark_background, set_light_background, Windows
from etaprogress.progress import ProgressBar

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

mclient = pymongo.MongoClient("mongodb://10.15.101.59:27017/")
mdb = mclient['cleaned']
mcol = mdb[db_table_name]

totalcount = mcol.count_documents({})
# bar = ProgressBar(totalcount)
bar = ProgressBar(10000)
bar.bar.CHAR_FULL = Color('{autoyellow}#{/autoyellow}')
bar.bar.CHAR_LEADING = Color('{autoyellow}#{/autoyellow}')
bar.bar.CHAR_LEFT_BORDER = Color('{autoblue}[{/autoblue}')
bar.bar.CHAR_RIGHT_BORDER = Color('{autoblue}]{/autoblue}')

print("collection :" + db_table_name)
print("uid column:" + uid_column_name)
print("event time column:" + event_time)
print("event type:" + event_type)
print("event name:" + event_name)
print("output file:" + output_json_filename + " ext: " + output_json_filename[-2:] + "  is gzipped?" + str(isgzipped))
print(" total count: %s" % totalcount)
counter = 0
str_buff_arr = []
if isgzipped == True:
    f = gzip.open(output_json_filename, "w")
else:
    f = open(output_json_filename, "w")
allcount = 0
try:
    allinfo = mcol.find()
    if totalcount <= WRITE_MAX:
        for user in allinfo:
            new_user = dict()
            for key, value in user.items():
                new_user["{}_{}".format(db_table_name, key)] = value
            counter += 1
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
        else:
            f.write(''.join(str_buff_arr))
            print(bar, end='\r')
    else:
        for user in allinfo:
            counter += 1
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
            dict_user["properties"] = user
            dict_user["project"] = project_name
            str_buff_arr.append(json.dumps(dict_user, default=default) + "\r\n")

            # merge per  write_max
            if (counter >= WRITE_MAX):
                f.write(''.join(str_buff_arr))
                str_buff_arr = []
                allcount += counter
                counter = 0
                bar.numerator = allcount
                print(bar, end='\r')

        print(bar)
except Exception as e:
    traceback.print_exc()
