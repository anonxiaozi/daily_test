# -*- coding: utf-8 -*-
# @Time: 2019/4/11
# @File: recursive_record_shop

import os
import re
import datetime
import argparse
import record_shop
import pymongo
from pymongo import errors
import sys


class OptCSV(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.top_path = self.args["top_path"]
        self.file_path_list = []
        self.have_end_time = re.compile(r"out(\d+?)\_?(\d+?)\.csv")
        self.not_have_end_time = re.compile(r"out(\d+?)\.csv")
        self.match_content_time = re.compile(r"下单时间：(\d{4}\-\d{2}\-\d{2}\s+?\d{2}\:\d{2})")
        self.the_result = []
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        self.process_db = self.conn[self.args['process_db']]
        self.process_tb = self.process_db[self.args['process_tb']]

    def find_file_path(self):
        for f in os.walk(self.top_path ):
            self.file_path_list.extend([os.path.join(f[0], x) for x in f[-1]])

    def get_file_name_date(self, file_name):
        if "_" in file_name:
            start_time_str, end_time_str = self.have_end_time.findall(file_name)[0]
        else:
            try:
                start_time_str, end_time_str = self.not_have_end_time.findall(file_name)[0], None
            except IndexError:
                self.the_result.append('read file {} failed: file name error'.format(file_name))
                return False, False
        start_time = datetime.datetime.strptime(start_time_str, "%Y%m%d")
        if not end_time_str:
            end_time = start_time + datetime.timedelta(days=1)
        else:
            end_time = datetime.datetime.strptime(end_time_str, "%Y%m%d") + datetime.timedelta(days=1)
        return start_time, end_time

    def detect_file_content_time(self):
        self.find_file_path()
        for file_path in self.file_path_list:
            drity_data = {"incorrect_time": [], "no_time": []}
            start_time, end_time = self.get_file_name_date(os.path.basename(file_path))
            if not start_time and not end_time:
                continue
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f.readlines():
                        fetch_result = self.match_content_time.findall(line)
                        if fetch_result:
                            content_time = datetime.datetime.strptime(fetch_result[0], "%Y-%m-%d %H:%M")
                            if not content_time >= start_time or not content_time <= end_time:
                                drity_data["incorrect_time"].append(line[:18])
                        else:
                            drity_data["no_time"].append(line[:18])
                self.args["filename"] = file_path
                record = record_shop.OptCSV(args=self.args)
                result = record.run()
                if result:
                    self.the_result.append('{}: {}'.format(file_path, record))
            except UnicodeDecodeError:
                print("read file {} faild. [ unicode error ]".format(file_path))
                continue
            except Exception as e:
                self.the_result.append("{}: {}".format(file_path, e))
                continue

    def start(self):
        self.detect_file_content_time()


def get_args():
    """
    命令行参数
    """
    arg = argparse.ArgumentParser(prog=os.path.abspath(__file__), usage='%(prog)s filter [options]')
    arg.add_argument("-H", "--host", type=str, help="DB host, default: %(default)s", default="10.15.101.63")
    arg.add_argument("-p", "--port", type=int, help="DB port, default: %(default)s", default=27027)
    arg.add_argument("-s", "--db", type=str, help="DB name, default: %(default)s", default="raw")
    arg.add_argument("-c", "--collection", type=str, help="Record collection name, default: %(default)s", default="LocationFromWeb")
    arg.add_argument("--top_path", type=str, help="Dir name", required=True)
    arg.add_argument("--process_db", type=str, help="Execution status record database, default: %(default)s", default="process")
    arg.add_argument("--process_tb", type=str, help="Execution status record collection, default: %(default)s", default="ProcessStatus")
    return arg


if __name__ == "__main__":
    args = vars(get_args().parse_args())
    opt = OptCSV(args=args)
    py_name = os.path.basename(__file__)
    try:
        opt.process_tb.insert_one({'_id': py_name, 'status': 0, 'desc': '', 'updated_at': datetime.datetime.now()})
    except pymongo.errors.DuplicateKeyError:
        opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 0, 'desc': '', 'updated_at': datetime.datetime.now()}})
    try:
        opt.start()
        if opt.the_result:
            opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 2, 'desc': ', '.join(opt.the_result), 'updated_at': datetime.datetime.now()}})
            sys.exit(2)
    except Exception as e:
        print('Error: ', e)
        opt.the_result.append(str(e))
        opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 2, 'desc': ', '.join(opt.the_result), 'updated_at': datetime.datetime.now()}})
        sys.exit(2)
    opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 1, 'desc': ', '.join(opt.the_result), 'updated_at': datetime.datetime.now()}})
