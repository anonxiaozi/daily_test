# -*- coding: utf-8 -*-
# @Time: 2019/3/28
# @File: compare_id

import pymongo
import datetime
import re


class OptMongodb(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        self.db = self.conn[self.args["db"]]
        self.collection = self.db[self.args["collection"]]
        # self.conn = pymongo.MongoClient(
        #     self.kwargs["host"],
        #     self.kwargs["port"],
        #     username=self.kwargs["user"],
        #     password=self.kwargs["password"],
        #     authSource=self.kwargs["db"],
        #     authMechanism=self.kwargs["SCRAM-SHA-256"]
        # )

    def check_index(self):
        indexes = self.collection.index_information()
        if not "serial_id" in indexes:
            self.collection.create_index("SerialID", name="serial_id")

    def get_serial_id(self, match):
        self.check_index()
        data = [x["SerialID"] for x in self.collection.find(match, {"SerialID": 1, "_id": 0})]
        # data = [x["SerialID"] for x in self.collection.find({}, {"SerialID": 1, "_id": 0})]
        return data

    def insert_many(self, data):
        try:
            self.collection.insert_many(data)
        except Exception as e:
            print("Error: {}.".format(e))

    def __del__(self):
        self.conn.close()


class CompareID(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]

    def left_conn(self):
        args = self.args["left"]
        conn = OptMongodb(args=args)
        serial_id = conn.get_serial_id(args["match_str"])
        return serial_id

    def right_conn(self):
        args = self.args["right"]
        conn = OptMongodb(args=args)
        serial_id = conn.get_serial_id(args["match_str"])
        return serial_id

    def run(self):
        left_serial_id = self.left_conn()
        right_serial_id = self.right_conn()
        left_diff = list(set(left_serial_id).difference(set(right_serial_id)))
        right_diff = list(set(right_serial_id).difference(set(left_serial_id)))
        same_data = list(set(right_serial_id).intersection(left_serial_id))
        return left_diff, len(left_serial_id), right_diff, len(right_serial_id), same_data, len(same_data)

if __name__ == "__main__":
    args = {
        "left": {
            "host": "",
            "port": 27017,
            "db": "",
            "collection": "",
            "match_str": {"OrderTime": {"$gt": "2019-02-18 00:00", "$lt": "2019-02-19 00:00"}}
        },
        "right": {
            "host": "",
            "port": 27017,
            "db": "",
            "collection": "",
            "match_str": {"CreateTime": {"$gt": datetime.datetime(2019, 2, 18), "$lt": datetime.datetime(2019, 2, 19)}}
        }
    }
    opt = CompareID(args=args)
    spider_diff, spider_count, php_diff, php_count, same_data, same_count = opt.run()

    # print(spider_diff)
    print("spider_diff [ {} ]".format(len(spider_diff)).center(50, "*"))

    print(php_diff)
    print("php_diff [ {} ]".format(len(php_diff)).center(50, "*"))

    # print(same_data)
    print("same_data [ {} ] [spider: {}] [php: {}]".format(same_count, spider_count, php_count).center(50, "*"))

