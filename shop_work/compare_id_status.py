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
        data = self.collection.find(match, {"SerialID": 1, "_id": 0, "Status": 1})
        # data = [x["SerialID"] for x in self.collection.find({}, {"SerialID": 1, "_id": 0})]
        data = [x for x in data]
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
        data = conn.get_serial_id(args["match_str"])
        serial_id = [x["SerialID"] for x in data]
        return data, serial_id

    def right_conn(self):
        args = self.args["right"]
        conn = OptMongodb(args=args)
        data = conn.get_serial_id(args["match_str"])
        serial_id = [x["SerialID"] for x in data]
        return data, serial_id

    def run(self):
        left_data, left_serial_id = self.left_conn()
        right_data, right_serial_id = self.right_conn()
        left_diff = list(set(left_serial_id).difference(set(right_serial_id)))
        right_diff = list(set(right_serial_id).difference(set(left_serial_id)))
        same_data = list(set(right_serial_id).intersection(left_serial_id))
        return left_diff, len(left_serial_id), right_diff, right_data, len(right_serial_id), same_data, len(same_data)


if __name__ == "__main__":
    args = {
        "left": {
            "host": "10.15.101.63",
            "port": 27027,
            "db": "blockchain_test",
            "collection": "LocationFromWeb",
            "match_str": {"OrderTime": {"$gte": "2019-03-01 00:00", "$lt": "2019-03-03 00:00"}}
        },
        "right": {
            "host": "10.15.101.63",
            "port": 27027,
            "db": "blockchain_test",
            "collection": "test_new_speed_order_sheets",
            "match_str": {"CreateTime": {"$gte": datetime.datetime(2019, 3, 1), "$lt": datetime.datetime(2019, 3, 3)}}
        }
    }
    opt = CompareID(args=args)
    spider_diff, spider_count, php_diff, php_data, php_count, same_data, same_count = opt.run()

    # print(spider_diff)
    print("spider_diff [ {} ]".format(len(spider_diff)).center(50, "*"))

    # print(php_diff)
    if php_diff:
        status_dict = {"交易关闭": [], "交易成功": []}
        php_dict = {}
        for item in php_data:
            php_dict[item["SerialID"]] = item["Status"]
        for serial_id in php_diff:
            if php_dict[serial_id] not in status_dict:
                status_dict[php_dict[serial_id]] = [serial_id]
            status_dict[php_dict[serial_id]].append(serial_id)
        for status, serial_list in status_dict.items():
            print(status, serial_list)

    print("php_diff [ {} ]".format(len(php_diff)).center(50, "*"))

    # print(same_data)
    print("same_data [ {} ] [spider: {}] [php: {}]".format(same_count, spider_count, php_count).center(50, "*"))
