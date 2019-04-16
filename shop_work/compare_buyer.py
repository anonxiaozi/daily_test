# -*- coding: utf-8 -*-
# @Time: 2019/4/12
# @File: compare_buyer

"""
  对比Transaction_hash表中store为空的集合与store不为空的集合中buyer字段相同的数量
"""

import pymongo


class OptMongodb(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"], socketTimeoutMS=2000)
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

    def get_data(self):
        cursor = self.collection.find()
        data = [x for x in cursor]
        non_zero_values = set([x["buyer"] for x in data if x["store"] != b""])
        zero_values = set([x["buyer"] for x in data if x["store"] == b""])
        auth_values = non_zero_values.intersection(zero_values)
        return list(auth_values)

    def run(self):
        data = self.get_data()
        return data


if __name__ == "__main__":
    args = {
        "host": "10.15.101.63",
        "port": 27027,
        "db": "blockchain_test",
        "collection": "Transaction_hash"
    }
    opt = OptMongodb(args=args)
    data = opt.run()
    print(len(data))

