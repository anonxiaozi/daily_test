# -*- coding: utf-8 -*-
# @Time: 2019/4/18
# @File: first.py

import pymongo
from bson import json_util
import json


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

    def get_data(self):
        same_filter = [
            {
                '$lookup': {
                    'from': 'LocationFromWeb',
                    'localField': 'serialID',
                    'foreignField': 'SerialID',
                    'as': 'data'
                }
            }, {
                '$group': {
                    '_id': '$store'
                }
            }
        ]
        full_filter = [
            {
                '$group': {
                    '_id': '$store'
                }
            }
        ]
        # full_data = self.collection.aggregate(full_filter, allowDiskUse=True)
        cursor = self.collection.aggregate(same_filter, allowDiskUse=True)
        data = [json.loads(json_util.dumps(x)) for x in cursor if not x['data']]
        # full_list = [x['_id'] for x in full_data]
        # same_list = [x['_id'] for x in same_data if x['data']]
        # print(len(full_list))
        # print(len(same_list))
        # diff_list = list(set(full_list).difference(set(same_list)))
        # return diff_list
        return data

    def run(self):
        data = self.get_data()
        return data


if __name__ == '__main__':
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'db': 'blockchain_test',
        'collection': 'Transaction_hash'
    }
    opt = OptMongodb(args=args)
    data = opt.run()
    for n, item in enumerate(data):
        print(n, item, sep=' -> ')
