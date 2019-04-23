# -*- coding: utf-8 -*-
# @Time: 2019/4/18
# @File: read_bson.py

import pymongo
from bson import json_util
import json


class OptMongodb(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        self.db = self.conn[self.args["db"]]
        self.collection = self.db[self.args["collection"]]

    def get_data(self):
        match_filter = [
            {
                '$match': {
                    'match': True
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
        match_cursor = self.collection.aggregate(match_filter, allowDiskUse=True)
        full_cursor = self.collection.aggregate(full_filter, allowDiskUse=True)
        match_data = [json_util.dumps(x) for x in match_cursor]
        full_data = [json_util.dumps(x) for x in full_cursor]
        match_data = [json.loads(x)['_id']['$binary'] for x in match_data]
        full_data = [json.loads(x)['_id']['$binary'] for x in full_data]
        dismatch_data = list(set(full_data).difference(set(match_data)))
        return dismatch_data

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
