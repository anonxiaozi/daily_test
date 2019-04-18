# -*- coding: utf-8 -*-
# @Time: 2019/4/18
# @File: get_binary_data.py

'''
用bson.json_util的dumps方法，将db中的数据转为str，用loads方法将str转为bson.json，可以用在聚合查询中
'''

import pymongo
import bson
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
        d = bson.binary.Binary(b'AIqolnYp1WtMyiV0jfyj1s0t4fU=', 0)
        get_filter = [
            {'$limit': 1},
            {'$project': {'_id': '$_id'}}
        ]
        match_filter = [
            {
                '$match': json_util.loads('{"_id": {"$binary": "0zkJCrEkRyPu8gRnUHYKNFPHUPE=", "$type": "00"}}')
            }
        ]
        cursor = self.collection.aggregate(match_filter)
        data = [x for x in cursor]
        data1 = json_util.dumps(data[0])
        data2 = json_util.dumps(data[0], json_options=json_util.CANONICAL_JSON_OPTIONS)
        data3 = json_util.dumps(data[0], json_options=json_util.RELAXED_JSON_OPTIONS)
        data3_json = json.loads(data3)
        # data = json_util.loads(data)
        print(data1)
        print(data2)
        print(data3)
        for key, value in data3_json.items():
            print(key, value, sep=' -> ')
        cursor.close()

    def run(self):
        data = self.get_data()
        return data


if __name__ == '__main__':
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'db': 'blockchain_test',
        'collection': 'ProductInfo_hash'
    }
    opt = OptMongodb(args=args)
    data = opt.run()
