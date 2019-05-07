# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: conn_mongo.py

import pymongo


class ConnectDB(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])

    @staticmethod
    def do_write(collection_obj, data):
        """
        写入数据库，一次写入2w条
        """
        n = 0
        total_num = len(data)
        while n < total_num:
            data2w = data[n:n+20000]
            collection_obj.insert_many(data2w, ordered=False)
            n += 20000

    def __del__(self):
        self.conn.close()

