#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-
# @Time: 2019/3/12
# @File: operate_mongo.py

import pymongo
import datetime
import random
import threading
import time
import sys
import signal


class MongoDB(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        # self.conn = pymongo.MongoClient(
        #     self.kwargs["host"],
        #     self.kwargs["port"],
        #     username=self.kwargs["user"],
        #     password=self.kwargs["password"],
        #     authSource=self.kwargs["db"],
        #     authMechanism=self.kwargs["SCRAM-SHA-256"]
        # )
        self.db = self.conn[self.args["db"]]
        self.collection = self.db[self.args["collection"]]

    def get(self):
        while self.obj.is_alive():
            # docs = self.collection.find({"id": random.randint(1, 10000)})
            try:
                docs = self.collection.find_one()
            except Exception as e:
                print("Error: {}.".format(e))

    def insert_one(self):
        while self.obj.is_alive():
            data = {
                "id": str(random.randint(1, 10000)),
                "data": datetime.datetime.utcnow()
            }
            try:
                self.collection.insert_one(data)
            except Exception as e:
                print("Error: {}.".format(e))

    def insert_many(self):
        while self.obj.is_alive():
            date = [{
                "id": str(i),
                "date": datetime.datetime.utcnow()
            } for i in range(10000)]
            try:
                self.collection.insert_many(date)
            except Exception as e:
                print("Error: {}.".format(e))

    def run(self, obj):
        self.obj = obj
        getattr(self, self.args["action"])()

    @staticmethod
    def timing(duration):
        for i in range(1, duration + 1).__reversed__():
            sys.stdout.write("{}".format(i).center(10, "=") + "\r")
            sys.stdout.flush()
            time.sleep(1)
        else:
            return

def start(**kwargs):
    args = kwargs["args"]
    timing = threading.Thread(target=MongoDB.timing, args=(args["duration"],))
    timing.start()
    for n in range(args["threads"]):
        conn = MongoDB(args=args)
        t = threading.Thread(target=conn.run, args=(timing,))
        t.setDaemon(True)
        t.start()
    timing.join(args["duration"])


if __name__ == "__main__":
    args = {
        "host": "10.15.101.253",
        "port": 27027,
        "db": "test",
        "collection": "test",
        "threads": 1,
        "action": "insert_many",    # 操作方法
        "duration": 90     # 持续多少秒
    }
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    try:
        start(args=args)
    except Exception as e:
        print("Error: {}".format(e))

