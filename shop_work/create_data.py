# -*- coding: utf-8 -*-
# @Time: 2019/4/2
# @File: create_data

import pymongo
import datetime
import random
import time
import bson.binary
import sys


class MongoDB(object):

    province = [
        '北京市', '天津市', '上海市', '重庆市', '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省', '安徽省', '福建省', '江西省', '山东省',
        '河南省', '湖北省', '湖南省', '广东省', '海南省', '四川省', '贵州省', '云南省', '陕西省', '甘肃省', '青海省', '台湾省', '内蒙古自治区', '广西壮族自治区',
        '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区', '香港特别行政区', '澳门特别行政区'
    ]

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        # self.conn = pymongo.MongoClient(
        #     self.kwargs["host"],count
        #     self.kwargs["port"],
        #     username=self.kwargs["user"],
        #     password=self.kwargs["password"],
        #     authSource=self.kwargs["db"],
        #     authMechanism=self.kwargs["SCRAM-SHA-256"]
        # )
        self.db = self.conn[self.args["db"]]

    def get(self):
        try:
            cursor = self.db[self.args["collection"]].aggregate(
                [
                    {
                        '$group': {
                            '_id': '$receiver',
                            'sum': {
                                '$sum': '$sum'
                            }
                        }
                    }
                ]
            )
            data = [x for x in cursor]
            cursor.close()
            return data
        except Exception as e:
            print("Error: {}.".format(e))

    def insert_one(self):
        data = {
            "id": str(random.randint(1, 10000)),
            "data": datetime.datetime.utcnow()
        }
        try:
            self.db[self.args["collection"]].insert_one(data)
        except Exception as e:
            print("Error: {}.".format(e))

    def random_time(self):
        date_result = []
        s, e = (2018,4,3,0,0,0,0,0,0), (2019,4,3,23,59,59,0,0,0)
        start, end = time.mktime(s), time.mktime(e)
        for i in range(20000):
            t = random.randint(start, end)
            date_tuple = time.localtime(t)
            date = time.strftime("%Y-%m-%d %H:%M:%S", date_tuple)
            date_result.append(date)
        else:
            return date_result

    def insert_many(self):
        # date_result = self.random_time()
        # receiver_list = self.get()
        # receiver_list = [x["_id"] for x in receiver_list]
        sample_data = "1234567890abcdefghijklmnopqrstuvwxyz" * 2
        data = []
        # for i in range(self.args["count"]):
        #     # receiver = "".join(random.sample(sample_data, 40)).encode("utf-8")
        #     receiver = random.sample(receiver_list, 1)[0]
        #     date = random.sample(date_result, 1)[0]
        #     date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        #     count = random.randint(1, 150)
        #     post = {
        #         "date" : date,
        #         "count" : count,
        #         "receiver" : bson.binary.Binary(receiver),
        #         "sum" : count * random.randint(20, 2000)
        #     }
        #     data.append(post)

        for item in range(60000):
            # item["_id"] = datetime.datetime.strftime(item["date"], "%Y-%m-%d") + item["receiver"].hex() + "".join(random.sample("abcdefghijklmnopqrstuvwxyz1234567890", 10))
            receiver = bson.binary.Binary(bytes("".join(random.sample(sample_data, 40)), encoding="utf-8"))
            post = {
                "receiver": receiver,
                "province": random.sample(self.province, 1)[0],
                "sum": random.randrange(5000, 500000)
            }
            data.append(post)
        try:
            # self.db[self.args["collection"]].insert_many(data)
            self.db["newTransaction_province"].insert_many(data)
        except Exception as e:
            print("Error: {}.".format(e))

    def run(self):
        return getattr(self, self.args["action"])()


if __name__ == "__main__":
    args = {
        "host": "10.15.101.254",
        "port": 27017,
        "db": "test",
        "collection": "newTransaction",
        "count": 1000000,
        "action": "insert_many",    # 操作方法
    }
    # mongo = MongoDB(args=args)
    # data = mongo.run()
