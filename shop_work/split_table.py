# -*- coding: utf-8 -*-
# @Time: 2019/4/4
# @File: split_table

import pymongo
import datetime
import random
import time
import bson.binary
import sys


class SplitDB(object):

    filter_rule = [
        {
            '$project': {
                '_id': 0,
                'date_format': {
                    '$dateToParts': {
                        'date': '$date',
                        'timezone': '+08:00'
                    }
                },
                'storeId': '$storeId',
                'accountname': '$accountname',
                'storename': '$storename',
                'date': '$date',
                'sum': '$sum',
                'count': '$count',
                'fulladdress': '$fulladdress',
                'province': '$province',
                'city': '$city',
                'district': '$district',
                'zone': '$zone',
                'telephone': '$telephone',
                'cellphone': '$cellphone',
                'longitude': '$longitude',
                'latitude': '$latitude'
            }
        }, {
            '$match': {
                'date_format.year': {
                    '$eq': 2019
                },
                'date_format.month': {
                    '$eq': 2
                }
            }
        }, {
            '$project': {
                'date_format': 0
            }
        }
    ]

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        # self.conn = pymongo.MongoClient(
        #     self.args["host"],count
        #     self.args["port"],
        #     username=self.args["user"],
        #     password=self.args["password"],
        #     authSource=self.args["db"],
        #     authMechanism=self.args["SCRAM-SHA-256"]
        # )
        self.db = self.conn[self.args["db"]]
        self.write_table = self.db[self.args["write_table"]]
        self.read_table = self.db[self.args["read_table"]]
        self.filter_rule.append({"$out": self.args["write_table"]})

    def insert_many(self, data):
        try:
            self.write_table.insert_many(data)
        except Exception as e:
            print("Error: {}.".format(e))

    def get_data(self):
        getattr(self, "do_{}".format(self.args["filter"]), self.echo)()
        if self.args["filter"] == "all":
            return
        if not isinstance(self.filter_rule, list):
            sys.exit(1)
        cursor = self.read_table.aggregate(self.filter_rule)
        cursor.close()

    def do_year(self):
        self.filter_rule[1]["$match"] = {
            "date_format.year": {"$eq": self.args["year"]}
        }

    def do_month(self):
        self.filter_rule[1]["$match"] = {
            "date_format.year": {"$eq": self.args["year"]},
            "date_format.month": {"$eq": self.args["month"]}
        }

    def do_day(self):
        self.filter_rule[1]["$match"] = {
            "date_format.year": {"$eq": self.args["year"]},
            "date_format.month": {"$eq": self.args["month"]},
            "date_format.day": {"$eq": self.args["day"]}
        }

    def do_week(self):
        s = datetime.datetime.strptime("{year}-{month}-{day}".format(**self.args), "%Y-%m-%d")
        e = s + datetime.timedelta(weeks=1)
        self.filter_rule[1]["$match"] = {
            'date': {
                '$gte': s,
                '$lt': e
            }
        }

    def do_season(self):
        # s = datetime.datetime.strptime("{year}-{month}-{day}".format(**self.args), "%Y-%m-%d")
        # self.filter_rule[1]["$match"] = {
        #     'date': {
        #         '$gte': s,
        #         '$lt': e
        #     }
        # }

    def do_all(self):
        self.filter_rule = self.filter_rule.append({"$out": self.args["write_table"]})

    def run(self):
        return getattr(self, self.args["operate"], self.echo)()

    @staticmethod
    def echo():
        print("Illega operation")


if __name__ == "__main__":
    args = {
        "host": "",
        "port": 27017,
        "filter": "week",   # 过滤规则
        "operate": "get_data",
        "db": "",
        "read_table": "",
        "write_table": "test111",
        "year": 2019,
        "month": 2,
        "day": 7
    }
    opt = SplitDB(args=args)
    data = opt.run()
