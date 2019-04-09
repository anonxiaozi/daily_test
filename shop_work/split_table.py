# -*- coding: utf-8 -*-
# @Time: 2019/4/4
# @File: split_table

import pymongo
import datetime
import sys
import calendar
import argparse


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
            'data': '$$ROOT'
        }
    }, {
        '$match': {
            'date_format.year': {
                '$eq': 2019
            },
            'date_format.month': {
                '$eq': 3
            }
        }
    }, {
        '$group': {
            '_id': '$data.storeId',
            'sum': {
                '$sum': '$data.sum'
            },
            'count': {
                '$sum': 1
            },
            'data': {
                '$push': '$data'
            }
        }
    }, {
        '$project': {
            'storeId': '$_id',
            'sum': '$sum',
            'count': '$count',
            'data0': {
                '$arrayElemAt': [
                    '$data', 0
                ]
            }
        }
    }, {
        '$project': {
            '_id': 0,
            'avg_sum': {
                '$divide': [
                    '$sum', '$count'
                ]
            },
            'storeId': '$_id',
            'accountname': '$data0.accountname',
            'storename': '$data0.storename',
            'sum': '$sum',
            'count': '$count',
            'fulladdress': '$data0.fulladdress',
            'province': '$data0.province',
            'city': '$dat0.city',
            'district': '$data0.district',
            'zone': '$data0.zone',
            'telephone': '$data0.telephone',
            'cellphone': '$data0.cellphone',
            'longitude': '$data0.longitude',
            'latitude': '$data0.latitude'
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
        self.date_format = datetime.datetime.strptime('{year}-{month}-{day}'.format(**self.args), '%Y-%m-%d')
        self.write_table_name = "cache_{}_{}".format(self.args['filter'], self.date_format.strftime('%Y-%m-%d').replace('-', ''))
        self.write_table = self.db[self.write_table_name]
        self.read_table = self.db[self.args["read"]]
        self.filter_rule.append({"$out": self.write_table_name})

    def insert_many(self):
        result = self.get_data()
        try:
            self.write_table.insert_many(result)
        except Exception as e:
            print("Error: {}.".format(e))

    def get_data(self):
        getattr(self, "do_{}".format(self.args["filter"]), self.echo)()
        if self.args["filter"] == "all":        # 拷贝collection全部内容
            cursor = self.read_table.aggregate(self.filter_rule)
            cursor.close()
            return
        if not isinstance(self.filter_rule, list):
            sys.exit(1)
        cursor = self.read_table.aggregate(self.filter_rule, allowDiskUse=True)
        cursor.close()

        # 添加Index
        count_idx = pymongo.IndexModel([('count', pymongo.ASCENDING)], name='count_idx')
        num_idx = pymongo.IndexModel([('num', pymongo.ASCENDING)], name='num_idx')
        storeId_idx = pymongo.IndexModel([('storeId', pymongo.ASCENDING)], name='storeId_idx')
        avg_sum_idx = pymongo.IndexModel([('avg_sum', pymongo.DESCENDING)], name='avg_sum_idx')
        province_idx = pymongo.IndexModel([('province', pymongo.ASCENDING)], name='province_idx')
        province_city_idx = pymongo.IndexModel([('province', pymongo.ASCENDING), ('city', pymongo.ASCENDING)], name='province_city_idx')
        province_city_district_idx = pymongo.IndexModel([
            ('province', pymongo.ASCENDING),
            ('city', pymongo.ASCENDING),
            ('district', pymongo.ASCENDING)
        ], name='province_city_district_idx')
        province_city_district_zone_idx = pymongo.IndexModel([
            ('province', pymongo.ASCENDING),
            ('city', pymongo.ASCENDING),
            ('district', pymongo.ASCENDING),
            ('zone', pymongo.ASCENDING)
        ], name='province_city_district_zone_idx')
        self.write_table.create_indexes([count_idx, num_idx, storeId_idx, avg_sum_idx, province_idx, province_city_idx, province_city_district_idx, province_city_district_zone_idx])

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
        e = s + datetime.timedelta(weeks=1) + datetime.timedelta(days=1)
        self.filter_rule[1]["$match"] = {
            'data.date': {
                '$gte': s,
                '$lt': e
            }
        }

    def do_season(self):
        start_date = datetime.datetime.strptime("{year}-{month}-{day}".format(**self.args), "%Y-%m-%d")
        current_month_days = calendar.monthrange(start_date.year, start_date.month)[1]
        current_month_remain_days = current_month_days - start_date.day
        next_month_1 = start_date + datetime.timedelta(days = current_month_remain_days) + datetime.timedelta(days = self.get_next_month_days(start_date))
        next_month_2 = next_month_1 + datetime.timedelta(days = self.get_next_month_days(next_month_1))
        end_season_date = next_month_2  + datetime.timedelta(days = start_date.day) + datetime.timedelta(days=1)
        self.filter_rule[1]["$match"] = {
            'data.date': {
                '$gte': start_date,
                '$lt': end_season_date
            }
        }
        return end_season_date      # 返回季度最后的日期给halfyear使用

    def do_halfyear(self):
        start_date = datetime.datetime.strptime("{year}-{month}-{day}".format(**self.args), "%Y-%m-%d")
        end_season_date_1 = self.do_season()
        current_month_days = calendar.monthrange(end_season_date_1.year, end_season_date_1.month)[1]
        current_month_remain_days = current_month_days - end_season_date_1.day
        next_month_1 = end_season_date_1 + datetime.timedelta(days=current_month_remain_days) + datetime.timedelta(days=self.get_next_month_days(end_season_date_1))
        next_month_2 = next_month_1 + datetime.timedelta(days=self.get_next_month_days(next_month_1))
        harf_year_date = next_month_2 + datetime.timedelta(days=start_date.day) + datetime.timedelta(days=1)
        self.filter_rule[1]["$match"] = {
            'data.date': {
                '$gte': start_date,
                '$lt': harf_year_date
            }
        }

    def do_all(self):
        self.filter_rule = [{'$out': self.write_table_name}]

    def run(self):
        return getattr(self, "get_data", self.echo)()

    @staticmethod
    def echo():
        print("Illega operation")

    @staticmethod
    def get_next_month_days(before_month_date):
        try:
            next_month_days = calendar.monthrange(before_month_date.year, before_month_date.month + 1)
        except calendar.IllegalMonthError:
            next_month_days = calendar.monthrange(before_month_date.year + 1, 1)
        return next_month_days[1]


def get_args():
    """
    命令行参数
    """
    arg = argparse.ArgumentParser(prog="Split_collection", usage='%(prog)s filter [options]')
    arg.add_argument("filter", type=str, choices=["year", "month", "week", "season", "halfyear", "all"], help="filter name")
    arg.add_argument("--host", type=str, help="DB host, default=%(default)s", default="10.15.101.63")
    arg.add_argument("--port", type=int, help="DB port, default=%(default)s", default=27027)
    arg.add_argument("--db", type=str, help="DB name, default=%(default)s", default="blockchain_test")
    arg.add_argument("-r", "--read", type=str, help="read collection name")
    # arg.add_argument("-w", "--write", type=str, help="write collection name")
    arg.add_argument("--year", type=int, help="year", required=True)
    arg.add_argument("--month", type=int, help="month")
    arg.add_argument("--day", type=int, help="day")
    return arg


if __name__ == "__main__":
    try:
        args = vars(get_args().parse_args())
        opt = SplitDB(args=args)
        data = opt.run()
    except Exception as e:
        print('Error: {}'.format(e))
