# -*- coding: utf-8 -*-
# @Time: 2019/4/10
# @File: split_table_myself

import pymongo
import datetime
import sys
import calendar
import argparse


class SplitDB(object):

    filter_rule = [
    {
        '$match': {
            'date': {
                '$gte': "",
                '$lt': ""
            }
        }
    }, {
        '$group': {
            '_id': '$storeId',
            'sum': {
                '$sum': '$sum'
            },
            'count': {
                '$sum': 1
            },
            'data': {
                '$push': '$$ROOT'
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
            'city': '$data0.city',
            'district': '$data0.district',
            'zone': '$data0.zone',
            'telephone': '$data0.telephone',
            'cellphone': '$data0.cellphone',
            'longitude': '$data0.longitude',
            'latitude': '$data0.latitude'
        }
    }, {
        '$out': ''
    }
    ]
    season_dict = {
        0: [1,2,3],
        1: [4,5,6],
        2: [7,8,9],
        3: [10,11,12]
    }

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
        self.read_table = self.db[self.args["read"]]
        self.get_start_end_date()

    def get_start_end_date(self):
        date = self.args["dateRange"]
        try:
            split_data = date.split("-")
            if len(split_data) == 1:
                self.real_start_time = datetime.datetime.strptime(split_data[0], "%Y%m%d")
                self.real_end_time = self.real_start_time + datetime.timedelta(days=1)
            else:
                self.real_start_time, self.real_end_time = datetime.datetime.strptime(split_data[0], "%Y%m%d"), datetime.datetime.strptime(split_data[1], "%Y%m%d")
            self.diff_day = (self.real_end_time - self.real_start_time).days
            self.real_start_month_num, self.real_start_year_num = self.real_start_time.month, self.real_start_time.year
            self.real_end_month_num, self.real_end_year_num = self.real_end_time.month, self.real_end_time.year
            # week
            self.start_week_time = self.real_start_time - datetime.timedelta(days=self.real_start_time.weekday()) - datetime.timedelta(weeks=1)
            self.end_week_time = self.real_end_time - datetime.timedelta(days=self.real_end_time.weekday()) - datetime.timedelta(weeks=1)
            # month
            if self.real_start_month_num == 1:
                self.start_month_time = self.real_start_time - datetime.timedelta(days=self.real_start_time.day - 1) - datetime.timedelta(days=calendar.monthrange(self.real_start_year_num - 1, 12)[1])
            else:
                self.start_month_time = self.real_start_time - datetime.timedelta(days=self.real_start_time.day - 1) - datetime.timedelta(days=calendar.monthrange(self.real_start_year_num, self.real_start_month_num - 1)[1])
            if self.real_end_month_num == 1:
                self.end_month_time = datetime.datetime.strptime("{}{}{}".format(self.real_start_year_num - 1, 12, calendar.monthrange(self.real_end_year_num - 1, 12)[1]), "%Y%m%d")
            else:
                self.end_month_time = self.real_end_time - datetime.timedelta(days=self.real_end_time.day)
            # season
            for season, months in self.season_dict.items():
                if self.real_start_month_num in months:
                    self.season_num = season
                    if season == 0:
                        self.start_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_start_year_num - 1, self.season_dict[3][0], 1), "%Y%m%d")
                    else:
                        self.start_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_start_year_num, self.season_dict[season - 1][0], 1), "%Y%m%d")
                if self.real_end_month_num in months:
                    if season == 0:
                        self.end_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_end_year_num - 1, self.season_dict[3][-1], calendar.monthrange(self.real_end_year_num - 1, self.season_dict[3][-1])[1]), "%Y%m%d")
                    else:
                        self.end_season_time = datetime.datetime.strptime("{}{}{}".format(self.real_end_year_num, self.season_dict[season - 1][-1], calendar.monthrange(self.real_end_year_num, self.season_dict[season - 1][-1])[1]), "%Y%m%d")
            # year
            self.start_year_time = datetime.datetime.strptime("{}0101".format(self.real_start_year_num - 1), "%Y%m%d")
            self.end_year_time = datetime.datetime.strptime("{}12{}".format(self.real_end_year_num - 1, calendar.monthrange(self.real_end_year_num, 12)[1]), "%Y%m%d")
            # halfyear
            self.start_halfyear_time = datetime.datetime.strptime("{}0101".format(self.real_start_year_num), "%Y%m%d")
            self.end_halfyear_time = datetime.datetime.strptime("{}0701".format(self.real_start_year_num), "%Y%m%d")
            return True
        except Exception as err:
            print("Increment Date Error: {}".format(str(err)))
            return False

    def do_work(self):
        self.do_week()
        self.do_month()
        self.do_season()
        self.do_year()
        self.do_halfyear()
        self.do_all()

    def aggregate_and_index(self, write_table_name):
        if not isinstance(self.filter_rule, list):
            print("Filter Rule Error: {}".format(str(self.filter_rule)))
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
        self.db[write_table_name].create_indexes([count_idx, num_idx, storeId_idx, avg_sum_idx, province_idx, province_city_idx, province_city_district_idx, province_city_district_zone_idx])

    def change_filter(self, s_time, e_time, filter):
        self.filter_rule[0]["$match"] = {
            'date': {
                '$gte': s_time,
                '$lt': e_time
            }
        }
        write_table_name = "cache_{}_{}".format(filter, s_time.strftime("%Y%m%d"))
        self.filter_rule[-1]["$out"] = write_table_name
        self.aggregate_and_index(write_table_name)

    def do_week(self):
        s = self.start_week_time
        while True:
            e = s + datetime.timedelta(weeks=1)
            self.change_filter(s, e, "week")
            s = e
            if s >= self.end_week_time:
                break

    def do_month(self):
        s = self.start_month_time
        while True:
            e = s + datetime.timedelta(days=calendar.monthrange(s.year, s.month)[1])
            self.change_filter(s, e, "month")
            s = e
            if s >= self.end_month_time:
                break

    def do_season(self):
        s = self.start_season_time
        while True:
            for season, months in self.season_dict.items():
                if s.month in months:
                    e = datetime.datetime.strptime("{}{}{}".format(s.year, months[-1], calendar.monthrange(s.year, months[-1])[1]), "%Y%m%d") + datetime.timedelta(days=1)
                    self.change_filter(s, e, "season")
                    s = e
                    break
            if s >= self.end_season_time:
                break

    def do_halfyear(self):
        s = self.start_halfyear_time
        while True:
            if s.month > 6:
                e = datetime.datetime.strptime("{}12{}".format(s.year, calendar.monthrange(s.year, 12)[1]), "%Y%m%d") + datetime.timedelta(days=1)
            else:
                e = datetime.datetime.strptime("{}6{}".format(s.year, calendar.monthrange(s.year, 6)[1]), "%Y%m%d") + datetime.timedelta(days=1)
            self.change_filter(s, e, "halfyear")
            s = e
            if s >= self.end_halfyear_time:
                break

    def do_year(self):
        s = self.start_year_time
        while True:
            e = datetime.datetime.strptime("{}12{}".format(s.year, calendar.monthrange(s.year, 12)[1]), "%Y%m%d") + datetime.timedelta(days=1)
            self.change_filter(s, e, "year")
            s = e
            if s >= self.end_year_time:
                break

    def do_all(self):
        del(self.filter_rule[0])
        write_table_name = "cache_{}_{}".format("all", 19700101)
        self.filter_rule[-1]["$out"] = write_table_name
        self.aggregate_and_index(write_table_name)

    # def insert_many(self):
    #     result = self.get_data()
    #     try:
    #         self.write_table.insert_many(result)
    #     except Exception as e:
    #         print("Error: {}.".format(e))


def get_args():
    """
    命令行参数
    """
    arg = argparse.ArgumentParser(prog="Split_collection", usage='%(prog)s filter [options]')
    arg.add_argument("--host", type=str, help="DB host, default=%(default)s", default="10.15.101.63")
    arg.add_argument("--port", type=int, help="DB port, default=%(default)s", default=27027)
    arg.add_argument("--db", type=str, help="DB name, default=%(default)s", default="blockchain_test")
    arg.add_argument("-r", "--read", type=str, help="read collection name")
    arg.add_argument("-d", "--dateRange", type=str, help="Date Range, such as: '20181112-20190410'", required=True)
    return arg


if __name__ == "__main__":
    try:
        args = vars(get_args().parse_args())
        opt = SplitDB(args=args)
        opt.do_work()
    except Exception as e:
        print('Error: {}'.format(e))
