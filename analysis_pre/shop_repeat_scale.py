# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  月复购稳定率，有订单的月份除以第一单起至今的月份数量，
  如果第一单日期是近两个月(本月或上月)，则认为稳定率为1.0，
  如果所有订单都在同一月份，并且次月份不是近两个月，则认为稳定率为0.0
'''

from conn_mongo import ConnectDB
from datetime import datetime
import calendar


class RepeatScale(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]

    def query(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    @staticmethod
    def handle_date(date_list):
        start, end = date_list[0], date_list[-1]
        start_year, end_year, start_month, end_month = start.year, end.year, start.month, end.month
        month_num = (end_year - start_year) * 12 + (end_month - start_month)
        month_list = set([x.strftime('%Y%m') for x in date_list])
        if month_num == 0:      # 所有订单都是同一个月，并且次月份不是近两个月(本月或上月)，返回0.0
            return 0.0
        return float('{:.2f}'.format(len(month_list) / month_num))

    def handle(self):
        data = self.query()
        new_data = []
        for item in data:
            new_item = {}
            new_item['_id'] = item['_id']
            item['date'].sort()     # 日期排序
            date_list = item['date']
            now = datetime.now()
            if now.month == 1:
                before_year, before_month = now.year - 1, 12
            else:
                before_year, before_month = now.year, now.month - 1
            days = (now - date_list[0]).days
            if days < (calendar.monthrange(now.year, now.month)[1] + calendar.monthrange(before_year, before_month)[1]):    # 如果第一单的时间是上过月或者是本月，则设为1.0
                new_item['scale'] = 1.0
                new_data.append(new_item)
                continue
            else:
                new_item['scale'] = self.handle_date(date_list)
                new_data.append(new_item)
                continue
        collection_name = 'RepeatScale'
        self.record_db[collection_name].drop()
        self.record_db[collection_name].insert_many(new_data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    year_num = 2019
    filter_scale = [
        {
            '$group': {
                '_id': '$storeId',
                'num': {
                    '$sum': 1
                },
                'date': {
                    '$push': '$date'
                }
            }
        }
    ]
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'filter': filter_scale,
        'record_db': 'analysis_pre'
    }
    opt = RepeatScale(args=args)
    opt.run()
