# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  月复购稳定率，有订单的月份除以第一单起至今的月份数量
'''

from conn_mongo import ConnectDB
from datetime import datetime


class RepeatScale(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]
        self.now = datetime(2019, 3, 1)

    def query(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def handle_date(self, date_list):
        start, end = date_list[0], date_list[-1]
        start_year, end_year, start_month, end_month = start.year, end.year, start.month, end.month
        month_num = (self.now.year - start_year) * 12 + (self.now.month - start_month)
        month_list = set([x.strftime('%Y%m') for x in date_list])
        if month_num <= 3:      # 有订单的月份数量小于3时，设为0.0
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
            new_item['scale'] = self.handle_date(date_list)
            new_data.append(new_item)
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
