# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: ShopRepeatScale

'''
  月复购稳定率，有订单的月份除以第一单起至今的月份数量
'''

from conn_mongo import ConnectDB
from datetime import datetime


class ShopRepeatScale(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]
        self.date_list = []

    def query(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def get_last_date(self, data):
        """
        计算当前日期：所有订单的日期中，最后日期的下个月1号
        """
        for item in data:
            self.date_list.extend(item['date'])
        self.date_list.sort(reverse=True)
        end = self.date_list[0]
        if end.month == 12:
            self.now = datetime(end.year + 1, 1, 1)
        else:
            self.now = datetime(end.year, end.month + 1, 1)

    def handle_date(self, date_list):
        start, end = date_list[0], date_list[-1]
        start_year, end_year, start_month, end_month = start.year, end.year, start.month, end.month
        month_num = (self.now.year - start_year) * 12 + (self.now.month - start_month)
        month_list = set([x.strftime('%Y%m') for x in date_list])
        if month_num <= 3:      # 有订单的月份数量小于3时，设为0.0
            return 0.0
        num = float('{:.2f}'.format(len(month_list) / month_num))
        if num > 1:
            print(start, end)
        return num

    def handle(self):
        data = self.query()
        self.get_last_date(data)
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
        'host': '10.15.101.252',
        'port': 27017,
        'filter': filter_scale,
        'record_db': 'analysis_pre'
    }
    opt = ShopRepeatScale(args=args)
    opt.run()
