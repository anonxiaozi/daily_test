# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: ShopMonthAvg

'''
  月订单金额，店铺在每个自然月的订单总金额除以总count
'''

from conn_mongo import ConnectDB
from decimal import Decimal


class ShopMonthAvg(ConnectDB):

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

    def parse_data(self):
        data = self.query()
        for item in data:
            tmp_date_list = []
            for date in item['date']:
                tmp_date_list.append(date.strftime('%Y-%m'))
            item['avg_month'] = Decimal("{}".format(item['num']/len(set(tmp_date_list)))).quantize(Decimal("0.01")).__float__()
            del(item['date'])
            del(item['num'])
        return data

    def handle(self):
        data = self.parse_data()
        collection_name = 'month_avg'
        collection_obj = self.record_db[collection_name]
        collection_obj.drop()
        self.do_write(collection_obj, data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_month = [
        {
            '$group': {
                '_id': '$storeId',
                'num': {
                    '$sum': "$sum"
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
        'filter': filter_month,
        'record_db': 'analysis_pre'
    }
    opt = ShopMonthAvg(args=args)
    opt.run()
