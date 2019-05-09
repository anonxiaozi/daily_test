# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  订单金额稳定率，店铺所有订单金额的标准差
'''

from conn_mongo import ConnectDB
from numpy import std
from decimal import Decimal


class GetMonthAvg(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]

    @staticmethod
    def standard_deviation(array, ddof=0):
        deviation = std(array, ddof=ddof).__float__()
        deviation = Decimal(deviation).quantize(Decimal('0.01')).__float__()
        return deviation

    def query(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = []
        for item in cursor:
            tmp_item = {'_id': item['_id']}
            tmp_item['deviation'] = self.standard_deviation(item['amount'])
            data.append(tmp_item)
        cursor.close()
        return data

    def handle(self):
        data = self.query()
        collection_name = 'AmountStdDeviation'
        collection_obj = self.record_db[collection_name]
        collection_obj.drop()
        self.do_write(collection_obj, data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_deviation = [
        {
            '$group': {
                '_id': '$storeId',
                'amount': {
                    '$push': '$sum'
                }
            }
        }
    ]
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'filter': filter_deviation,
        'record_db': 'analysis_pre'
    }
    opt = GetMonthAvg(args=args)
    opt.run()
