# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  月订单金额，店铺在每个自然月的订单总金额除以总count
'''

from conn_mongo import ConnectDB


class GetMonthAvg(ConnectDB):

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

    def handle(self):
        data = self.query()
        collection_name = 'month_avg'
        collection_obj = self.record_db[collection_name]
        collection_obj.drop()
        self.do_write(collection_obj, data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_month = [
        {
            '$project': {
                'year': {
                    '$year': {
                        'date': '$date'
                    }
                },
                'month': {
                    '$month': {
                        'date': '$date'
                    }
                },
                'storeId': '$storeId',
                'sum': '$sum',
                'count': '$count'
            }
        }, {
            '$group': {
                '_id': {
                    'month': '$month',
                    'year': '$year',
                    'storeId': '$storeId'
                },
                'count': {
                    '$sum': '$count'
                },
                'sum': {
                    '$sum': '$sum'
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'storeId': '$_id.storeId',
                'avg': {
                    '$divide': [
                        '$sum', '$count'
                    ]
                },
                'year': '$_id.year',
                'month': '$_id.month'
            }
        }
    ]
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'filter': filter_month,
        'record_db': 'analysis_pre'
    }
    opt = GetMonthAvg(args=args)
    opt.run()
