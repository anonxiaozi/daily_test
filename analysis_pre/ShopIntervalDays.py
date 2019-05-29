# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: ShopIntervalDays

'''
  平均复购天数，店铺所有订单间隔的平均天数，
  如果只下单一次，则认为平均天数为0
'''

from conn_mongo import ConnectDB


class ShopIntervalDays(ConnectDB):

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
        new_data = []
        for item in data:
            new_item = {}
            new_item['_id'] = item['_id']
            item['date'].sort()     # 日期排序
            if len(item['date']) >= 2:
                total_days = (item['date'][-1] - item['date'][0]).days  # 最后下单日期减去第一次下单的日期
                new_item['IntervalDays'] = float('{:.2f}'.format(total_days / (item['num'] - 1)))  # 保留两位小数
            else:
                new_item['IntervalDays'] = float('0.0')
            new_data.append(new_item)

        collection_name = 'IntervalDays'
        self.record_db[collection_name].drop()
        self.record_db[collection_name].insert_many(new_data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_interval = [
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
        'filter': filter_interval,
        'record_db': 'analysis_pre'
    }
    opt = ShopIntervalDays(args=args)
    opt.run()
