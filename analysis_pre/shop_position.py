# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  平均复购天数，店铺所有订单间隔的平均天数，
  如果只下单一次，则认为平均天数为0
'''

from conn_mongo import ConnectDB
from decimal import Decimal


class GetIntervalDays(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]
        self.threshold = 0.2828

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
        result = []
        for item in data:
            item['longitude'] = item['_id']['longitude']
            item['latitude'] = item['_id']['latitude']
            item['_id'] = sorted(item['info'], key=lambda x:x['sum'], reverse=True)[0]['storeId']
            del(item['info'])

        for item in data:
            longitude, latitude = float(item['longitude']), float(item['latitude'])
            sum = 0.0
            for sub_item in data:
                if item == sub_item: continue
                sub_longitude, sub_latitude = float(sub_item['longitude']), float(sub_item['latitude'])
                dis = (((longitude - sub_longitude) ** 2 + (latitude - sub_latitude) ** 2) ** 0.5)
                if dis <= self.threshold:
                    value = Decimal('{}'.format(sub_item['sum'] / dis)).quantize(Decimal('0.00001')).__float__()
                    if value > (10 ** 8):
                        continue
                    sum += value
            result.append({'_id': item['_id'], 'around_v': sum})

        collection_name = 'StoreAround'
        self.record_db[collection_name].drop()
        self.record_db[collection_name].insert_many(result)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_interval = [
        {
            '$match': {
                'province': '浙江省',
                'city': '温州市'
            }
        }, {
            '$group': {
                '_id': {
                    'longitude': '$longitude',
                    'latitude': '$latitude'
                },
                'sum': {
                    '$sum': '$sum'
                },
                'info': {
                    '$push': {
                        'storeId': '$storeId',
                        'sum': {
                            '$sum': '$sum'
                        }
                    }
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
    opt = GetIntervalDays(args=args)
    opt.run()
