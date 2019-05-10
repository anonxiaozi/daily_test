# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  1. 店铺与其他店铺之间的距离 = ((店铺经度 - 其他店铺经度) ** 2 + (店铺纬度 - 其他店铺纬度) ** 2) ** 0.5
  2. 如果两个店铺的距离小于0.0018，则直接用0.0018作为距离的值
'''

from conn_mongo import ConnectDB
from decimal import Decimal


class GetIntervalDays(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]
        self.threshold, self.min = 0.2828, 0.0018
        self.dis_list = []

    def query(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def get_shop_detail(self):
        """
        获取所有receiver的经纬度信息
        """
        cursor = self.conn['raw']['DetailAddressInfo'].aggregate(
            [
                {
                    '$project': {
                        '_id': '$receiver_id',
                        'longitude': '$longitude',
                        'latitude': '$latitude'
                    }
                }
            ], allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def handle(self):
        store_data = self.query()
        receiver_data = self.get_shop_detail()
        receiver_dict, store_dict, info_dict, info_list = dict(), dict(), dict(), []
        result = []
        for item in store_data:
            store_dict[item['_id']] = {'receiver': item['receiver'], 'sum': item['sum']}
        for item in receiver_data:
            receiver_dict[item['_id']] = {'longitude': item['longitude'], 'latitude': item['latitude']}
        for store, info in store_dict.items():
            receiver_info = receiver_dict.get(info['receiver'], {'longitude': 0.0, 'latitude': 0.0})    # 如果receiver没有匹配到详细信息，则设置经纬度为0.0
            info_dict[store] = {'sum': info['sum'], 'longitude': receiver_info['longitude'], 'latitude': receiver_info['latitude']}
        for key, value in info_dict.items():
            tmp = {'_id': key, 'sum': value['sum'], 'longitude': value['longitude'], 'latitude': value['latitude']}
            info_list.append(tmp)
        for item in info_list:
            sum, rec_dis_acc, near_store_count = 0.0, 0.0, 0
            longitude, latitude = float(item['longitude']), float(item['latitude'])
            for other_item in info_list:
                if item == other_item: continue
                other_longitude, other_latitude = float(other_item['longitude']), float(other_item['latitude'])
                dis = (((longitude - other_longitude) ** 2 + (latitude - other_latitude) ** 2) ** 0.5)
                if dis <= self.threshold:
                    if dis < self.min:
                        dis = self.min
                    value = other_item['sum'] / dis
                    if value > (10 ** 8): continue
                    sum += value
                    rec_dis_acc += (1/dis)
                    near_store_count += 1
            sum, rec_dis_acc = self.handle_decimal(sum), self.handle_decimal(rec_dis_acc)
            if near_store_count == 0:
                result.append({
                    '_id': item['_id'], 'around_v': sum, 'rec_dis_acc': rec_dis_acc, 'near_store_account': near_store_count,
                    'sum': item['sum'], 'longitude': item['longitude'], 'latitude': item['latitude'], 'rec_dis_avg': 0
                })
            else:
                result.append({
                    '_id': item['_id'], 'around_v': sum, 'rec_dis_acc': rec_dis_acc, 'near_store_account': near_store_count,
                    'sum': item['sum'], 'longitude': item['longitude'], 'latitude': item['latitude'],
                    'rec_dis_avg': self.handle_decimal(rec_dis_acc / near_store_count)
                })

        collection_name = 'StoreAround'
        self.record_db[collection_name].drop()
        self.record_db[collection_name].insert_many(result)

    @staticmethod
    def handle_decimal(number):
        return Decimal('{}'.format(number)).quantize(Decimal('0.00001')).__float__()

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
                '_id': '$storeId',
                'sum': {
                    '$sum': '$sum'
                }
            }
        }, {
            '$lookup': {
                'from': 'Transaction',
                'let': {
                    'storeId': '$_id'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$store', '$$storeId'
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            'receiver': '$receiver'
                        }
                    }
                ],
                'as': 'receiver'
            }
        }, {
            '$project': {
                'sum': '$sum',
                'receiver': {
                    '$arrayElemAt': [
                        '$receiver', 0
                    ]
                }
            }
        }, {
            '$project': {
                'sum': '$sum',
                'receiver': '$receiver.receiver'
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
