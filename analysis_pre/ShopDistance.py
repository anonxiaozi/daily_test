# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: ShopDistance

'''
  1. 店铺与其他店铺之间的距离 = ((店铺经度 - 其他店铺经度) ** 2 + (店铺纬度 - 其他店铺纬度) ** 2) ** 0.5
  2. 如果两个店铺的距离小于0.0018，则直接用0.0018作为距离的值
'''

from conn_mongo import ConnectDB
from decimal import Decimal


class ShopDistance(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]
        self.threshold, self.min = 0.2828, 0.0018
        self.dis_list = []
        self.choice_list = ['乐事', '炫迈', '奥利奥']

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
        for store in store_data:
            company_sum, _id = store['company_sum'], store['_id']
            tmp_company_dict = {}.fromkeys(self.choice_list, 0.0)
            if _id not in store_dict:
                store_dict[_id] = {
                    'receiver': store['receiver'], 'sum': 0.0, 'province': store['province'],
                     'city': store['city'], 'district': store['district'], 'zone': store['zone']
                }
                store_dict[_id].update(tmp_company_dict)
            company = store['company_sketch']
            if company in store_dict[_id]:
                store_dict[_id][company] += store['company_sum']
            store_dict[_id]['sum'] += store['company_sum']

        for item in receiver_data:
            receiver_dict[item['_id']] = {'longitude': item['longitude'], 'latitude': item['latitude']}

        for store, info in store_dict.items():
            receiver_info = receiver_dict.get(info['receiver'], {'longitude': 0.0, 'latitude': 0.0})    # 如果receiver没有匹配到详细信息，则设置经纬度为0.0
            info_dict[store] = info
            info_dict[store]['longitude'] = receiver_info['longitude']
            info_dict[store]['latitude'] = receiver_info['latitude']

        for key, value in info_dict.items():
            tmp = {
                '_id': key, 'sum': value['sum'], 'longitude': value['longitude'], 'latitude': value['latitude'],
                'province': value['province'], 'city': value['city'], 'district': value['district'], 'zone': value['zone']
            }
            for company_name in self.choice_list:
                tmp[company_name] = value[company_name]
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
            tmp_result = {
                '_id': item['_id'], 'around_v': sum, 'rec_dis_acc': rec_dis_acc, 'near_store_account': near_store_count,
                'sum': self.handle_decimal(item['sum']), 'longitude': item['longitude'], 'latitude': item['latitude'],
                'province': item['province'], 'city': item['city'], 'district': item['district'], 'zone': item['zone']
            }
            if near_store_count == 0:
                tmp_result['rec_dis_avg'] = 0
            else:
                tmp_result['rec_dis_avg'] = self.handle_decimal(tmp_result['rec_dis_acc'] / tmp_result['near_store_account'])
            for company_name in self.choice_list:
                tmp_result[company_name] = self.handle_decimal(item[company_name])
            tmp_result.update({'province': item['province'], 'city': item['city'], 'district': item['district'], 'zone': item['zone']})
            result.append(tmp_result)

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
                },
                'address': {
                    '$push': {
                        'province': '$province',
                        'city': '$city',
                        'district': '$district',
                        'zone': '$zone'
                    }
                }
            }
        }, {
            '$project': {
                'sum': '$sum',
                'address': {
                    '$arrayElemAt': [
                        '$address', 0
                    ]
                }
            }
        }, {
            '$project': {
                'sum': '$sum',
                'province': '$address.province',
                'city': '$address.city',
                'district': '$address.district',
                'zone': '$address.zone'
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
                            '_id': 1,
                            'receiver': '$receiver'
                        }
                    }
                ],
                'as': 'transaction'
            }
        }, {
            '$unwind': {
                'path': '$transaction'
            }
        }, {
            '$lookup': {
                'from': 'ProductTransaction',
                'let': {
                    'transaction_id': '$transaction._id'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$transaction', '$$transaction_id'
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            'productinfo': '$productInfo',
                            'product_price': '$price',
                            'product_amount': '$amount'
                        }
                    }
                ],
                'as': 'product'
            }
        }, {
            '$unwind': {
                'path': '$product'
            }
        }, {
            '$lookup': {
                'from': 'ProductInfo',
                'let': {
                    'productinfo': '$product.productinfo'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$_id', '$$productinfo'
                                ]
                            }
                        }
                    }, {
                        '$project': {
                            '_id': 0,
                            'company_sketch': '$company_sketch'
                        }
                    }
                ],
                'as': 'product_info'
            }
        }, {
            '$unwind': {
                'path': '$product_info'
            }
        }, {
            '$project': {
                'province': '$province',
                'city': '$city',
                'district': '$district',
                'zone': '$zone',
                'company_sum': {
                    '$multiply': [
                        '$product.product_price', '$product.product_amount'
                    ]
                },
                'receiver': '$transaction.receiver',
                'company_sketch': '$product_info.company_sketch'
            }
        }
    ]
    args = {
        'host': '10.15.101.252',
        'port': 27017,
        'filter': filter_interval,
        'record_db': 'analysis_pre'
    }
    opt = ShopDistance(args=args)
    opt.run()
