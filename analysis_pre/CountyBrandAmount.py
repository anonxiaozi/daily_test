# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: CountyBrandAmount

'''
  温州每个县/区的乐事（薯片）、炫迈（口香糖）、奥利奥（饼干）的总购买金额
'''

from conn_mongo import ConnectDB


class CountyBrandAmount(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]

    def get_county_position(self):
        cursor = self.conn['test02']['county'].aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'citycode': 0
                    }
                }
            ], allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

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
        county_data = self.get_county_position()
        county_new_data = dict()
        new_data_dict, result = dict(), []
        choice_list = ['乐事', '炫迈', '奥利奥']
        for item in data:
            address = '{province}{city}{district}'.format(**item)
            company_sketch = item['company_sketch']
            if company_sketch not in choice_list:
                continue
            if address not in new_data_dict:
                new_data_dict[address] = {
                    'company': {company_sketch: item['sum']}, 'province': item['province'], 'city': item['city'],
                    'district': item['district']
                }
                continue
            if company_sketch not in new_data_dict[address]['company']:
                new_data_dict[address]['company'][company_sketch] = item['sum']
                continue
            new_data_dict[address]['company'][company_sketch] += item['sum']

        for county in county_data:
            county_new_data['{province}{city}{county}'.format(**county)] = {
                'Longitude': county['Longitude'], 'latitude': county['latitude']
            }
        for address, item in new_data_dict.items():
            posation = county_new_data.get(address, {'Longitude': 0.0, 'latitude': 0.0})
            tmp_result = {
                'province': item['province'], 'city': item['city'], 'district': item['district'],
                'longitude': float(posation['Longitude']), 'latitude': float(posation['latitude'])
            }
            company_info = item['company']
            for company in choice_list:
                value = company_info.get(company, 0.0)
                tmp_result[company] = value
            result.append(tmp_result)

        collection_name = 'StoreBrandByLocation'
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
        },
        {
            '$group': {
                '_id': '$storeId',
                'address': {
                    '$push': {
                        'province': '$province',
                        'city': '$city',
                        'district': '$district'
                    }
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'storeId': '$_id',
                'address': {
                    '$arrayElemAt': [
                        '$address', 0
                    ]
                }
            }
        }, {
            '$project': {
                'storeId': '$storeId',
                'province': '$address.province',
                'city': '$address.city',
                'district': '$address.district'
            }
        }, {
            '$lookup': {
                'from': 'Transaction',
                'let': {
                    'storeId': '$storeId'
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
                            '_id': 1
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
                'sum': {
                    '$multiply': [
                        '$product.product_price', '$product.product_amount'
                    ]
                },
                'company_sketch': '$product_info.company_sketch'
            }
        }
    ]
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'filter': filter_interval,
        'record_db': 'analysis_pre'
    }
    opt = CountyBrandAmount(args=args)
    opt.run()
