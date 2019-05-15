# -*- coding: utf-8 -*-
# @Time: 2019/5/14
# @File: ShopBrandLvJian

from conn_mongo import ConnectDB


class ShopBrandLvJian(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]

    def query(self):
        cursor = self.conn['core']['ProductInfo'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data_dict = dict()
        for item in cursor:
            data_dict[item['transaction']] = item
        cursor.close()
        return data_dict

    def get_transaction(self):
        cursor = self.conn['core']['ProductTransaction'].aggregate(
            [
                {
                    '$project': {
                        '_id': '$transaction',
                        'sum': {
                            '$multiply': [
                                '$price', '$amount'
                            ]
                        }
                    }
                }, {
                '$group': {
                    '_id': '$_id',
                    'sum': {
                        '$sum': '$sum'
                    }
                }
            }
            ], allowDiskUse=True
        )
        data_dict = dict()
        for item in cursor:
            data_dict[item['_id']] = item['sum']
        cursor.close()
        return data_dict

    def get_store(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            [
                {
                    '$group': {
                        '_id': '$storeId',
                        'fulladdress': {
                            '$push': '$fulladdress'
                        }
                    }
                }, {
                '$project': {
                    'fulladdress': {
                        '$arrayElemAt': [
                            '$fulladdress', 0
                        ]
                    }
                }
            }
            ], allowDiskUse=True
        )
        data_dict = dict()
        for item in cursor:
            data_dict[item['_id']] = item['fulladdress']
        cursor.close()
        return data_dict

    def handle(self):
        before_data = self.query()
        transaction_data = self.get_transaction()
        store_data = self.get_store()
        transaction_result = dict()
        for trans, value in before_data.items():
            sum = transaction_data.get(trans, 0.0)
            store = value['store']
            scale = sum / value['after_sum']
            value['sum'] = value['after_sum'] / scale
            for i in ['after_sum', 'before_sum', 'transaction']:
                del(value[i])
            if store not in transaction_result:
                transaction_result[store] = {
                    'storeid': store, 'sum': value['sum'], 'amount': value['amount'],
                    'fulladdress': store_data.get(store, ''), 'title': value['title']
                }
            else:
                transaction_result[store]['sum'] += value['sum']
                transaction_result[store]['amount'] += value['amount']
        data = list(transaction_result.values())

        collection_name = 'BrandLvJian'
        self.record_db[collection_name].drop()
        self.record_db[collection_name].insert_many(data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_lvjian = [
        {'$match': {
            'title': { '$regex': '绿箭' }
        }},
        {'$project': {
            'title': '$title'
        }},
        {'$lookup': {
            'from': 'ProductTransaction',
            'let': {
                'productid': '$_id'
            },
            'pipeline': [
                {
                '$match': {
                    '$expr': {
                        '$eq': [
                            '$productInfo', '$$productid'
                        ]
                    }
                }},
                {
                '$project': {
                    'transaction': '$transaction',
                    'amount': '$amount',
                    'sum': { '$multiply': ['$price', '$amount'] }
                }}
            ],
                'as': 'producttransaction'
            }},
            {'$unwind': {
                'path': '$producttransaction'
            }},
            {'$lookup': {
                'from': 'Transaction',
                'let': {
                    'transactionid': '$producttransaction.transaction'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$_id', '$$transactionid'
                                ]
                            }
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'paiedTime': '$paiedTime',
                            'store': '$store',
                            'sum': '$sum'
                        }
                    }
                ],
                'as': 'transaction'
            }},
            {'$unwind': {
                'path': '$transaction'
            }},
            {'$project': {
                'store': '$transaction.store',
                'paiedTime': '$transaction.paiedTime',
                'amount': '$producttransaction.amount',
                'before_sum': '$producttransaction.sum',
                'after_sum': '$transaction.sum',
                'title': '$title',
                'transaction': '$producttransaction.transaction',
                '_id': 0
            }}
        ]
    args = {
        'host': '10.15.101.252',
        'port': 27017,
        'filter': filter_lvjian,
        'record_db': 'analysis_pre'
    }

    opt = ShopBrandLvJian(args=args)
    opt.run()
