# -*- coding: utf-8 -*-
# @Time: 2019/5/14
# @File: BrandShopDay

from conn_mongo import ConnectDB


class BrandShopDay(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]

    def query(self):
        cursor = self.conn['core']['ProductInfo'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def handle(self):
        data = self.query()
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
                            'store': '$store'
                        }
                    }
                ],
                'as': 'transaction'
            }},
            {'$unwind': {
                'path': '$transaction'
            }},
            {'$lookup': {
                'from': 'StoreTransaction',
                'let': {
                    'store': '$transaction.store'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$storeId', '$$store'
                                ]
                            }
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'fulladdress': '$fulladdress'
                        }
                    }
                ],
                'as': 'storetransaction'
            }},
            {'$unwind': {
                'path': '$storetransaction'
            }},
            {'$project': {
                'storeid': '$transaction.store',
                'title': '$title',
                'amount': '$producttransaction.amount',
                'sum': '$producttransaction.sum',
                'fulladdress': '$storetransaction.fulladdress'
            }},
            {'$group': {
                '_id': '$storeid',
                'sum': {'$sum': '$sum'},
                'amount': {'$sum': '$amount'},
                'data': {'$push': {
                    'title': '$title',
                    'fulladdress': '$fulladdress'
                }},
            }},
            {'$project': {
                '_id': 0,
                'storeid': '$_id',
                'sum': '$sum',
                'amount': '$amount',
                'data': {'$arrayElemAt': ['$data', 0]},
            }},
            {'$project': {
                'storeid': '$storeid',
                'sum': '$sum',
                'amount': '$amount',
                'fulladdress': '$data.fulladdress',
                'title': '$data.title',
                }
            }
        ]
    args = {
        'host': '10.15.101.252',
        'port': 27017,
        'filter': filter_lvjian,
        'record_db': 'analysis_pre'
    }

    opt = BrandShopDay(args=args)
    opt.run()
