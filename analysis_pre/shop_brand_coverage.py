# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  品牌覆盖率，店铺购买的品牌，占总品牌数的百分比，结果是浮点数
'''

from conn_mongo import ConnectDB
from datetime import datetime
from decimal import Decimal


class GetMonthAvg(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]
        self.start = datetime.now()

    def get_brand_num(self):
        cursor = self.conn['core']['ProductInfo'].aggregate(
            [
                {
                    '$group': {
                        '_id': '$company_sketch'
                    }
                }, {
                '$count': 'count'
            }
            ],
            allowDiskUse=True
        )
        data = cursor.__next__()
        cursor.close()
        return data['count']

    def query(self):
        total_brank_num = self.get_brand_num()
        filter_brand = self.args['filter']
        filter_brand[-1]['$project']['scale']['$divide'].append(total_brank_num)
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            filter_brand,
            allowDiskUse=True
        )
        data = [x for x in cursor]
        return data

    def handle(self):
        data = self.query()
        for item in data:
            item['scale'] = Decimal(item['scale']).quantize(Decimal('.01')).__float__()
        collection_name = 'BrandCoverage'
        collection_obj = self.record_db[collection_name]
        collection_obj.drop()
        self.do_write(collection_obj, data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_brand = [
        {
            '$group': {
                '_id': '$storeId'
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
                            'productinfo': '$productInfo'
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
                '_id': 0,
                'storeId': '$_id',
                'company_sketch': '$product_info.company_sketch'
            }
        }, {
            '$group': {
                '_id': '$storeId',
                'company_sketches': {
                    '$push': '$company_sketch'
                }
            }
        }, {
            '$project': {
                '_id': '$_id',
                'company_sketches': {
                    '$setUnion': '$company_sketches'
                }
            }
        }, {
            '$project': {
                '_id': '$_id',
                'scale': {
                    '$divide': [
                        {
                            '$size': '$company_sketches'
                        },
                    ]
                }
            }
        }
    ]
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'filter': filter_brand,
        'record_db': 'analysis_pre'
    }
    opt = GetMonthAvg(args=args)
    opt.run()
