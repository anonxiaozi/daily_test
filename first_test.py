# -*- coding: utf-8 -*-
# @Time: 2019/3/12
# @File: first_test

import pymongo
import bson


class OptMongodb(object):
    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        self.db = self.conn[self.args["db"]]
        # self.collection = self.db[self.args["collection"]]

    def get_transaction_data(self):
        cursor = self.db["cache_week_20190318"].aggregate([
            # {
            #     '$limit': 1
            # },
            {
                '$project': {
                    'province': '$province',
                    'city': '$city',
                    'district': '$district',
                    'zone': '$zone',
                    'storeId': '$storeId',
                    'sum': '$sum',
                    'count': '$count',
                    'avg_sum': '$avg_sum'
                }
            },
            {
                '$lookup': {
                    'from': 'Transaction_hash',
                    'localField': 'storeId',
                    'foreignField': 'store',
                    'as': 'transaction'
                }
            }, {
                '$unwind': {
                    'path': '$transaction'
                }
            }, {
                '$lookup': {
                    'from': 'ProductTransaction_hash',
                    'localField': 'transaction._id',
                    'foreignField': 'transaction',
                    'as': 'product'
                }
            }, {
                '$unwind': {
                    'path': '$product'
                }
            }, {
                '$lookup': {
                    'from': 'ProductInfo_hash',
                    'localField': 'product.productInfo',
                    'foreignField': '_id',
                    'as': 'product_info'
                }
            }, {
                '$unwind': {
                    'path': '$product_info'
                }
            }, {
                '$project': {
                    '_id': 0,
                    'province': '$province',
                    'city': '$city',
                    'district': '$district',
                    'zone': '$zone',
                    # 'transaction_sum': '$transaction.sum',
                    'title': '$product_info.title',
                    'company_sketch': '$product_info.company_sketch',
                    'product_sum': {'$multiply': ['$product.price', '$product.amount']}
                }
            },
            {
                '$facet': {
                    'province_title_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.title',
                                'title_num': {'$sum': 1},
                                'title_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'address': '$data0.province'
                            }
                        }, {
                            '$sort': {'title_num': -1}
                        }, {
                            '$limit': 10
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'title_num': '$title_num',
                                    'title_sum': '$title_sum'
                                }}
                            }
                        }
                    ],
                    'province_company_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.company_sketch',
                                'company_num': {'$sum': 1},
                                'company_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'address': '$data0.province'
                            }
                        }, {
                            '$sort': {'company_sum': -1}
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'company_num': '$company_num',
                                    'company_sum': '$company_sum'
                                }}
                            }
                        }
                    ],
                    'province_city_title_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province', 'city': '$city'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.title',
                                'title_num': {'$sum': 1},
                                'title_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'address': {'$concat': ['$data0.province', ' ', '$data0.city']}
                            }
                        }, {
                            '$sort': {'title_num': -1}
                        }, {
                            '$limit': 10
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'title_num': '$title_num',
                                    'title_sum': '$title_sum'
                                }}
                            }
                        }
                    ],
                    'province_city_company_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province', 'city': '$city'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.company_sketch',
                                'company_num': {'$sum': 1},
                                'company_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'address': {'$concat': ['$data0.province', ' ', '$data0.city']}
                            }
                        }, {
                            '$sort': {'company_sum': -1}
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'company_num': '$company_num',
                                    'company_sum': '$company_sum'
                                }}
                            }
                        }
                    ],
                    'province_city_district_title_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province', 'city': '$city', 'district': '$district'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.title',
                                'title_num': {'$sum': 1},
                                'title_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'address': {'$concat': ['$data0.province', ' ', '$data0.city', ' ', '$data0.district']}
                            }
                        }, {
                            '$sort': {'title_num': -1}
                        }, {
                            '$limit': 10
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'title_num': '$title_num',
                                    'title_sum': '$title_sum'
                                }}
                            }
                        }
                    ],
                    'province_city_district_company_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province', 'city': '$city', 'district': '$district'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.company_sketch',
                                'company_num': {'$sum': 1},
                                'company_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'address': {'$concat': ['$data0.province', ' ', '$data0.city', ' ', '$data0.district']}
                            }
                        }, {
                            '$sort': {'company_sum': -1}
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'company_num': '$company_num',
                                    'company_sum': '$company_sum'
                                }}
                            }
                        }
                    ],
                    'province_city_district_zone_title_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province', 'city': '$city', 'district': '$district', 'zone': '$zone'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.title',
                                'title_num': {'$sum': 1},
                                'title_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'title_num': '$title_num',
                                'title_sum': '$title_sum',
                                'data0': '$data0',
                                'address': {'$concat': ['$data0.province', ' ', '$data0.city', ' ', '$data0.district', ' ', '$data0.zone']}
                            }
                        },
                        {
                            '$sort': {'title_num': -1}
                        }, {
                            '$limit': 10
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'title_num': '$title_num',
                                    'title_sum': '$title_sum'
                                }}
                            }
                        }
                    ],
                    'province_city_district_zone_company_info': [
                        {
                            '$group': {
                                '_id': {'province': '$province', 'city': '$city', 'district': '$district', 'zone': '$zone'},
                                'data': {'$push': '$$ROOT'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'data': '$data'
                            }
                        }, {
                            '$unwind': {
                                'path': '$data'
                            }
                        }, {
                            '$group': {
                                '_id': '$data.company_sketch',
                                'company_num': {'$sum': 1},
                                'company_sum': {'$sum': '$data.product_sum'},
                                'data': {'$push': '$_id'}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'data0': {'$arrayElemAt': ['$data', 0]}
                            }
                        }, {
                            '$project': {
                                '_id': '$_id',
                                'company_num': '$company_num',
                                'company_sum': '$company_sum',
                                'address': {'$concat': ['$data0.province', ' ', '$data0.city', ' ', '$data0.district', ' ', '$data0.zone']}
                            }
                        }, {
                            '$sort': {'company_sum': -1}
                        }, {
                            '$group': {
                                '_id': '$address',
                                'data': {'$push': {
                                    'title': '$_id',
                                    'company_num': '$company_num',
                                    'company_sum': '$company_sum'
                                }}
                            }
                        }
                    ]
                }
            },
            {
                '$out': 'test01'
            }
        ], {'allowUseDisk': True})
        data = [x for x in cursor]
        cursor.close()
        return data

    def handle_data(self):
        data = self.get_transaction_data()
        data_dict = {}


if __name__ == "__main__":
    args = {
        "host": "10.15.101.63",
        "port": 27027,
        "db": "test",
        "collection": "cache_week_20190318"
    }
    opt = OptMongodb(args=args)
    data = opt.get_transaction_data()
    for key, value in data[0].items():
        print(key)
        for item in value:
            print(item)
