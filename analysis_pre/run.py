# -*- coding: utf-8 -*-
# @Time: 2019/5/10
# @File: run.py

import importlib
import traceback
import logging
import argparse
import os


class Run(object):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        self.filter_dict = args['filter']
        self.func_list = [x.split('_filter')[0] for x in self.filter_dict.keys()]
        self.logger = self.record_log()

    def record_log(self):
        logger = logging.Logger('Analysis')
        stream = logging.StreamHandler()
        stream.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] %(message)s', datefmt='%y/%m/%d %H:%M:%S')
        stream.setFormatter(formatter)
        logger.addHandler(stream)
        return logger

    def import_func(self):
        for func in self.func_list:
            try:
                self.logger.info('Start exec {} .'.format(func))
                module = importlib.import_module(func)
                args = self.args
                args['filter'] = self.filter_dict['{}_filter'.format(func)]
                opt = getattr(module, func)(args=args)
                getattr(opt, 'run')()
            except Exception:
                error = traceback.format_exc()
                self.logger.error('Exec {} Error: {}'.format(func, error))
            else:
                self.logger.info('Exec {} successfully.'.format(func))

    def run(self):
        self.import_func()


def get_args():
    """
    命令行参数
    """
    arg = argparse.ArgumentParser(prog=os.path.basename(__file__), usage='%(prog)s filter [options]')
    arg.add_argument("-H", "--host", type=str, help="DB host, default: %(default)s", default="10.15.101.252")
    arg.add_argument("-p", "--port", type=int, help="DB port, default: %(default)s", default=27017)
    arg.add_argument("-d", "--record_db", type=str, help="DB name, default: %(default)s", default="analysis_pre")
    return arg


if __name__ == '__main__':
    filter_dict = {
        'ShopMonthAvg_filter': [
        {
            '$group': {
                '_id': '$storeId',
                'num': {
                    '$sum': "$sum"
                },
                'date': {
                    '$push': '$date'
                }
            }
        }
    ],
        'ShopRepeatBrandCoverage_filter': [
        {
            '$lookup': {
                'from': 'ProductTransaction',
                'let': {
                    'transaction_id': '$_id'
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
                'storeId': '$store',
                'paiedTime': '$paiedTime',
                'company_sketch': '$product_info.company_sketch'
            }
        }
    ],
        'ShopRepeatScale_filter': [
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
    ],
        'ShopDistance_filter': [
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
                            'district': '$district'
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
                    'district': '$address.district'
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
                    'company_sum': {
                        '$multiply': [
                            '$product.product_price', '$product.product_amount'
                        ]
                    },
                    'receiver': '$transaction.receiver',
                    'company_sketch': '$product_info.company_sketch'
                }
            }
        ],
        'ShopIntervalDays_filter': [
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
        ],
        'ShopAmountStability_filter': [
            {
                '$group': {
                    '_id': '$storeId',
                    'amount': {
                        '$push': '$sum'
                    }
                }
            }
        ],
        'ShopBrandCoverage_filter': [
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
        ],
        'CountyAmount_filter': [
            {
                '$match': {
                    'province': '浙江省',
                    'city': '温州市'
                }
            }, {
                '$group': {
                    '_id': {
                        'province': '$province',
                        'city': '$city',
                        'district': '$district'
                    },
                    'sum': {
                        '$sum': '$sum'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'province': '$_id.province',
                    'city': '$_id.city',
                    'district': '$_id.district',
                    'sum': '$sum'
                }
            }
        ],
        'CountyBrandAmount_filter': [
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
    }
    args = vars(get_args().parse_args())
    args['filter'] = filter_dict
    opt=Run(args=args)
    opt.run()

