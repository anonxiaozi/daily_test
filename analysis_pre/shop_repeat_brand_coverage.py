# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: shop_avg_month

'''
  品牌复购覆盖率，店铺连续3个月购买的品牌 / 销售总品牌数量
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
        cursor = self.conn['core']['Transaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def parse_data(self):
        data = self.query()
        total_company_num = self.get_brand_num()

        get_data_time = datetime.now()
        get_data_cost = (get_data_time - self.start).seconds
        print('Get data cost: {} s'.format(get_data_cost))
        self.start = get_data_time

        result = {}
        final = []
        for item in data:
            date_list = [
                datetime(2018, 1, 1), datetime(2018, 4, 1), datetime(2018, 7, 1), datetime(2018, 10, 1),
                datetime(2019, 1, 1), datetime(2019, 4, 1), datetime(2019, 7, 1), datetime(2019, 10, 1)
            ]
            date_dict = {
                datetime(2018, 1, 1): [], datetime(2018, 4, 1): [], datetime(2018, 7, 1): [], datetime(2018, 10, 1): [],
                datetime(2019, 1, 1): [], datetime(2019, 4, 1): [], datetime(2019, 7, 1): [], datetime(2019, 10, 1): []
            }
            item_storeId, item_paiedTime, item_company_sketch = item['storeId'], item['paiedTime'], item['company_sketch']
            if item_storeId not in result:
                result[item_storeId] = date_dict
            for date in date_list:
                if item_paiedTime < date:       # 如果日期小于date，则认为是此date的前一个季节范围内的订单
                    idx = date_list.index(date)
                    if idx != 0:        # 如果等于0，说明是2018-01-01之前的订单，也放到2018-01-01开始的季节中
                        idx -= 1
                    result[item_storeId][date_list[idx]].append(item_company_sketch)
                    break
        else:
            for key, value in result.items():
                tmp_dict = dict()
                tmp_dict['_id'] = key
                for sub_key, sub_value in value.items():
                    tmp_dict[sub_key.strftime('%Y-%m')] = Decimal(len(sub_value) / total_company_num).quantize(Decimal('0.01')).__float__()
                final.append(tmp_dict)
        return final

    def handle(self):
        data = self.parse_data()
        parse_data_time = datetime.now()
        parse_data_cost = (parse_data_time - self.start).seconds
        print('Parse data cost: {} s'.format(parse_data_cost))
        for item in data:
            print(item)
        collection_name = 'SeasonBrandCoverage'
        collection_obj = self.record_db[collection_name]
        collection_obj.drop()
        self.do_write(collection_obj, data)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_repeat_brand = [
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
    ]
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'filter': filter_repeat_brand,
        'record_db': 'analysis_pre'
    }
    opt = GetMonthAvg(args=args)
    opt.run()
