# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: ShopRepeatBrandCoverage

'''
  品牌复购覆盖率，店铺连续3个月购买的品牌 / 销售总品牌数量
'''

from conn_mongo import ConnectDB
from datetime import datetime
from decimal import Decimal


class ShopRepeatBrandCoverage(ConnectDB):

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
        result = {}
        final = []
        for item in data:
            item_storeId, item_paiedTime, item_company_sketch = item['storeId'], item['paiedTime'], item['company_sketch']
            if item_storeId not in result:
                result[item_storeId] = dict()
                result[item_storeId][item_company_sketch] = [item_paiedTime]
                continue
            if item_company_sketch not in result[item_storeId]:
                result[item_storeId][item_company_sketch] = [item_paiedTime]
                continue
            result[item_storeId][item_company_sketch].append(item_paiedTime)
        for store, company_info in result.items():
            tmp_result = dict()
            tmp_result[store] = 0.0
            for company, date_list in company_info.items():
                if len(date_list) < 3:  # 同品牌购买次数不足3次，不计算
                    continue
                size = self.compare_date(date_list)
                tmp_result[store] += size
            tmp_result[store] = Decimal("{}".format(tmp_result[store] / total_company_num)).quantize(Decimal("0.01")).__float__()
            final.append({"_id": store, "repeat_scale": tmp_result[store]})
        return final

    @staticmethod
    def compare_date(date_list):
        num, count = 0, 0
        date_list.sort()
        while date_list:
            if count == 3:
                num = 1
                break
            date_end = date_list.pop()
            if not date_list:
                break
            date = date_list[-1]
            month_num = (date_end.year - date.year) * 12 + (date_end.month - date.month)
            if month_num == 0:
                continue
            if month_num == 1:
                count += 1
                continue
        return num

    def handle(self):
        data = self.parse_data()
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
    opt = ShopRepeatBrandCoverage(args=args)
    opt.run()
