# -*- coding: utf-8 -*-
# @Time: 2019/4/16
# @File: create_rank

'''
  通过区域名称划分排序，每个区域中单品数量排序，每个区域中品牌总金额排序，(reverse=True)
'''

import pymongo
import argparse
import sys
import copy


class OptMongodb(object):
    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        self.db = self.conn[self.args["db"]]

    def check_index(self):
        transaction_idx = self.db['Transaction_hash'].index_information()
        if 'createTime_idx' not in transaction_idx:
            self.db['Transaction_hash'].create_index('createTime', name='createTime_idx')
        if 'store_idx' not in transaction_idx:
            self.db['Transaction_hash'].create_index('store', name='store_idx')

        productTransaction_idx = self.db['ProductTransaction_hash'].index_information()
        if 'productInfo_idx' not in productTransaction_idx:
            self.db['ProductTransaction_hash'].create_index('productInfo', name='productInfo_idx')
        if 'transaction_idx' not in productTransaction_idx:
            self.db['ProductTransaction_hash'].create_index('transaction', name='transaction_idx')

    def get_transaction_data(self):
        self.check_index()
        cursor = self.db[self.args["collection"]].aggregate([
            # { '$limit': 1 },
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
                    'title': '$product_info.title',
                    'company_sketch': '$product_info.company_sketch',
                    'product_sum': {'$multiply': ['$product.price', '$product.amount']}
                }
            }
        ], allowDiskUse=True)
        data = [x for x in cursor]
        cursor.close()
        return data

    def handle_data(self):
        data = self.get_transaction_data()
        data_dict = {}
        national_data = {'_id': '全国', 'title_info': {}, 'company_info': {}}
        for item in data:
            title = item['title']
            company = item['company_sketch']
            product_sum = item['product_sum']
            province = item['province']
            city = province + ' ' + item['city']
            district = city + ' ' + item['district']
            zone = district + ' ' + item['zone']
            national_data = self.change_national_data(national_data, title, company, product_sum)
            for addr in [province, city, district, zone]:
                data = data_dict.get(addr)
                if not data:
                    data_dict[addr] = {
                        'title_info': {title: {'_id': addr, 'sum': product_sum, 'num': 1, 'company': company}},
                        'company_info': {company: {'_id': addr, 'sum': product_sum, 'num': 1}}
                    }
                    continue
                data_dict[addr] = self.change_data(data, title, company, product_sum, addr)
        return data_dict, national_data

    def change_data(self, address_dict, title, company, product_sum, address):
        if title in address_dict['title_info']:
            address_dict['title_info'][title]['sum'] += product_sum
            address_dict['title_info'][title]['num'] += 1
        else:
            address_dict['title_info'][title] = {'_id': address, 'sum': product_sum, 'num': 1, 'company': company}
        if company in address_dict['company_info']:
            address_dict['company_info'][company]['sum'] += product_sum
            address_dict['company_info'][company]['num'] += 1
        else:
            address_dict['company_info'][company] = {'_id': address, 'sum': product_sum, 'num': 1}
        return address_dict

    def change_national_data(self, national_data, title, company, product_sum):
        if title in national_data['title_info']:
            national_data['title_info'][title]['sum'] += product_sum
            national_data['title_info'][title]['num'] += 1
        else:
            national_data['title_info'][title] = {'_id': '全国', 'sum': product_sum, 'num': 1, 'company': company}
        if company in national_data['company_info']:
            national_data['company_info'][company]['sum'] += product_sum
            national_data['company_info'][company]['num'] += 1
        else:
            national_data['company_info'][company] = {'_id': '全国', 'sum': product_sum, 'num': 1}
        return national_data

    def insert_data(self):
        result, national_data = self.handle_data()
        data = []
        product_data = []  # 存放所有的title数据
        title_full = []
        for addr, addr_info in result.items():
            sub_data = {}
            sub_product_data = []
            sub_data['_id'] = addr
            sub_data['company_info'] = []
            sub_data['title_info'] = []
            for key, value in addr_info['title_info'].items():
                t_data = {'title_name': key, 'title_sum': value['sum'], 'title_num': value['num'], 'company': value['company']}
                sub_data['title_info'].append(t_data)
                title_full.append(t_data)
            for key, value in addr_info['company_info'].items():
                c_data = {'company_name': key, 'company_sum': value['sum'], 'company_num': value['num'], 'avg_sum': (value['sum'] / value['num'])}
                sub_data['company_info'].append(c_data)
            title_list = sub_data['title_info']
            company_list = sub_data['company_info']
            title_result = sorted(title_list, key=lambda x: x['title_sum'], reverse=True)
            sub_data['title_info'] = copy.deepcopy(title_result[:10])
            for n, item in enumerate(title_result):
                item['address'] = addr
                item['rank'] = n
                item['avg_sum'] = item['title_sum'] / item['title_num']
                sub_product_data.append(item)
            company_result = sorted(company_list, key=lambda x: x['company_sum'], reverse=True)
            sub_data['company_info'] = company_result
            data.append(sub_data)
            product_data.extend(sub_product_data)

        national_data_b_info = national_data['title_info']
        national_data_b_company = national_data['company_info']
        national_data_title_info, national_data_company_info = [], []
        for title, title_info in national_data_b_info.items():
            national_data_title_info.append({'title_name': title, 'title_sum': title_info['sum'], 'title_num': title_info['num']})
        national_data_title_info = sorted(national_data_title_info, key=lambda x:x['title_num'], reverse=True)
        for company, company_info in national_data_b_company.items():
            national_data_company_info.append({'company_name': company, 'company_sum': company_info['sum'], 'company_num': company_info['num'], 'avg_sum': (company_info['sum'] / company_info['num'])})
        national_data_company_info = sorted(national_data_company_info, key=lambda x:x['company_sum'], reverse=True)
        national_data['title_info'] = copy.deepcopy(national_data_title_info[:10])
        national_data['company_info'] = national_data_company_info

        data.append(national_data)
        for n, item in enumerate(national_data_title_info):
            item['address'] = '全国'
            item['rank'] = n
            item['avg_sum'] = item['title_sum'] / item['title_num']
            product_data.append(item)
        rank_table_name = self.args["collection"] + '_rank'
        product_rank_table_name = self.args["collection"] + '_product_rank'
        self.do_write(rank_table_name, data)
        self.do_write(product_rank_table_name, product_data)
        self.db[product_rank_table_name].create_index('address', name='address_idx')
        self.db[product_rank_table_name].create_index('title_name', name='title_name_idx')

    def do_write(self, collection, data):
        """
        写入数据库，一次写入2k条
        """
        self.db[collection].drop()
        n = 0
        while n < len(data):
            data2w = data[n:n+20000]
            self.db[collection].insert_many(data2w)
            n += 20000


def get_args():
    """
    命令行参数
    """
    arg = argparse.ArgumentParser(prog="create_rank", usage='%(prog)s [options]')
    arg.add_argument("--host", type=str, help="DB host, default=%(default)s", default="10.15.101.63")
    arg.add_argument("--port", type=int, help="DB port, default=%(default)s", default=27027)
    arg.add_argument("--db", type=str, help="DB name, default=%(default)s", default="blockchain_test")
    arg.add_argument("-c", "--collection", type=str, help="read collection name", required=True)
    return arg


if __name__ == "__main__":
    try:
        args = vars(get_args().parse_args())
        opt = OptMongodb(args=args)
        opt.insert_data()
    except Exception as e:
        print(e)
        sys.exit(1)
