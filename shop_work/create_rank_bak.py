# -*- coding: utf-8 -*-
# @Time: 2019/4/16
# @File: create_rank

'''
  通过区域名称划分排序，每个区域中单品数量排序，每个区域中品牌总金额排序，(reverse=True)
'''

import pymongo
import argparse
import sys
import os
import copy
import datetime
from pymongo import errors


class OptMongodb(object):
    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        self.source_db = self.conn[self.args["source_db"]]
        self.target_db = self.conn[self.args["target_db"]]
        self.process_db = self.conn[self.args['process_db']]
        self.process_tb = self.process_db[self.args['process_tb']]
        self.result = []

    def check_index(self):
        transaction_idx = self.source_db['Transaction_hash'].index_information()
        if 'createTime_idx' not in transaction_idx:
            self.source_db['Transaction_hash'].create_index('createTime', name='createTime_idx')
        if 'store_idx' not in transaction_idx:
            self.source_db['Transaction_hash'].create_index('store', name='store_idx')

        productTransaction_idx = self.source_db['ProductTransaction_hash'].index_information()
        if 'productInfo_idx' not in productTransaction_idx:
            self.source_db['ProductTransaction_hash'].create_index('productInfo', name='productInfo_idx')
        if 'transaction_idx' not in productTransaction_idx:
            self.source_db['ProductTransaction_hash'].create_index('transaction', name='transaction_idx')

    def get_transaction_data(self):
        self.check_index()
        cursor = self.source_db[self.args["collection"]].aggregate([
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

    @staticmethod
    def change_data(address_dict, title, company, product_sum, address):
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

    @staticmethod
    def change_national_data(national_data, title, company, product_sum):
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
        company_data = []   # 存放所有的company数据
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
            d = copy.deepcopy(title_result[:10])
            sub_data['title_info'] = sorted(d, key=lambda x: x['title_sum'], reverse=True)
            for n, item in enumerate(title_result):
                item['address'] = addr
                item['rank'] = n
                item['avg_sum'] = item['title_sum'] / item['title_num']
                sub_product_data.append(item)
            for item in company_list:
                item['address'] = addr
            company_result = sorted(company_list, key=lambda x: x['company_sum'], reverse=True)
            sub_data['company_info'] = company_result
            data.append(sub_data)
            company_data.extend(company_result)
            product_data.extend(sub_product_data)

        national_data_b_info = national_data['title_info']
        national_data_b_company = national_data['company_info']
        national_data_title_info, national_data_company_info = [], []
        for title, title_info in national_data_b_info.items():
            national_data_title_info.append({'title_name': title, 'title_sum': title_info['sum'], 'title_num': title_info['num'], 'company': title_info['company']})
        national_data_title_info = sorted(national_data_title_info, key=lambda x:x['title_sum'], reverse=True)
        for company, company_info in national_data_b_company.items():
            national_data_company_info.append({'company_name': company, 'company_sum': company_info['sum'], 'company_num': company_info['num'], 'avg_sum': (company_info['sum'] / company_info['num']), 'address': '全国'})
        national_data_company_info = sorted(national_data_company_info, key=lambda x:x['company_sum'], reverse=True)
        national_data['title_info'] = sorted(copy.deepcopy(national_data_title_info[:10]), key=lambda x:x['title_sum'], reverse=True)
        national_data['company_info'] = national_data_company_info

        company_data.extend(national_data_company_info)
        data.append(national_data)
        for n, item in enumerate(national_data_title_info):
            item['address'] = '全国'
            item['rank'] = n
            item['avg_sum'] = item['title_sum'] / item['title_num']
            product_data.append(item)
        rank_table_name = self.args["collection"] + '_rank'
        product_rank_table_name = self.args["collection"] + '_product_rank'
        company_rank_table_name = self.args["collection"] + '_company_rank'
        self.target_db[rank_table_name].drop()
        self.result.append('drop table {}/{}'.format(self.args['target_db'], rank_table_name))
        self.target_db[product_rank_table_name].drop()
        self.result.append('drop table {}/{}'.format(self.args['target_db'], product_rank_table_name))
        self.target_db[company_rank_table_name].drop()
        self.result.append('drop table {}/{}'.format(self.args['target_db'], company_rank_table_name))
        self.do_write(rank_table_name, data)
        self.do_write(product_rank_table_name, product_data)
        self.do_write(company_rank_table_name, company_data)
        self.target_db[product_rank_table_name].create_index('address', name='address_idx')
        self.result.append('Create address_idx index successfully for {}/{} table'.format(self.args['target_db'], product_rank_table_name))
        self.target_db[product_rank_table_name].create_index('title_name', name='title_name_idx')
        self.result.append('Create title_name_idx index successfully for {}/{} table'.format(self.args['target_db'], product_rank_table_name))
        self.target_db[company_rank_table_name].create_index('address', name='address_idx')
        self.result.append('Create address_idx index successfully for {}/{} table'.format(self.args['target_db'], company_rank_table_name))
        self.target_db[company_rank_table_name].create_index('company_name', name='company_name_idx')
        self.result.append('Create company_name_idx index successfully for {}/{} table'.format(self.args['target_db'], company_rank_table_name))

    def do_write(self, collection, data):
        """
        写入数据库，一次写入2w条
        """
        n = 0
        while n < len(data):
            data2w = data[n:n+20000]
            self.target_db[collection].insert_many(data2w)
            n += 20000
        self.result.append('write table {}/{} completed.'.format(self.args['target_db'], collection))


def get_args():
    """
    命令行参数
    """
    arg = argparse.ArgumentParser(prog=os.path.basename(__file__), usage='%(prog)s filter [options]')
    arg.add_argument("-H", "--host", type=str, help="DB host, default=%(default)s", default="10.15.101.63")
    arg.add_argument("-p", "--port", type=int, help="DB port, default=%(default)s", default=27027)
    arg.add_argument("-s", "--source_db", type=str, help="DB name, default=%(default)s", default="test")
    arg.add_argument("-t", "--target_db", type=str, help="DB name, default=%(default)s", default="test01")
    arg.add_argument("-c", "--collection", type=str, help="read collection name", required=True)
    # arg.add_argument("-d", "--dateRange", type=str, help="Date Range, such as: \"20181112-20190410\"", required=True)
    arg.add_argument("--process_db", type=str, help="Execution status record database, default: %(default)s", default="process")
    arg.add_argument("--process_tb", type=str, help="Execution status record collection, default: %(default)s", default="ProcessStatus")
    return arg


if __name__ == "__main__":
    args = vars(get_args().parse_args())
    opt = OptMongodb(args=args)
    py_name = os.path.basename(__file__)
    try:
        opt.process_tb.insert_one({'_id': py_name, 'status': 0, 'desc': '', 'updated_at': datetime.datetime.now()})
    except pymongo.errors.DuplicateKeyError:
        opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 0, 'desc': '', 'updated_at': datetime.datetime.now()}})
    try:
        opt.insert_data()
    except Exception as e:
        opt.result.append('Error: {}'.format(e))
        opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 2, 'desc': ', '.join(opt.result), 'updated_at': datetime.datetime.now()}})
        sys.exit(2)
    opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 1, 'desc': ', '.join(opt.result), 'updated_at': datetime.datetime.now()}})
