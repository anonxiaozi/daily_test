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
from datetime import timezone
from pymongo import errors


class OptMongodb(object):

    title_filter = [
        {
            '$match': {
                '$and': [
                    {
                        'date': {
                            '$gte': datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                            '$lt': datetime.datetime(2019, 7, 1, 0, 0, 0, tzinfo=timezone.utc)
                        }
                    }, {
                        'province': '安徽省'
                    }
                ]
            }
        }, {
        '$group': {
            '_id': {
                'province': '$province',
                'title': '$title'
            },
            'company_sketch': {
                '$push': '$company_sketch'
            },
            'province_title_num': {
                '$sum': 1
            },
            'province_title_sum': {
                '$sum': '$product_sum'
            }
        }
    }, {
        '$project': {
            '_id': 0,
            'address': '$_id.province',
            'title_name': '$_id.title',
            'company': {
                '$arrayElemAt': [
                    '$company_sketch', 0
                ]
            },
            'title_num': '$province_title_num',
            'title_sum': '$province_title_sum',
            # 'title_avg': {
            #     '$divide': [
            #         '$province_title_sum', '$province_title_num'
            #     ]
            # }
        }
    }, {
        '$group': {
            '_id': '$address',
            'title_info': {
                '$push': '$$ROOT'
            }
        }
        }
    ]
    company_filter = [
            {
                '$match': {
                    '$and': [
                        {
                            'date': {
                                '$gte': datetime.datetime(2019, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                                '$lt': datetime.datetime(2019, 7, 1, 0, 0, 0, tzinfo=timezone.utc)
                            }
                        }, {
                            'province': '安徽省'
                        }
                    ]
                }
            }, {
                '$group': {
                    '_id': {
                        'province': '$province',
                        'company': '$company_sketch'
                    },
                    'province_company_num': {
                        '$sum': 1
                    },
                    'province_company_sum': {
                        '$sum': '$product_sum'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'address': '$_id.province',
                    'company_name': '$_id.company',
                    'company_num': '$province_company_num',
                    'company_sum': '$province_company_sum',
                    'avg_sum': {
                        '$divide': [
                            '$province_company_sum', '$province_company_num'
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': '$address',
                    'company_info': {
                        '$push': '$$ROOT'
                    }
                }
            }
        ]

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"], unicode_decode_error_handler='ignore')
        self.source_db = self.conn[self.args["source_db"]]
        self.target_db = self.conn[self.args["target_db"]]
        self.process_db = self.conn[self.args['process_db']]
        self.process_tb = self.process_db[self.args['process_tb']]
        self.result = []
        self.start = datetime.datetime.now()

    # def check_index(self):
    #     transaction_idx = self.source_db['Transaction_hash'].index_information()
    #     if 'createTime_idx' not in transaction_idx:
    #         self.source_db['Transaction_hash'].create_index('createTime', name='createTime_idx')
    #     if 'store_idx' not in transaction_idx:
    #         self.source_db['Transaction_hash'].create_index('store', name='store_idx')
    #
    #     productTransaction_idx = self.source_db['ProductTransaction_hash'].index_information()
    #     if 'productInfo_idx' not in productTransaction_idx:
    #         self.source_db['ProductTransaction_hash'].create_index('productInfo', name='productInfo_idx')
    #     if 'transaction_idx' not in productTransaction_idx:
    #         self.source_db['ProductTransaction_hash'].create_index('transaction', name='transaction_idx')

    def get_transaction_data(self):
        # self.check_index()
        title_cursor = self.source_db[self.args["collection"]].aggregate(self.title_filter)
        company_cursor = self.source_db[self.args["collection"]].aggregate(self.company_filter)
        title_data = [x for x in title_cursor]
        company_data = [x for x in company_cursor]
        title_cursor.close()
        company_cursor.close()
        date = datetime.datetime.now()
        print('get date cost {} s [{}]'.format((date - self.start).seconds, date))
        self.start = date
        return title_data[0], company_data[0]

    def operator(self):

        address = '安徽省'
        # start_date, end_date = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2019-07-01', '%Y-%m-%d')
        self.title_filter[0]['$match'] = self.company_filter[0]['$match'] = {
            '$and': [

                # {
                #     'date': {
                #         '$gte': start_date,
                #         '$lt': end_date
                #     }
                # },
                {
                    'province': address
                },
            ]
        }
        # address = '全国'
        # start_date, end_date = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2019-07-01', '%Y-%m-%d')
        # self.title_filter[0]['$match'] = self.company_filter[0]['$match'] = {
        #     '$and': [
        #         {
        #             'date': {
        #                 '$gte': start_date,
        #                 '$lt': end_date
        #             }
        #         },
        #     ]
        # }
        # self.company_filter[1]['$group']['_id'] = self.title_filter[1]['$group']['_id'] = {'company': '$company_sketch'}
        # self.title_filter[2]['$project']['address'] = self.company_filter[2]['$project']['address'] = address

        # del(self.title_filter[0])
        # del (self.company_filter[0])
        # self.company_filter[0]['$group']['_id'] = {'company': '$company_sketch'}
        # self.title_filter[0]['$group']['_id'] = {'title': '$title'}
        # self.title_filter[1]['$project']['address'] = self.company_filter[1]['$project']['address'] = address

    def handle_data(self):
        self.operator()
        title_data, company_data = self.get_transaction_data()
        addr = title_data['_id']
        title_source = title_data['title_info']
        company_source = company_data['company_info']
        new_data = {addr: {'title_info': [], 'company_info': []}}
        new_data[addr]['title_info'] = sorted(title_source, key=lambda x: x['title_sum'], reverse=True)
        new_data[addr]['company_info'] = sorted(company_source, key=lambda x: x['company_sum'], reverse=True)
        return new_data

    def insert_data(self):
        data = self.handle_data()
        parse_time = datetime.datetime.now()
        print('parse data cost {} μs [{}]'.format((parse_time - self.start).microseconds, parse_time))
        self.start = parse_time
        result = []
        for key, value in data.items():
            result.append({'_id': key, 'title_info': value['title_info'][:10], 'company_info': value['company_info']})
        insert_time = datetime.datetime.now()
        print('insert data 2 cost {} μs'.format((insert_time - self.start).microseconds))

        # for key, value in data.items():
        #     print(key.center(50, "*"))
        #     print(value['_id'])
        #     for title in value['title_info']:
        #         print(title)
        #     for company in value['company_info']:
        #         print(company)

        rank_table_name = self.args["collection"] + '_rank'
        product_rank_table_name = self.args["collection"] + '_product_rank'
        company_rank_table_name = self.args["collection"] + '_company_rank'
        self.target_db[rank_table_name].drop()
        self.result.append('drop table {}/{}'.format(self.args['target_db'], rank_table_name))
        self.target_db[product_rank_table_name].drop()
        self.result.append('drop table {}/{}'.format(self.args['target_db'], product_rank_table_name))
        self.target_db[company_rank_table_name].drop()
        self.result.append('drop table {}/{}'.format(self.args['target_db'], company_rank_table_name))
        self.do_write(rank_table_name, result)
        # self.do_write(product_rank_table_name, product_data)
        # self.do_write(company_rank_table_name, company_data)
    #     self.target_db[product_rank_table_name].create_index('address', name='address_idx')
    #     self.result.append('Create address_idx index successfully for {}/{} table'.format(self.args['target_db'], product_rank_table_name))
    #     self.target_db[product_rank_table_name].create_index('title_name', name='title_name_idx')
    #     self.result.append('Create title_name_idx index successfully for {}/{} table'.format(self.args['target_db'], product_rank_table_name))
    #     self.target_db[company_rank_table_name].create_index('address', name='address_idx')
    #     self.result.append('Create address_idx index successfully for {}/{} table'.format(self.args['target_db'], company_rank_table_name))
    #     self.target_db[company_rank_table_name].create_index('company_name', name='company_name_idx')
    #     self.result.append('Create company_name_idx index successfully for {}/{} table'.format(self.args['target_db'], company_rank_table_name))

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
    # try:
    #     opt.process_tb.insert_one({'_id': py_name, 'status': 0, 'desc': '', 'updated_at': datetime.datetime.now()})
    # except pymongo.errors.DuplicateKeyError:
    #     opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 0, 'desc': '', 'updated_at': datetime.datetime.now()}})
    # try:
    opt.insert_data()
    # except Exception as e:
    #     opt.result.append('Error: {}'.format(e))
    #     opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 2, 'desc': ', '.join(opt.result), 'updated_at': datetime.datetime.now()}})
    #     sys.exit(2)
    # opt.process_tb.update_one({'_id': py_name}, {'$set': {'status': 1, 'desc': ', '.join(opt.result), 'updated_at': datetime.datetime.now()}})
