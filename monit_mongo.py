# -*- coding: utf-8 -*-
# @Time: 2019/11/7
# @File: monit_mongo

"""
    通过mongostat和serverStatus指令的结果来获取mongodb的状态，
    上次结果会记录到metrics文件中，当前状态记录在result文件中
"""

import pymongo
import re
import json
import time
import subprocess
import sys
import os


class Monitor(object):
    init_result = {
        'now': time.time(), 'insert': 0, 'query': 0, 'update': 0, 'delete': 0, 'getmore': 0, 'command': 0,
        'conn': 0, 'vsize': 0, 'netin': 0, 'netout': 0, 'total': 0, 'error': False
    }

    def __init__(self):
        self.host = '10.15.101.79'
        self.mongo_port = 27017
        self.stat_file = os.path.join(os.path.dirname(__file__), 'metrics')
        self.result_file = os.path.join(os.path.dirname(__file__), 'result')
        self.stat_cmd = '/usr/local/mongodb-4.0.4/bin/mongostat -n 1 --noheaders'
        self.current_result = {}
        self.remain_result = {}

    def get_current_data(self, field=None):
        try:
            with open(self.result_file, 'r') as f:
                result = json.load(f)
        except Exception:
            result = self.init_result
        finally:
            if not field:
                print(result)
            else:
                print(result[field])

    def get_remain_data(self):
        try:
            with open(self.stat_file, 'r') as f:
                self.pre_result = json.load(f)
        except FileNotFoundError:
            self.pre_result = self.init_result
        finally:
            print(self.pre_result)

    def set_data(self, arg=None):
        self.get_remain_data()
        self.get_mongostat()
        self.get_serverStatus()
        self.record_result()
        self.record_remain_result()

    def get_mongostat(self):
        result = subprocess.run(self.stat_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            self.current_result = self.pre_result
            self.current_result['error'] = True
            self.record_result()
        else:
            data = result.stdout.decode('utf-8')
            data = data.rstrip().replace('*', '')
            data = re.sub(r'\s+', ' ', data)
            data_list = data.split()
            insert, query, update, delete, getmore, command, flushes, mapped, vsize, res, faults, qrw, arw, net_in, net_out, conn = data_list[:16]
            self.current_result['now'] = time.time()
            self.current_result['conn'] = self.str_to_int(conn)
            self.current_result['vsize'] = self.str_to_bytes(vsize)
            self.current_result['netin'] = self.str_to_bytes(net_in)
            self.current_result['netout'] = self.str_to_bytes(net_out)

    def get_serverStatus(self):
        insert, query, update, delete, getmore, command = self.pre_result['insert'], self.pre_result['query'], self.pre_result['update'], \
                                                          self.pre_result['delete'], self.pre_result['getmore'], self.pre_result['command']
        mongo = pymongo.MongoClient(self.host, self.mongo_port, connectTimeoutMS=5000)
        status_data = mongo.admin.command('serverStatus')
        opcounters = status_data['opcounters']
        self.remain_result.update(self.current_result)
        self.remain_result.update(opcounters)
        now = time.time()
        duration = int((now - float(self.pre_result['now'])) / 60) or 1
        self.current_result['insert'] = int((float(opcounters['insert']) - float(insert)) / duration)
        self.current_result['update'] = int((float(opcounters['update']) - float(update)) / duration)
        self.current_result['delete'] = int((float(opcounters['delete']) - float(delete)) / duration)
        self.current_result['query'] = int((float(opcounters['query']) - float(query)) / duration)
        self.current_result['getmore'] = int((float(opcounters['getmore']) - float(getmore)) / duration)
        self.current_result['command'] = int((float(opcounters['command']) - float(command)) / duration)
        self.current_result['total'] = opcounters['insert'] + opcounters['update'] + opcounters['delete'] + opcounters['query'] + opcounters['getmore'] + \
                                       opcounters['command'] - int(insert) - int(update) - int(delete) - int(query) - int(getmore) - int(command)
        self.remain_result['total'] = opcounters['insert'] + opcounters['update'] + opcounters['delete'] + opcounters['query'] + opcounters['getmore'] + opcounters['command']

    def record_remain_result(self):
        with open(self.stat_file, 'w') as f:
            json.dump(self.remain_result, f)

    def record_result(self):
        with open(self.result_file, 'w') as f:
            json.dump(self.current_result, f)

    @staticmethod
    def str_to_int(s):
        m = re.match('(\d+)(\S?)', s)
        r = re.match('(\d+).(\d+)(\S?)', s)
        if r:
            m = r
        if m:
            i = int(m.group(1))
            if m.group(2) == 'k':
                i = i * 1000
            elif m.group(2) == 'm':
                i = i * 1000 * 1000
        else:
            try:
                i = int(s)
            except Exception:
                i = 0
        return i

    @staticmethod
    def str_to_bytes(s):
        m = re.match('(\d+)(\S?)', s)
        r = re.match('(\d+).(\d+)(\S?)', s)
        if r:
            m = r
        if m:
            i = int(m.group(1))
            if m.group(2) == 'k' or m.group(2) == 'K':
                i = i * 1024
            elif m.group(2) == 'm' or m.group(2) == 'M':
                i = i * 1024 * 1024
            elif m.group(2) == 'g' or m.group(2) == 'G':
                i = i * 1024 * 1024 * 1024
        else:
            try:
                i = int(s)
            except Exception:
                i = 0
        return i

    def run(self, func, arg=None):
        getattr(self, func)(arg)


if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print('Usage: {} get_current_data/set_data [field]'.format(__file__))
        sys.exit(1)
    func = args[0]
    field = None
    if len(args) > 1:
        field = args[1]
    monitor = Monitor()
    monitor.run(func, field)
    # monitor.run('get_current_data', 'total')
    # monitor.run('set_data')
