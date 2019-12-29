# -*- coding: utf-8 -*-
# @Time: 2019/10/31
# @File: nginx_request


import re
import os
import sys
import traceback


class GetRequest(object):

    log_match = re.compile(r'(?P<ip>\d{1,4}\.\d{1,4}\.\d{1,4}\.\d{1,4})\s\-\s\-\s\[(?P<time>.*?)\]\s"(?P<method>\S+?)\s(?P<uri>\S+?)\sHTTP\/1\.1"\s(?P<status>\d+?)\s(?P<length>\d+?)\s"(?P<referer>\S+?)"\s"(?P<user_agent>.+?)"')

    def __init__(self, app):
        self.app = app
        self.data_dict = {'status': {'success': 0, 'failed': 0}, 'code': {}, 'pv': 0}
        self.total_count = 0
        self.seek_dir = os.path.join(os.path.dirname(__file__), 'seeks')
        self.log_dir = '/var/log/nginx'
        self.log_file = os.path.join(self.log_dir, '{}_access.log'.format(app))
        self.count_file = os.path.join(self.seek_dir, '{}.count'.format(app))
        self.seek = self.get_count()

    def get_result(self):
        with open(self.log_file) as f:
            f.seek(self.seek)
            for line in f.readlines():
                data = self.log_match.match(line)
                if data:
                    data_dict = data.groupdict()
                    if 'status' in data_dict:
                        code = data_dict['status']
                        status = self.check_code(code)
                        if code in self.data_dict['code']:
                            self.data_dict['code'][code] += 1
                        else:
                            self.data_dict['code'][code] = 1
                        self.data_dict['status'][status] += 1
                    else:
                        self.data_dict['failed'] += 1
                self.data_dict['pv'] += 1
                self.total_count += 1
            self.set_seek(f.tell())

    def check_seek(self):
        size = os.path.getsize(self.log_file)
        if size < self.seek:
            self.seek = 0

    def check_file_exists(self):
        if not os.path.exists(self.seek_dir):
            os.mkdir(self.seek_dir)
        if not os.path.exists(self.log_file):
            raise IOError('file {} don\'t exists.'.format(self.log_file))

    @staticmethod
    def check_code(code):
        success_code = ['200', '304']
        # failed_code = ['404', '500', '503']
        if code in success_code:
            return 'success'
        else:
            return 'failed'

    def get_count(self):
        if os.path.exists(self.count_file):
            with open(self.count_file, 'r') as f:
                seek = f.read()
                if seek:
                    return int(seek)
        else:
            return 0

    def set_seek(self, seek):
        with open(self.count_file, 'w') as f:
            f.write(str(seek))

    def run(self):
        self.check_seek()
        self.check_file_exists()
        self.get_result()


if __name__ == '__main__':
    app, key = sys.argv[1], sys.argv[2]
    match = GetRequest(app)
    try:
        match.run()
        print(match.data_dict[key])
    except Exception as e:
        print(traceback.format_exc())
