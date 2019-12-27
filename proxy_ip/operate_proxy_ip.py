#!/usr/bin/env python
# coding=utf-8
# Author: bloke

from multiprocessing import Process, Manager
import socketserver
from datetime import datetime
import logging
import os
import requests


class RecordLog(object):

    """
    记录日志
    """

    def __init__(self, log_name=os.path.join(os.path.dirname(__file__), 'proxy_ip.log')):
        self.log_name = log_name
        self.logger = self.record_log()

    def record_log(self):
        logger = logging.Logger(self.log_name)
        fh = logging.FileHandler(self.log_name, 'a', 'utf-8')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] %(message)s', datefmt='%y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


class OperateIP(RecordLog):

    """
    处理代理IP的具体方法
    """

    def __init__(self, api_url, check_ip_url, get_ip_balance, username='bloke_anon@126.com', password='www.fct321.com'):
        super().__init__()
        self.api_url = api_url      # 获取代理IP的API链接
        self.check_ip_url = check_ip_url    # 检查代理IP是否有效的API链接
        self.get_ip_balance = get_ip_balance    # 获取当前订单中剩余代理IP数量的API链接
        self.username = username        # KDL用户名
        self.password = password        # KDL密码
        self.ip_file_name = os.path.join(os.path.dirname(__file__),
                                         'ip_list.txt_{}'.format(datetime.now().date().strftime('%Y-%m')))
        # 不可用代理IP的记录文件
        self.expired_file = open(os.path.join(os.path.dirname(__file__), 'expired_ip.txt'), 'a', encoding='utf-8')

    def get_ip_list(self):
        """
        从KDL获取代理IP地址，首先检查余额，如果余额为0，则返回'End...'表示代理IP地址已用完，
        否则将获取到的代理IP地址都放入队列中，
        由队列长度来阻塞获取代理IP的API调用频率
        """
        while True:
            try:
                balance_req = requests.get(self.get_ip_balance)
                if balance_req.status_code == 200:
                    balance = balance_req.json()['data']['balance']
                    self.logger.info('IP balance: {}'.format(balance))
                    if not int(balance) > 0:
                        queue.put('End...')
                        self.logger.info('End...')
                        break
                req = requests.get(self.api_url)
                if req.status_code == 200:
                    result = req.content.decode('utf-8')
                    if not result.startswith('ERROR'):
                        ip_list = result.split('\n')
                        self.logger.info('Get {} IP from KDL: {}'.format(len(ip_list), ','.join(ip_list)))
                        for ip in ip_list:
                            queue.put(ip.strip())
                            self.logger.info('Put {} to Queue'.format(ip))
            except Exception as e:
                self.logger.error('request KDL failed: {}'.format(str(e)))

    def get_ip(self):
        """
        针对之前批量下载的代理IP地址的保存文件
        """
        count = 0
        while True:
            self.logger.info('Current count: {}'.format(count))
            with open(self.ip_file_name, 'r', encoding='utf-8') as f:
                source_list = f.readlines()
            if len(source_list) > count:
                data = source_list[count: count + 5]
                for item in data:
                    queue.put(item.strip())
                    print(item.strip())
                count += 5
            else:
                queue.put('End...')
                self.logger.info('End...')
                break

    def check_error_ip(self):
        """
        检查error_ip_list的长度，达到长度上限时，发送列表中的IP地址到KDL检查是否真实可用，
        如果可用则重新加入队列，不可用则记录expired文件
        """
        while True:
            if len(error_ip_list) >= 50:
                try:
                    req = requests.get(self.check_ip_url + ','.join(error_ip_list))
                    if req.status_code == 200:
                        result = req.json()['data']
                        print('check result: {}'.format(result))
                        for ip in error_ip_list:
                            if result[ip]:
                                queue.put(ip)
                            else:
                                self.expired_file.write(ip + '\n')
                                self.logger.info('Expired IP: {}'.format(ip))
                            error_ip_list.remove(ip)
                except Exception as e:
                    self.logger.error('check error ip failed: {}'.format(str(e)))

    @staticmethod
    def send_ip():
        """
        从队列中获取IP地址，如果队列为空，则发送'End...'
        """
        if queue.empty():
            return 'End...'
        else:
            return queue.get()


class MYHandler(socketserver.BaseRequestHandler):

    """
    socketServer handler
    """

    recv_size = 1024

    def setup(self):
        logger = RecordLog()
        self.logger = logger.logger

    def handle(self):
        while True:
            try:
                recv_data = self.request.recv(self.recv_size)
            except ConnectionResetError as e:
                print('Client [%s: %s] is disconnected.' % self.client_address)
                break
            if not recv_data:
                break
            recv_data = recv_data.decode('utf-8').strip().split()
            self.logger.info('Receive: {}'.format(' '.join(recv_data)))
            getattr(self, recv_data[0], self.other)(recv_data)

    def get_ip(self, *args):
        """
        发送单个IP给客户端
        """
        ip = OperateIP.send_ip()
        self.request.sendall(bytes(ip, 'utf-8'))
        self.logger.info('Send IP [{}] to {}'.format(ip, self.client_address))

    def expired_ip(self, *args):
        """
        接受客户端发送的失效IP地址，首先进行反查，然后记录
        """
        arg = args[0]
        addr_info = ' '.join(arg[1:])
        self.logger.info('Receive error IP info from {}: {}'.format(self.client_address, addr_info))
        print(addr_info)
        error_ip_list.append(addr_info)
        self.request.sendall(bytes(addr_info, 'utf-8'))

    def finish(self):
        print('Client [%s: %s] is disconnected.' % self.client_address)

    def other(self, *args):
        self.request.sendall('error command: {}'.format(' '.join(args[0])))


def run_server(host, port, request_q_size=50):
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    server = socketserver.ThreadingTCPServer((host, port), MYHandler)
    server.request_queue_size = request_q_size
    print('start server on {}:{}'.format(host, port))
    with server:
        server.serve_forever()


if __name__ == '__main__':
    manager = Manager()
    queue = manager.Queue(5)
    error_ip_list = manager.list()
    process_list = []
    pull_ip_num = 50
    order_id = ''
    api_url = 'http://dps.kdlapi.com/api/getdps/?orderid={}&num={}&area=%E6%B2%B3%E5%8D%97%2C%E5%90%89%E6%9E%97%2C%E5%B1%B1%E8%A5%BF%2C%E5%B1%B1%E4%B8%9C%2C%E6%B9%96%E5%8C%97%2C%E5%86%85%E8%92%99%E5%8F%A4%2C%E5%AE%89%E5%BE%BD%2C%E7%94%98%E8%82%83%2C%E5%AE%81%E5%A4%8F%2C%E5%9B%9B%E5%B7%9D%2C%E5%B9%BF%E8%A5%BF&pt=1&dedup=1&sep=2'.format(
        order_id, pull_ip_num)
    signature = ''
    check_ip_expired_url = 'https://dps.kdlapi.com/api/checkdpsvalid?orderid={}&signature={}&proxy='.format(order_id,
                                                                                                            signature)
    get_ip_balance = 'https://dps.kdlapi.com/api/getipbalance?orderid={}&signature={}'.format(order_id, signature)
    operate_ip = OperateIP(api_url, check_ip_expired_url, get_ip_balance)
    get_ip_process = Process(target=operate_ip.get_ip_list)
    process_list.append(get_ip_process)
    check_ip_process = Process(target=operate_ip.check_error_ip)
    process_list.append(check_ip_process)
    host, port = 'localhost', 10241
    socket_process = Process(target=run_server, args=(host, port))
    process_list.append(socket_process)
    for process in process_list:
        process.start()
    for process in process_list:
        process.join()
