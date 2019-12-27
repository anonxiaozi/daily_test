#!/usr/bin/env python
# coding=utf-8
# Author: bloke

import socket


class CLIENT(object):

    def __init__(self, server_addr):
        self.server_addr = server_addr
        self.recv_size = 1024
        self.connect()

    def connect(self):
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect(self.server_addr)
        # print('Has connected to [%s:%s]' % self.server_addr)

    def operate(self, data):
        self.client.sendall(bytes(data, 'utf-8'))
        recv_data = self.client.recv(self.recv_size)
        # print('RECV: %s' % recv_data.decode('utf-8'))
        return recv_data.decode('utf-8')

    def __exit__(self):
        self.client.shutdown(socket.SHUT_RDWR)
        self.client.close()


def client_run(command, server_addr=('localhost', 10241)):
    client = CLIENT(server_addr)
    return client.operate(command)


if __name__ == '__main__':
    ip = client_run('get_ip')
    print(ip)

    # client_run('expired_ip 192.168.100.1:8888')


