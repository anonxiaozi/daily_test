# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: conn_mongo.py

import pymongo
import ssl


class ConnectDB(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(
            host=self.args["host"],
            port=self.args["port"],
            compressors="zlib",
            ssl=True,
            authSource='$external',
            authMechanism="MONGODB-X509",
            ssl_cert_reqs=ssl.CERT_REQUIRED,
            ssl_ca_certs=args['ca_file'],
            ssl_certfile=args['cert_file'],
        )

    def query(self):
        cursor = self.conn['bloke']['first'].aggregate([{'$limit': 10}])
        for item in cursor:
            print(item)


if __name__ == '__main__':
    args = {
        'host': 'node01.bloke.com',
        'port': 27017,
        'username': 'bloke',
        'password': 'www.123',
        'ca_file': 'ca.pem',
        'cert_file': 'client.pem',
        'db': 'bloke'
    }
    conn = ConnectDB(args=args)
    conn.query()
