# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: ShopInfo


from conn_mongo import ConnectDB


class ShopInfo(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]

    def query(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def handle(self):
        result = self.query()
        collection_name = 'StoreInfo'
        self.record_db[collection_name].drop()
        self.record_db[collection_name].insert_many(result)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_interval = [
        {
            '$group': {
                '_id': '$storeId',
                'sum': {
                    '$sum': '$sum'
                },
                'address': {
                    '$push': {
                        'province': '$province',
                        'city': '$city',
                        'district': '$district',
                        'zone': '$zone',
                        'longitude': '$longitude',
                        'latitude': '$latitude'
                    }
                }
            }
        }, {
            '$project': {
                'sum': '$sum',
                'address': {
                    '$arrayElemAt': [
                        '$address', 0
                    ]
                }
            }
        }, {
            '$project': {
                'sum': '$sum',
                'province': '$address.province',
                'city': '$address.city',
                'district': '$address.district',
                'zone': '$address.zone',
                'longitude': '$address.longitude',
                'latitude': '$address.latitude'
            }
        }
    ]
    args = {
        'host': '10.15.101.252',
        'port': 27017,
        'filter': filter_interval,
        'record_db': 'analysis_pre'
    }
    opt = ShopInfo(args=args)
    opt.run()
