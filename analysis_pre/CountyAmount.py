# -*- coding: utf-8 -*-
# @Time: 2019/5/5
# @File: CountyAmount

'''
  温州每个县/区的总购买金额
'''

from conn_mongo import ConnectDB


class CountyAmount(ConnectDB):

    def __init__(self, **kwargs):
        self.args = kwargs['args']
        super().__init__(args=self.args)
        self.record_db = self.conn[self.args['record_db']]

    def get_county_position(self):
        cursor = self.conn['test02']['county'].aggregate(
            [
                {
                    '$project': {
                        '_id': 0,
                        'citycode': 0
                    }
                }
            ], allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def query(self):
        cursor = self.conn['core']['StoreTransaction'].aggregate(
            self.args['filter'],
            allowDiskUse=True
        )
        data = [x for x in cursor]
        cursor.close()
        return data

    def handle(self):
        data = self.query()
        county_data = self.get_county_position()
        county_new_data = dict()
        result = []
        for county in county_data:
            county_new_data['{province}{city}{county}'.format(**county)] = {'Longitude': county['Longitude'], 'latitude': county['latitude']}
        for item in data:
            address = '{province}{city}{district}'.format(**item)
            posation = county_new_data.get(address, {'Longitude': 0.0, 'latitude': 0.0})
            result.append({
                'province': item['province'], 'city': item['city'], 'district': item['district'], 'sum': item['sum'],
                'longitude': float(posation['Longitude']), 'latitude': float(posation['latitude'])
            })
        collection_name = 'StoreSumByLocation'
        self.record_db[collection_name].drop()
        self.record_db[collection_name].insert_many(result)

    def run(self):
        self.handle()


if __name__ == "__main__":
    filter_interval = [
        {
            '$match': {
                'province': '浙江省',
                'city': '温州市'
            }
        }, {
            '$group': {
                '_id': {
                    'province': '$province',
                    'city': '$city',
                    'district': '$district'
                },
                'sum': {
                    '$sum': '$sum'
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'province': '$_id.province',
                'city': '$_id.city',
                'district': '$_id.district',
                'sum': '$sum'
            }
        }
    ]
    args = {
        'host': '10.15.101.63',
        'port': 27027,
        'filter': filter_interval,
        'record_db': 'analysis_pre'
    }
    opt = CountyAmount(args=args)
    opt.run()
