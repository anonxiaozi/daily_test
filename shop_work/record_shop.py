# -*- coding: utf-8 -*-
# @Time: 2019/3/22
# @File: first

import re
import pymongo
import os
import datetime


class OptMongodb(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.conn = pymongo.MongoClient(self.args["host"], self.args["port"])
        self.db = self.conn[self.args["db"]]
        self.collection = self.db[self.args["collection"]]
        # self.conn = pymongo.MongoClient(
        #     self.kwargs["host"],
        #     self.kwargs["port"],
        #     username=self.kwargs["user"],
        #     password=self.kwargs["password"],
        #     authSource=self.kwargs["db"],
        #     authMechanism=self.kwargs["SCRAM-SHA-256"]
        # )

    def check_index(self):
        indexes = self.collection.index_information()
        if not "serial_id" in indexes:
            self.collection.create_index("SerialID", name="serial_id")

    def get_serial_id(self):
        self.check_index()
        data = [x["SerialID"] for x in self.collection.find({}, {"SerialID": 1, "_id": 0})]
        return data

    def insert_many(self, data):
        try:
            self.collection.insert_many(data)
        except Exception as e:
            print("Error: {}.".format(e))


class OptCSV(OptMongodb):

    def __init__(self, **kwargs):
        self.match_info = re.compile(r"\n?(\d{18}),(.+?),(.+?)\s(\S+?)\s+?，(\+?\d?\d?\??\d+?\-?\s?\??\d+?\??\-?\s?\d+?\??),下单时间：(.*)")
        self.args = kwargs["args"]
        self.illegal_data = {"illegal": [], "exists": []}
        self.result = {}
        self.num = 0
        super().__init__(args=kwargs["args"])

    def read_fake_csv(self):
        with open(os.path.join("csv_data", self.args["filename"]), "r", encoding="utf-8") as f:
            for line in f.readlines():
                self.num += 1
                result = self.match_info.findall(line)
                if result:
                    self.result[result[0][0]] = result[:]
                else:
                    self.illegal_data["illegal"].append(line)

    def path_diff_info(self):
        """
        对比 Receiver 和 FullAddress 相同比例
        """
        same_list = []
        for value in self.result.values():
            if value[1] == value[2]:
                same_list.append(value)
            else:
                print(value[1])
                print(value[2])
                print("".center(50, "*"))
        print("Same rate: {:0.2%}".format(len(same_list) / len(self.result.keys())))

    def record_data(self):
        """
        写入数据库，一次写入2k条
        """
        n = 0
        data = list(self.result.values())
        while n < len(data):
            data2k = [
                {
                    "SerialID": x[0][0],
                    "Receiver": x[0][1],
                    "FullAddress": x[0][2],
                    "Name": x[0][3],
                    "Cellphone": x[0][4],
                    "OrderTime": x[0][5]
                } for x in data[n:n+2000]
            ]
            self.insert_many(data2k)
            n += 2000

    def compare_serialID(self):
        """
        将订单ID从MongoDB中取出，然后对比当前数据文件，如果订单ID相同，则去重
        """
        db_data = set(self.get_serial_id())
        csv_serial_id = set([x for x in self.result.keys()])
        same_data = list(db_data.intersection(csv_serial_id))
        if same_data:
            for key in same_data:
                self.illegal_data["exists"].append(self.result[key])
                self.result.pop(key, "Remove {} failed".format(key))

    def run(self):
        self.read_fake_csv()    # 读取csv数据
        self.compare_serialID() # 去重
        self.record_data()      # 写入DB
        if self.illegal_data:   # 打印未写入DB的数据
            f = open("result", "a", encoding="utf-8")
            f.write("{} [ {} ]".format(str(datetime.datetime.now()), self.args["filename"]).center(50, "*") + "\n")
            illegal_data = "Illegal Data".center(50, "=")
            f.write(illegal_data + "\n")
            print(illegal_data)
            for key, value in self.illegal_data.items():
                for sub_value in value:
                    d = key + ":" + str(sub_value)
                    print(d)
            else:
                for illegal in self.illegal_data["illegal"]:
                    f.write(illegal + "\n")
            insert_num = len(self.result)   # 写入DB的数据number
            final_result = "Total {} data | Insert {} data | Illegal data: {} | Exists data: {} | Record rate: {:.2%}"\
                .format(self.num, insert_num, len(self.illegal_data["illegal"]), len(self.illegal_data["exists"]), insert_num/self.num)
            f.write(final_result + "\n")
            print(final_result)
            f.close()


if __name__ == "__main__":
    args = {
        "host": "10.15.101.63",
        "port": 27027,
        "db": "blockchain_test",
        "collection": "LocationFromWeb",
        "filename": "out0228-0231.csv"
    }
    opt = OptCSV(args=args)
    opt.run()
