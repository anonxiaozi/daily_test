# -*- coding: utf-8 -*-
# @Time: 2019/4/11
# @File: batch_record_shop

import os
import re
import datetime
import argparse
import record_shop


class OptCSV(object):

    def __init__(self, **kwargs):
        self.args = kwargs["args"]
        self.top_path = self.args["top_path"]
        self.file_path_list = []
        self.have_end_time = re.compile(r"out(\d+?)\_?(\d+?)\.csv")
        self.not_have_end_time = re.compile(r"out(\d+?)\.csv")
        self.match_content_time = re.compile(r"下单时间：(\d{4}\-\d{2}\-\d{2}\s+?\d{2}\:\d{2})")

    def find_file_path(self):
        for f in os.walk(self.top_path ):
            self.file_path_list.extend([os.path.join(f[0], x) for x in f[-1]])

    def get_file_name_date(self, file_name):
        if "_" in file_name:
            start_time_str, end_time_str = self.have_end_time.findall(file_name)[0]
        else:
            start_time_str, end_time_str = self.not_have_end_time.findall(file_name)[0], None
        start_time = datetime.datetime.strptime(start_time_str, "%Y%m%d")
        if not end_time_str:
            end_time = start_time + datetime.timedelta(days=1)
        else:
            end_time = datetime.datetime.strptime(end_time_str, "%Y%m%d") + datetime.timedelta(days=1)
        return start_time, end_time

    def detect_file_content_time(self):
        self.find_file_path()
        for file_path in self.file_path_list:
            drity_data = {"incorrect_time": [], "no_time": []}
            start_time, end_time = self.get_file_name_date(os.path.basename(file_path))
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f.readlines():
                        fetch_result = self.match_content_time.findall(line)
                        if fetch_result:
                            content_time = datetime.datetime.strptime(fetch_result[0], "%Y-%m-%d %H:%M")
                            if not content_time >= start_time or not content_time <= end_time:
                                drity_data["incorrect_time"].append(line[:18])
                        else:
                            drity_data["no_time"].append(line[:18])
                    else:
                        with open("result", "a", encoding="utf-8") as f:
                            f.write("{}".format(file_path).center(60, "=") + "\n")
                            if drity_data["no_time"] or drity_data["incorrect_time"]:
                                for key, value in drity_data.items():
                                    f.write("[ {} ]".format(key) + ": ")
                                    f.write(str(value) + "\n")
                self.args["filename"] = file_path
                record = record_shop.OptCSV(args=self.args)
                record.run()
            except Exception as e:
                with open("result", "a", encoding="utf-8") as f:
                    f.write("{}".format(file_path).center(60, "=") + "\n")
                    f.write("!!!!!!!!!!!!!!!!! Read File Failed: {}\n".format(e))

    def start(self):
        self.detect_file_content_time()


def get_args():
    """
    命令行参数
    """
    arg = argparse.ArgumentParser(prog="Record_Shop", usage='%(prog)s filter [options]')
    arg.add_argument("--host", type=str, help="DB host, default=%(default)s", default="10.15.101.63")
    arg.add_argument("--port", type=int, help="DB port, default=%(default)s", default=27027)
    arg.add_argument("--db", type=str, help="DB name, default=%(default)s", default="test")
    arg.add_argument("--collection", type=str, help="Record collection name, default=%(default)s", default="LocationFromWeb")
    arg.add_argument("--top_path", type=str, help="Dir name", required=True)
    return arg


if __name__ == "__main__":
    try:
        args = vars(get_args().parse_args())
        opt = OptCSV(args=args)
        opt.start()
    except Exception as e:
        with open("result", "a") as f:
            f.write("\nExit Error: {}".format(e))
        print(e)
