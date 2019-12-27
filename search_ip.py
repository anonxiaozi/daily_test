# -*- coding: utf-8 -*-
# @Time: 2019/12/27
# @File: search_ip

import requests
from bs4 import BeautifulSoup
import time


def getIpAddr(url, ip):
    response = requests.get(url + ip)
    response.encoding = response.apparent_encoding
    content = response.text
    soup = BeautifulSoup(content, 'html.parser')
    data = soup.find_all(class_='Whwtdhalf w50-0')[-1]
    area = data.get_text()
    return area


if __name__ == '__main__':
    url = "http://ip.tool.chinaz.com/"
    ip_list = [
        "106.46.161.1",
    ]
    for ip in ip_list:
        print(getIpAddr(url, ip))
        time.sleep(0.5)
