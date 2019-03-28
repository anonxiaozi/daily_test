# -*- coding: utf-8 -*-
# @Time: 2019/3/20
# @File: taobao_spider

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time

url = "https://login.taobao.com/member/login.jhtml?redirectURL=https://i.taobao.com/my_taobao.htm?spm=a2107.1.0.0.7f5411d9vNUmrS&nekot=dGIzMTkyMzUxOTg=1553072545464&useMobile=false&"

# 设置Header
headers = {
    "authority": "login.taobao.com",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
    "Referer": "https://i.taobao.com/my_taobao.htm?nekot=dGIzMTkyMzUxOTg%3D1553072545464",
}
headers_dict = dict(DesiredCapabilities.CHROME, **headers)

# 设置代理IP：47.244.56.248:45769
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--proxy-server=http://127.0.0.1:8080")

# driver = webdriver.Chrome(desired_capabilities=headers_dict, options=chrome_options)
driver = webdriver.Chrome(desired_capabilities=headers_dict)
driver.get(url)
assert "淘宝网" in driver.title
user_elem = driver.find_element_by_id("TPL_username_1")
pass_elem = driver.find_element_by_id("TPL_password_1")
user_elem.clear()
pass_elem.clear()
user_elem.send_keys("16619725851")
time.sleep(1)
pass_elem.send_keys("jintianshigehaorizi1")
time.sleep(1)

# 解锁
bar_element = driver.find_element_by_id('nc_1_n1z')
ActionChains(driver).drag_and_drop_by_offset(bar_element, 258, 0).perform()
time.sleep(0.5)

# login_elem = driver.find_element_by_id("J_SubmitStatic").click()

# driver.close()
