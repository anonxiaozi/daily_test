# -*- coding: utf-8 -*-
# @Time: 2019/3/20
# @File: 51cto_spider.py

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


url = "https://home.51cto.com"

# 设置Header
headers = {
    "origin": "https://home.51cto.com",
    "referer": "https://home.51cto.com/index",
    "user-agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
}
headers_dict = dict(DesiredCapabilities.CHROME, **headers)

# 设置代理IP：47.244.56.248:45769
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument("--proxy-server=http://47.244.56.248:45769")

# driver = webdriver.Chrome(desired_capabilities=headers_dict, options=chrome_options)
driver = webdriver.Chrome(desired_capabilities=headers_dict)
driver.get(url)
assert "51CTO" in driver.title
account_elem = driver.find_element_by_link_text("账号密码登录").click()
user_elem = driver.find_element_by_id("loginform-username")
pass_elem = driver.find_element_by_id("loginform-password")
user_elem.clear()
pass_elem.clear()
user_elem.send_keys("bloke_anon@126.com")
pass_elem.send_keys("")
login_elem = driver.find_element_by_name("login-button").click()
driver.close()
