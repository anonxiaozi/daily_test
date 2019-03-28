# -*- coding: utf-8 -*-
# @Time: 2019/3/21
# @File: 1688_2

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

chrome_options = Options()
chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:6789")
driver = webdriver.Chrome(options=chrome_options)

driver.find_element_by_xpath("//a[@title='交易']").click()
time.sleep(2)

d = driver.find_element_by_xpath("//div[@class='iframe-container']/iframe[1]")
driver.switch_to.frame(d)
d = driver.find_element_by_id("orderlist-no-items-warn")
print(d.text)

driver.close()

