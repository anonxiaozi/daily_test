# -*- coding: utf-8 -*-
# @Time: 2019/3/21
# @File: 1688

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time
import random

url = "https://login.taobao.com/member/login.jhtml?style=b2b&css_style=b2b&from=b2b&newMini2=true&full_redirect=true&redirect_url=" \
      "https://login.1688.com/member/jump.htm?target=https://login.1688.com/member/marketSigninJump.htm?Done=http%3A%2F%2Fmember.1688" \
      ".com%2Fmember%2Foperations%2Fmember_operations_jump_engine.htm%3Ftracelog%3Dlogin%26operSceneId%3Dafter_pass_from_taobao_new%26de" \
      "faultTarget%3Dhttp%253A%252F%252Fwork.1688.com%252F%253Ftracelog%253Dlogin_target_is_blank_1688&reg=http://member.1688.com/member/jo" \
      "in/enterprise_join.htm?lead=http://member.1688.com/member/operations/member_operations_jump_engine.htm?tracelog=login&operSceneId=after" \
      "_pass_from_taobao_new&defaultTarget=http%3A%2F%2Fwork.1688.com%2F%3Ftracelog%3Dlogin_target_is_blank_1688&leadUrl=http://member.1688.com/m" \
      "ember/operations/member_operations_jump_engine.htm?spm=a2107.1.0.0.44ceFYI2FYI2Gv&tracelog=login&operSceneId=after_pass_from_taobao_new&defau" \
      "ltTarget=http%3A%2F%2Fwork.1688.com%2F%3Ftracelog%3Dlogin_target_is_blank_1688&tracelog=login_s_reg&useMobile=false&"

# 设置Header
headers = {
    "authority": "login.1688.com",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "user-agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
}
headers_dict = dict(DesiredCapabilities.CHROME, **headers)

# 设置代理IP：47.244.56.248:45769
chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument("--proxy-server=http://127.0.0.1:8080")

cookies = {
    "name": "ua",
    "value": "115#6GE3E11O1TwJdAWOTCCY1Csod51GB9A11g2mOh2/r5FcAyv5iASY6CtuhsTCyzEgkjRPe1L8ykZgbbvJhUU4AkaOxY6fyzFdvsfletT4ukNQiQJJhUU89BdDaLBAuyjtRj5yetsYyWNQi/JJhUU4OWGcaLpXyrrQORRPeK/4ukdKCX33KcnfCyOVZ7NsW5wQSFAF5LOrOCYkrpT97+ttO5ClUmvt2bGAxNagVpsdNDzFxrpyC8q9W4RS6jvl/RbI6rY1WJBKmrP6gOu4AyBYoTC0OASwNs0M9yE94+q9iNb+k7auD9LyXkKykekNh4v8y+ld5vvnizUIgcg1NR5eOPQJe81l2RBGCOlHPodLgVrN1NQN9LVg6KD68GmY5lYe5hTcJYaQaz1A09HIxGgjWEnu0CjNsIRiXs90OJjKxDzvZIUvlN/IBzFwxov5dopr8WiM+DbzVW2hnjOKwUsADR75wUNhevbfC9ZSFrl4iWtPTqXVyfvS6ABYUtUaQNCzfn7LchWJN1lc+8aw0/3L0DsAzdHeM7eE+QeDxWhIQp5ywvTiwkxGHxog1tbZWqn+9UHG7t3rT0y18Q8Lugz0McrRI11RbkYB6jPEDhGPeo/Go6xyUv+jyysDVtyLVj8LMEn9Obp8/zC33eoPdTNF6oNchdKcRvVb0NVDhHWtBjKhLtNdOcU/I2HPeCayMlU5LVcGDs8yCWDdXaJMQF/C+pTw1ZSVCGPEcD4gibzC5AotxKzJ/3owUdWcsqn4rLk6541LO9ZYFUe0dP5jECrAIzdegzu2YKK6xo4FhHvNnNH7SJChhjjGvpYQ72h5pUySIkK2v0tBEwAyPG+jCzCq8YseiJfEQDIPvBxuQW4lFOeUla2o/hMyLQC1eCvkPnRgKUpQo7YJ3WEiDM4fJCMpvhm1QWyqApnN2skDk+jkYma72jINh4jdPMC3qaqX0AgSA9NIav9F7quRuAUD/phyK9o1OoEI3+CuEPCWjP7AAfVvngxqcBrq0tWda0GMlGo8unwBx1zlKZ3ybC6R7WUi2T24Vb4GDdScARKMl0Z0zRl3dOm1q2ilwOb8rA8w1L9N8i9yvxZG/dluoJAHgyTuyNLnaaSjGXE/IQJ/Fn0mw6NXxJHzkTvl+oN/3ueI9P93ziCMuOuvF8rq7V573ZFMLoR3P0qZzuHqpRQ9XVKFOZCHO0zUn5rEyGqrK0b0FoGXiMyHi9GccC+HRQcZkadHpathgVTKTC8HtqflPUPYSxjl2Gvdwd2x8Lck+DhhZ0MlSxVeKKJn4RzenSC9xotjmKyo/nyu4U0U8Zp4qqO2DB+3Ria4ozpi38bp527MXsaTvhz0q+3Ged6ERs5dAKKWtYNAgkuHxSYo36E9pV8On+NgFpWdRGH3QTZjkKIlx12/9VFgT9Lu3tYx9+P1oeF0Wo2Z7Wkt5EZIOka/LItpma5aPLdtFj0g5MndDNdCUczlyiM8SEtI21sWUn1NeacPqLg3cuxth1hkYbJkzF3seh9Jw9nKU3ceUF/YWDzQArIuuRml1+eQEzXcHFwflp9ysPRTlC=="
}

binary = FirefoxBinary(r"D:\Program Files\Mozilla Firefox\firefox.exe")
# driver = webdriver.Firefox(desired_capabilities=headers_dict, options=chrome_options, firefox_binary=binary, capabilities={"marionette": False})
driver = webdriver.Chrome(desired_capabilities=headers_dict, options=chrome_options)
driver.maximize_window()
driver.get(url)
driver.add_cookie(cookies)

# assert "1688.com" in driver.title
user_elem = driver.find_element_by_id("TPL_username_1")
pass_elem = driver.find_element_by_id("TPL_password_1")
user_elem.clear()
pass_elem.clear()
user_elem.send_keys("16619725851")
time.sleep(1)
pass_elem.send_keys("jintianshigehaorizi1")
time.sleep(1)

driver.execute_script("Object.defineProperties(navigator,{webdriver:{get:() => false}})")
driver.execute_script("window.navigator.chrome = { runtime: {},  };")
driver.execute_script("Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });")
driver.execute_script("Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5,6], }); ")

time.sleep(1.5)

# 解锁
# bar_element = driver.find_element_by_id('nc_1_n1z')
# ActionChains(driver).drag_and_drop_by_offset(bar_element, 258, 0).perform()
# time.sleep(0.5)

# login_elem = driver.find_element_by_id("J_SubmitStatic").click()

# driver.close()

