#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import sys


## 必须使用requests 来完成数据传递

payload = sys.argv[1] 

#r = requests.get('https://www.wxtongteng.com/tender/api/query/{}'.format(payload))
r = requests.get('http://localhost:8881/tender/api/query/{}'.format(payload))
#r = requests.get('https://www.wxtongteng.com/tender/api/db')
#r = requests.get('http://localhost:8881/tender/api/db')

print((r.text).encode('latin-1').decode('unicode_escape'))
