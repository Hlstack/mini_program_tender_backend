#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql.cursors 
from datetime import datetime

class Fetch_Db_data(object):
    
    def __init__(self):
        dbname = 'tender'    
        self.connection = pymysql.connect(host='localhost',
                             user='root',
                             password='siliver88',
                             db=dbname,
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

    def fetch_data(self):

        showcase = []
        try:
            with self.connection.cursor() as cursor:
                results =[]
                sql = '''SHOW TABLES'''
                cursor.execute(sql)
                result = cursor.fetchall()
                for i in range(len(result)):
                    results.append(result[i]['Tables_in_tender'])
                for tbname in results:
                    checksql = '''SELECT isAlive, zb_url, file_url, title, ctime,
                           deadline, bsdeadline, projectcode, kbsj, bsfyxs, xmjs, telephone,
                           projectclass, tenderee, contact, agent FROM {}'''.format(tbname)
                    cursor.execute(checksql)
                    foo =  cursor.fetchall()
                    showcase.append(foo[0])
            
        finally:
            self.connection.close()
        return showcase

    def res_sorted(self):
        lst = self.fetch_data()
        #for i in range(len(lst)):
        #    nice.append(lst[i])
        res = sorted(lst, key=lambda s:datetime.strptime(s['ctime'],'%Y-%m-%d'), reverse=True)
        return res


if __name__ == "__main__":
    fb = Fetch_Db_data() 
    print(fb.res_sorted())
