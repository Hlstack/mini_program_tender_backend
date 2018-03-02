#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from whoosh.fields import Schema, ID, TEXT
from jieba.analyse import ChineseAnalyzer
from whoosh.filedb.filestore import FileStorage
import pymysql.cursors

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='siliver88',
                             db='tender',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

class CreateIndex(object):

    def Gettabs(self):
        dbtable_names = []
        
        with connection.cursor() as cursor:
            sql = '''SHOW TABLES'''
            cursor.execute(sql)
            dbtable_names_for_now = cursor.fetchall()
            for i in range(len(dbtable_names_for_now)):
                dbtable_names.append(
                    dbtable_names_for_now[i]['Tables_in_tender'])
        return dbtable_names        

    def BuiltIndex(self):
        analyzer = ChineseAnalyzer()
        # define schema
        schema = Schema(
            title=TEXT(sortable=True),
            zb_url=TEXT(sortable=True),
            ctime=TEXT(sortable=True),
            deadline=TEXT(sortable=True),
            bsdeadline=TEXT(sortable=True),
            dbtb=TEXT(sortable=True),
            content=TEXT(sortable=True, analyzer=analyzer),
            lettercard=TEXT(sortable=True, analyzer=analyzer)
        )
        dirname = './whoosh_index'
        storage = FileStorage(dirname)
        if not os.path.exists(dirname):
            os.mkdir(dirname)
            # create index file
            ix = storage.create_index(schema, indexname='Hello')
        else:
            ix = storage.open_index(indexname='Hello')

        writer = ix.writer()

        # fetch rows from DB
        num = 0
        try:
            with connection.cursor() as cursor:
                for tbname in self.Gettabs():
                    sql = '''SELECT `title`, `zb_url`, `ctime`, `deadline`, `bsdeadline`,`dbtb`, `content`, `lettercard` FROM {}'''.format(tbname)
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    # write the rows into indexes
                    for row in rows:
                        writer.add_document(title=str(row["title"]),
                                           zb_url=str(row["zb_url"]),
                                           ctime=str(row["ctime"]),
                                           deadline=str(row['deadline']),
                                           bsdeadline=str(row['bsdeadline']),
                                           dbtb=str(row["dbtb"]),
                                           content=str(row["content"]),
                                           lettercard=str(row["lettercard"])
                                           )

                        num += 1
                writer.commit()
        finally:
            connection.close()
        print("%d docs indexed!" % num)

createindex = CreateIndex()
createindex.BuiltIndex()
