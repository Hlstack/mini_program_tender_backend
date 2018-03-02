#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from whoosh.filedb.filestore import FileStorage 
from whoosh.qparser import QueryParser
from datetime import datetime


class CustomizeSearch(object):
   
    def __init__(self, keyword):
        self.keyword = keyword     
    
    def WhooshSearch(self):
        dirname = './whoosh_index'   
        storage = FileStorage(dirname)
        ix = storage.open_index(indexname='Hello')
        searcher = ix.searcher()
        parser = QueryParser("content", schema=ix.schema)
        myquery = parser.parse(self.keyword)
        search_results = {}
        result_for_now = []

        try: 
            # limit 设定输出条数
            results = searcher.search(myquery, limit=200)
            # 限制高亮字数
            results.fragmenter.charlimit = 80
        
        finally:
            foo = []
            # 由于小程序不认<b>标签，加了两个replace
            for i in range(len(results)):
                foo.append({'dbtb':results[i]['dbtb'],
                    'zb_url':results[i]['zb_url'],
                   'title':results[i]['title'],
                   'ctime':results[i]['ctime'],
                   'deadline':results[i]['deadline'],
                   'bsdeadline':results[i]['bsdeadline'],
                   'keywords':results[i].highlights('content')
                              .replace('<b class="match term0">','')
                              .replace('</b>','')
                   })
            for i in range(len(results)):
                search_results[i] = foo[i]   
        searcher.close()
        return search_results

    def Result_sorted(self):
        lst = self.WhooshSearch()                   
        nice = []
        for i in range(len(lst)):
            nice.append(lst[i])    
        res = sorted(nice, key=lambda s:datetime.strptime(s['ctime'],'%Y-%m-%d'), reverse=True) 
        return res

'''
if __name__ == "__main__":
    keyword ='屏蔽' 
    cs = CustomizeSearch(keyword)
    print(cs.Result_sorted())
'''
