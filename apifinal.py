#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from whoosh.filedb.filestore import FileStorage 
from whoosh.qparser import QueryParser
from datetime import datetime
from pyrestful import mediatypes
from pyrestful.rest import get

import tornado.ioloop
import pyrestful.rest
import stf, fetchtables
import urllib


class CustomerDataBase(object):
    
    def DBdata(self):
        dd = fetchtables.Fetch_Db_data().res_sorted() 
        return dd

    def Querydata(self, keyword):
        nice = stf.CustomizeSearch(keyword).Result_sorted() 
        return nice

class CustomerResource(pyrestful.rest.RestHandler):

    def initialize(self, database):
        self.database = database
    
    @get(_path="/tender/api/db",_produces=mediatypes.APPLICATION_JSON)
    def getListTenderInfo(self):
        infolist = self.database.DBdata()
        return infolist

    @get(_path="/tender/api/query/{keyword}",_produces=mediatypes.APPLICATION_JSON)
    def getListQueryInfo(self, keyword):
        querylist = self.database.Querydata(urllib.parse.unquote(keyword))
        return querylist

if __name__=="__main__":
    try:
        print("API service started")
        database = CustomerDataBase()
        app = pyrestful.rest.RestService(
            [CustomerResource],dict(database=database))
        app.listen(8881)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print("\nStop the service")
