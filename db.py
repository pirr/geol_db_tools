# -*- coding: utf-8 -*-


import requests
from setup import COUCH_URL, cdb


class DBConn:

    def __init__(self, conn=cdb, couch_url=COUCH_URL):
        self.conn = conn
        self.conn_url = couch_url

    def bulk_save(self, list_data_dict):
        self.conn.update(list_data_dict)

    def save(self, doc, data_dict):
        self.conn[doc] = data_dict

    def get_docs(self, **kwargs):
        selector = {'selector': kwargs, 'limit': 1000000}
        r = requests.post('/'.join([self.conn_url, self.conn.name, '_find']), json=selector)
        return r.json()['docs']

