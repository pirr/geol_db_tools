# -*- coding: utf-8 -*-


import requests
from setup import COUCH_URL, cdb


class DBConn:

    def __init__(self, conn=cdb, couch_url=COUCH_URL):
        self.conn = conn
        self.conn_url = couch_url
        self.regs_info = self.conn['regs_info']

    def bulk_save(self, list_data_dict):
        self.conn.update(list_data_dict)

    def save(self, doc, data_dict):
        self.conn[doc] = data_dict

    def get_docs(self, **kwargs):
        selector = {'selector': kwargs, 'limit': 1000000}
        r = requests.post('/'.join([self.conn_url, self.conn.name, '_find']), json=selector)
        return r.json()['docs']

    # def get_reg_ids(self):
    #     reg_ids = [int(id) for id in list(self.regs_info) if id not in ('_id', '_rev')]
    #     if reg_ids:
    #         id_reg = str(max(reg_ids) + 1)
    #     else:
    #         id_reg = '1'
    #     self.regs_info[id_reg] = {'created': t,
    #                              'modified': '',
    #                              'reg_name': session['reg_name']}

