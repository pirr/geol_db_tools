# -*- coding: utf-8 -*-

from datetime import datetime

import requests
from setup import COUCH_URL, cdb, couch


class DBConn:
    '''
        Класс-коннектор БД
    '''

    # def __init__(self, conn):
    #     self.conn = conn

    def save(self, doc, data_dict):
        '''
            сохранение в БД
            data_dict -- словарь для сохранения
        '''
        pass

    def bulk_save(self, list_data_dict):
        '''
            массовое сохранение в БД
            list_data_dict -- список словарей для сохранения
        '''
        pass


class DBConnCouch(DBConn):
    '''
        Класс-коннектор для CouchDB
    '''
    conn = cdb
    url = COUCH_URL
    server = couch

    def __init__(self):
        self.regs_info = self.conn['regs_info']

    def bulk_save(self, list_data_dict):
        self.conn.update(list_data_dict)

    def save(self, doc, data_dict):
        self.conn[doc] = data_dict

    def get_selected(self, **kwargs):
        '''
            выборка из БД по фильтру
            возвращает список словарей
            kwargs -- параметры выборки
        '''
        selector = {'selector': kwargs, 'limit': 1000000}
        r = requests.post(
            '/'.join([self.url, self.conn.name, '_find']), json=selector)
        return r.json()['docs']

    @staticmethod
    def get_time_now():
        '''
            дата и время сейчас
        '''
        return datetime.now().strftime("%Y-%m-%d_%H-%M")

    def __calc_reg_id(self):
        '''
            получение последнего + 1 id реестра
        '''
        reg_ids = [int(id) for id in list(self.regs_info)
                   if id not in ('_id', '_rev')]
        if reg_ids:
            id_reg = str(max(reg_ids) + 1)
        else:
            id_reg = '1'

        return id_reg

    def get_reg_id_info(self, id_reg=None, reg_name=None):
        '''
            внесение информации о реестре
            возвращает id реестра (id_reg) и информацию о реестре (reg_info)
            id_reg -- id реестра d БД
            reg_name -- название реестра
        '''
        t = self.get_time_now()
        if id_reg is not None:
            self.regs_info[id_reg]['modified'] = t
        else:
            id_reg = self.__calc_reg_id()
            print('id_reg:', id_reg)
            self.regs_info[id_reg] = {'created': t,
                                      'modified': '',
                                      'reg_name': reg_name}

        return id_reg, self.regs_info[id_reg]

    def write_reg_info(self):
        self.save('regs_info', self.regs_info)

    def get_revisions_by_id(self, _id):
        '''
            возвращает ревизии документа по его _id начиная с 1 ревизии,
                т.к. 0 - последняя ревизия
            _id -- id документа
        '''
        for i, rev in enumerate(self.cdb.revisions(_id)):
            if i:
                yield {k: v for k, v in rev.items()}
