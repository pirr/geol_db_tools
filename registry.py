# -*- coding: utf-8 -*-

import os
import re
import numpy as np
import pandas as pd

from flask import flash

from setup import app

def message_former_from(message_dict):
    message = '\n'.join(
        ': '.join([pk, ', '.join(pv)]) for pk, pv in message_dict.items()
    )

    return message

class RegistryExc(Exception):
    pass


def read_excel(filename, actual=False):
    try:
        f = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    except Exception as e:
        flash('''Возникли проблемы на стороне сервера обратитесь к администратору''', category='error')
        raise e
    try:
        df = pd.read_excel(f, sheetname='reestr', skiprows=2,
                           converters={'Дата регистрации': str, 'Дата': str, '№ объекта в документе регистрации': str})
    except Exception as e:
        flash('''Проблемы при чтении файла. Возможно в файле {} нет листа reestr'''.format(
            filename), category='error')
        os.remove(f)
        raise e
    
    return df


class Registry:
    '''
        верификация и форматирование реестра для импорта в БД
    '''

    def __init__(self, registry_df, registry_cols_dict, actual_cols_list=[]):
        self.registry = registry_df
        self.errors = dict()
        self.cols = registry_cols_dict
        self.actual_cols = actual_cols_list
    
    # сбор ошибок верификации реестра
    def __append_errors(self, err_name, err_str):
        if err_name in self.errors:
            self.errors[err_name].extend(err_str)
        else:
            self.errors[err_name] = [err_str]
    
    # удаление переносов и других непробельных символов в названии колонок реестра
    def _columns_strip(self):
        pattern = re.compile(r'\s+')
        self.registry.columns = [pattern.sub(' ', c) for c in self.registry.columns]
    
    # проверяет колонки, если отсутсвуют то записывают отсутствующие в соотвествующую ошибку
    def __check_columns(self, *cols):
        none_cols = [c for c in cols if c not in self.registry.columns]
        if none_cols:
            self.__append_errors('В реестре отсутствуют колонки', ', '.join(none_cols))

    def _check_registry_cols(self):
        self.__check_columns(*self.cols.keys())

    def _check_actual_cols(self):
        self.__check_columns(*self.actual_cols)

    # обновление названий колонок для БД
    def update_column_names_for_db(self):
        self.registry.columns = [c if c in self.actual_cols else self.cols[c] for c in self.registry.columns]
    
    # округление чисел с плавающей точкой
    def fix_float(self):
        for col in self.registry.columns:
            if self.registry[col].dtype == np.float64:
                self.registry[col] = np.round(self.registry[col], 8)

    # проверяет дубликаты актуальных строк, если они есть записывает пары в соответствующую ошибку
    def _check_actual_duplicates(self, n_col, *checking_cols):
        duplicates = self.registry[checking_cols].duplicated(keep=False)
        if not self.registry[duplicates].empty:
            duplicates = self.registry.groupby(checking_cols)[n_col].apply(list).tolist()
            self.__append_errors('Дубликаты актуальных строк', ', '.join(duplicates))

    def former_imp_registry(self, *chunk_fields):
        concat_df = pd.DataFrame()
        for field in chunk_fields:
            chunk_df = self.registry[~pd.isnull(self.registry[field])]
            concat_df = pd.concat([concat_df, chunk_df])
        concat_df.drop_duplicates(inplace=True)
        self.registry = concat_df

    # def duplicates_other(self, other):
    #     none_duplicates = self.registry[~self.registry['_id'].duplicated(keep=False)]
    #     concat_df = pd.concat([none_duplicates, other])
    #     concat_df.drop(['N_change', 'actual', 'id_reg'], axis=1, inplace=True)
    #     concat_df_duplicates_id = concat_df.loc[concat_df.duplicated(keep=False), '_id']
    #     self.registry = self.registry[~self.registry['_id'].isin(concat_df_duplicates_id)]

    def check_errors(self):
        if self.errors:
            flash(message_former_from(self.errors))
            print(message_former_from(self.errors))
            raise RegistryExc(message_former_from(self.errors))

    def registry_errors(self):
        self.fix_float()
        if self.actual_cols:
            self._check_actual_cols()
            self._check_actual_duplicates('N', 'actual', '_id')

    def make_registry_for_import(self):
        self._columns_strip()
        self._check_registry_cols()
        self.check_errors()
        self.fix_float()
        if self.actual_cols:
            self._check_actual_cols()
            self._check_actual_duplicates('N', 'actual', '_id')
        # self.check_errors()
        self.update_column_names_for_db()
        self.former_imp_registry('actual', 'change_type')
        self.registry.fillna('', inplace=True)
        return self.registry
       


