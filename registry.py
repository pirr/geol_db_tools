# -*- coding: utf-8 -*-

import os
import re
from collections import OrderedDict

import numpy as np
import pandas as pd
from flask import flash

from setup import app
from _help_fun import flash_mess, message_former_from


REGISTRY_COLUMNS = OrderedDict([('№ строки', 'N'),
                                ('Актуальность строки', 'actual'),
                                ('№ изменений', 'N_change'),
                                ('Операция внесения (добавление, изменение, удаление)', 'change_type'),
                                ('№ объекта', 'N_obj'),
                                ('Признак комплексного', 'complex'),
                                ('Вид документа регистрации1)', 'doc_type'),
                                ('Наличие ГКМ паспорта в группе', 'obj_with_gkm'),
                                ('Орган регистрации (ТФИ, РГФ, ВСЕГЕИ, ЦНИГРИ, Роснедра, Минприроды, ГСЭ)',
                                 'organ_regs'),
                                ('Номер документа', 'doc_num'),
                                ('Дата регистрации', 'doc_date'),
                                ('Год регистрации (для сортировки)', 'doc_date_num'),
                                ('№ объекта в документе регистрации', 'obj_num_in_doc'),
                                ('Федеральный округ', 'fed_distr'),
                                ('Субъект РФ', 'subj_distr'),
                                ('Административный район', 'adm_distr'),
                                ('Лист м-ба 1000', '1000_map'),
                                ('Лист м-ба 200 (араб.)', '200_map'),
                                ('Вид объекта2)', 'geol_type_obj'),
                                ('Название объекта', 'name_obj'),
                                ('Фонд недр (Р-распред., НР-нераспред.)', 'fund'),
                                ('Вид пользования недрами (ГИН/Р+Д/ГИН+Р+Д)', 'use_type'),
                                ('Группа ПИ в госпрограмме3)', 'gover_type_pi'),
                                ('ПИ (перечень для объекта)', 'pi'),
                                ('Название нормализ.', 'norm_pi'),
                                ('Название ПИ по ГБЗ', 'gbz_pi'),
                                ('Группа ПИ ИС недра', 'isnedra_pi'),
                                ('Ед. измерения ПИ', 'unit_pi'),
                                ('P3', 'P3_cat'),
                                ('P2', 'P2_cat'),
                                ('P1', 'P1_cat'),
                                ('С2', 'C2_res'),
                                ('Без категор.', 'none_cat'),
                                ('Запасы ABC1', 'ABC_res'),
                                ('Признак наличия ресурсных оценок', 'res_exist'),
                                ('Наличие прогнозных ресурсов', 'cat_avaibil'),
                                ('Признак наличия запасов', 'res_avaibil'),
                                ('Вид документа апробации (протокол, отчет)', 'probe_doc_type'),
                                ('Номер', 'probe_doc_num'),
                                ('Дата', 'probe_doc_date'),
                                ('Орган апробации', 'probe_doc_organ'),
                                ('№ в таблице координат для полигонов', 'N_poly_table'),
                                ('Территория органа апробации', 'probe_organ_subj'),
                                ('Вид координат (Т-точка, П-полигон)', 'coord_type'),
                                ('Площадь, км2', 'area'),
                                ('Координата центра X', 'lon'),
                                ('Координата центра Y', 'lat'),
                                ('Источник координат4)', 'coord_source'),
                                ('Входимость в лицензионыый участок', 'license_area'),
                                ('Достоверность координат', 'coord_reliability'),
                                ('Координаты треб. проверки', 'coord_for_check'),
                                ('Данные о районе (для определения координат)', 'territory_descript'),
                                ('Другие документы об объекте (вид документа, №, год, стадия ГРР, авторы, организация)',
                                 'other_source'),
                                ('Рекомендуемые работы (оценка ПР, апробация ПР, в фонд заявок, поиски, оценка и др.)',
                                 'recommendations')])

actual_cols = ('_id', '_rev', 'id_reg', 'filename')


class RegistryExc(Exception):
    pass


class RegistryFormatter:
    '''
        верификация и форматирование реестра для импорта в БД
    '''

    def __init__(self, registry_df, registry_cols_dict=REGISTRY_COLUMNS, type=''):
        self.registry = registry_df
        self.errors = dict()
        self.cols = registry_cols_dict
        if type == 'actual':
            self.actual_cols = actual_cols
        else:
            self.actual_cols = []

    # сбор ошибок верификации реестра
    def __append_errors(self, err_name, err_str):
        if err_name in self.errors:
            self.errors[err_name].extend(err_str)
        else:
            self.errors[err_name] = [err_str]

    # удаление переносов и других непробельных символов в названии колонок
    # реестра
    def _columns_strip(self):
        pattern = re.compile(r'\s+')
        self.registry.columns = [pattern.sub(
            ' ', c) for c in self.registry.columns]

    # проверка наличия колонок, если отсутсвуют то записать их в
    # соотвествующую ошибку
    def __check_columns(self, *cols):
        none_cols = [c for c in cols if c not in self.registry.columns]
        if none_cols:
            self.__append_errors(
                'В реестре отсутствуют колонки', ', '.join(none_cols))

    # проверка наличия основных колонок реестра
    def _check_registry_cols(self):
        self.__check_columns(*self.cols.keys())

    # проверка наличия колонок реестра для актуализации
    def _check_actual_cols(self):
        self.__check_columns(*self.actual_cols)

    # обновление названий колонок для БД
    def update_column_names_for_db(self):
        self.registry.columns = [c if c in self.actual_cols else self.cols[
            c] for c in self.registry.columns]

    # округление чисел с плавающей точкой
    def fix_float(self):
        for col in self.registry.columns:
            if self.registry[col].dtype == np.float64:
                self.registry[col] = np.round(self.registry[col], 8)

    # проверяет дубликаты актуальных строк, если они есть записывает пары в
    # соответствующую ошибку
    def _check_actual_duplicates(self, n_col, *checking_cols):
        duplicates = self.registry[checking_cols].duplicated(keep=False)
        if not self.registry[duplicates].empty:
            duplicates = self.registry.groupby(
                checking_cols)[n_col].apply(list).tolist()
            self.__append_errors(
                'Дубликаты актуальных строк', ', '.join(duplicates))

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
            mess = message_former_from(self.errors)
            flash_mess(mess)
            raise RegistryExc(mess)

    def registry_errors(self):
        self.fix_float()
        if self.actual_cols:
            self._check_actual_cols()
            self._check_actual_duplicates('N', 'actual', '_id')

    def make_validate(self):
        self._columns_strip()
        self._check_registry_cols()
        self.check_errors()
        self.update_column_names_for_db()
        self.fix_float()
        if self.actual_cols:
            self._check_actual_cols()
            self.check_errors()
            self._check_actual_duplicates('N', 'actual', '_id')
        self.check_errors()
        self.former_imp_registry('actual', 'change_type')
        self.registry.fillna('', inplace=True)
