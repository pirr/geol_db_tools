# -*- coding: utf-8 -*-

import os
import re
from collections import OrderedDict

import numpy as np
import pandas as pd

from setup import app
from _help_fun import flash_mess, message_former_from
from db import DBConnCouch

db = DBConnCouch()

REGISTRY_COLUMNS = OrderedDict([(u'№ строки', 'N'),
                                (u'Актуальность строки', 'actual'),
                                (u'№ изменений', 'N_change'),
                                (u'Операция внесения (добавление, изменение, удаление)',
                                 'change_type'),
                                (u'№ объекта', 'N_obj'),
                                (u'Признак комплексного', 'complex'),
                                (u'Вид документа регистрации1)', 'doc_type'),
                                (u'Наличие ГКМ паспорта в группе', 'obj_with_gkm'),
                                (u'Орган регистрации (ТФИ, РГФ, ВСЕГЕИ, ЦНИГРИ, Роснедра, Минприроды, ГСЭ)',
                                 'organ_regs'),
                                (u'Номер документа', 'doc_num'),
                                (u'Дата регистрации', 'doc_date'),
                                (u'Год регистрации (для сортировки)', 'doc_date_num'),
                                ('№ объекта в документе регистрации',
                                 'obj_num_in_doc'),
                                (u'Федеральный округ', 'fed_distr'),
                                (u'Субъект РФ', 'subj_distr'),
                                (u'Административный район', 'adm_distr'),
                                (u'Лист м-ба 1000', '1000_map'),
                                (u'Лист м-ба 200 (араб.)', '200_map'),
                                (u'Вид объекта2)', 'geol_type_obj'),
                                (u'Название объекта', 'name_obj'),
                                (u'Фонд недр (Р-распред., НР-нераспред.)', 'fund'),
                                (u'Вид пользования недрами (ГИН/Р+Д/ГИН+Р+Д)', 'use_type'),
                                (u'Группа ПИ в госпрограмме3)', 'gover_type_pi'),
                                (u'ПИ (перечень для объекта)', 'pi'),
                                (u'Название нормализ.', 'norm_pi'),
                                (u'Название ПИ по ГБЗ', 'gbz_pi'),
                                (u'Группа ПИ ИС недра', 'isnedra_pi'),
                                (u'Ед. измерения ПИ', 'unit_pi'),
                                (u'P3', 'P3_cat'),
                                (u'P2', 'P2_cat'),
                                (u'P1', 'P1_cat'),
                                (u'С2', 'C2_res'),
                                (u'Без категор.', 'none_cat'),
                                (u'Запасы ABC1', 'ABC_res'),
                                (u'Признак наличия ресурсных оценок', 'res_exist'),
                                (u'Наличие прогнозных ресурсов', 'cat_avaibil'),
                                (u'Признак наличия запасов', 'res_avaibil'),
                                (u'Вид документа апробации (протокол, отчет)',
                                 'probe_doc_type'),
                                (u'Номер', 'probe_doc_num'),
                                (u'Дата', 'probe_doc_date'),
                                (u'Орган апробации', 'probe_doc_organ'),
                                (u'№ в таблице координат для полигонов', 'N_poly_table'),
                                (u'Территория органа апробации', 'probe_organ_subj'),
                                (u'Вид координат (Т-точка, П-полигон)', 'coord_type'),
                                (u'Площадь, км2', 'area'),
                                (u'Координата центра X', 'lon'),
                                (u'Координата центра Y', 'lat'),
                                (u'Источник координат4)', 'coord_source'),
                                (u'Входимость в лицензионыый участок', 'license_area'),
                                (u'Достоверность координат', 'coord_reliability'),
                                (u'Координаты треб. проверки', 'coord_for_check'),
                                (u'Данные о районе (для определения координат)',
                                 'territory_descript'),
                                (u'Другие документы об объекте (вид документа, №, год, стадия ГРР, авторы, организация)',
                                 'other_source'),
                                (u'Рекомендуемые работы (оценка ПР, апробация ПР, в фонд заявок, поиски, оценка и др.)',
                                 'recommendations')])

INVERT_REGISTRY_COLUMNS = OrderedDict(
    [(v, k) for k, v in REGISTRY_COLUMNS.items()])

actual_cols = ('_id', '_rev', 'id_reg', 'filename')


class RegistryExc(Exception):
    pass


class RegistryDB:
    '''
        коннектор между базой данных и реестром
    '''
    # получение строк из БД
    def get_rows_by_id(id_reg):
        db_docs = db.get_selected(**{'id_reg': {'$eq': id_reg}})
        return pd.DataFrame(db_docs)

    # обновление названий колонок
    def update_column_names(cols, registry):
        return [cols[c] for c in registry.columns]


class RegistryFormatter:
    '''
        верификация и форматирование реестра для импорта в БД
    '''

    def __init__(self, registry_df, registry_cols_dict):
        self.registry = registry_df
        self.cols = registry_cols_dict
        self.errors = dict()

    # сбор ошибок верификации реестра
    def _append_errors(self, err_name, err_str):
        if err_name in self.errors:
            self.errors[err_name].extend(err_str)
        else:
            self.errors[err_name] = [err_str]

    # проверка наличия ошибок
    def check_errors(self):
        if self.errors:
            mess = message_former_from(self.errors)
            flash_mess(mess)
            raise RegistryExc(mess)

    # удаление переносов и других непробельных символов в названии колонок
    # реестра
    def columns_strip(self):
        pattern = re.compile(r'\s+')
        self.registry.columns = [pattern.sub(
            ' ', c) for c in self.registry.columns]

    # проверка наличия колонок, если отсутсвуют то записать их в
    # соотвествующую ошибку
    def check_columns(self):
        none_cols = [c for c in self.cols.keys()
                     if c not in self.registry.columns]
        if none_cols:
            self._append_errors(
                'В реестре отсутствуют колонки', ', '.join(none_cols))

    # обновление названий колонок для БД
    def update_column_names_for_db(self):
        self.registry.columns = [self.cols[c] for c in self.registry.columns]

    # округление чисел с плавающей точкой
    def fix_float(self):
        for col in self.registry.columns:
            if self.registry[col].dtype == np.float64:
                self.registry[col] = np.round(self.registry[col], 8)

    def format(self):
        self.columns_strip()
        self.check_columns()
        self.check_errors()
        self.fix_float()
        self.update_column_names_for_db()
        self.registry.fillna('', inplace=True)


class RegistryFormatterNew(RegistryFormatter):

    def __init__(self, registry_df):
        RegistryFormatter.__init__(self,
                                   registry_df=registry_df,
                                   registry_cols_dict=REGISTRY_COLUMNS)
        self.dict_cols = ('doc_type', 'organ_regs', 'fed_distr',
                          'subj_distr', 'adm_distr', 'geol_type_obj',
                          'gover_type_pi', 'norm_pi', 'gbz_pi',
                          'isnedra_pi', 'probe_doc_type', 'probe_organ_subj')
        self.date_cols = ('doc_date', 'probe_doc_date')

    def dict_cols_clear(self, column):
        '''
            проверка колонки со справочником
            column -- название колонки
        '''
        pass

    def date_cols_clear(self, column):
        '''
            проверка колонки на содержание только даты
        '''
        pass


class RegistryFormatterUpdate(RegistryFormatter):

    def __init__(self, registry_df, id_reg):
        RegistryFormatter.__init__(self,
                                   registry_df=registry_df,
                                   registry_cols_dict=REGISTRY_COLUMNS)
        self.id_reg = id_reg

        for k in actual_cols:
            self.cols[k] = k

    # получение строк реестра, которые в единственном экземпляре
    def __get_none_duplicates(self):
        duplicates = self.registry.duplicated(keep=False)
        return self.registry[~duplicates]

    def __clear_db_duplicates(self):
        none_duplicates = self.__get_none_duplicates()
        print('len none_duplicates', len(none_duplicates))
        db_rows = RegistryDB.get_rows_by_id(self.id_reg)
        print('len db rows:', len(db_rows))
        db_rows = db_rows.append(none_duplicates)
        db_rows.drop(['N_change', 'actual', 'id_reg', 'filename'],
                     axis=1, inplace=True)
        db_duplicates = db_rows.duplicated(keep=False)
        db_duplicates_id = db_rows.loc[db_duplicates, '_id']
        print('len db_duplicates_id:', len(db_duplicates_id))
        self.registry = self.registry[
            ~self.registry['_id'].isin(db_duplicates_id)]
        if self.registry.empty:
            self._append_errors(
                'Загружаемый реестр не содержит новых строк', '')

    def __former_imp_registry(self, chunk_fields=['actual', 'change_type']):
        concat_df = pd.DataFrame()
        for field in chunk_fields:
            chunk_df = self.registry[self.registry[field] != '']
            concat_df = pd.concat([concat_df, chunk_df])
        concat_df.drop_duplicates(inplace=True)
        self.registry = concat_df

    # проверяет дубликаты актуальных строк, если они есть записывает пары в
    # соответствующую ошибку
    def __check_actual_duplicates(self, n_col='N', checking_cols=['actual', '_id']):
        duplicates = self.registry[self.registry['_id'] != '']
        duplicates = duplicates[checking_cols].duplicated(keep=False)
        if not self.registry[duplicates[duplicates].index].empty:
            duplicates = self.registry[duplicates].groupby(
                checking_cols)[n_col].apply(list).tolist()
            duplicates = ['-'.join(str(row_num)
                                   for row_num in d) for d in duplicates]
            self._append_errors(
                'Строки дублируются (нужно указывать актуальность только для одной строки) ', ', '.join(duplicates))

    def __get_new_rows(self):
        new_rows = self.registry[self.registry['_id'] == '']
        new_rows.drop(['_id', '_rev'], axis=1, inplace=True)
        return new_rows

    def __get_update_rows(self):
        return self.registry[self.registry['_id'] != '']

    def format_actual(self):
        self.format()
        self.__check_actual_duplicates()
        self.check_errors()
        self.__former_imp_registry()
        self.__clear_db_duplicates()
        self.check_errors()

    def split_on_new_update(self):
        self.registry = (self.__get_new_rows(), self.__get_update_rows())


class RegistryDownloader:

    def __init__(self, id_reg, columns):
        self.id_reg = id_reg
        self.cols = columns
        self.registry = RegistryDB.get_rows_by_id(self.id_reg)

    def _deleted_row_handler(self):
        registry_del_rows = self.registry.loc[
            self.registry['change_type'] == 'удаление', '_id']
        self.registry.loc[self.registry['_id'].isin(
            registry_del_rows), 'actual'] = ''

    def _rev_num_field_former(self):
        self.registry['rev_num'] = self.registry[
            '_rev'].str.split('-').str.get(0)

    def _add_type_field_former(self):
        self.registry.loc[self.registry['change_type']
                          == 'добавление', 'N_change'] = 1
        self.registry.loc[self.registry['change_type'] != 'добавление', 'N_change'] = self.registry['rev_num'].apply(
            lambda x: int(x) - 1 if int(x) > 1 else '')
        self.registry.drop('rev_num', axis=1, inplace=True)

    def write_to_excel(self, writer):
        self._deleted_row_handler()
        self._rev_num_field_former()
        self._add_type_field_former()
        self.registry = self.registry[list(self.cols.keys())]
        self.registry.columns = RegistryDB.update_column_names(
            self.cols, self.registry)
        self.registry.to_excel(writer, startrow=2, merge_cells=False,
                               sheet_name='reestr', index=False)


class RegistryDownloaderWork(RegistryDownloader):

    def __init__(self, id_reg):
        RegistryDownloader.__init__(self, id_reg=id_reg,
                                    columns=INVERT_REGISTRY_COLUMNS)

    def _get_row_with_revisions(self):
        registry_revs = self.registry.loc[self.registry[
            'change_type'] == 'изменение', '_id']
        return registry_revs

    def write_revisions_to_registry(self):
        '''
            добавляет ревизии (версии документов) в реестр
        '''
        registry_revs = self._get_row_with_revisions()
        if not registry_revs.empty:
            for _id in registry_revs:
                for rev in db.get_revisions_by_id(_id):
                    self.registry = self.registry.append(
                        rev, ignore_index=True)


class RegistryDownloaderActual(RegistryDownloader):

    def __init__(self, id_reg):
        RegistryDownloader.__init__(self, id_reg=id_reg,
                                    columns=INVERT_REGISTRY_COLUMNS)
        for k in actual_cols:
            self.cols[k] = k
