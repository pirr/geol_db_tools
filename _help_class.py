# -*- coding: utf-8 -*-

import os
from datetime import datetime

import pandas as pd
from flask import flash

from registry import Registry, RegistryExc


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

_REGISTRY_COLUMNS = OrderedDict([(v, k) for k, v in REGISTRY_COLUMNS.items()])
actual_cols = ('_id', '_rev', 'id_reg', 'filename')


def mango_query(db, **kwargs):
    cdb = db.name
    selector = {'selector': kwargs, 'limit': 100000}
    r = requests.post('/'.join([COUCH_URL, cdb, '_find']), json=selector)
    return r.json()['docs']

def flash_mess(mes):
    flash(mes,category='error')

def message_former_from(message_dict):
    message = '\n'.join(
        ': '.join([pk, str(pv)]) for pk, pv in message_dict.items()
    )

    return message


class RegistryImporter:

    def __init__(self, upload_folder, filename, db, session, actual=False):
        self.upload_folder = upload_folder
        self.filename = filename
        self.db = db
        self.session = session
        self.actual = actual

        self.regs_info = self.db['regs_info']
        self.id_reg = self.get_id_reg()
        
        self.registry_inst = self.get_registry_inst()
        self.registry_inst['id_reg'] = self.id_reg
        self.registry_inst['filename'] = self.filename.split('.')[0]
        
        if self.actual:
            self.drop_db_duplicates()
            self.import_registry = self.get_splited_registry()
        else:
            self.import_registry = [self.registry_inst]

    def __read_excel(self):
        try:
            f = os.path.join(self.upload_folder, self.filename)

        except Exception as e:
            flash_mess('''Возникли проблемы на стороне сервера обратитесь к администратору''')
            os.remove(f)
            raise e
        try:
            # Столбцы реестра: Дата регистрации и Дата содержат смешанные значения - даты, числа
            df = pd.read_excel(f, sheetname='reestr', skiprows=2,
                            converters={'Дата регистрации': str, 'Дата': str, '№ объекта в документе регистрации': str})
        except Exception as e:
            flash_mess('''Проблемы при чтении файла. Возможно в файле {} нет листа reestr'''.format(
                self.filename))
            os.remove(f)
            raise e
        return df

    def get_registry_inst(self):
        registry_df = self.__read_excel()
        if self.actual:
            actual_cols = actual_cols
        else:
            actual_cols = False
        registry_inst = Registry(registry_df, REGISTRY_COLUMNS, actual_cols)
        try:
            registry_inst.make_registry_for_import()
        except RegistryExc:
            flash_mess(message_former_from(registry_inst.errors))
        except Exception:
            flash_mess('Неизвестная ошибка, обратитесь к администратору')
        
        return registry_inst
    
    def drop_db_duplicates(self):
        duplicates = self.registry_inst['_id'].duplicated(keep=False)
        none_duplicates = self.registry_inst[~duplicates]
        print(len(none_duplicates))

        selector = {'id_reg': {'$eq': self.id_reg}}
        docs = mango_query(self.db, **selector)
        df_db = pd.DataFrame(docs)
        df_db = df_db.append(none_duplicates)
        df_db = df_db.drop(['N_change', 'actual', 'id_reg'], axis=1)
        print(len(df_db))

        df_db.fillna('', inplace=True)
        db_duplicates = df_db.duplicated(keep=False)
        db_dupl_id = df_db.loc[db_duplicates, '_id']
        print(len(db_dupl_id))
        self.registry_inst = self.registry_inst[~self.registry_inst['_id'].isin(db_dupl_id)]

    def get_splited_registry(self):
        splited_registry = []
        new_rows = self.registry_inst[pd.isnull(self.registry_inst['_id'])]
        new_rows.drop(['_id', '_rev'], axis=1, inplace=True)
        if not new_rows.empty:
            splited_registry.append(new_rows)
        updated_rows = self.registry_inst[~pd.isnull(self.registry_inst['_id'])]
        if not updated_rows.empty:
            splited_registry.append(updated_rows)
        return splited_registry

    def get_id_reg(self):
        if self.actual and 'id_reg' in self.session:
            return self.session['id_reg']
        else:
            reg_ids = [int(id) for id in list(self.regs_info)
                       if id not in ('_id', '_rev')]
            if reg_ids:
                return str(max(reg_ids) + 1)
        return '1'

    def info_writer(self):
        t = datetime.now().strftime("%Y-%m-%d_%H-%M")
        if self.actual:
            self.regs_info[self.id_reg]['modified'] = t
        else:
            self.regs_info[self.id_reg] = {'created': t,
                                            'modified': '',
                                            'reg_name': self.session['reg_name']
                                        }
        self.db['regs_info'] = self.regs_info

    def make_import(self):
        data_dict = self.import_registry
        try:
            self.db.update(data_dict)
        except Exception as e:
            flash_mess('Ошибка базы данных')
            raise e




        