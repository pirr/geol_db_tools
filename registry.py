# -*- coding: utf-8 -*-


import re
import numpy as np
import pandas as pd


def message_former_from(message_dict):
    message = '\n'.join(
        ': '.join([pk, str(pv)]) for pk, pv in message_dict.items()
    )

    return message

class RegistryExc(Exception):
    pass


class Registry:
    def __init__(self, registry_df, registry_cols_dict, actual_cols_list=False):
        self.registry = registry_df
        self.errors = dict()
        self.cols = registry_cols_dict
        self.actual_cols = actual_cols_list

        self.make_registry_for_import()
        
    def __append_errors(self, err_name, err_str):
        if err_name in self.errors:
            self.errors[err_name].extend(err_str)
        else:
            self.errors[err_name] = [err_str]

    def _columns_strip(self):
        pattern = re.compile(r'\s+')
        self.registry.columns = [pattern.sub(' ', c) for c in self.registry.columns]

    def __check_columns(self, *cols):
        none_cols = [c for c in cols if c not in self.registry.columns]
        if none_cols:
            self.__append_errors('В реестре отсутствуют колонки', ', '.join(none_cols))

    def _check_registry_cols(self):
        self.__check_columns(*self.cols.keys())

    def _check_actual_cols(self):
        self.__check_columns(*self.actual_cols)

    def update_column_names_for_db(self):
        self.registry.columns = [c if c in self.actual_cols else self.cols[c] for c in self.registry.columns]

    def fix_float(self):
        for col in self.registry.columns:
            if self.registry[col].dtype == np.float64:
                self.registry[col] = np.round(self.registry[col], 8)

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

    def registry_errors(self):
        self._columns_strip()
        self._check_registry_cols()
        if self.actual_cols:
            self._check_actual_cols()
            self._check_actual_duplicates('N', 'actual', '_id')
        if self.errors:
            raise RegistryExc

    def make_registry_for_import(self):
        self.registry_errors()
        self.update_column_names_for_db()
        self.former_imp_registry('actual', 'change_type')
        self.fix_float()
        self.registry.fillna('', inplace=True)



