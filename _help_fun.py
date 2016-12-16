import os
import numpy as np
import pandas as pd

from setup import app, REGISTRY_COLUMNS
from flask import flash

from setup import cdb
from views import mango_query


def check_df(df, update=False):
    valid = True
    problems_dict = dict()
    sub = False

    if update:
        valid_cols = set(['Актуальность строки', '№ строки', '_id', '_rev'])
        sub = ['Актуальность строки', '_id']

    else:
        valid_cols = set(['Актуальность строки', '№ строки'])

    if valid_cols & set(df.columns) != valid_cols:
        problems_dict['Нет необходимых колонок'] = list(
            valid_cols - set(df.columns))
        valid = False

    if sub and valid:
        duplicates = df[sub].duplicated(keep=False)
        if not df[duplicates].empty:
            problems_dict['Дубликаты актуальных строк'] = df[duplicates].groupby(sub)[
                '№ строки'].apply(list).tolist()

    if problems_dict:
        valid = False

    return problems_dict, valid


def former_df(df, chunk_fields):
    concat_df = pd.DataFrame()

    for field in chunk_fields:
        chunk_df = df[~pd.isnull(df[field])]
        concat_df = pd.concat([concat_df, chunk_df])
    concat_df.drop_duplicates(inplace=True)

    return concat_df


# TODO list formatter
def message_former_from(message_dict):
    message = '\n'.join(
        ': '.join([pk, str(pv)]) for pk, pv in message_dict.items()
    )

    return message


def read_excel(filename, actual=False):
    try:
        f = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    except Exception as e:
        flash('''Возникли проблемы на стороне сервера обратитесь к администратору''', category='error')
        raise e
    try:
        df = pd.read_excel(f, sheetname='reestr', skiprows=2, 
        converters={'Дата регистрации':str,'Дата':str, '№ объекта в документе регистрации': str})
    except Exception as e:
        flash('''Проблемы при чтении файла. Возможно в файле {} нет листа reestr'''.format(
            filename), category='error')
        os.remove(f)
        print(str(e))
        raise e

    # TODO validate function
    if actual:
        df.columns = [REGISTRY_COLUMNS + ['_id', '_rev']]
        df_new_rows = df[pd.isnull(df['_id'])]
    else:
        df.columns = [REGISTRY_COLUMNS]
        df_new_rows = df

    problems_array = []
    if not df_new_rows.empty:
        problems_1, valid = check_df(df_new_rows, update=False)
        if not valid:
            problems_array.append(problems_1)

    if actual:
        df_update_rows = df[~pd.isnull(df['_id'])]
        if not df_update_rows.empty:
            problems_2, valid = check_df(df_update_rows, update=True)
            if not valid:
                problems_array.append(problems_2)

    if problems_array:
        problems = dict()
        for probl_dict in problems_array:
            for p in list(probl_dict):
                if p in problems:
                    problems[p].extend(probl_dict[p])
                else:
                    problems[p] = probl_dict[p]

        flash(message_former_from(problems), category='error')
        print(message_former_from(problems))
        os.remove(f)
        raise Exception('Invalid registry file')

    df = former_df(df, ['Актуальность строки',
                        'Операция внесения (добавление, изменение, удаление)'])
    df['filename'] = filename.split('.')[0]

    # TODO function
    if actual:
        duplicates = df['_id'].duplicated(keep=False)
        none_duplicates = df[~duplicates]
        print(len(none_duplicates))

        selector = {'filename': {'$eq': filename.split('.')[0]}}
        docs = mango_query(cdb, **selector)
        df_db = pd.DataFrame(docs)
        df_db = df_db.append(none_duplicates)
        df_db = df_db[REGISTRY_COLUMNS + ['_id', '_rev']].drop(['№ изменений', 'Актуальность строки'], axis=1)
        print(len(df_db))
        
        df_db.fillna('', inplace=True)
        db_duplicates = df_db.duplicated(keep=False)
        db_dupl_id = df_db.loc[db_duplicates, '_id']
        print(len(db_dupl_id))
        df = df[~df['_id'].isin(db_dupl_id)]
    
    if df.empty:
        flash('Реестр пуст или нет новых строк')
        raise Exception('Реестр пуст или нет новых строк')

    return df
