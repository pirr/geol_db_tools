import os
import pandas as pd

from setup import app
from flask import flash


def check_df(df, update=False):
    valid = True
    problems_dict = dict()
    sub = False

    if update:
        valid_cols = set(['1а', '1', '_id', '_rev'])
        sub = ['1а', '_id']

    else:
        valid_cols = set(['1а', '1'])

    if valid_cols & set(df.columns) != valid_cols:
        problems_dict['Нет необходимых колонок'] = list(
            valid_cols - set(df.columns))
        valid = False

    if sub and valid:
        duplicates = df[sub].duplicated(keep=False)
        if not df[duplicates].empty:
            problems_dict['Дубликаты актуальных строк'] = df[duplicates].groupby(sub)[
                '1'].apply(list).tolist()

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
        # xls = xlrd.open_workbook(f, on_demand=True)
        # print(xls.sheet_names())
        df = pd.read_excel(f, sheetname='reestr', skiprows=2)

    except Exception as e:
        flash('''Проблемы при чтении файла. Возможно в файле {} нет листа reestr'''.format(
            filename), category='error')
        os.remove(f)
        print(str(e))
        raise e

    df.columns = [str(c) for c in df.columns]
    
    # TODO function
    problems_array = []
    df_new_rows = df[pd.isnull(df['_id'])]
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

    df = former_df(df, ['1а', 'change_info'])
    df['filename'] = filename.split('.')[0]

    return df


# def make_geol_docs(row):
#     df = df[[c for c in list(df.columns) if c not in[10, 34, 35, 36, 39]]]
#     pi_cols = [c for c in df.columns if c in [i for i in range(22, 34)]]
#     df = df.replace(np.nan, -1)
#     df_gr = df.groupby(
#         [c for c in df.columns if c not in pi_cols + ['1а', 1, 2, 3]], sort=False)
#     row_num_lists = df_gr[1].apply(list).as_matrix()
#     actuals_lists = df_gr['1а'].apply(list).as_matrix()
#     changes_num_lists = df_gr[2].apply(list).as_matrix()
#     operat_changes_lists = df_gr[3].apply(list).as_matrix()

#     pis_lists = list(zip(*[df_gr[c].apply(list).as_matrix() for c in pi_cols]))
#     pis_ress = []
#     for pis_list in pis_lists:
#         pis_dicts = [{'type_pi': r[0],
#                       'raw_name_pi': r[1],
#                       'norm_name_pi': r[2],
#                       'gbz_name_pi': r[3],
#                       'is_nedra': r[4],
#                       'unit': r[5],
#                       'P3': r[6],
#                       'P2': r[7],
#                       'P1': r[8],
#                       'C2': r[9],
#                       'non_cat': r[10],
#                       'ABC1': r[11]} for r in list(zip(*pis_list))]
#         for pi in pis_dicts:
#             for key in list(pi):
#                 if pi[key] == -1:
#                     del pi[key]
#         pis_ress.append(pis_dicts)

#     res_df = df_gr.apply(list).reset_index().drop(0, axis=1)
#     res_df['pis'] = pis_ress
#     res_df['row_num'] = row_num_lists
#     res_df['actual'] = actuals_lists
#     res_df['changes_num'] = changes_num_lists
#     res_df['operate_changes'] = operat_changes_lists

#     res_df = res_df.replace(-1, np.nan)
