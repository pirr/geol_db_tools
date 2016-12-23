#-*- coding: utf-8 -*-


import importlib as imp
import os
from datetime import datetime
from io import BytesIO
from shutil import move

import numpy as np
import pandas as pd
from flask import (request, redirect, url_for,
                   render_template, send_from_directory,
                   send_file, session)
from werkzeug.utils import secure_filename

import forms
from _help_fun import read_excel
from setup import app, cdb, _REGISTRY_COLUMNS
from views import mango_query


# from logger import log_to_file


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload/<type>', methods=['GET', 'POST'])
def upload_file(type):
    imp.reload(forms)
    if type == 'actual':
        form = forms.ActualUploadForm()
    else:
        form = forms.NewUploadForm()
    if form.validate_on_submit():
        filename = secure_filename(form.file.data.filename)
        form.file.data.save(os.path.join(
            app.config['UPLOAD_FOLDER'], filename))

        if type == 'new':
            session['reg_name'] = form.reg_name.data
        else:
            session['id_reg'] = form.regs_select.data

        return redirect(url_for('uploads_file', filename=filename, type=type))

    filename = None

    if type == 'new':
        title = 'Загрузить новый реестр'
    else:
        title = 'Актуализировать реестр'

    return render_template('upload_form.html',
                           form=form,
                           filename=filename,
                           type=type,
                           title=title)


@app.route('/uploads/<filename>-<type>')
def uploads_file(filename, type):
    send_from_directory(app.config['UPLOAD_FOLDER'], filename)

    if type == 'actual':
        move(os.path.join(app.config['UPLOAD_FOLDER'], filename),
             os.path.join(app.config['UPLOAD_FOLDER'], filename))
        # filename = session['reg_name'] + '.xls'

    return redirect(url_for('import_file', filename=filename, type=type))


@app.route('/import/<filename>-<type>')
def import_file(filename, type):
    try:
        regs_info = cdb['regs_info']
        t = datetime.now().strftime("%Y-%m-%d_%H-%M")
        dfs = []

        if type == 'actual':
            data = read_excel(filename, actual=True)
            
            df_new_rows = data[pd.isnull(data['_id'])]
            df_new_rows.drop(['_id', '_rev'], axis=1, inplace=True)
            if not df_new_rows.empty:
                dfs.append(df_new_rows)
            df_updated_rows = data[~pd.isnull(data['_id'])]

            if not df_updated_rows.empty:
                dfs.append(df_updated_rows)
            id_reg = session['id_reg']
            regs_info[id_reg]['modified'] = t

        else:
            data = read_excel(filename, actual=False)
            dfs.append(data)
            reg_ids = [int(id) for id in list(regs_info) if id not in ('_id', '_rev')]
            if reg_ids:
                id_reg = str(max(reg_ids) + 1)
            else:
                id_reg = '1'
            regs_info[id_reg] = {'created': t,
                                 'modified': '',
                                 'reg_name': session['reg_name']}

        if dfs:
            for df in dfs:
                try:
                    df.fillna('', inplace=True)
                    df['id_reg'] = id_reg
                    df.replace('nan', '', inplace=True)
                    data_dict = df.to_dict(orient='records')
                    res = cdb.update(data_dict)
                except Exception as e:
                    raise e

            cdb['regs_info'] = regs_info

    except Exception as e:
        # os.remove(filename)
        # return redirect(url_for('upload_file', type=type))
        raise e

    return redirect(url_for('regs_list'))


@app.route('/download/<id_reg>-<with_revs>', methods=['GET', 'POST'])
def download_regist(id_reg, with_revs):
    selector = {'id_reg': {'$eq': id_reg}}
    docs = mango_query(cdb, **selector)
    # db_cols = list(_REGISTRY_COLUMNS.keys()) + ['_id', '_rev']
    df = pd.DataFrame(docs)
    print(len(df))

    if with_revs == 'yes':
        cols = list(_REGISTRY_COLUMNS.keys())

        df_revs = df.loc[(df['N_change'].astype(str) != '') & (
            df['change_type'] != 'удаление'), '_id']

        if not df_revs.empty:
            print('revs!')

            for _id in df_revs:
                for i, rev in enumerate(cdb.revisions(_id)):
                    if i:
                        df_rev = pd.DataFrame(
                            {k: v for k, v in rev.items()}, index=[0])
                        df = df.append(df_rev, ignore_index=True)

    else:
        cols = list(_REGISTRY_COLUMNS.keys()) + ['_id', '_rev', 'id_reg', 'filename']

    df_deleted = df.loc[
        df['change_type'] == 'удаление', '_id']
    df.loc[df['_id'].isin(df_deleted), 'actual'] = ''

    df['rev_num'] = df['_rev'].str.split('-').str.get(0)
    df.at[df['change_type'] == 'добавление', 'N_change'] = 1
    df.at[df['change_type'] != 'добавление', 'N_change'] = df['rev_num'].apply(
        lambda x: int(x) - 1 if int(x) > 1 else np.nan)
    df.drop('rev_num', axis=1, inplace=True)

    output = BytesIO()
    writer = pd.ExcelWriter(output)

    df = df[cols]
    df.columns = [c if c in ('_id', '_rev', 'id_reg', 'filename') else _REGISTRY_COLUMNS[
        c] for c in cols]
    df.to_excel(writer, startrow=2, merge_cells=False,
                sheet_name='reestr', index=False)
    writer.close()
    output.seek(0)

    return send_file(output,
                     attachment_filename="{}.xls".format('reestr_'+id_reg),
                     as_attachment=True
                     )


@app.route('/get_download', methods=['GET', 'POST'])
def get_download():
    with_revs = request.args.get('withRevs')
    id_reg = request.args.get('reg_name')
    return redirect(url_for('download_regist', id_reg=id_reg, with_revs=with_revs))


@app.route('/regs')
def regs_list():
    regs_info = cdb['regs_info']
    all_regs = []
    for id_reg in regs_info:
        if id_reg not in ('_id', '_rev'):
            all_regs.append((id_reg, regs_info[id_reg]))
    # all_regs = [reg for reg in cdb['regs_info'] if reg not in ('_id', '_rev')]

    return render_template('all_dbs.html', dbs=all_regs)


@app.route('/all_rows')
def all_rows():
    rows = cdb.view('_all_docs', include_docs=True)

    return render_template('rows.html', rows=rows)


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
