#-*- coding: utf-8 -*-


import os
import json
from shutil import move

from io import BytesIO
from datetime import datetime
from flask import (request, redirect, url_for,
                   render_template, send_from_directory,
                   session, send_file, session)
import requests
# from forms import RequestForm, RequestFormIzuch
# from manage import User
from forms import NewUploadForm, ActualUploadForm
from werkzeug.utils import secure_filename
from setup import app, db, couch, cdb, REGISTRY_COLUMNS
import numpy as np
import pandas as pd
# import xlrd

from views import mango_query
from _help_fun import read_excel
# from logger import log_to_file


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload/<type>', methods=['GET', 'POST'])
def upload_file(type):
    if type == 'actual':
        form = ActualUploadForm()
    else:
        form = NewUploadForm()
    if form.validate_on_submit():
        filename = secure_filename(form.file.data.filename)
        # filename = filename.split('.')
        # filename = ''.join(filename[:-1])\
        #     + '_' + datetime.now().strftime("%Y-%m-%d_%H-%M")\
        #     + '.' + filename[-1]
        # print(filename)
        form.file.data.save(os.path.join(
            app.config['UPLOAD_FOLDER'], filename))

        if type == 'actual':
            session['reg_name'] = form.regs_select.data

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
             os.path.join(app.config['UPLOAD_FOLDER'], session['reg_name'] + '.xls'))
        filename = session['reg_name'] + '.xls'

    return redirect(url_for('import_file', filename=filename, type=type))


@app.route('/import/<filename>-<type>')
def import_file(filename, type):
    try:

        if type == 'actual':
            actual = True
        else:
            actual = False

        data = read_excel(filename, actual=actual)

        dfs = []

        if actual:
            df_new_rows = data[pd.isnull(data['_id'])]
            df_new_rows.drop(['_id', '_rev'], axis=1, inplace=True)
            if not df_new_rows.empty:
                dfs.append(df_new_rows)
            df_updated_rows = data[~pd.isnull(data['_id'])]

            if not df_updated_rows.empty:
                dfs.append(df_updated_rows)
        else:
            dfs.append(data)

        if dfs:
            for df in dfs:
                try:
                    df.fillna('', inplace=True)
                    df.replace('nan', '', inplace=True)
                    data_dict = df.to_dict(orient='records')
                    res = cdb.update(data_dict)
                except Exception as e:
                    raise e

            regs_info = cdb['regs_info']
            reg_name = filename.split('.')[0]
            t = datetime.now().strftime("%Y-%m-%d_%H-%M")

            if type == 'new':
                regs_info[reg_name] = {'created': t,
                                       'modified': t}
            else:
                regs_info[reg_name]['modified'] = t

            cdb['regs_info'] = regs_info

    except Exception as e:
        return redirect(url_for('upload_file', type=type))

    return redirect(url_for('regs_list'))


@app.route('/download/<reg_name>-<with_revs>', methods=['GET', 'POST'])
def download_regist(reg_name, with_revs):
    selector = {'filename': {'$eq': reg_name}}
    docs = mango_query(cdb, **selector)
    df = pd.DataFrame(docs)

    if with_revs == 'yes':

        df_revs = df.loc[~pd.isnull(df['№ изменений']), '_id']

        if not df_revs.empty:
            print('revs!')
            
            for _id in df_revs:
                for i, rev in enumerate(cdb.revisions(_id)):
                    if i:
                        ref_dv = pd.DataFrame({k: v for k, v in rev.items()}, index=[0])
                        df = df.append(ref_dv, ignore_index=True)
        df_deleted = df.loc[df['Операция внесения (добавление, изменение, удаление)'] == 'удаление', '_id']
        df.loc[df['_id'].isin(df_deleted), 'Актуальность строки'] = ''
    
    df['rev_num'] = df['_rev'].str.split('-').str.get(0)
    df['№ изменений'] = df['rev_num'].apply(lambda x: int(x) - 1 if int(x) > 1 else np.nan)

    output = BytesIO()
    writer = pd.ExcelWriter(output)

    df[REGISTRY_COLUMNS + ['_id', '_rev']].to_excel(writer, startrow=2, merge_cells=False,
                sheet_name='reestr', index=False)
    writer.close()
    output.seek(0)

    return send_file(output,
                     attachment_filename="{}.xls".format(reg_name),
                     as_attachment=True
                     )


@app.route('/get_download', methods=['GET', 'POST'])
def get_download():
    with_revs = request.args.get('withRevs')
    reg_name = request.args.get('reg_name')
    return redirect(url_for('download_regist', reg_name=reg_name, with_revs=with_revs))


@app.route('/regs')
def regs_list():
    all_regs = [reg for reg in cdb['regs_info'] if reg not in ('_id', '_rev')]

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
