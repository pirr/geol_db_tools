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
from _help_fun import read_excel, flash_mess
from setup import app, cdb
from views import mango_query
from registry import (RegistryFormatterNew, RegistryFormatterUpdate,
                      RegistryDownloaderWork, RegistryDownloaderActual,
                      RegistryExc)
from db import DBConnCouch


# from logger import log_to_file
ddb = DBConnCouch()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload/<type>', methods=['POST', 'GET'])
def upload_file(type):
    print('upload')
    imp.reload(forms)
    
    if type == 'new':
        title = 'Загрузить новый реестр'
        form = forms.NewUploadForm()
    elif type == 'actual':
        title = 'Актуализировать реестр'
        form = forms.ActualUploadForm()

    if form.validate_on_submit():
        filename = secure_filename(form.file.data.filename)
        form.file.data.save(os.path.join(
            app.config['UPLOAD_FOLDER'], filename))

        if type == 'new':
            session['reg_name'] = form.reg_name.data
        elif type == 'actual':
            session['id_reg'] = form.regs_select.data

        return redirect(url_for('uploads_file', filename=filename, type=type))

    filename = None

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
    '''
    читаем реестр из excel файла
    проверяем реестр на ошибки, форматируем и сохраняем в бд
    '''
    def saver(reg, id_reg):
        if not reg.empty:
            reg['id_reg'] = id_reg
            reg['filename'] = filename
            ddb.bulk_save(reg.to_dict(orient='records'))
            ddb.write_reg_info()

    try:
        data = read_excel(filename)

        if type == 'new':
            reg_format = RegistryFormatterNew(data)
            reg_format.format()
            registry = reg_format.registry
            id_reg, _ = ddb.get_reg_id_info(reg_name=session['reg_name'])
            saver(registry, id_reg)

        elif type == 'actual':
            reg_format = RegistryFormatterUpdate(
                data, id_reg=session['id_reg'])
            reg_format.format_actual()
            reg_format.split_on_new_update()
            registries = reg_format.registry
            ddb.get_reg_id_info(id_reg=session['id_reg'])
            print(registries)
            for reg in registries:
                saver(reg, session['id_reg'])

    except RegistryExc as e:
        return redirect(url_for('upload_file', type=type))

    except Exception as e:
        flash_mess('Ошибка. Обратитесь к администратору.')
        return redirect(url_for('upload_file', type=type))

    return redirect(url_for('regs_list'))


@app.route('/get_download', methods=['GET', 'POST'])
def get_download():
    wtype = request.args.get('wtype')
    id_reg = request.args.get('id_reg')
    return redirect(url_for('download_regist', id_reg=id_reg, wtype=wtype))


@app.route('/download/<id_reg>-<wtype>', methods=['GET', 'POST'])
def download_regist(id_reg, wtype):
    selector = {'id_reg': {'$eq': id_reg}}
    docs = mango_query(cdb, **selector)
    df = pd.DataFrame(docs)
    print(len(df))

    if wtype == 'work':
        registry_downloader = RegistryDownloaderWork(id_reg)
        registry_downloader.write_revisions_to_registry()
    elif wtype == 'actual':
        registry_downloader = RegistryDownloaderActual(id_reg)

    output = BytesIO()
    writer = pd.ExcelWriter(output)
    registry_downloader.write_to_excel(writer)
    writer.close()
    output.seek(0)

    return send_file(output,
                     attachment_filename="{}.xls".format('reestr_' + id_reg),
                     as_attachment=True
                     )


@app.route('/regs')
def regs_list():
    regs_info = cdb['regs_info']
    all_regs = []
    for id_reg in regs_info:
        if id_reg not in ('_id', '_rev'):
            all_regs.append((id_reg, regs_info[id_reg]))
    all_regs.sort(key=lambda x: x[0])
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
