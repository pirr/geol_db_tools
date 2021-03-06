#-*- coding: utf-8 -*-


import importlib as imp
import os
from datetime import datetime
from io import BytesIO
from shutil import move
from collections import OrderedDict
from functools import wraps

import sys
import pandas as pd
from flask import (request, redirect, url_for,
                   render_template, send_from_directory,
                   send_file, session)
from werkzeug.utils import secure_filename


import forms
from _help_fun import flash_mess
from setup import app, cdb
from views import mango_query
from registry import (RegistryFormatterNew, RegistryFormatterUpdate,
                      RegistryDownloaderWork, RegistryDownloaderActual,
                      RegistryExc, read_excel, delete_registry)
from db import DBConnCouch


app = app
FILTERED_FIELDS = OrderedDict(
    [('norm_pi', 'ПИ'), ('geol_type_obj', 'Вид объекта')])
ddb = DBConnCouch()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash_mess('Необходимо войти в систему')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload/<type>', methods=['POST', 'GET'])
@login_required
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
@login_required
def uploads_file(filename, type):
    send_from_directory(app.config['UPLOAD_FOLDER'], filename)
    filename_list = filename.split('.')
    upload_filename = '{}_{}.{}'.format(filename_list[0], datetime.now().strftime("%Y-%m-%d_%H-%M-%S"), filename_list[-1])
    move(os.path.join(app.config['UPLOAD_FOLDER'], filename),
         os.path.join(app.config['UPLOAD_FOLDER'], upload_filename))

    return redirect(url_for('import_file', filename=upload_filename, type=type))


@app.route('/import/<filename>-<type>')
@login_required
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
            for reg in registries:
                saver(reg, session['id_reg'])

    except RegistryExc as e:
        return redirect(url_for('upload_file', type=type))

    except Exception as e:
        print(e, file=sys.stderr)
        flash_mess('Ошибка. Обратитесь к администратору.')
        return redirect(url_for('upload_file', type=type))

    return redirect(url_for('regs_list'))


@app.route('/get_download', methods=['GET', 'POST'])
def get_download():
    print(request.args)
    wtype = request.args.get('wtype')
    id_reg = request.args.get('id_reg')

    return redirect(url_for('download_regist', id_reg=id_reg, wtype=wtype))


@app.route('/download/<id_reg>-<wtype>', methods=['GET', 'POST'])
def download_regist(id_reg, wtype):

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


@app.route('/regs/delete-<id_reg>', methods=['GET', 'POST'])
@login_required
def delete_reg(id_reg):
    delete_registry(id_reg)
    return redirect(url_for('regs_list'))


@app.route('/get_filters', methods=['GET', 'POST'])
def filters():
    filters_dict = OrderedDict()
    '''
        получение значений из БД для составления фильтров
    '''
    # получить из БД значения
    for field, field_name in FILTERED_FIELDS.items():
        #
        # print([doc for doc in ddb.conn][0].items())
        filters_dict[field_name] = sorted(list({doc['doc'][field] for doc in ddb.conn.view(
            '_all_docs', include_docs=True) if field in doc['doc']}))
        print(len(filters_dict[field_name]))
    # отфильтровать значения в соответсвии с запросом
    # записать значения фильтра в хэшированный список фильтров (словарь)
    # вернуть в темплейт список фильтров
    return render_template('filters.html', filters=filters_dict)


@app.route('/all_rows')
def all_rows():
    rows = cdb.view('_all_docs', include_docs=True)

    return render_template('rows.html', rows=rows)


@app.route('/register', methods=['GET', 'POST'])
def register():
    form = forms.RegistrationForm(request.form)

    if request.method == 'POST' and form.validate():
        user = (form.username.data,
                form.email.data,
                form.password.data)
        ddb.add_user(*user)
        flash_mess('Введит зарегистрированые имя и пароль')
        return redirect(url_for('login'))

    return render_template('register.html', form=form)

users = {'foo': {'password': 'secret'}}


@app.route('/login', methods=['GET', 'POST'])
def login():
    form = forms.LoginForm()

    if form.validate_on_submit():
        username = form.username.data
        if (username not in users) or (form.password.data != users[username]['password']):
            flash_mess('Неверное имя пользователя или пароль')
            return redirect(url_for('login'))

        session['username'] = username
        return redirect(url_for('upload_file', type='new'))

    return render_template('login_form.html', form=form)


@app.route('/logout', methods=['GET', 'POST'])
@login_required
def logout():
    session.clear()

    return redirect(url_for('login'))


@app.errorhandler(404)
def page_not_found(e):

    return render_template('404.html'), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0")
