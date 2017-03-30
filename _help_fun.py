import os
import pandas as pd

from setup import app
from flask import flash


def flash_mess(mess):
    flash(mess, category='error')


def message_former_from(message_dict):
    messages = []
    for mess_name, mess in message_dict.items():
        mess_str = ', '.join(mess)
        if mess_str:
            messages.append(': '.join([mess_name, mess_str]) + '.')
        else:
            messages.append(mess_name + '.')

    return '\n'.join(messages)


def read_excel(filename, actual=False):
    try:
        f = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    except Exception as e:
        flash_mess('''Возникли проблемы на стороне сервера обратитесь к администратору''')
        raise e
    try:
        df = pd.read_excel(f, sheetname='reestr', skiprows=2,
                           converters={'Дата регистрации': str, 'Дата': str, '№ объекта в документе регистрации': str})
    except Exception as e:
        flash_mess('''Проблемы при чтении файла. Возможно в файле {} нет листа reestr'''.format(
            filename))
        os.remove(f)
        raise e

    return df
