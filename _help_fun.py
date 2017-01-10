import os
import re
import numpy as np
import pandas as pd

from setup import app, REGISTRY_COLUMNS
from flask import flash, session


def flash_mess(mess):
    flash(mess, category='error')


def message_former_from(message_dict):
    message = '\n'.join(
        ': '.join([pk, ', '.join(pv)]) for pk, pv in message_dict.items()
    )

    return message


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
