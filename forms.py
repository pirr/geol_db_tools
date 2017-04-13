#-*- coding: utf-8 -*-


from flask_wtf import FlaskForm
from flask_wtf.file import FileField
from wtforms import SelectField, StringField, PasswordField, validators

from setup import cdb

ALLOWED_EXTENSIONS = ['xls', 'xlsx']
# all_regs = [reg for reg in cdb['regs_info'] if reg not in ('_id', '_rev')]


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

def all_regs(regs_info='regs_info'):
    return [reg for reg in cdb['regs_info'] if reg not in ('_id', '_rev')]


class NewUploadForm(FlaskForm):
    file = FileField('Выберите файл (только лат.)')
    reg_name = StringField('Введите название реестра')
    all_regs = all_regs()

    def validate(self):
        filename = self.file.data.filename
        reg_name = self.reg_name.data
        self.file.errors = list(self.file.errors)
        self.reg_name.errors = list(self.reg_name.errors)
        # if filename in os.listdir(app.config['UPLOAD_FOLDER']):
        #     self.file.errors.append('Файл с таким именем уже существует')
        #     return False
        if filename.strip() == '':
            self.file.errors.append('Файл не выбран')
            return False

        if reg_name.strip() == '':
            self.reg_name.errors.appens('Введите название реестра')
            return False

        if not allowed_file(filename):
            self.file.errors.append('Только {} файлы'.format(
                ', '.join(ALLOWED_EXTENSIONS)))
            return False

        if reg_name in self.all_regs:
            self.reg_name.errors.append(
                '{} - такой реестр уже существует'.format(reg_name))
            return False

        self.file.errors = tuple(self.file.errors)
        self.reg_name.errors = tuple(self.reg_name.errors)

        return True


class ActualUploadForm(FlaskForm):
    file = FileField('Выберите файл (только лат.)')
    all_regs = all_regs()
    regs_select = SelectField('Выберите реестр для актуализации', choices=[(
        '', '---')] + [(reg, cdb['regs_info'][reg]['reg_name']) for reg in all_regs])

    def validate(self):
        filename = self.file.data.filename
        reg_name = self.regs_select.data
        self.file.errors = list(self.file.errors)
        self.regs_select.errors = list(self.regs_select.errors)
        # if filename in os.listdir(app.config['UPLOAD_FOLDER']):
        #     self.file.errors.append('Файл с таким именем уже существует')
        #     return False
        if filename.strip() == '':
            self.file.errors.append('Файл не выбран')
            return False

        if not allowed_file(filename):
            self.file.errors.append('Только {} файлы'.format(
                ', '.join(ALLOWED_EXTENSIONS)))
            return False

        if reg_name not in self.all_regs:
            self.regs_select.errors.append(
                'Выберите реестр для обновления из выпадающего списка.'.format(reg_name))
            return False

        self.file.errors = tuple(self.file.errors)
        self.regs_select.errors = tuple(self.regs_select.errors)

        return True

class RegistrationForm(FlaskForm):
    username = StringField('Имя пользователя', [validators.Length(min=4, max=25)])
    email = StringField('Email', [validators.Length(min=6, max=35)])
    password = PasswordField('Пароль', [
        validators.DataRequired(),
        validators.EqualTo('confirm', message='Пароль должен совпадать')
    ])
    confirm = PasswordField('Повторить пароль')


class LoginForm(FlaskForm):
    username = StringField('Имя пользователя', [validators.DataRequired('Введите имя')])
    password = PasswordField('Пароль', [validators.DataRequired('Ввведите пароль')])
