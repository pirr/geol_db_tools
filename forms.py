#-*- coding: utf-8 -*-

import os
from flask import session
from flask_wtf import FlaskForm
from wtforms import SelectField, StringField
from flask_wtf.file import FileField, FileAllowed, FileRequired
from setup import app, cdb


ALLOWED_EXTENSIONS = ['xls', 'xlsx']
ALL_REGS = [reg for reg in cdb['regs_info'] if reg not in ('_id', '_rev')]

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


class NewUploadForm(FlaskForm):
    file = FileField('Выберите файл (только лат.)')
    reg_name = StringField('Введите название реестра')

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
            self.file.errors.append('Только {} файлы'.format(', '.join(ALLOWED_EXTENSIONS)))
            return False
        
        if reg_name in ALL_REGS:
            self.reg_name.errors.append('{} - такой реестр уже существует'.format(reg_name))
            return False
        
        self.file.errors = tuple(self.file.errors)
        self.reg_name.errors = tuple(self.reg_name.errors)

        return True

    
class ActualUploadForm(FlaskForm):
    file = FileField('Выберите файл (только лат.)')
    regs_select = SelectField('Выберите реестр для актуализации', choices=[('', '---')] + [(reg, reg) for reg in ALL_REGS])

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
            self.file.errors.append('Только {} файлы'.format(', '.join(ALLOWED_EXTENSIONS)))
            return False
        
        if reg_name not in ALL_REGS:
            self.regs_select.errors.append('Выберите реестр для обновления из выпадающего списка.'.format(reg_name))
            return False
        
        self.file.errors = tuple(self.file.errors)
        self.regs_select.errors = tuple(self.regs_select.errors)

        return True




# class RequestFormIzuch(FlaskForm):
#     authors_filter = StringField('Авторы документа')
#     text = StringField('Текст')


# class UserForm(FlaskForm):
#     email = StringField('Email', [validators.Length(min=6, max=35)])
#     password = PasswordField('Пароль', [
#         validators.DataRequired(message='Введите пароль'),
#         validators.EqualTo('confirm',
#                            message='Пароль должен совпадать')
#     ])
#     confirm = PasswordField('Повторить пароль')


# class RegisterForm(UserForm):

#     def validate(self):
#         rv = FlaskForm.validate(self)
#         if not rv:
#             return False
#         user = User.query.filter_by(email=self.email.data).first()

#         if user is not None:
#             self.email.errors.append('Этот адрес уже зарегестрирован')
#             return False

#         return True


# class EditForm(UserForm):

#     def validate(self):
#         rv = FlaskForm.validate(self)
#         if not rv:
#             return False

#         # проверка на совпадение email другого пользователя
#         check_user = User.query.filter_by(email=self.email.data).first()
#         if check_user and check_user.id != session['user']:
#             self.email.errors.append('Этот адрес уже зарегестрирован')
#             return False

#         return True


# class LoginForm(FlaskForm):
#     email = StringField('Email', [validators.DataRequired(
#         message='Введите электронный адрес')])
#     password = PasswordField(
#         'Пароль', [validators.DataRequired(message='Введите пароль')])

#     def validate(self):
#         rv = FlaskForm.validate(self)
#         if not rv:
#             return False

#         user = User.query.filter_by(
#             email=self.email.data, password=self.password.data).first()
#         if user is None:
#             self.email.errors.append('Неверный адрес или пароль')
#             return False

#         return True
