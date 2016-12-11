#-*- coding: utf-8 -*-

import os
from flask import session
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed, FileRequired
from setup import app
# from wtforms import StringField, PasswordField, validators


ALLOWED_EXTENSIONS = ['xls', 'xlsx']


class UploadForm(FlaskForm):
    file = FileField('Название файла (только лат.)', validators=[
        FileRequired('Файл не выбран'), FileAllowed(ALLOWED_EXTENSIONS,
        'Только {} файлы'.format(', '.join(ALLOWED_EXTENSIONS)))])

    # def validate(self):
    #     filename = self.file.data.filename
    #     self.file.errors = list(self.file.errors)
    #     # if filename in os.listdir(app.config['UPLOAD_FOLDER']):
    #     #     self.file.errors.append('Файл с таким именем уже существует')
    #     #     return False
    #     if filename == '':
    #         self.file.errors.append('Файл не выбран')
    #         return False
    #     if '.' in filename and filename.split('.')[-1] in ALLOWED_EXTENSIONS
    #     self.file.errors = tuple(self.file.errors)
    #     # if self.file.errors:
    #     #     return False
    #     return True


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
