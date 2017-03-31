#-*- coding: utf-8 -*-


import os
import couchdb
from flask import Flask
from flask_sqlalchemy import SQLAlchemy


UPLOAD_FOLDER = os.path.abspath('uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

ALLOWED_EXTENSIONS = {'xls', 'xlsx'}
COUCH_URL = 'http://localhost:5984'

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///app.db'
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'
app.config['WTF_CSRF_ENABLED'] = False
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.debug = True

db = SQLAlchemy(app)
couch = couchdb.Server(COUCH_URL)

try:
    cdb = couch['test_2112']
except Exception as e:
    cdb = couch.create('test_2112')

if 'regs_info' not in cdb:
    cdb.save({'_id': 'regs_info'})
    
