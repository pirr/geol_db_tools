#-*- coding: utf-8 -*-


import couchdb
from flask import Flask
from flask_sqlalchemy import SQLAlchemy


UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = set(['xls', 'xlsx'])
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
    cdb = couch['test_db']
except Exception as e:
    cdb = couch.create('test_db')

if 'regs_info' not in cdb:
    cdb.save({'_id': 'regs_info'})
    
