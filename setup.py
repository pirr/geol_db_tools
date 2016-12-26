#-*- coding: utf-8 -*-


from collections import OrderedDict
import os
import couchdb
from flask import Flask
from flask_sqlalchemy import SQLAlchemy


UPLOAD_FOLDER = os.path.abspath('uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

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

REGISTRY_COLUMNS = OrderedDict([('№ строки', 'N'), 
('Актуальность строки', 'actual'), 
('№ изменений', 'N_change'), 
('Операция внесения (добавление, изменение, удаление)', 'change_type'),
('№ объекта', 'N_obj'),
('Признак комплексного', 'complex'),
('Вид документа регистрации1)', 'doc_type'),
('Наличие ГКМ паспорта в группе', 'obj_with_gkm'),
('Орган регистрации (ТФИ, РГФ, ВСЕГЕИ, ЦНИГРИ, Роснедра, Минприроды, ГСЭ)', 'organ_regs'),
('Номер документа', 'doc_num'),
('Дата регистрации', 'doc_date'), 
('Год регистрации (для сортировки)', 'doc_date_num'),
('№ объекта в документе регистрации', 'obj_num_in_doc'),
('Федеральный округ', 'fed_distr'),
('Субъект РФ', 'subj_distr'),
('Административный район', 'adm_distr'),
('Лист м-ба 1000', '1000_map'),
('Лист м-ба 200 (араб.)', '200_map'),
('Вид объекта2)', 'geol_type_obj'),
('Название объекта', 'name_obj'),
('Фонд недр (Р-распред., НР-нераспред.)', 'fund'),
('Вид пользования недрами (ГИН/Р+Д/ГИН+Р+Д)', 'use_type'),
('Группа ПИ в госпрограмме3)', 'gover_type_pi'),
('ПИ (перечень для объекта)', 'pi'), 
('Название нормализ.', 'norm_pi'),
('Название ПИ по ГБЗ', 'gbz_pi'),
('Группа ПИ ИС недра', 'isnedra_pi'),
('Ед. измерения ПИ', 'unit_pi'), 
('P3', 'P3_cat'),
('P2', 'P2_cat'),
('P1', 'P1_cat'),
('С2', 'C2_res'),
('Без категор.', 'none_cat'),
('Запасы ABC1', 'ABC_res'),
('Признак наличия ресурсных оценок', 'res_exist'),
('Наличие прогнозных ресурсов', 'cat_avaibil'), 
('Признак наличия запасов', 'res_avaibil'),
('Вид документа апробации (протокол, отчет)', 'probe_doc_type'), 
('Номер', 'probe_doc_num'),
('Дата', 'probe_doc_date'),
('Орган апробации', 'probe_doc_organ'),
('№ в таблице координат для полигонов', 'N_poly_table'),
('Территория органа апробации', 'probe_organ_subj'),
('Вид координат (Т-точка, П-полигон)', 'coord_type'),
('Площадь, км2', 'area'),
('Координата центра X', 'lon'),
('Координата центра Y', 'lat'),
('Источник координат4)', 'coord_source'),
('Входимость в лицензионыый участок', 'license_area'), 
('Достоверность координат', 'coord_reliability'),
('Координаты треб. проверки', 'coord_for_check'),
('Данные о районе (для определения координат)', 'territory_descript'), 
('Другие документы об объекте (вид документа, №, год, стадия ГРР, авторы, организация)', 'other_source'),
('Рекомендуемые работы (оценка ПР, апробация ПР, в фонд заявок, поиски, оценка и др.)', 'recommendations')])

_REGISTRY_COLUMNS = OrderedDict([(v, k) for k, v in REGISTRY_COLUMNS.items()])

try:
    cdb = couch['test_2112']
except Exception as e:
    cdb = couch.create('test_2112')

if 'regs_info' not in cdb:
    cdb.save({'_id': 'regs_info'})
    
