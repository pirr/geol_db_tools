from setup import COUCH_URL
from couchdb.client import Database
import requests


# kwargs = {'filename': {'$eq': 'test_io_reestr.xls'}}
def mango_query(db, **kwargs):
    cdb = db.name
    selector = {'selector': kwargs, 'limit': 100000}
    r = requests.post('/'.join([COUCH_URL, cdb, '_find']), json=selector)
    return r.json()['docs']

# print(mango_query('registries', **kwargs))
# class couchdb2_0(Database):
#     def mquery(self, **kwargs):


