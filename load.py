from collections import OrderedDict
import sqlite3
import json
import glob

def nsdict(d, k):
    if k not in d:
        return
    subdict = {}
    for subkey, subval in d[k].items():
        new_key = k + "__" + subkey # "key.subkey"
        subdict[new_key] = subval
        #del subdict[subkey]
    d.update(subdict)
    del d[k]

def subdict(d, ks):
    return {k:v for k, v in d.items() if k in ks}

keys = ['obj_id', 'occurred_at', 'subj_id', 'id', 'evidence_record', 'relation_type_id', 'subj__pid', 'subj__url', 'subj__type', 'subj__title', 'obj__pid', 'obj__url']

def mkrow(row):
    keepers = ['id', 'subj_id', 'relation_type_id', 'obj_id', 'occurred_at', 'evidence_record', 'subj', 'obj']
    new_row = subdict(row, keepers)
    nsdict(new_row, 'subj')
    nsdict(new_row, 'obj')
    new_row = subdict(new_row, keys)
    return new_row

def create_rows(fname):
    data = json.load(open(fname, 'r'))['message']['events']
    return map(mkrow, data)

def insert_row(cursor, row):
    row = OrderedDict(row)
    sql = "INSERT INTO event_data (%s) VALUES (%s);"
    keys = ", ".join(row.keys())
    vals = ", ".join(["?"] * len(row))
    sql = sql % (keys, vals)
    return cursor.execute(sql, list(row.values()))

def insert_many_rows(conn, cursor, rows):
    for row in rows:
        insert_row(cursor, row)
    conn.commit()

key_type_map = {}
key_types = ", ".join([key + " " + key_type_map.get(key, 'text') for key in keys])

sql_create_table = """CREATE TABLE event_data (%s);""" % key_types

conn = sqlite3.connect('db.sqlite3')
cursor = conn.cursor()
cursor.execute(sql_create_table)
conn.commit()

for fname in glob.glob('event-data-*'):
    insert_many_rows(conn, cursor, create_rows(fname))
