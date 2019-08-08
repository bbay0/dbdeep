import cx_Oracle
from dbconn.models import DBconn
def connect_db(history):
    dbconn = history.get_db_conn()
    print(dbconn.username)
    dsn_tns = cx_Oracle.makedsn(dbconn.server_ip, dbconn.port, dbconn.sid)
    print(dsn_tns)
    db = cx_Oracle.connect(dbconn.username, dbconn.password, dsn_tns)

    print(db.version)
    # cursor = db.cursor()
    return 

def execute_sql(sqlcommand, params, cursor):
    if params == '':
        cursor.execute(sqlcommand)
    else:
        cursor.execute(sqlcommand, params)
    return cursor
