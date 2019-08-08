import cx_Oracle
import os
from util.database import sql_info

os.environ["NLS_LANG"] = ".AL32UTF8"

class ConnectionManager:

    def __init__(self, history):
        print("create connection manager")
        self.history = history
    
    def connect_db(self):
        dbconn = self.history.get_db_conn()
        dsn_tns = cx_Oracle.makedsn(dbconn.server_ip, dbconn.port, dbconn.sid)
        db = cx_Oracle.connect(dbconn.username, dbconn.password, dsn_tns, mode=cx_Oracle.SYSDBA)
        cursor = db.cursor()
        return cursor

    def execute_sql(self, sqlcommand, params, cursor):
        print(type(sqlcommand))
        if params == '':
            cursor.execute(sqlcommand)
        else:
            cursor.execute(sqlcommand, params)
        return cursor

    def get_parameters(self, cursor):
        args = {}
        
        params = ''
        ## DONE
        ### 채울 필요 없음. 시스템모니터링할 때 사용하는 것(개발자에게는 필요없음)
        ### 앞 param 에 우리 프로젝트이름 넣기 ( 안 넣어도됨 ㅎㅎ) 
        sql = "CALL DBMS_APPLICATION_INFO.SET_MODULE('DB_DEEP','')" #***** DBMS의 모듈을 설정, 여기서 말하는 모듈이란?
        cursor = self.execute_sql(sql, params, cursor)
        ### 채울 필요 없음. 클라이언트 정보 제공 용도
        sql = "CALL DBMS_APPLICATION_INFO.SET_CLIENT_INFO('')" #***** 세션의 CLIENT INFO를 설정
        cursor = self.execute_sql(sql, params, cursor)
        ### 채울 필요 없음. 데이터 보여주기 최적화 용도
        sql = "ALTER SESSION SET OPTIMIZER_MODE=ALL_ROWS" #*****
        cursor = self.execute_sql(sql, params, cursor)
        ### 채울 필요 없음.
        sql = "SELECT * FROM dba_hist_database_instance where DBID <> (SELECT DBID FROM V$DATABASE)"
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['DBID'] = result_row[0][0]
    
        # needs instance_number which you want to analyze
         
        sql = "select instance_number from v$instance"
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['INSTANCE_NUMBER'] = result_row[0][0]
        # args['INSTANCE_NUMBER'] = 1
    
        # needs snapshot begin - end inputs
        args['BEGIN_DATE'] = self.history.start_date
        args['END_DATE'] = self.history.end_date
    
        # needs max rownum
        args['RNUM'] = 15
    
        args['REPORT_INFO'] = sql_info.ReportInfo
    
        # ***** END_DATE이내에 접속한 가장최근 접속기록 / STARTUP_TIME: 인스턴스가 시작된 시간
        
        sql = "SELECT TO_CHAR(MAX(STARTUP_TIME),'YYYYMMDDHH24MISS') FROM DBA_HIST_DATABASE_INSTANCE WHERE DBID= :DBID AND INSTANCE_NUMBER= :INSTANCE_NUMBER AND STARTUP_TIME<=TO_DATE(:END_DATE,'YYYYMMDDHH24MI')"
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'], 'END_DATE': args['END_DATE']}
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['STARTUP_TIME'] = result_row[0][0]
    
        # ***** http://wiki.gurubee.net/pages/viewpage.action?pageId=26742261
        # SNAP_ID가 가장큰거 (SNAP_ID가 가장 큰게 가장 최근), 가장 최근 END_INTERVAL_TIME (END_INTERVAL_TIME이 없으면 사용자가 입력한 BEGIN_DATE)
        # -> 결과물을 만들어낼 구간의 시작시간 찾기
        sql = "SELECT NVL(MAX(SNAP_ID),1),NVL(MAX(TO_CHAR(END_INTERVAL_TIME,'YYYYMMDDHH24MI')),TO_CHAR(TRUNC(TO_DATE(:BEGIN_DATE,'YYYYMMDDHH24MI')),'YYYYMMDDHH24MI')) FROM DBA_HIST_SNAPSHOT WHERE DBID=:DBID AND INSTANCE_NUMBER=:INSTANCE_NUMBER AND END_INTERVAL_TIME<=TO_DATE(:BEGIN_DATE,'YYYYMMDDHH24MI')"
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'], 'BEGIN_DATE': args['BEGIN_DATE']}
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['BEGIN_SNAP_ID'] = result_row[0][0]
        args['BEGIN_ASH_DATE'] = result_row[0][1]
    
        # ***** 위에 query랑 같은데 max -> min
        # 가장 오래된 기록 (단, 사용자가 지정한 END_DATE이내에서)
        # -> 결과물을 만들어낼 구간의 끝 시간 찾기
        sql = "SELECT NVL(MIN(SNAP_ID),9999999999999),NVL(MIN(TO_CHAR(END_INTERVAL_TIME,'YYYYMMDDHH24MI')),TO_CHAR(TRUNC(TO_DATE(:END_DATE,'YYYYMMDDHH24MI'))+0.9999999,'YYYYMMDDHH24MI')) FROM DBA_HIST_SNAPSHOT WHERE DBID=:DBID AND INSTANCE_NUMBER=:INSTANCE_NUMBER AND END_INTERVAL_TIME>=TO_DATE(:END_DATE,'YYYYMMDDHH24MI')"
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'], 'END_DATE': args['END_DATE']}
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['END_SNAP_ID'] = result_row[0][0]
        args['END_ASH_DATE'] = result_row[0][1]
    
        # ***** 일주일 전 sanp id, 지금 없는 기능, 나중에 쓰려고 만들어놓음
        args['DIFF_DAYS'] = 7
        sql = "SELECT MIN(SNAP_ID) SNAP_ID FROM DBA_HIST_SNAPSHOT WHERE DBID=:DBID AND INSTANCE_NUMBER=:INSTANCE_NUMBER AND SNAP_ID<:END_SNAP_ID AND END_INTERVAL_TIME>=(SELECT MAX(TRUNC(END_INTERVAL_TIME)-:DIFF_DAYS) FROM DBA_HIST_SNAPSHOT WHERE DBID=:DBID AND INSTANCE_NUMBER=:INSTANCE_NUMBER AND SNAP_ID<:END_SNAP_ID)"
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'DIFF_DAYS': args['DIFF_DAYS']}
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['B_SNAP_ID'] = result_row[0][0]
    
        # ***** Q) ROWNUM <= 10
        sql = "SELECT DECODE(COUNT(*),10,1,0) FROM DBA_HIST_SEG_STAT WHERE DBID=:DBID AND INSTANCE_NUMBER=:INSTANCE_NUMBER AND SNAP_ID BETWEEN :BEGIN_SNAP_ID AND :END_SNAP_ID AND ROWNUM<=10"
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        segstat_cnt = result_row[0][0]
        if segstat_cnt == 0:
            args['REPORT_INFO'][16][2] = False
    
        # ***** 현재시스템 시간
        sql = "SELECT TO_CHAR(SYSDATE,'YYYY.MM.DD') FROM DUAL"
        params = ''
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['SYSDATE'] = result_row[0][0]
    
        args['LEFT_TOP'] = "A13"
    
        # RUN ADDM
        args['TASK_NAME'] = "T_" + str(args['DBID']) + "_" + str(args['BEGIN_SNAP_ID']) + "_" + str(args['END_SNAP_ID'])
    
        # *****
        # ADDM: https://m.blog.naver.com/PostView.nhn?blogId=kkj871001&logNo=100189924300&proxyReferer=https%3A%2F%2Fwww.google.com%2F
        # TASK_NAME은 ADDM:2312608101_1_2형식 (T_로 하면 아무 결과도 안나옴)
        sql = "SELECT COUNT(*) FROM DBA_ADDM_TASKS WHERE TASK_NAME=:TASK_NAME"
        params = {'TASK_NAME': args['TASK_NAME']}
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        addm_cnt = result_row[0][0]
        if addm_cnt == 1:
            cursor.callproc('DBMS_ADDM.DELETE', [args['TASK_NAME']])
    
        # # *****
        sql = "CALL DBMS_ADDM.ANALYZE_DB(:TASK_NAME,:BEGIN_SNAP_ID,:END_SNAP_ID,:DBID)"
        params = {'TASK_NAME': args['TASK_NAME'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'DBID': args['DBID']}
        cursor = self.execute_sql(sql, params, cursor)
    
        # *****
        sql = "SELECT TASK_ID FROM DBA_ADDM_TASKS WHERE TASK_NAME=:TASK_NAME"
        params = {'TASK_NAME': args['TASK_NAME']}
        cursor = self.execute_sql(sql, params, cursor)
        result_row = cursor.fetchall()
        args['TASK_ID'] = result_row[0][0]
    
        return args, cursor

    def exeute(self):
        cursor = self.connect_db()
        args, cursor = self.get_parameters(cursor)
        
        for ind in (0,5,13):
            print(ind)
            (sqlcommand, params) = sql_info.get_sql_command(ind, args)
            cursor = self.execute_sql(sqlcommand, params, cursor) 
            print(cursor.fetchall())
