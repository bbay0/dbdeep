
ReportInfo = [["DB_INFO", "DB Info", True],  # 0 *****
             ["SQL_ASH","ASH Grouping", True],  # 1
             ["SQL_StatEvent","전체 Stat,Event 지표", True],  # 2
             ["SQL_AWRStat","주요 Stat 지표", True],  # 3
             ["SQL_AWREvent", "주요 Event 지표", True],  # 4
             ["SQL_TOP10Event", "주요(Top10) Event 지표", True],  # 5 *****
             ["SQL_IOStat", "IO 성능지표", True],  # 6
             ["SQL_TimeModel", "TimeModel", True],  # 7
             ["SQL_SysMetric", "SQL_SysMetric", True],  # 8
             ["Estd.Interconnect Traffic", "Estd.Interconnect Traffic", True],  # 9
             ["OS Stat", "OSStat", True],  # 10
             ["STAT_TREND", "주요 Stat 추이", True],  #11
             ["SQL Stat", "SQLStat", True],  # 12
             ["SQL Stat (SQL Only)", "SQLStat (SQL Only)", True],  # 13 *****
             ["Plan_Diff_SQL", "Plan Diff SQL", True],  # 14
             ["ADDM_Report", "ADDM Report", True],  # 15
             ["Segment_Stat", "Segment Stat", True],  # 16
             ["Mutex", "Mutex", True]  # 17
             ]


def init_parameters():
    return

def get_sql_command(ind, args):
    if ind == 0:
        sqlcommand = """
        SELECT 'MEASURE TIME'                             FTYPE,
               TO_CHAR (SYSDATE, 'YYYY.MM.DD HH24:MI:SS') NAME,
               NULL                                       VALUE,
               NULL                                       NOTE
          FROM DUAL
        UNION ALL
        SELECT 'CURRENT SCN'                                          FTYPE,
               TO_CHAR (CURRENT_SCN, 'FM999,999,999,999,999,999,999') NAME,
               NULL,
               NULL
          FROM GV$DATABASE
         WHERE INST_ID = :INSTANCE_NUMBER
        UNION ALL
        SELECT 'DB_NAME' FTYPE,
               DB_NAME,
               NULL      VALUE,
               NULL      VTYPE
          FROM DBA_HIST_DATABASE_INSTANCE
         WHERE INSTANCE_NUMBER = :INSTANCE_NUMBER AND STARTUP_TIME = TO_DATE (:STARTUP_TIME, 'YYYYMMDDHH24MISS')
        UNION ALL
        SELECT 'DBID'         FTYPE,
               TO_CHAR (DBID) NAME,
               NULL,
               NULL
          FROM DBA_HIST_DATABASE_INSTANCE
         WHERE INSTANCE_NUMBER = :INSTANCE_NUMBER AND STARTUP_TIME = TO_DATE (:STARTUP_TIME, 'YYYYMMDDHH24MISS')
        UNION ALL
        SELECT 'INSTANCE NUMBER'         FTYPE,
               TO_CHAR (INSTANCE_NUMBER) NAME,
               NULL,
               NULL
          FROM DBA_HIST_DATABASE_INSTANCE
         WHERE INSTANCE_NUMBER = :INSTANCE_NUMBER AND STARTUP_TIME = TO_DATE (:STARTUP_TIME, 'YYYYMMDDHH24MISS')
        UNION ALL
        SELECT 'INSTANCE NAME' FTYPE,
               INSTANCE_NAME   NAME,
               NULL,
               NULL
          FROM DBA_HIST_DATABASE_INSTANCE
         WHERE INSTANCE_NUMBER = :INSTANCE_NUMBER AND STARTUP_TIME = TO_DATE (:STARTUP_TIME, 'YYYYMMDDHH24MISS')
        UNION ALL
        SELECT 'HOST NAME' FTYPE,
               HOST_NAME   NAME,
               NULL,
               NULL
          FROM DBA_HIST_DATABASE_INSTANCE
         WHERE INSTANCE_NUMBER = :INSTANCE_NUMBER AND STARTUP_TIME = TO_DATE (:STARTUP_TIME, 'YYYYMMDDHH24MISS')
        UNION ALL
        SELECT 'STARTUP TIME'                                  FTYPE,
               TO_CHAR (STARTUP_TIME, 'YYYY.MM.DD HH24:MI:SS') NAME,
               NULL,
               NULL
              FROM DBA_HIST_DATABASE_INSTANCE
         WHERE INSTANCE_NUMBER = :INSTANCE_NUMBER AND STARTUP_TIME = TO_DATE (:STARTUP_TIME, 'YYYYMMDDHH24MISS')
        UNION ALL
        SELECT 'VERSION' FTYPE,
               VERSION   NAME,
               NULL,
               NULL
          FROM DBA_HIST_DATABASE_INSTANCE
         WHERE INSTANCE_NUMBER = :INSTANCE_NUMBER AND STARTUP_TIME = TO_DATE (:STARTUP_TIME, 'YYYYMMDDHH24MISS')
        UNION ALL
        SELECT 'PARAM'         FTYPE,
               PARAMETER_NAME  NAME,
               TO_CHAR (VALUE) VALUE,
               NULL
          FROM DBA_HIST_PARAMETER
         WHERE DBID = :DBID AND INSTANCE_NUMBER = :INSTANCE_NUMBER AND SNAP_ID = :END_SNAP_ID
        UNION ALL
        SELECT 'SGA'                                   FTYPE,
               NAME,
               ROUND (VALUE / 1024 / 1024, 1) || ' MB' VALUE,
               NULL
          FROM DBA_HIST_SGA
         WHERE DBID = :DBID AND INSTANCE_NUMBER = :INSTANCE_NUMBER AND SNAP_ID = :END_SNAP_ID
        UNION ALL
        SELECT 'PGA'                                   FTYPE,
               NAME,
               ROUND (VALUE / 1024 / 1024, 1) || ' MB' VALUE,
               NULL
          FROM DBA_HIST_PGASTAT
         WHERE DBID = :DBID AND INSTANCE_NUMBER = :INSTANCE_NUMBER AND SNAP_ID = :END_SNAP_ID
        UNION ALL
        SELECT 'CURRENT SGA'                           FTYPE,
               NAME,
               ROUND (BYTES / 1024 / 1024, 1) || ' MB' VALUE,
               RESIZEABLE                              VTYPE
          FROM GV$SGAINFO
         WHERE INST_ID = :INSTANCE_NUMBER
        UNION ALL
        SELECT 'CURRENT PGA'
                   FTYPE,
               NAME,
               DECODE (
                   UNIT,
                   'bytes', TO_CHAR (ROUND (VALUE / 1024 / 1024, 1),
                                     'FM999,999,999,999.9'),
                   TO_CHAR (VALUE))
                   VALUE,
               DECODE (UNIT, 'bytes', 'MB', UNIT)
                   VTYPE
          FROM GV$PGASTAT
         WHERE INST_ID = :INSTANCE_NUMBER
        UNION ALL
        SELECT 'CURRENT SIZE : SEGMENT'
                   FTYPE,
               TO_CHAR (SUM (BLOCKS), 'FM999,999,999,999') || ' Blocks',
                  TO_CHAR (ROUND ((SUM (BYTES) / 1024 / 1024 / 1024), 1),
                           'FM999,999,999,999')
               || ' GB',
               NULL
                   VTYPE
          FROM DBA_SEGMENTS
        UNION ALL
        SELECT 'CURRENT SIZE : DATAFILE'
                   FTYPE,
               TO_CHAR (SUM (BLOCKS), 'FM999,999,999,999') || ' Blocks',
                  TO_CHAR (ROUND ((SUM (BYTES) / 1024 / 1024 / 1024), 1),
                           'FM999,999,999,999')
               || ' GB',
               NULL
                   VTYPE
          FROM DBA_DATA_FILES
        ORDER BY 1, 2
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'STARTUP_TIME': args['STARTUP_TIME'], 'END_SNAP_ID': args['END_SNAP_ID']}

    elif ind == 1:
        sqlcommand = """
        SELECT
          SAMPLE_TIME
          ,\"CPU\"
          ,\"User I/O\"
          ,\"System I/O\"
          ,\"Commit\"
          ,\"Concurrency\"
          ,\"Cluster\"
          ,\"Other\"
          ,\"Application\"
          ,\"Network\"
          ,\"Administrative\"
          ,\"Configuration\"
          ,\"Scheduler\"
          ,\"Idle\"
          ,\"Total\"
        FROM
          (
          SELECT
              DECODE(A.D,1,SAMPLE_TIME,2,'SUM') SAMPLE_TIME
              ,SUM(\"CPU\")             \"CPU\"
              ,SUM(\"User I/O\")        \"User I/O\"
              ,SUM(\"System I/O\")      \"System I/O\"
              ,SUM(\"Commit\")          \"Commit\"
              ,SUM(\"Concurrency\")     \"Concurrency\"
              ,SUM(\"Cluster\")         \"Cluster\"
              ,SUM(\"Other\")           \"Other\"
              ,SUM(\"Application\")     \"Application\"
              ,SUM(\"Network\")         \"Network\"
              ,SUM(\"Administrative\")  \"Administrative\"
              ,SUM(\"Configuration\")   \"Configuration\"
              ,SUM(\"Scheduler\")       \"Scheduler\"
              ,SUM(\"Idle\")            \"Idle\"
              ,SUM(\"Total\")           \"Total\"
          FROM
              (
              SELECT TO_CHAR(STIME.STIME,'DD HH24:MI:SS') SAMPLE_TIME
               ,NVL(\"CPU\",0) \"CPU\"
               ,NVL(\"User I/O\",0) \"User I/O\"
               ,NVL(\"System I/O\",0) \"System I/O\"
               ,NVL(\"Commit\",0) \"Commit\"
               ,NVL(\"Concurrency\",0) \"Concurrency\"
               ,NVL(\"Cluster\",0) \"Cluster\"
               ,NVL(\"Other\",0) \"Other\"
               ,NVL(\"Application\",0) \"Application\"
               ,NVL(\"Network\",0) \"Network\"
               ,NVL(\"Administrative\",0) \"Administrative\"
               ,NVL(\"Configuration\",0) \"Configuration\"
               ,NVL(\"Scheduler\",0) \"Scheduler\"
               ,NVL(\"Idle\",0) \"Idle\"
               ,NVL(\"CPU\"+\"Concurrency\"+\"Cluster\"+\"Commit\"+\"Other\"+\"System I/O\"+\"User I/O\"+\"Application\"+\"Network\"+\"Administrative\"+\"Configuration\"+\"Scheduler\"+\"Idle\",0) \"Total\"
              FROM
                 ( SELECT /*+ LEADING(DHS) */
                     TO_DATE(TO_CHAR(DHASH.SAMPLE_TIME,'YYYYMMDDHH24MI')
                         ||TRUNC(TO_CHAR(DHASH.SAMPLE_TIME,'SS'),-1),'YYYYMMDDHH24MISS') COMP_TIME
                   ,COUNT(DECODE(EN.WAIT_CLASS,NULL,1)) \"CPU\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Concurrency',1)) \"Concurrency\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Cluster',1)) \"Cluster\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Commit',1)) \"Commit\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Other',1)) \"Other\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'System I/O',1)) \"System I/O\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'User I/O',1)) \"User I/O\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Application',1)) \"Application\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Network',1)) \"Network\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Administrative',1)) \"Administrative\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Configuration',1)) \"Configuration\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Scheduler',1)) \"Scheduler\"
                   ,COUNT(DECODE(EN.WAIT_CLASS,'Idle',1)) \"Idle\"
                 FROM DBA_HIST_SNAPSHOT DHS
                   ,DBA_HIST_ACTIVE_SESS_HISTORY DHASH
                   ,V$EVENT_NAME EN
                 WHERE DHS.SNAP_ID            =DHASH.SNAP_ID
                 AND DHS.INSTANCE_NUMBER      = DHASH.INSTANCE_NUMBER
                 AND DHS.DBID                 =DHASH.DBID
                 AND DHS.DBID                 = :DBID
                 AND DHS.INSTANCE_NUMBER      = :INSTANCE_NUMBER
                 AND DHS.SNAP_ID >= :BEGIN_SNAP_ID
                 AND DHS.SNAP_ID <= :END_SNAP_ID
                 AND DHASH.SAMPLE_TIME >= TO_DATE(:BEGIN_ASH_DATE,'YYYYMMDDHH24MI')
                 AND DHASH.SAMPLE_TIME <= TO_DATE(:END_ASH_DATE,'YYYYMMDDHH24MI')
                 AND DHASH.INSTANCE_NUMBER    = DHS.INSTANCE_NUMBER
                 AND DHASH.EVENT_ID           = EN.EVENT_ID(+)
                 GROUP BY TO_DATE(TO_CHAR(DHASH.SAMPLE_TIME,'YYYYMMDDHH24MI')
                         ||TRUNC(TO_CHAR(DHASH.SAMPLE_TIME,'SS'),-1),'YYYYMMDDHH24MISS')
                 ORDER BY TO_DATE(TO_CHAR(DHASH.SAMPLE_TIME,'YYYYMMDDHH24MI')
                         ||TRUNC(TO_CHAR(DHASH.SAMPLE_TIME,'SS'),-1),'YYYYMMDDHH24MISS')
                 ) ASH
                 ,(
                 SELECT TO_DATE(:BEGIN_ASH_DATE,'YYYYMMDDHH24MI') + (ROWNUM-1)*10/(24*60*60) STIME
                 FROM DUAL
                 CONNECT BY ROWNUM<=(TO_DATE(:END_ASH_DATE,'YYYYMMDDHH24MI') - TO_DATE(:BEGIN_ASH_DATE,'YYYYMMDDHH24MI'))*24*60*6
                 ) STIME
              WHERE
                 STIME.STIME=ASH.COMP_TIME(+)
                 AND ROWNUM<32000
              ) V
              ,(SELECT 1 D FROM DUAL UNION SELECT 2 D FROM DUAL) A
          GROUP BY
              DECODE(A.D,1,SAMPLE_TIME,2,'SUM')
          ORDER BY SAMPLE_TIME NULLS LAST
          )
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID'],
                  'BEGIN_ASH_DATE': args['BEGIN_ASH_DATE'], 'END_ASH_DATE': args['END_ASH_DATE']}

    elif ind == 2:
        sqlcommand = """
        SELECT 'STAT' FTYPE
          ,STAT_NAME NAME
          ,TO_CHAR(SUM(TOT_VALUE),'FM999,999,999,999,999,999') \"TOT_VALUE(MIN)\"
          ,TO_CHAR(ROUND(SUM(TOT_VALUE)/SUM(TOT_TIME_SEC),2),'FM999,999,999,999,999,999.99') AVG_VALUE_PER_SEC
          ,TO_CHAR(SUM(TOT_TIME_SEC),'FM999,999,999.99') \"TOTAL_TIME(MAX)\"
        FROM
            ( SELECT STAT_NAME
              ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
              ,DECODE(SNAP_TIME_2,NULL,0 ,ROUND((VALUE_1-VALUE_2))) TOT_VALUE
              ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
            FROM
                ( SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) */ SNAP.END_INTERVAL_TIME SNAP_TIME_1
                  ,STAT.STAT_NAME
                  ,STAT.VALUE VALUE_1
                  ,LAG(STAT.VALUE) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                  ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                  ,SNAP.SNAP_ID
                FROM
                    (
                    SELECT /*+ NO_MERGE */ DI.DBID
                      ,DI.INSTANCE_NUMBER
                      ,DI.STARTUP_TIME
                    FROM
                      DBA_HIST_DATABASE_INSTANCE DI
                    WHERE
                    DI.DBID = :DBID
                    AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                    AND ROWNUM<=1
                    ) DBI
                  ,DBA_HIST_SNAPSHOT SNAP
                  ,DBA_HIST_SYSSTAT STAT
                WHERE DBI.DBID               = SNAP.DBID
                AND DBI.INSTANCE_NUMBER      = SNAP.INSTANCE_NUMBER
                AND SNAP.SNAP_ID>= :BEGIN_SNAP_ID
                AND SNAP.SNAP_ID<= :END_SNAP_ID
                AND SNAP.DBID                = STAT.DBID
                AND SNAP.INSTANCE_NUMBER     = STAT.INSTANCE_NUMBER
                AND SNAP.SNAP_ID             = STAT.SNAP_ID
                ORDER BY SNAP.SNAP_ID
                )
            )
        GROUP BY STAT_NAME
        UNION ALL
        SELECT 'EVENT' FTYPE
          ,EVENT_NAME NAME
          ,TO_CHAR(ROUND(SUM(TOT_VALUE)/1000000,2),'FM999,999,999,999,999,999') TOT_VALUE
          ,TO_CHAR(ROUND(SUM(TOT_VALUE)/(SUM(TOT_TIME_SEC)*1000000),2),'FM999,999,999,999,999,999.99') AVG_VALUE_PER_SEC
          ,TO_CHAR(SUM(TOT_TIME_SEC),'FM999,999,999.99')
        FROM
            ( SELECT EVENT_NAME
              ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
              ,DECODE(SNAP_TIME_2,NULL,0,ROUND((VALUE_1-VALUE_2))) TOT_VALUE
              ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
            FROM
                ( SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) */ SNAP.END_INTERVAL_TIME SNAP_TIME_1
                  ,EVENT.EVENT_NAME
                  ,EVENT.TIME_WAITED_MICRO VALUE_1
                  ,LAG(EVENT.TIME_WAITED_MICRO) OVER (PARTITION BY EVENT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                  ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY EVENT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                  ,SNAP.SNAP_ID
                FROM
                    ( SELECT /*+ NO_MERGE */ DI.DBID
                      ,DI.INSTANCE_NUMBER
                      ,DI.STARTUP_TIME
                    FROM
                      DBA_HIST_DATABASE_INSTANCE DI
                    WHERE
                    DI.DBID = :DBID
                    AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                    AND ROWNUM<=1
                    ) DBI
                  ,DBA_HIST_SNAPSHOT SNAP
                  ,DBA_HIST_SYSTEM_EVENT EVENT
                WHERE DBI.DBID               = SNAP.DBID
                AND DBI.INSTANCE_NUMBER      = SNAP.INSTANCE_NUMBER
                AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                AND SNAP.SNAP_ID<=:END_SNAP_ID
                AND SNAP.DBID                = EVENT.DBID
                AND SNAP.INSTANCE_NUMBER     = EVENT.INSTANCE_NUMBER
                AND SNAP.SNAP_ID             = EVENT.SNAP_ID
                ORDER BY SNAP.SNAP_ID
                )
            )
        GROUP BY EVENT_NAME
        UNION ALL
        SELECT 'ACTIVE SESSION' FTYPE
          ,'Active Session Count' NAME
          ,TO_CHAR(ROUND(MIN(CNT)/1000000,2),'FM999,999') TOT_VALUE
          ,TO_CHAR(ROUND(AVG(CNT),2),'FM999,999')
          ,TO_CHAR(MAX(CNT),'FM999,999,999')
        FROM
            ( SELECT SAMPLE_TIME
              ,COUNT(*) CNT
            FROM DBA_HIST_ACTIVE_SESS_HISTORY DHASH
              ,DBA_HIST_SNAPSHOT SNAP
            WHERE
            DHASH.DBID=:DBID
            AND DHASH.INSTANCE_NUMBER=:INSTANCE_NUMBER
            AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
            AND SNAP.SNAP_ID<=:END_SNAP_ID
            AND DHASH.INSTANCE_NUMBER    = SNAP.INSTANCE_NUMBER
            AND DHASH.SNAP_ID            = SNAP.SNAP_ID
            AND DHASH.DBID               = SNAP.DBID
            GROUP BY SAMPLE_TIME
            )
        UNION ALL
        SELECT 'TIME MODEL' FTYPE
          ,STAT_NAME NAME
          ,TO_CHAR(SUM(TOT_VALUE),'FM999,999,999,999,999,999') TOT_VALUE
          ,TO_CHAR(ROUND(SUM(TOT_VALUE)/SUM(TOT_TIME_SEC),2),'FM999,999,999,999,999,999.99') AVG_VALUE_PER_SEC
          ,TO_CHAR(SUM(TOT_TIME_SEC),'FM999,999,999.99')
        FROM
            ( SELECT STAT_NAME
              ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
              ,DECODE(SNAP_TIME_2
                    ,NULL,0
                    ,ROUND((VALUE_1-VALUE_2)))
                TOT_VALUE
              ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
            FROM
                ( SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) */ SNAP.END_INTERVAL_TIME SNAP_TIME_1
                  ,STAT.STAT_NAME
                  ,STAT.VALUE VALUE_1
                  ,LAG(STAT.VALUE) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                  ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                  ,SNAP.SNAP_ID
                FROM
                    (
                    SELECT /*+ NO_MERGE */ DI.DBID
                      ,DI.INSTANCE_NUMBER
                      ,DI.STARTUP_TIME
                    FROM
                      DBA_HIST_DATABASE_INSTANCE DI
                    WHERE
                    DI.DBID = :DBID
                    AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                    AND ROWNUM<=1
                    ) DBI
                  ,DBA_HIST_SNAPSHOT SNAP
                  ,DBA_HIST_SYS_TIME_MODEL STAT
                WHERE DBI.DBID               = SNAP.DBID
                AND DBI.INSTANCE_NUMBER      = SNAP.INSTANCE_NUMBER
                AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                AND SNAP.SNAP_ID<=:END_SNAP_ID
                AND SNAP.DBID                = STAT.DBID
                AND SNAP.INSTANCE_NUMBER     = STAT.INSTANCE_NUMBER
                AND SNAP.SNAP_ID             = STAT.SNAP_ID
                ORDER BY SNAP.SNAP_ID
                )
            )
        GROUP BY STAT_NAME
        UNION ALL
        SELECT 'SYSMETRIC' FTYPE
            ,METRIC_NAME NAME
            ,TO_CHAR(MIN(MINVAL),'FM999,999,999,999.99')
            ,TO_CHAR(AVG(AVERAGE),'FM999,999,999,999,999,999.99') AVG_VALUE_PER_SEC
            ,TO_CHAR(MAX(MAXVAL),'FM999,999,999,999,999,999.99')
        FROM DBA_HIST_SYSMETRIC_SUMMARY DHSS
        WHERE
            DHSS.DBID = :DBID
            AND DHSS.INSTANCE_NUMBER = :INSTANCE_NUMBER
            AND DHSS.SNAP_ID >=:BEGIN_SNAP_ID + 1
            AND DHSS.SNAP_ID <=:END_SNAP_ID + 1
        GROUP BY
           METRIC_NAME
        UNION ALL
        SELECT 'DLM MISC' FTYPE
          ,STAT_NAME NAME
          ,TO_CHAR(SUM(TOT_VALUE),'FM999,999,999,999,999,999') TOT_VALUE
          ,TO_CHAR(ROUND(SUM(TOT_VALUE)/SUM(TOT_TIME_SEC),2),'FM999,999,999,999,999,999.99') AVG_VALUE_PER_SEC
          ,TO_CHAR(SUM(TOT_TIME_SEC),'FM999,999,999,999,999.99')
        FROM
            ( SELECT STAT_NAME
              ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
              ,DECODE(SNAP_TIME_2,NULL,0 ,ROUND((VALUE_1-VALUE_2))) TOT_VALUE
              ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
            FROM
                ( SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) */ SNAP.END_INTERVAL_TIME SNAP_TIME_1
                  ,STAT.NAME STAT_NAME
                  ,STAT.VALUE VALUE_1
                  ,LAG(STAT.VALUE) OVER (PARTITION BY NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                  ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                  ,SNAP.SNAP_ID
                FROM
                    (
                    SELECT /*+ NO_MERGE */ DI.DBID
                      ,DI.INSTANCE_NUMBER
                      ,DI.STARTUP_TIME
                    FROM
                      DBA_HIST_DATABASE_INSTANCE DI
                    WHERE
                    DI.DBID = :DBID
                    AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                    AND ROWNUM<=1
                    ) DBI
                  ,DBA_HIST_SNAPSHOT SNAP
                  ,DBA_HIST_DLM_MISC STAT
                WHERE DBI.DBID               = SNAP.DBID
                AND DBI.INSTANCE_NUMBER      = SNAP.INSTANCE_NUMBER
                AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                AND SNAP.SNAP_ID<=:END_SNAP_ID
                AND SNAP.DBID                = STAT.DBID
                AND SNAP.INSTANCE_NUMBER     = STAT.INSTANCE_NUMBER
                AND SNAP.SNAP_ID             = STAT.SNAP_ID
                ORDER BY SNAP.SNAP_ID
                )
            )
        GROUP BY STAT_NAME
        ORDER BY FTYPE,NAME
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}
    elif ind == 3:
        sqlcommand = """
        SELECT SNAP_TIME
          ,SNAP_TIME_RANGE
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'session logical reads',VALUE)),'FM999,999,999,999.9') \"Session Logical Reads/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical reads',VALUE)),'FM999,999,999.9') \"Physical Reads/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'execute count',VALUE)),'FM999,999,999.9') \"Execute Count/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'user calls',VALUE)),'FM999,999,999.9') \"User Calls/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'user commits',VALUE)),'FM999,999,999.9') \"User Commits/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'user rollbacks',VALUE)),'FM999,999,999.9') \"User Rollbacks/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'CPU used by this session',VALUE)),'FM999,999,999.9') \"CPU used by this session/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'DB time',VALUE)),'FM999,999,999.9') \"DB time/Sec\"
          ,TO_CHAR((SUM(DECODE(STAT_NAME,'DB time',VALUE))*10)/DECODE(SUM(DECODE(STAT_NAME,'user calls',VALUE)),0,1,SUM(DECODE(STAT_NAME,'user calls',VALUE))),'FM999,999,999.999') \"User Response Time(ms)/Sec\"
          ,TO_CHAR((SUM(DECODE(STAT_NAME,'DB time',VALUE))*10)/DECODE((SUM(DECODE(STAT_NAME,'user commits',VALUE))+SUM(DECODE(STAT_NAME,'user rollbacks',VALUE))),0,1,(SUM(DECODE(STAT_NAME,'user commits',VALUE))+SUM(DECODE(STAT_NAME,'user rollbacks',VALUE)))),'FM999,999,999.999') \"TX Response Time(ms)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'application wait time',VALUE)),'FM999,999,999.9') \"Application Wait Time(cs)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cluster wait time',VALUE)),'FM999,999,999.9') \"Cluster Wait Time(cs)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'concurrency wait time',VALUE)),'FM999,999,999.9') \"Concurrency Wait Time(cs)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'user I/O wait time',VALUE)),'FM999,999,999.9') \"User I/O Wait Time(cs)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'CR blocks created',VALUE)),'FM999,999,999.9') \"CR blocks created/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'consistent gets',VALUE)),'FM999,999,999.9') \"Consistent Gets/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'consistent changes',VALUE)),'FM999,999,999.9') \"Consistent Changes/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'data blocks consistent reads - undo records applied',VALUE)),'FM999,999,999.9') \"DB Blocks cr-undo rec appl/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'db block gets',VALUE)),'FM999,999,999.9') \"DB Block Gets/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'db block changes',VALUE)),'FM999,999,999.9') \"DB Block Changes/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'enqueue releases',VALUE)),'FM999,999,999.9') \"Enqueue Releases/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'enqueue waits',VALUE)),'FM999,999,999.9') \"Enqueue Waits/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'free buffer inspected',VALUE)),'FM999,999,999.9') \"Free Buffer Inspected/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'free buffer requested',VALUE)),'FM999,999,999.9') \"Free Buffer Requested/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'DBWR checkpoints',VALUE)),'FM999,999,999.9') \"DBWR checkpoints/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'DBWR lru scans',VALUE)),'FM999,999,999.9') \"DBWR lru scans/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'DBWR undo block writes',VALUE)),'FM999,999,999.9') \"DBWR undo block writes/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical writes',VALUE)),'FM999,999,999.9') \"Physical Writes/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical writes direct',VALUE)),'FM999,999,999.9') \"Physical Writes Direct/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'branch node splits',VALUE)),'FM999,999,999.9') \"Branch Node Splits/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'leaf node splits',VALUE)),'FM999,999,999.9') \"Leaf Node Splits/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'leaf node 90-10 splits',VALUE)),'FM999,999,999.9') \"Leaf Node 90-10 Splits/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'commit cleanouts',VALUE)),'FM999,999,999.9') \"Commit Cleanouts/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cleanouts only - consistent read gets',VALUE)),'FM999,999,999.9') \"Cleanouts Only-CRgets/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cleanouts and rollbacks - consistent read gets',VALUE)),'FM999,999,999.9') \"Cleanouts&Rollbacks-CRgets/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'index fast full scans (full)',VALUE)),'FM999,999,999.9') \"Index FastFullScans(full)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'table fetch by rowid',VALUE)),'FM999,999,999.9') \"Table Fetch By Rowid/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'table fetch continued row',VALUE)),'FM999,999,999.9') \"Table Fetch Continued Row/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'table scans (long tables)',VALUE)),'FM999,999,999.9') \"Table Scans (long tables)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'table scans (short tables)',VALUE)),'FM999,999,999.9') \"Table Scans (short tables)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'table scan blocks gotten',VALUE)),'FM999,999,999.9') \"Table Scan Blocks gotten/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'table scan rows gotten',VALUE)),'FM999,999,999.9') \"Table Scan Rows gotten/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'logons cumulative',VALUE)),'FM999,999,999.9') \"Logons Cumulative/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'opened cursors cumulative',VALUE)),'FM999,999,999.9') \"Opened Cursors Cumulative/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'parse count (total)',VALUE)),'FM999,999,999.9') \"Parse Count (total)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'parse count (hard)',VALUE)),'FM999,999,999.9') \"Parse Count (hard)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'parse count (failures)',VALUE)),'FM999,999,999.9') \"Parse Count (failures)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'parse time cpu',VALUE)),'FM999,999,999.9') \"Parse Time CPU/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'parse time elapsed',VALUE)),'FM999,999,999.9') \"Parse Time Elapsed/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'session cursor cache count',VALUE)),'FM999,999,999.9') \"Session Cursor Cache Count/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'session cursor cache hits',VALUE)),'FM999,999,999.9') \"Session Cursor Cache Hits/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'redo entries',VALUE)),'FM999,999,999.9') \"Redo Entries/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'redo size',VALUE)),'FM999,999,999.9') \"Redo Size/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'redo write time',VALUE)),'FM999,999,999.9') \"Redo Write Time/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'sorts (disk)',VALUE)),'FM999,999,999.9') \"Sorts (disk)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'sorts (memory)',VALUE)),'FM999,999,999.9') \"Sorts (memory)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'sorts (rows)',VALUE)),'FM999,999,999.9') \"Sorts (rows)/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'transaction tables consistent reads - undo records applied',VALUE)),'FM999,999,999.9') \"TX cr Undo Record applied/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'transaction tables consistent read rollbacks',VALUE)),'FM999,999,999.9') \"TX cr read rollbacks/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'undo change vector size',VALUE)),'FM999,999,999.9') \"Undo Change Vector Size/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'logons current',VALUE)),'FM999,999,999.9') \"Logons Current\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'opened cursors current',VALUE)),'FM999,999,999.9') \"Opened Cursors Current\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'SQL*Net roundtrips to/from client',VALUE)),'FM999,999,999.9') \"SQLNet RoundTrip Client/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical read total bytes',VALUE)),'FM999,999,999.9') \"Physical Read Total Bytes/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cell physical IO interconnect bytes',VALUE)),'FM999,999,999.9') \"CellPhy.IOInterconnectByte/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cell physical IO bytes saved by storage index',VALUE)),'FM999,999,999.9') \"CellPhy.IOBytesSavedbySInd/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cell physical IO interconnect bytes returned by smart scan',VALUE)),'FM999,999,999.9') \"CellPhy.IOByteReturnedbySS/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical read total IO requests',VALUE)),'FM999,999,999.9') \"Physical Read Total IO Req/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical read requests optimized',VALUE)),'FM999,999,999.9') \"Physical Read Req Opt./Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cell flash cache read hits',VALUE)),'FM999,999,999.9') \"Cell Flash Cache Read Hits/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cell scans',VALUE)),'FM999,999,999.9') \"Cell Scans/Sec\"
          ,TO_CHAR(SUM(DECODE(STAT_NAME,'cell index scans',VALUE)),'FM999,999,999.9') \"Cell Index Scans/Sec\"
        FROM
            ( SELECT STAT_NAME
              ,DECODE(G1
                    ,1,'SUB AVG'
                    ,SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1))
                SNAP_TIME
              ,SNAP_TIME SNAP_TIME_RANGE
              ,VALUE
              ,VALUE_DIFF
           FROM
                ( SELECT STAT_NAME
                  ,START_TIME
                        || '-'
                        || END_TIME SNAP_TIME
                  ,ROUND(AVG(NVL(VALUE,0)),1) VALUE
                  ,ROUND(AVG(NVL(VALUE_DIFF,0)),1) VALUE_DIFF
                  ,GROUPING(START_TIME
                        || '-'
                        || END_TIME) G1
                  ,GROUPING(STAT_NAME) G2
               FROM
                    ( SELECT STAT_NAME
                      ,TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                      ,TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                      ,DECODE(SNAP_TIME_C2
                            ,NULL,0
                            ,ROUND((CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)),1))
                        VALUE
                      ,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END) VALUE_DIFF
                      ,ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) RNUM
                      ,SNAP_ID
                      ,INSTANCE_NUMBER
                   FROM
                        ( SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) */ SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                          ,LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                          ,STAT.STAT_NAME
                          ,STAT.VALUE VALUE_1
                          ,LEAD(STAT.VALUE) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                          ,SNAP.SNAP_ID
                          ,DBI.INSTANCE_NUMBER
                       FROM
                             ( SELECT /*+ NO_MERGE */ DI.DBID
                               ,DI.INSTANCE_NUMBER
                               ,DI.STARTUP_TIME
                             FROM
                               DBA_HIST_DATABASE_INSTANCE DI
                             WHERE
                                 DI.DBID = :DBID
                                 AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                 AND ROWNUM<=1
                             ) DBI
                          ,DBA_HIST_SNAPSHOT SNAP
                          ,DBA_HIST_SYSSTAT STAT
                      WHERE DBI.DBID = SNAP.DBID
                        AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                        AND DBI.DBID = SNAP.DBID
                        AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                        AND SNAP.SNAP_ID<=:END_SNAP_ID
                        AND SNAP.DBID = STAT.DBID
                        AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                        AND SNAP.SNAP_ID = STAT.SNAP_ID
                        AND STAT.STAT_NAME IN (
                            'session logical reads'
                            ,'physical reads'
                            ,'execute count'
                            ,'user calls'
                            ,'user commits'
                            ,'user rollbacks'
                            ,'CPU used by this session'
                            ,'DB time'
                            ,'application wait time'
                            ,'cluster wait time'
                            ,'concurrency wait time'
                            ,'user I/O wait time'
                            ,'CR blocks created'
                            ,'consistent changes'
                            ,'consistent gets'
                            ,'db block changes'
                            ,'data blocks consistent reads - undo records applied'
                            ,'db block gets'
                            ,'enqueue releases'
                            ,'enqueue waits'
                            ,'DBWR checkpoints'
                            ,'DBWR lru scans'
                            ,'DBWR undo block writes'
                            ,'free buffer inspected'
                            ,'free buffer requested'
                            ,'physical writes'
                            ,'physical writes direct'
                            ,'branch node splits'
                            ,'leaf node 90-10 splits'
                            ,'leaf node splits'
                            ,'cleanouts and rollbacks - consistent read gets'
                            ,'cleanouts only - consistent read gets'
                            ,'commit cleanouts'
                            ,'index fast full scans (full)'
                            ,'table fetch by rowid'
                            ,'table fetch continued row'
                            ,'table scan blocks gotten'
                            ,'table scan rows gotten'
                            ,'table scans (long tables)'
                            ,'table scans (short tables)'
                            ,'logons cumulative'
                            ,'opened cursors cumulative'
                            ,'parse count (total)'
                            ,'parse count (hard)'
                            ,'parse count (failures)'
                            ,'parse time cpu'
                            ,'parse time elapsed'
                            ,'session cursor cache count'
                            ,'session cursor cache hits'
                            ,'redo entries'
                            ,'redo size'
                            ,'redo sync time'
                            ,'redo write time'
                            ,'sorts (disk)'
                            ,'sorts (memory)'
                            ,'sorts (rows)'
                            ,'sql area evicted'
                            ,'sql area purged'
                            ,'undo change vector size'
                            ,'SQL*Net roundtrips to/from client'
                            ,'transaction tables consistent reads - undo records applied'
                            ,'transaction tables consistent read rollbacks'
                            ,'physical read total bytes'
                            ,'physical read total IO requests'
                            ,'physical read requests optimized'
                            ,'cell physical IO interconnect bytes'
                            ,'cell physical IO bytes saved by storage index'
                            ,'cell physical IO interconnect bytes returned by smart scan'
                            ,'cell flash cache read hits'
                            ,'cell scans'
                            ,'cell index scans'
                            )
                   ORDER BY SNAP.SNAP_ID
                        )
                   WHERE SNAP_TIME_C2 <> SNAP_TIME_C1
                    )
              WHERE START_TIME IS NOT NULL
                AND END_TIME IS NOT NULL
           GROUP BY ROLLUP(STAT_NAME,START_TIME
                        || '-'
                        || END_TIME)
                )
          WHERE NOT (G1=1
            AND G2=1)
        UNION ALL
         SELECT STAT_NAME
              ,DECODE(G1
                    ,1,'SUB AVG'
                    ,SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1))
                SNAP_TIME
              ,SNAP_TIME SNAP_TIME_RANGE
              ,VALUE
              ,NULL VALUE_DIFF
           FROM
                ( SELECT STAT_NAME
                  ,START_TIME
                        || '-'
                        || END_TIME SNAP_TIME
                  ,ROUND(AVG(DECODE(SIGN(VALUE)
                                  ,-1,0
                                  ,NVL(VALUE,0))
                    ),1) VALUE
                  ,GROUPING(START_TIME
                        || '-'
                        || END_TIME) G1
                  ,GROUPING(STAT_NAME) G2
               FROM
                    ( SELECT STAT_NAME
                      ,TO_CHAR(SNAP_TIME_1,'MM.DD HH24:MI') START_TIME
                      ,TO_CHAR(SNAP_TIME_2,'MM.DD HH24:MI') END_TIME
                      ,DECODE(SNAP_TIME_2
                            ,NULL,0
                            ,ROUND((VALUE_1-VALUE_2),1))
                        VALUE
                      ,ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) RNUM
                      ,SNAP_ID
                      ,INSTANCE_NUMBER
                   FROM
                        ( SELECT SNAP.BEGIN_INTERVAL_TIME SNAP_TIME_1
                          ,STAT.STAT_NAME
                          ,STAT.VALUE VALUE_1
                          ,0 VALUE_2
                          ,SNAP.END_INTERVAL_TIME SNAP_TIME_2
                          ,SNAP.SNAP_ID
                          ,DBI.INSTANCE_NUMBER
                       FROM
                             ( SELECT DI.DBID
                               ,DI.INSTANCE_NUMBER
                               ,DI.STARTUP_TIME
                             FROM
                               DBA_HIST_DATABASE_INSTANCE DI
                             WHERE
                                 DI.DBID = :DBID
                                 AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                 AND ROWNUM<=1
                             ) DBI
                          ,DBA_HIST_SNAPSHOT SNAP
                          ,DBA_HIST_SYSSTAT STAT
                      WHERE DBI.DBID = SNAP.DBID
                        AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                        AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                        AND SNAP.SNAP_ID<=:END_SNAP_ID
                        AND SNAP.DBID = STAT.DBID
                        AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                        AND SNAP.SNAP_ID = STAT.SNAP_ID
                        AND STAT.STAT_NAME IN (
                             'opened cursors current'
                             ,'logons current'
                             ,'session pga memory'
                             ,'session uga memory'
                             ,'session pga memory max'
                             ,'session uga memory max'
                             )
                   ORDER BY SNAP.SNAP_ID
                        )
                    )
              WHERE RNUM>1
           GROUP BY ROLLUP(STAT_NAME,START_TIME
                        || '-'
                        || END_TIME)
                )
          WHERE NOT (G1=1
            AND G2=1)
        ORDER BY STAT_NAME,SNAP_TIME
            )
        GROUP BY SNAP_TIME
          ,SNAP_TIME_RANGE
        ORDER BY SNAP_TIME
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 4:
        sqlcommand = """
        SELECT SNAP_TIME
          ,SNAP_TIME_RANGE
          ,NVL(SUM(DECODE(STAT_NAME,'db file sequential read',VALUE)),0) \"db file sequential read/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file scattered read',VALUE)),0) \"db file scattered read/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file parallel write',VALUE)),0) \"db file parallel write/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file parallel write',VALUE)),0) \"log file parallel write/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file sync',VALUE)),0) \"log file sync/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file switch completion',VALUE)),0) \"log file switch completion/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'buffer busy waits',VALUE)),0) \"buffer busy waits/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc buffer busy',VALUE)),0) \"gc buffer busy/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc cr block busy',VALUE)),0) \"gc cr block busy/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: library cache',VALUE)),0) \"latch: library cache/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: shared pool',VALUE)),0) \"latch: shared pool/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: cache buffers chains',VALUE)),0) \"latch:cache buffers chains/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - row lock contention',VALUE)),0) \"enq:TX - RowLockContention/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - index contention',VALUE)),0) \"enq:TX - IndexContention/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read',VALUE)),0) \"direct path read/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write',VALUE)),0) \"direct path write/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read temp',VALUE)),0) \"direct path read temp/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write temp',VALUE)),0) \"direct path write temp/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache pin',VALUE)),0) \"library cache pin/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache: mutex X',VALUE)),0) \"library cache: mutex X/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex S',VALUE)),0) \"cursor: mutex S/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex X',VALUE)),0) \"cursor: mutex X/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S',VALUE)),0) \"cursor: pin S/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin X',VALUE)),0) \"cursor: pin X/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S wait on X',VALUE)),0) \"cursor: pin S wait on X/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart table scan',VALUE)),0) \"CellSmartTableScan/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart index scan',VALUE)),0) \"CellSmartIndexScan/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell single block physical read',VALUE)),0) \"CellSingleBlk.Phy.Read/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell multiblock physical read',VALUE)),0) \"CellMultiBlk.Phy.Read/Sec\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file sequential read',WAITS)),0) \"db file sequential r avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file scattered read',WAITS)),0) \"db file scattered r avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file parallel write',WAITS)),0) \"db file parallel w avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file parallel write',WAITS)),0) \"log file parallel w avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file sync',WAITS)),0) \"log file sync avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file switch completion',WAITS)),0) \"log file switch comp avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'buffer busy waits',WAITS)),0) \"buffer busy waits avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc buffer busy',WAITS)),0) \"gc buffer busy avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc cr block busy',WAITS)),0) \"gc cr block busy avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: library cache',WAITS)),0) \"latch: library cache avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: shared pool',WAITS)),0) \"latch: shared pool avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: cache buffers chains',WAITS)),0) \"latch:cache buffers c avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - row lock contention',WAITS)),0) \"enq:TX - row lock c avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - index contention',WAITS)),0) \"enq:TX - index c avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read',WAITS)),0) \"direct path read avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write',WAITS)),0) \"direct path write avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read temp',WAITS)),0) \"direct path read t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write temp',WAITS)),0) \"direct path write t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache pin',WAITS)),0) \"library cache pin t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache: mutex X',WAITS)),0) \"lib. cache: mutex X t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex S',WAITS)),0) \"cursor: mutex S t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex X',WAITS)),0) \"cursor: mutex X t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S',WAITS)),0) \"cursor: pin S t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin X',WAITS)),0) \"cursor: pin X t. avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S wait on X',WAITS)),0) \"cursor:pin S wait on X avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart table scan',WAITS)),0) \"CellSmartTableScan avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart index scan',WAITS)),0) \"CellSmartIndexScan avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell single block physical read',WAITS)),0) \"CellSingleBlk.Phy.Read avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell multiblock physical read',WAITS)),0) \"CellMultiBlk.Phy.Read avg(ms)\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file sequential read',VALUE_DIFF)),0) \"Total db file sequential read\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file scattered read',VALUE_DIFF)),0) \"Total db file scattered read\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file parallel write',VALUE_DIFF)),0) \"Total db file parallel write\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file parallel write',VALUE_DIFF)),0) \"Total log file parallel write\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file sync',VALUE_DIFF)),0) \"Total log file sync\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file switch completion',VALUE_DIFF)),0) \"Total log file switch compl.\"
          ,NVL(SUM(DECODE(STAT_NAME,'buffer busy waits',VALUE_DIFF)),0) \"Total buffer busy waits\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc buffer busy',VALUE_DIFF)),0) \"Total gc buffer busy\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc cr block busy',VALUE_DIFF)),0) \"Total gc cr block busy\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: library cache',VALUE_DIFF)),0) \"Total latch: library cache\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: shared pool',VALUE_DIFF)),0) \"Total latch: shared pool\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: cache buffers chains',VALUE_DIFF)),0) \"Total latch: cache buffers ch.\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - row lock contention',VALUE_DIFF)),0) \"Total enq: TX - row lock cont.\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - index contention',VALUE_DIFF)),0) \"Total enq: TX - index cont.\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read',VALUE_DIFF)),0) \"Total direct path read\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write',VALUE_DIFF)),0) \"Total direct path write\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read temp',VALUE_DIFF)),0) \"Total direct path read\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write temp',VALUE_DIFF)),0) \"Total direct path write\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache pin',VALUE_DIFF)),0) \"Total library cache pin\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache: mutex X',VALUE_DIFF)),0) \"Total library cache: mutex X\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex S',VALUE_DIFF)),0) \"Total cursor: mutex S\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex X',VALUE_DIFF)),0) \"Total cursor: mutex X\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S',VALUE_DIFF)),0) \"Total cursor: pin S\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin X',VALUE_DIFF)),0) \"Total cursor: pin X\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S wait on X',VALUE_DIFF)),0) \"Total cursor: pin S wait on X\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart table scan',VALUE_DIFF)),0) \"Total CellSmartTableScan\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart index scan',VALUE_DIFF)),0) \"Total CellSmartIndexScan\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell single block physical read',VALUE_DIFF)),0) \"Total CellSingleBlk.Phy.Read\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell multiblock physical read',VALUE_DIFF)),0) \"Total CellMultiBlk.Phy.Read\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file sequential read',WAITS_DIFF)),0) \"db file sequential read WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file scattered read',WAITS_DIFF)),0) \"db file scattered read WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'db file parallel write',WAITS_DIFF)),0) \"db file parallel write WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file parallel write',WAITS_DIFF)),0) \"log file parallel write WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file sync',WAITS_DIFF)),0) \"log file sync WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'log file switch completion',WAITS_DIFF)),0) \"log file switch compl. WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'buffer busy waits',WAITS_DIFF)),0) \"buffer busy waits WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc buffer busy',WAITS_DIFF)),0) \"gc buffer busy WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'gc cr block busy',WAITS_DIFF)),0) \"gc cr block busy WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: library cache',WAITS_DIFF)),0) \"latch: library cache WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: shared pool',WAITS_DIFF)),0) \"latch: shared pool WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'latch: cache buffers chains',WAITS_DIFF)),0) \"latch: cache buffers ch. WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - row lock contention',WAITS_DIFF)),0) \"enq: TX - row lock cont. WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'enq: TX - index contention',WAITS_DIFF)),0) \"enq: TX - index cont. WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read',WAITS_DIFF)),0) \"direct path read WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write',WAITS_DIFF)),0) \"direct path write WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path read temp',WAITS_DIFF)),0) \"direct path read WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'direct path write temp',WAITS_DIFF)),0) \"direct path write WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache pin',WAITS_DIFF)),0) \"library cache pin WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'library cache: mutex X',WAITS_DIFF)),0) \"library cache: mutex X WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex S',WAITS_DIFF)),0) \"cursor: mutex S WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: mutex X',WAITS_DIFF)),0) \"cursor: mutex X WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S',WAITS_DIFF)),0) \"cursor: pin S WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin X',WAITS_DIFF)),0) \"cursor: pin X WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cursor: pin S wait on X',WAITS_DIFF)),0) \"cursor: pin S wait on X WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart table scan',WAITS_DIFF)),0) \"CellSmartTableScan WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell smart index scan',WAITS_DIFF)),0) \"CellSmartIndexScan WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell single block physical read',WAITS_DIFF)),0) \"CellSingleBlk.Phy.Read WT\"
          ,NVL(SUM(DECODE(STAT_NAME,'cell multiblock physical read',WAITS_DIFF)),0) \"CellMultiBlk.Phy.Read WT\"
        FROM
            ( SELECT STAT_NAME
              ,DECODE(G1
                    ,1,'SUB AVG'
                    ,SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1))
                SNAP_TIME
              ,SNAP_TIME SNAP_TIME_RANGE
              ,NVL(VALUE,0) VALUE
              ,NVL(VALUE_DIFF,0) VALUE_DIFF
              ,NVL(WAITS,0) WAITS
              ,NVL(WAITS_DIFF,0) WAITS_DIFF
           FROM
                ( SELECT STAT_NAME
                  ,START_TIME
                        || '-'
                        || END_TIME SNAP_TIME
                  ,ROUND(AVG(NVL(VALUE,0)),3) VALUE
                  ,ROUND(AVG(NVL(VALUE_DIFF,0)),3) VALUE_DIFF
                  ,ROUND(AVG(NVL(WAITS,0)),3) WAITS
                  ,ROUND(AVG(NVL(WAITS_DIFF,0)),3) WAITS_DIFF
                  ,GROUPING(START_TIME
                        || '-'
                        || END_TIME) G1
                  ,GROUPING(STAT_NAME) G2
               FROM
                    ( SELECT STAT_NAME
                      ,TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                      ,TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                      ,ROUND(DECODE(SNAP_TIME_C2
                            ,NULL,0
                            ,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1))) / 1000000,3)
                        VALUE
                      ,ROUND((CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/1000000,1) VALUE_DIFF
                      ,ROUND(DECODE(SNAP_TIME_C2
                            ,NULL,0
                            ,(CASE WHEN WAITS_2<=WAITS_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(WAITS_2-WAITS_1))/1000,3)
                        WAITS
                      ,ROUND((CASE WHEN WAITS_2<=WAITS_1 THEN 0 ELSE WAITS_2-WAITS_1 END),1) WAITS_DIFF
                      ,ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) RNUM
                      ,SNAP_ID
                      ,INSTANCE_NUMBER
                   FROM
                        ( SELECT SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                          ,LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                          ,STAT.EVENT_NAME STAT_NAME
                          ,STAT.TIME_WAITED_MICRO VALUE_1
                          ,STAT.TOTAL_WAITS WAITS_1
                          ,LEAD(STAT.TIME_WAITED_MICRO) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                          ,LEAD(STAT.TOTAL_WAITS) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) WAITS_2
                          ,SNAP.SNAP_ID
                          ,DBI.INSTANCE_NUMBER
                       FROM
                             ( SELECT DI.DBID
                               ,DI.INSTANCE_NUMBER
                               ,DI.STARTUP_TIME
                             FROM
                               DBA_HIST_DATABASE_INSTANCE DI
                             WHERE
                                 DI.DBID = :DBID
                                 AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                 AND ROWNUM<=1
                             ) DBI
                          ,DBA_HIST_SNAPSHOT SNAP
                          ,DBA_HIST_SYSTEM_EVENT STAT
                      WHERE DBI.DBID = SNAP.DBID
                        AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                        AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                        AND SNAP.SNAP_ID<=:END_SNAP_ID
                        AND SNAP.DBID = STAT.DBID
                        AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                        AND SNAP.SNAP_ID = STAT.SNAP_ID
                        AND STAT.EVENT_NAME IN (
                                              'db file sequential read'
                                             ,'db file scattered read'
                                             ,'db file parallel write'
                                             ,'log file parallel write'
                                             ,'log file sync'
                                             ,'log file switch completion'
                                             ,'buffer busy waits'
                                             ,'gc buffer busy'
                                             ,'gc cr block busy'
                                             ,'latch: library cache'
                                             ,'latch: shared pool'
                                             ,'latch: cache buffers chains'
                                             ,'enq: TX - row lock contention'
                                             ,'enq: TX - index contention'
                                             ,'direct path read'
                                             ,'direct path write'
                                             ,'direct path read temp'
                                             ,'direct path write temp'
                                             ,'library cache: mutex X'
                                             ,'library cache pin'
                                             ,'cursor: mutex X'
                                             ,'cursor: mutex S'
                                             ,'cursor: pin X'
                                             ,'cursor: pin S','cursor: pin S wait on X'
                                             ,'cell smart table scan'
                                             ,'cell smart index scan'
                                             ,'cell single block physical read'
                                             ,'cell multiblock physical read'
                                             )
                   ORDER BY SNAP.SNAP_ID
                        )
                   WHERE SNAP_TIME_C2 <> SNAP_TIME_C1
                   AND WAITS_2 <> WAITS_1
                    )
              WHERE START_TIME IS NOT NULL
                AND END_TIME IS NOT NULL
           GROUP BY ROLLUP(STAT_NAME,START_TIME
                        || '-'
                        || END_TIME)
                )
          WHERE NOT (G1=1
            AND G2=1)
            )
        GROUP BY SNAP_TIME
          ,SNAP_TIME_RANGE
        ORDER BY SNAP_TIME
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 5:
        sqlcommand = """
        SELECT SNAP_TIME
          , TO_CHAR(SUM(DECODE(RNK, 1,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_1\"
          , TO_CHAR(SUM(DECODE(RNK, 2,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_2\"
          , TO_CHAR(SUM(DECODE(RNK, 3,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_3\"
          , TO_CHAR(SUM(DECODE(RNK, 4,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_4\"
          , TO_CHAR(SUM(DECODE(RNK, 5,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_5\"
          , TO_CHAR(SUM(DECODE(RNK, 6,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_6\"
          , TO_CHAR(SUM(DECODE(RNK, 7,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_7\"
          , TO_CHAR(SUM(DECODE(RNK, 8,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_8\"
          , TO_CHAR(SUM(DECODE(RNK, 9,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_9\"
          , TO_CHAR(SUM(DECODE(RNK, 10,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_10\"
          , TO_CHAR(SUM(DECODE(RNK, 11,VALUE) ),'FM999,999,999,999.999') \"VALUE Per SEC_ETC\"
          , TO_CHAR(SUM(DECODE(RNK, 1,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_1
          , TO_CHAR(SUM(DECODE(RNK, 2,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_2
          , TO_CHAR(SUM(DECODE(RNK, 3,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_3
          , TO_CHAR(SUM(DECODE(RNK, 4,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_4
          , TO_CHAR(SUM(DECODE(RNK, 5,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_5
          , TO_CHAR(SUM(DECODE(RNK, 6,VALUE_DIFF) ),'FM999,999,999,999,999')  VALUE_DIFF_6
          , TO_CHAR(SUM(DECODE(RNK, 7,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_7
          , TO_CHAR(SUM(DECODE(RNK, 8,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_8
          , TO_CHAR(SUM(DECODE(RNK, 9,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_9
          , TO_CHAR(SUM(DECODE(RNK, 10,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_10
          , TO_CHAR(SUM(DECODE(RNK, 11,VALUE_DIFF) ),'FM999,999,999,999,999') VALUE_DIFF_ETC
        FROM
            (
            SELECT
                A.EVENT_NAME, B.EVENT_NAME B_EVENT_NAME
              , A.SNAP_TIME
              , A.VALUE
              , A.VALUE_DIFF
              , B.RNK RNK
            FROM
                (
                SELECT
                   EVENT_NAME
                  , DECODE(G1, 1,'SUB AVG', SNAP_TIME) SNAP_TIME
                  , VALUE
                  , VALUE_DIFF
                FROM
                    (SELECT EVENT_NAME
                      , END_TIME SNAP_TIME
                      , AVG(NVL(VALUE,0)) VALUE
                      , AVG(NVL(VALUE_DIFF,0)) VALUE_DIFF
                      , GROUPING(END_TIME) G1
                      , GROUPING(EVENT_NAME) G2
                    FROM
                        (SELECT EVENT_NAME
                          , TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                          , TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                          , DECODE(SNAP_TIME_C2, NULL,0, (
                            CASE
                                WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                            END) /(1000000*(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)))) VALUE
                          , (
                            CASE
                                WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                            END) /1000000 VALUE_DIFF
                          , ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP_ID) RNUM
                          , SNAP_ID
                          , INSTANCE_NUMBER
                        FROM
                            (SELECT SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                              , LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                              , EVENT.EVENT_NAME
                              , EVENT.TIME_WAITED_MICRO VALUE_1
                              , LEAD(EVENT.TIME_WAITED_MICRO) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                              , SNAP.SNAP_ID
                              , DBI.INSTANCE_NUMBER
                            FROM
                                (SELECT DI.DBID
                                  ,DI.INSTANCE_NUMBER
                                  ,DI.STARTUP_TIME
                                FROM DBA_HIST_DATABASE_INSTANCE DI
                                WHERE DI.DBID = :DBID
                                AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                AND ROWNUM<=1
                                ) DBI
                              ,DBA_HIST_SNAPSHOT SNAP
                              ,DBA_HIST_SYSTEM_EVENT EVENT
                            WHERE DBI.DBID = SNAP.DBID
                            AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                            AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                            AND SNAP.SNAP_ID<=:END_SNAP_ID
                            AND SNAP.DBID = EVENT.DBID
                            AND SNAP.INSTANCE_NUMBER = EVENT.INSTANCE_NUMBER
                            AND SNAP.SNAP_ID = EVENT.SNAP_ID
                            AND UPPER(EVENT.WAIT_CLASS) <> 'IDLE'
                            ORDER BY SNAP.SNAP_ID
                            )
                        )
                    WHERE START_TIME IS NOT NULL
                    AND END_TIME IS NOT NULL
                    GROUP BY ROLLUP(EVENT_NAME,END_TIME)
                    )
                WHERE NOT ( G1=1 AND G2=1 )
                ) A
              , (
              SELECT EVENT_NAME
                  , RNK
                FROM
                    (
                    SELECT
                        EVENT_NAME
                      , RNK
                    FROM
                        (SELECT
                            EVENT_NAME
                          , RANK() OVER(ORDER BY VALUE_DIFF DESC,EVENT_NAME ASC NULLS LAST) RNK
                        FROM
                            (
                            SELECT
                               EVENT_NAME
                              , DECODE(G1, 1,'SUB AVG', SNAP_TIME) SNAP_TIME
                              , VALUE
                              , VALUE_DIFF
                            FROM
                                (SELECT EVENT_NAME
                                  , END_TIME SNAP_TIME
                                  , AVG(NVL(VALUE,0)) VALUE
                                  , AVG(NVL(VALUE_DIFF,0)) VALUE_DIFF
                                  , GROUPING(END_TIME) G1
                                  , GROUPING(EVENT_NAME) G2
                                FROM
                                    (SELECT EVENT_NAME
                                      , TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                                      , TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                                      , DECODE(SNAP_TIME_C2, NULL,0, (
                                        CASE
                                            WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                                        END) /(1000000*(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)))) VALUE
                                      , (
                                        CASE
                                            WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                                        END) /1000000 VALUE_DIFF
                                      , ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP_ID) RNUM
                                      , SNAP_ID
                                      , INSTANCE_NUMBER
                                    FROM
                                        (SELECT SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                                          , LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                                          , EVENT.EVENT_NAME
                                          , EVENT.TIME_WAITED_MICRO VALUE_1
                                          , LEAD(EVENT.TIME_WAITED_MICRO) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                                          , SNAP.SNAP_ID
                                          , DBI.INSTANCE_NUMBER
                                        FROM
                                            (SELECT DI.DBID
                                              ,DI.INSTANCE_NUMBER
                                              ,DI.STARTUP_TIME
                                            FROM DBA_HIST_DATABASE_INSTANCE DI
                                            WHERE DI.DBID = :DBID
                                            AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                            AND ROWNUM<=1
                                            ) DBI
                                          ,DBA_HIST_SNAPSHOT SNAP
                                          ,DBA_HIST_SYSTEM_EVENT EVENT
                                        WHERE DBI.DBID = SNAP.DBID
                                        AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                                        AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                                        AND SNAP.SNAP_ID<=:END_SNAP_ID
                                        AND SNAP.DBID = EVENT.DBID
                                        AND SNAP.INSTANCE_NUMBER = EVENT.INSTANCE_NUMBER
                                        AND SNAP.SNAP_ID = EVENT.SNAP_ID
                                        AND UPPER(EVENT.WAIT_CLASS) <> 'IDLE'
                                        ORDER BY SNAP.SNAP_ID
                                        )
                                    )
                                WHERE START_TIME IS NOT NULL
                                AND END_TIME IS NOT NULL
                                GROUP BY ROLLUP(EVENT_NAME,END_TIME)
                                )
                            WHERE NOT ( G1=1 AND G2=1 )
                            )
                        WHERE SNAP_TIME='SUB AVG'
                        )
                    WHERE RNK<=10
                    )
              ) B
              
            WHERE A.EVENT_NAME = B.EVENT_NAME
            
            UNION ALL
            
            SELECT ' 나머지 Event Sum' EVENT_NAME, NULL B_EVENT_NAME
              , A.SNAP_TIME
              , SUM(A.VALUE) VALUE
              , SUM(A.VALUE_DIFF) VALUE_DIFF
              , 11 RNK
            FROM
                (
                SELECT
                   EVENT_NAME
                  , DECODE(G1, 1,'SUB AVG', SNAP_TIME) SNAP_TIME
                  , VALUE
                  , VALUE_DIFF
                FROM
                    (SELECT EVENT_NAME
                      , END_TIME SNAP_TIME
                      , AVG(NVL(VALUE,0)) VALUE
                      , AVG(NVL(VALUE_DIFF,0)) VALUE_DIFF
                      , GROUPING(END_TIME) G1
                      , GROUPING(EVENT_NAME) G2
                    FROM
                        (SELECT EVENT_NAME
                          , TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                          , TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                          , DECODE(SNAP_TIME_C2, NULL,0, (
                            CASE
                                WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                            END) /(1000000*(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)))) VALUE
                          , (
                            CASE
                                WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                            END) /1000000 VALUE_DIFF
                          , ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP_ID) RNUM
                          , SNAP_ID
                          , INSTANCE_NUMBER
                        FROM
                            (SELECT SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                              , LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                              , EVENT.EVENT_NAME
                              , EVENT.TIME_WAITED_MICRO VALUE_1
                              , LEAD(EVENT.TIME_WAITED_MICRO) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                              , SNAP.SNAP_ID
                              , DBI.INSTANCE_NUMBER
                            FROM
                                (SELECT DI.DBID
                                  ,DI.INSTANCE_NUMBER
                                  ,DI.STARTUP_TIME
                                FROM DBA_HIST_DATABASE_INSTANCE DI
                                WHERE DI.DBID = :DBID
                                AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                AND ROWNUM<=1
                                ) DBI
                              ,DBA_HIST_SNAPSHOT SNAP
                              ,DBA_HIST_SYSTEM_EVENT EVENT
                            WHERE DBI.DBID = SNAP.DBID
                            AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                            AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                            AND SNAP.SNAP_ID<=:END_SNAP_ID
                            AND SNAP.DBID = EVENT.DBID
                            AND SNAP.INSTANCE_NUMBER = EVENT.INSTANCE_NUMBER
                            AND SNAP.SNAP_ID = EVENT.SNAP_ID
                            AND UPPER(EVENT.WAIT_CLASS) <> 'IDLE'
                            ORDER BY SNAP.SNAP_ID
                            )
                        )
                    WHERE START_TIME IS NOT NULL
                    AND END_TIME IS NOT NULL
                    GROUP BY ROLLUP(EVENT_NAME,END_TIME)
                    )
                WHERE NOT ( G1=1 AND G2=1 )
                ) A
            WHERE A.EVENT_NAME NOT IN
                (
                SELECT EVENT_NAME
                FROM
                  (
                  SELECT EVENT_NAME
                      , RNK
                    FROM
                        (
                        SELECT
                            EVENT_NAME
                          , RNK
                        FROM
                            (SELECT
                                EVENT_NAME
                              , RANK() OVER(ORDER BY VALUE_DIFF DESC,EVENT_NAME ASC NULLS LAST) RNK
                            FROM
                                (
                                SELECT
                                   EVENT_NAME
                                  , DECODE(G1, 1,'SUB AVG', SNAP_TIME) SNAP_TIME
                                  , VALUE
                                  , VALUE_DIFF
                                FROM
                                    (SELECT EVENT_NAME
                                      , END_TIME SNAP_TIME
                                      , AVG(NVL(VALUE,0)) VALUE
                                      , AVG(NVL(VALUE_DIFF,0)) VALUE_DIFF
                                      , GROUPING(END_TIME) G1
                                      , GROUPING(EVENT_NAME) G2
                                    FROM
                                        (SELECT EVENT_NAME
                                          , TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                                          , TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                                          , DECODE(SNAP_TIME_C2, NULL,0, (
                                            CASE
                                                WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                                            END) /(1000000*(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)))) VALUE
                                          , (
                                            CASE
                                                WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1
                                            END) /1000000 VALUE_DIFF
                                          , ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP_ID) RNUM
                                          , SNAP_ID
                                          , INSTANCE_NUMBER
                                        FROM
                                            (SELECT SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                                              , LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                                              , EVENT.EVENT_NAME
                                              , EVENT.TIME_WAITED_MICRO VALUE_1
                                              , LEAD(EVENT.TIME_WAITED_MICRO) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                                              , SNAP.SNAP_ID
                                              , DBI.INSTANCE_NUMBER
                                            FROM
                                                (SELECT DI.DBID
                                                  ,DI.INSTANCE_NUMBER
                                                  ,DI.STARTUP_TIME
                                                FROM DBA_HIST_DATABASE_INSTANCE DI
                                                WHERE DI.DBID = :DBID
                                                AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                                AND ROWNUM<=1
                                                ) DBI
                                              ,DBA_HIST_SNAPSHOT SNAP
                                              ,DBA_HIST_SYSTEM_EVENT EVENT
                                            WHERE DBI.DBID = SNAP.DBID
                                            AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                                            AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                                            AND SNAP.SNAP_ID<=:END_SNAP_ID
                                            AND SNAP.DBID = EVENT.DBID
                                            AND SNAP.INSTANCE_NUMBER = EVENT.INSTANCE_NUMBER
                                            AND SNAP.SNAP_ID = EVENT.SNAP_ID
                                            AND UPPER(EVENT.WAIT_CLASS) <> 'IDLE'
                                            ORDER BY SNAP.SNAP_ID
                                            )
                                        )
                                    WHERE START_TIME IS NOT NULL
                                    AND END_TIME IS NOT NULL
                                    GROUP BY ROLLUP(EVENT_NAME,END_TIME)
                                    )
                                WHERE NOT ( G1=1 AND G2=1 )
                                )
                            WHERE SNAP_TIME='SUB AVG'
                            )
                        WHERE RNK<=10
                        )
                  ) B
                )
            GROUP BY A.SNAP_TIME
              , A.SNAP_TIME
            ORDER BY RNK
              , SNAP_TIME
            )
        GROUP BY SNAP_TIME
        ORDER BY SNAP_TIME
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 6:
        sqlcommand = """
        SELECT
             SNAP_TIME
             ,SNAP_TIME_RANGE
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'session logical reads',VALUE)),'FM999,999,999,999.9') \"Session Logical Reads/Sec\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical reads',VALUE)),'FM999,999,999.9') \"Physical Reads/Sec\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical read total IO requests',VALUE))+SUM(DECODE(STAT_NAME,'physical write total IO requests',VALUE)),'FM999,999,999.9') \"IOPS\"
             ,TO_CHAR((SUM(DECODE(STAT_NAME,'physical read total bytes',VALUE))+SUM(DECODE(STAT_NAME,'physical write total bytes',VALUE)))/1024/1024,'FM999,999,999.99') \"MBPS\"
             ,TO_CHAR(
                 GREATEST(LEAST((
                     1-
                     (
                         SUM(DECODE(STAT_NAME,'physical reads',VALUE))
                         -SUM(DECODE(STAT_NAME,'physical reads direct',VALUE))
                         -SUM(DECODE(STAT_NAME,'physical reads direct (lob)',VALUE))
                     )/
                     DECODE(
                         (
                             SUM(DECODE(STAT_NAME,'session logical reads',VALUE))
                             -SUM(DECODE(STAT_NAME,'physical reads direct',VALUE))
                             -SUM(DECODE(STAT_NAME,'physical reads direct (lob)',VALUE))
                         )
                         ,0,1
                         ,(
                             SUM(DECODE(STAT_NAME,'session logical reads',VALUE))
                             -SUM(DECODE(STAT_NAME,'physical reads direct',VALUE))
                             -SUM(DECODE(STAT_NAME,'physical reads direct (lob)',VALUE))
                         )
                     )
                 ) * 100,100),0)
                 ,'FM999.99') \"Buffer Cache Hit Ratio(%)\"
             ,TO_CHAR(NVL(SUM(DECODE(STAT_NAME,'db file sequential read',WAITS)),0),'FM999,999,999.99') \"db file sequential r avg.(ms)\"
             ,TO_CHAR(NVL(SUM(DECODE(STAT_NAME,'db file scattered read',WAITS)),0),'FM999,999,999.99') \"db file scattered r avg.(ms)\"
             ,TO_CHAR(NVL(SUM(DECODE(STAT_NAME,'db file parallel write',WAITS)),0),'FM999,999,999.99') \"db file parallel w avg.(ms)\"
             ,TO_CHAR(NVL(SUM(DECODE(STAT_NAME,'log file parallel write',WAITS)),0),'FM999,999,999.99') \"log file parallel w avg.(ms)\"
             ,TO_CHAR(NVL(SUM(DECODE(STAT_NAME,'log file sync',WAITS)),0),'FM999,999,999.99') \"log file sync avg.(ms)\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical read total IO requests',VALUE)),'FM999,999,999,999.9') \"Physical R total IO req./Sec\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical write total IO requests',VALUE)),'FM999,999,999,999.9') \"Physical W total IO req./Sec\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical read total bytes',VALUE))/1024/1024,'FM999,999,999,999.9') \"Physical R total MB/Sec\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical write total bytes',VALUE))/1024/1024,'FM999,999,999,999.9') \"Physical W total MB/Sec\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical reads direct',VALUE)),'FM999,999,999,999.9') \"Physical Reads Direct/Sec\"
             ,TO_CHAR(SUM(DECODE(STAT_NAME,'physical reads direct (lob)',VALUE)),'FM999,999,999,999.9') \"Physical Reads Direct(LOB)/Sec\"
         FROM
             (
             SELECT
                 STAT_NAME
                 ,DECODE(G1,1,'SUB AVG',SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1)) SNAP_TIME
                 ,SNAP_TIME SNAP_TIME_RANGE
                 ,NVL(VALUE,0) VALUE
                 ,NVL(VALUE_DIFF,0) VALUE_DIFF
                 ,NVL(WAITS,0) WAITS
                 ,NVL(WAITS_DIFF,0) WAITS_DIFF
             FROM
                 (
                 SELECT
                     STAT_NAME
                     ,START_TIME|| '-' || END_TIME SNAP_TIME
                     ,ROUND(AVG(NVL(VALUE,0)),3) VALUE
                     ,ROUND(AVG(NVL(VALUE_DIFF,0)),3) VALUE_DIFF
                     ,ROUND(AVG(NVL(WAITS,0)),3) WAITS
                     ,ROUND(AVG(NVL(WAITS_DIFF,0)),3) WAITS_DIFF
                     ,GROUPING(START_TIME|| '-' || END_TIME) G1
                     ,GROUPING(STAT_NAME) G2
                 FROM
                     (
                     SELECT
                         STAT_NAME
                         ,TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                         ,TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                         ,DECODE(SNAP_TIME_C2,NULL,0,ROUND((CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)),1)) VALUE
                         ,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END) VALUE_DIFF
                         ,NULL WAITS
                         ,NULL WAITS_DIFF
                         ,SNAP_ID
                         ,INSTANCE_NUMBER
                     FROM
                         (
                         SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) */
                             SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                             ,LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                             ,STAT.STAT_NAME
                             ,STAT.VALUE VALUE_1
                             ,LEAD(STAT.VALUE) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                             ,SNAP.SNAP_ID
                             ,DBI.INSTANCE_NUMBER
                         FROM
                             (
                             SELECT /*+ NO_MERGE */
                                 DI.DBID
                                 ,DI.INSTANCE_NUMBER
                                 ,DI.STARTUP_TIME
                             FROM DBA_HIST_DATABASE_INSTANCE DI
                             WHERE
                                 DI.DBID = :DBID
                                 AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                 AND ROWNUM<=1
                             ) DBI
                             ,DBA_HIST_SNAPSHOT SNAP
                             ,DBA_HIST_SYSSTAT STAT
                         WHERE
                             DBI.DBID = SNAP.DBID
                             AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                             AND DBI.DBID = SNAP.DBID
                             AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                             AND SNAP.SNAP_ID<=:END_SNAP_ID
                             AND SNAP.DBID = STAT.DBID
                             AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                             AND SNAP.SNAP_ID = STAT.SNAP_ID
                             AND STAT.STAT_NAME IN ('session logical reads','physical reads','physical read total IO requests','physical write total IO requests','physical read total bytes','physical write total bytes','physical reads direct','physical reads direct (lob)')
                         ORDER BY SNAP.SNAP_ID
                         )
                     WHERE
                         SNAP_TIME_C2 <> SNAP_TIME_C1
                     UNION ALL
                     SELECT
                         STAT_NAME
                         ,TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                         ,TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                         ,ROUND(DECODE(SNAP_TIME_C2,NULL,0,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1))) / 1000000,3) VALUE
                         ,ROUND((CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/1000000,1) VALUE_DIFF
                         ,ROUND(DECODE(SNAP_TIME_C2,NULL,0,(CASE WHEN WAITS_2<=WAITS_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(WAITS_2-WAITS_1))/1000,3) WAITS
                         ,ROUND((CASE WHEN WAITS_2<=WAITS_1 THEN 0 ELSE WAITS_2-WAITS_1 END),1) WAITS_DIFF
                         ,SNAP_ID
                         ,INSTANCE_NUMBER
                     FROM
                         (
                         SELECT
                             SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                             ,LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                             ,STAT.EVENT_NAME STAT_NAME
                             ,STAT.TIME_WAITED_MICRO VALUE_1
                             ,LEAD(STAT.TIME_WAITED_MICRO) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                             ,STAT.TOTAL_WAITS WAITS_1
                             ,LEAD(STAT.TOTAL_WAITS) OVER (PARTITION BY DBI.INSTANCE_NUMBER,EVENT_NAME ORDER BY SNAP.SNAP_ID) WAITS_2
                             ,SNAP.SNAP_ID
                             ,DBI.INSTANCE_NUMBER
                         FROM
                             (
                             SELECT
                                 DI.DBID
                                 ,DI.INSTANCE_NUMBER
                                 ,DI.STARTUP_TIME
                             FROM DBA_HIST_DATABASE_INSTANCE DI
                             WHERE
                                 DI.DBID = :DBID
                                 AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                 AND ROWNUM<=1
                             ) DBI
                             ,DBA_HIST_SNAPSHOT SNAP
                             ,DBA_HIST_SYSTEM_EVENT STAT
                         WHERE
                             DBI.DBID = SNAP.DBID
                             AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                             AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                             AND SNAP.SNAP_ID<=:END_SNAP_ID
                             AND SNAP.DBID = STAT.DBID
                             AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                             AND SNAP.SNAP_ID = STAT.SNAP_ID
                             AND STAT.EVENT_NAME IN ('db file sequential read','db file scattered read','db file parallel write','log file parallel write','log file sync')
                         ORDER BY SNAP.SNAP_ID
                         )
                     WHERE
                         SNAP_TIME_C2 <> SNAP_TIME_C1
                         AND WAITS_2 <> WAITS_1
                     )
                 WHERE
                     START_TIME IS NOT NULL
                     AND END_TIME IS NOT NULL
                 GROUP BY
                     ROLLUP(STAT_NAME,START_TIME|| '-' || END_TIME)
                 )
             WHERE
                 NOT (G1=1 AND G2=1)
             )
         GROUP BY
             SNAP_TIME
             ,SNAP_TIME_RANGE
         ORDER BY
             SNAP_TIME
        """
        sqlcommand = str(sqlcommand, "utf-8")
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}
    elif ind == 7:
        sqlcommand = """
        SELECT   DECODE(G1,
                        1,'SUB AVG',
                        SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1))
                           SNAP_TIME       ,
                 SNAP_TIME SNAP_TIME_RANGE ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'DB time',VALUE)
                 ),'FM999,999,999,999,999.9') \"DB time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'DB CPU',VALUE)
                 ),'FM999,999,999,999,999.9') \"DB CPU/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'background cpu time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Background CPU Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'background elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Background Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'SQL execute elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"SQL Exec Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'parse time elapsed',VALUE)
                 ),'FM999,999,999,999,999.9') \"Parse Time Elapsed/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'hard parse elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Hard Parse Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'hard parse (bind mismatch) elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"H Parse B Miss Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'hard parse (sharing criteria) elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"H Parse S Crit Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'failed parse elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Failed Parse Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'repeated bind elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Repeated Bind Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'failed parse (out of shared memory) elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"F Parse O Mem Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'PL/SQL execution elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"PL/SQL Exec Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'PL/SQL compilation elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"PL/SQL Compile Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'inbound PL/SQL rpc elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Inbound PL/SQL Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'sequence load elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Sequence Load Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'Java execution elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Java Exec Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'RMAN cpu time (backup/restore)',VALUE)
                 ),'FM999,999,999,999,999.9') \"RMAN CPU Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'connection management call elapsed time',VALUE)
                 ),'FM999,999,999,999,999.9') \"Conn Mngt Call Elap. Time/SEC\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'DB CPU',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"DB CPU DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'DB time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"DB time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'background cpu time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Background CPU Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'background elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Background Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'SQL execute elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"SQL Exec Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'parse time elapsed',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Parse Time Elapsed DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'hard parse elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Hard Parse Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'hard parse (bind mismatch) elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"H Parse B Miss Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'hard parse (sharing criteria) elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"H Parse S Crit Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'failed parse elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Failed Parse Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'failed parse (out of shared memory) elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"F Parse O Mem Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'repeated bind elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Repeated Bind Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'PL/SQL execution elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"PL/SQL Exec Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'PL/SQL compilation elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"PL/SQL Compile Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'inbound PL/SQL rpc elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Inbound PL/SQL Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'sequence load elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Sequence Load Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'Java execution elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Java Exec Elap. Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'RMAN cpu time (backup/restore)',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"RMAN CPU Time DIFF\" ,
                 TO_CHAR(SUM(DECODE(STAT_NAME,
                            'connection management call elapsed time',VALUE_DIFF)
                 ),'FM999,999,999,999,999,999,999') \"Conn Mngt Call Elap. Time DIFF\"
        FROM
                 ( SELECT  STAT_NAME ,
                          START_TIME
                                   || '-'
                                   || END_TIME            SNAP_TIME  ,
                          ROUND(AVG(NVL(VALUE,0)),1)      VALUE      ,
                          ROUND(AVG(NVL(VALUE_DIFF,0)),1) VALUE_DIFF ,
                          GROUPING(START_TIME
                                   || '-'
                                   || END_TIME) G1 ,
                          GROUPING(STAT_NAME)   G2
                 FROM
                          ( SELECT  STAT_NAME                                       ,
                                   TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME ,
                                   TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME   ,
                                   DECODE(SNAP_TIME_C2,
                                          NULL,0,
                                          ROUND((
                                                            CASE
                                                                     WHEN VALUE_2<VALUE_1
                                                                     THEN 0
                                                                     ELSE VALUE_2-VALUE_1
                                                            END)                 /(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)),1))
                                   VALUE ,
                                   (
                                            CASE
                                                     WHEN VALUE_2<VALUE_1
                                                     THEN 0
                                                     ELSE VALUE_2-VALUE_1
                                            END)                                                              VALUE_DIFF ,
                                   ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) RNUM       ,
                                   SNAP_ID                                                                               ,
                                   INSTANCE_NUMBER
                          FROM
                                   ( SELECT  SNAP.END_INTERVAL_TIME                                                                              SNAP_TIME_C1 ,
                                            LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2 ,
                                            STAT.STAT_NAME                                                                                                    ,
                                            STAT.VALUE                                                                               VALUE_1                                                                                                ,
                                            LEAD(STAT.VALUE) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2                                                                                                ,
                                            SNAP.SNAP_ID                                                                                                                                                                                    ,
                                            DBI.INSTANCE_NUMBER
                                   FROM
                                            ( SELECT DI.DBID
                                              ,DI.INSTANCE_NUMBER
                                              ,DI.STARTUP_TIME
                                            FROM
                                              DBA_HIST_DATABASE_INSTANCE DI
                                            WHERE
                                                DI.DBID = :DBID
                                                AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                                AND ROWNUM<=1
                                            ) DBI
                                            ,DBA_HIST_SNAPSHOT SNAP
                                            ,DBA_HIST_SYS_TIME_MODEL STAT
                                   WHERE    DBI.DBID               = SNAP.DBID
                                        AND DBI.INSTANCE_NUMBER    = SNAP.INSTANCE_NUMBER
                                        AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                                        AND SNAP.SNAP_ID<=:END_SNAP_ID
                                        AND SNAP.DBID            = STAT.DBID
                                        AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                                        AND SNAP.SNAP_ID         = STAT.SNAP_ID
                                   ORDER BY SNAP.SNAP_ID
                                   )
                          WHERE SNAP_TIME_C2 <> SNAP_TIME_C1
                          )
                 WHERE    START_TIME IS NOT NULL
                      AND END_TIME IS NOT NULL
                 GROUP BY ROLLUP(STAT_NAME,START_TIME
                                   || '-'
                                   || END_TIME)
                 )
        WHERE    NOT (
                          G1=1
                      AND G2=1
                 )
        GROUP BY DECODE(G1,
                        1,'SUB AVG',
                        SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1))
                 ,
                 SNAP_TIME
        ORDER BY SNAP_TIME
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}


    elif ind == 8:
        sqlcommand = """
        SELECT   to_char(min(begin_time),'MM.DD HH24:MI') BEGIN_TIME,
                 to_char(max(end_time),'MM.DD HH24:MI') END_TIME,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Background Checkpoints Per Sec' THEN average END),'FM999,999,999.99') \"Background Checkpoints/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'CR Blocks Created Per Sec' THEN average END),'FM999,999,999.99') \"CR Blocks Created/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'CR Undo Records Applied Per Sec' THEN average END),'FM999,999,999.99') \"CR Undo Records Applied/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Consistent Read Changes Per Sec' THEN average END),'FM999,999,999.99') \"Consistent Read Changes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Consistent Read Gets Per Sec' THEN average END),'FM999,999,999,999.99') \"Consistent Read Gets/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'DB Block Changes Per Sec' THEN average END),'FM999,999,999.99') \"DB Block Changes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'DB Block Gets Per Sec' THEN average END),'FM999,999,999,999.99') \"DB Block Gets/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'DBWR Checkpoints Per Sec' THEN average END),'FM999,999,999.99') \"DBWR Checkpoints/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Host CPU Utilization (%)' THEN average END),'FM999,999,999.99') \"Host CPU Utilization (%)\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'CPU Usage Per Sec' THEN average END),'FM999,999,999.99') \"CPU Usage(cs)/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Database Time Per Sec' THEN average END),'FM999,999,999.99') \"Database Time(cs)/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Response Time Per Txn' THEN average END),'FM999,999,999.99') \"Response Time(cs)/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'SQL Service Response Time' THEN average END),'FM999,999,999.99') \"SQL Service Response Time(cs)\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Disk Sort Per Sec' THEN average END),'FM999,999,999.99') \"Disk Sort/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Deadlocks Per Sec' THEN average END),'FM999,999,999.99') \"Enqueue Deadlocks/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Requests Per Sec' THEN average END),'FM999,999,999.99') \"Enqueue Requests/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Timeouts Per Sec' THEN average END),'FM999,999,999.99') \"Enqueue Timeouts/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Waits Per Sec' THEN average END),'FM999,999,999.99') \"Enqueue Waits/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Executions Per Sec' THEN average END),'FM999,999,999.99') \"Executions/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Full Index Scans Per Sec' THEN average END),'FM999,999,999.99') \"Full Index Scans/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'GC CR Block Received Per Second' THEN average END),'FM999,999,999.99') \"GC CR Block Received/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'GC Current Block Received Per Second' THEN average END),'FM999,999,999.99') \"GC Current Block Received/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Hard Parse Count Per Sec' THEN average END),'FM999,999,999.99') \"Hard Parse Count/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Branch Node Splits Per Sec' THEN average END),'FM999,999,999.99') \"Branch Node Splits/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Leaf Node Splits Per Sec' THEN average END),'FM999,999,999.99') \"Leaf Node Splits/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Logical Reads Per Sec' THEN average END),'FM999,999,999.99') \"Logical Reads/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Logons Per Sec' THEN average END),'FM999,999,999.99') \"Logons/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Long Table Scans Per Sec' THEN average END),'FM999,999,999.99') \"Long Table Scans/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Network Traffic Volume Per Sec' THEN average END),'FM999,999,999.99') \"Network Traffic(bytes)/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Open Cursors Per Sec' THEN average END),'FM999,999,999.99') \"Open Cursors/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'PX downgraded 1 to 25% Per Sec' THEN average END),'FM999,999,999.99') \"PX downgraded 1 to 25%/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'PX downgraded 25 to 50% Per Sec' THEN average END),'FM999,999,999.99') \"PX downgraded 25 to 50%/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'PX downgraded 50 to 75% Per Sec' THEN average END),'FM999,999,999.99') \"PX downgraded 50 to 75%/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'PX downgraded 75 to 99% Per Sec' THEN average END),'FM999,999,999.99') \"PX downgraded 75 to 99%/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'PX operations not downgraded Per Sec' THEN average END),'FM999,999,999.99') \"PX operations not downgr./Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'PX downgraded to serial Per Sec' THEN average END),'FM999,999,999.99') \"PX downgraded to serial/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Parse Failure Count Per Sec' THEN average END),'FM999,999,999.99') \"Parse Failure Count/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Read Bytes Per Sec' THEN average END),'FM999,999,999.99') \"Physical Read Bytes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Read IO Requests Per Sec' THEN average END),'FM999,999,999.99') \"Physical Read IO Requests/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Read Total Bytes Per Sec' THEN average END),'FM999,999,999.99') \"Physical Read Total Bytes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Read Total IO Requests Per Sec' THEN average END),'FM999,999,999.99') \"Phy Read Total IO Requests/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Reads Direct Lobs Per Sec' THEN average END),'FM999,999,999.99') \"Physical Reads Direct Lobs/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Reads Direct Per Sec' THEN average END),'FM999,999,999.99') \"Physical Reads Direct/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Reads Per Sec' THEN average END),'FM999,999,999.99') \"Physical Reads/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Write Bytes Per Sec' THEN average END),'FM999,999,999.99') \"Physical Write Bytes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Write IO Requests Per Sec' THEN average END),'FM999,999,999.99') \"Physical Write IO Requests/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Write Total Bytes Per Sec' THEN average END),'FM999,999,999.99') \"Physical Write Total Bytes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Write Total IO Requests Per Sec' THEN average END),'FM999,999,999.99') \"Phy Write Tot IO Requests/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Writes Direct Lobs Per Sec' THEN average END),'FM999,999,999.99') \"Phy Writes Direct Lobs/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Writes Direct Per Sec' THEN average END),'FM999,999,999.99') \"Physical Writes Direct/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Writes Per Sec' THEN average END),'FM999,999,999.99') \"Physical Writes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Recursive Calls Per Sec' THEN average END),'FM999,999,999.99') \"Recursive Calls/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Redo Generated Per Sec' THEN average END),'FM999,999,999.99') \"Redo Generated(bytes)/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Redo Writes Per Sec' THEN average END),'FM999,999,999.99') \"Redo Writes/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Index Scans Per Sec' THEN average END),'FM999,999,999.99') \"Total Index Scans/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Parse Count Per Sec' THEN average END),'FM999,999,999.99') \"Total Parse Count/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Table Scans Per Sec' THEN average END),'FM999,999,999.99') \"Total Table Scans/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Calls Per Sec' THEN average END),'FM999,999,999.99') \"User Calls/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Commits Per Sec' THEN average END),'FM999,999,999.99') \"User Commits/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Rollback UndoRec Applied Per Sec' THEN average END),'FM999,999,999.99') \"User UndoRecord Applied/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Rollbacks Per Sec' THEN average END),'FM999,999,999.99') \"User Rollbacks/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Transaction Per Sec' THEN average END),'FM999,999,999.99') \"User Transaction/Sec\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Buffer Cache Hit Ratio' THEN average END),'FM999,999,999.99') \"Buffer Cache Hit Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Cursor Cache Hit Ratio' THEN average END),'FM999,999,999.99') \"Cursor Cache Hit Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Database CPU Time Ratio' THEN average END),'FM999,999,999.99') \"Database CPU Time Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Database Wait Time Ratio' THEN average END),'FM999,999,999.99') \"Database Wait Time Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Execute Without Parse Ratio' THEN average END),'FM999,999,999.99') \"Execute Without Parse Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Library Cache Hit Ratio' THEN average END),'FM999,999,999.99') \"Library Cache Hit Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Library Cache Miss Ratio' THEN average END),'FM999,999,999.99') \"Library Cache Miss Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Row Cache Hit Ratio' THEN average END),'FM999,999,999.99') \"Row Cache Hit Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Row Cache Miss Ratio' THEN average END),'FM999,999,999.99') \"Row Cache Miss Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Shared Pool Free %' THEN average END),'FM999,999,999.99') \"Shared Pool Free %\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Memory Sorts Ratio' THEN average END),'FM999,999,999.99') \"Memory Sorts Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Redo Allocation Hit Ratio' THEN average END),'FM999,999,999.99') \"Redo Allocation Hit Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Soft Parse Ratio' THEN average END),'FM999,999,999.99') \"Soft Parse Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Calls Ratio' THEN average END),'FM999,999,999.99') \"User Calls Ratio\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Commits Percentage' THEN average END),'FM999,999,999.99') \"User Commits Percentage\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Session Limit %' THEN average END),'FM999,999,999.99') \"Session Limit %\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Limit %' THEN average END),'FM999,999,999.99') \"User Limit %\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Process Limit %' THEN average END),'FM999,999,999.99') \"Process Limit %\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'PGA Cache Hit %' THEN average END),'FM999,999,999.99') \"PGA Cache Hit %\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Rollbacks Percentage' THEN average END),'FM999,999,999.99') \"User Rollbacks Percentage\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Current Logons Count' THEN average END),'FM999,999,999') \"Current Logons Count\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Current OS Load' THEN average END),'FM999,999,999.99') \"Current OS Load(# of Process)\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Current Open Cursors Count' THEN average END),'FM999,999,999') \"Current Open Cursors Count\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Branch Node Splits Per Txn' THEN average END),'FM999,999,999.99') \"Branch Node Splits/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'CPU Usage Per Txn' THEN average END),'FM999,999,999.99') \"CPU Usage(cs)/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'CR Blocks Created Per Txn' THEN average END),'FM999,999,999.99') \"CR Blocks Created/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'CR Undo Records Applied Per Txn' THEN average END),'FM999,999,999.99') \"CR Undo Records Applied/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Consistent Read Changes Per Txn' THEN average END),'FM999,999,999.99') \"Consistent Read Changes/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Consistent Read Gets Per Txn' THEN average END),'FM999,999,999.99') \"Consistent Read Gets/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'DB Block Changes Per Txn' THEN average END),'FM999,999,999.99') \"DB Block Changes/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'DB Block Changes Per User Call' THEN average END),'FM999,999,999.99') \"DB Block Changes/User Call\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'DB Block Gets Per Txn' THEN average END),'FM999,999,999,999.99') \"DB Block Gets/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'DB Block Gets Per User Call' THEN average END),'FM999,999,999,999.99') \"DB Block Gets/User Call\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Disk Sort Per Txn' THEN average END),'FM999,999,999.99') \"Disk Sort/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Deadlocks Per Txn' THEN average END),'FM999,999,999.99') \"Enqueue Deadlocks/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Requests Per Txn' THEN average END),'FM999,999,999.99') \"Enqueue Requests/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Timeouts Per Txn' THEN average END),'FM999,999,999.99') \"Enqueue Timeouts/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Enqueue Waits Per Txn' THEN average END),'FM999,999,999.99') \"Enqueue Waits/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Executions Per Txn' THEN average END),'FM999,999,999.99') \"Executions/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Executions Per User Call' THEN average END),'FM999,999,999.99') \"Executions/User Call\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Full Index Scans Per Txn' THEN average END),'FM999,999,999.99') \"Full Index Scans/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'GC CR Block Received Per Txn' THEN average END),'FM999,999,999.99') \"GC CR Block Received/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'GC Current Block Received Per Txn' THEN average END),'FM999,999,999.99') \"GC Current Block Received/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Global Cache Average CR Get Time' THEN average END),'FM999,999,999.99') \"Global Cache Avg CR Get Tm(cs)\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Global Cache Average Current Get Time' THEN average END),'FM999,999,999.99') \"GlobalCache Avg Cur Get Tm(cs)\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Global Cache Blocks Corrupted' THEN average END),'FM999,999,999.99') \"Global Cache Blocks Corrupted\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Global Cache Blocks Lost' THEN average END),'FM999,999,999.99') \"Global Cache Blocks Lost\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Hard Parse Count Per Txn' THEN average END),'FM999,999,999.99') \"Hard Parse Count/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Leaf Node Splits Per Txn' THEN average END),'FM999,999,999.99') \"Leaf Node Splits/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Logical Reads Per Txn' THEN average END),'FM999,999,999.99') \"Logical Reads/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Logical Reads Per User Call' THEN average END),'FM999,999,999.99') \"Logical Reads/User Call\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Logons Per Txn' THEN average END),'FM999,999,999.99') \"Logons/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Long Table Scans Per Txn' THEN average END),'FM999,999,999.99') \"Long Table Scans/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Open Cursors Per Txn' THEN average END),'FM999,999,999.99') \"Open Cursors/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Parse Failure Count Per Txn' THEN average END),'FM999,999,999.99') \"Parse Failure Count/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Reads Direct Lobs Per Txn' THEN average END),'FM999,999,999.99') \"Physical Reads Direct Lobs/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Reads Direct Per Txn' THEN average END),'FM999,999,999.99') \"Physical Reads Direct/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Reads Per Txn' THEN average END),'FM999,999,999.99') \"Physical Reads/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Writes Direct Lobs Per  Txn' THEN average END),'FM999,999,999.99') \"Phy Writes Direct Lobs/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Writes Direct Per Txn' THEN average END),'FM999,999,999.99') \"Physical Writes Direct/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Physical Writes Per Txn' THEN average END),'FM999,999,999.99') \"Physical Writes/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Recursive Calls Per Txn' THEN average END),'FM999,999,999.99') \"Recursive Calls/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Redo Generated Per Txn' THEN average END),'FM999,999,999.99') \"Redo Generated(bytes)/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Redo Writes Per Txn' THEN average END),'FM999,999,999.99') \"Redo Writes/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Rows Per Sort' THEN average END),'FM999,999,999.99') \"Rows/Sort\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Index Scans Per Txn' THEN average END),'FM999,999,999.99') \"Total Index Scans/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Parse Count Per Txn' THEN average END),'FM999,999,999.99') \"Total Parse Count/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Sorts Per User Call' THEN average END),'FM999,999,999.99') \"Total Sorts/User Call\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Table Scans Per Txn' THEN average END),'FM999,999,999.99') \"Total Table Scans/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Total Table Scans Per User Call' THEN average END),'FM999,999,999.99') \"Total Table Scans/User Call\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'Txns Per Logon' THEN average END),'FM999,999,999.99') \"Txns/Logon\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Calls Per Txn' THEN average END),'FM999,999,999.99') \"User Calls/Txn\" ,
                 TO_CHAR(SUM( CASE metric_name WHEN 'User Rollback Undo Records Applied Per Txn' THEN average END),'FM999,999,999.99') \"User Undo Records Applied/Txn\"
        FROM DBA_HIST_SYSMETRIC_SUMMARY DHSS
        WHERE
            DHSS.DBID = :DBID
            AND DHSS.INSTANCE_NUMBER = :INSTANCE_NUMBER
            AND DHSS.SNAP_ID >=:BEGIN_SNAP_ID+1
            AND DHSS.SNAP_ID <=:END_SNAP_ID+1
        GROUP BY snap_id
        ORDER BY snap_id        
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 9:
        sqlcommand = """
        SELECT
            SNAP_TIME
            ,SNAP_TIME_RANGE
            ,TO_CHAR(\"Estd.Interconnect Traffic(KB)\",'FM999,999,999.9') \"Estd.Interconnect Traffic(KB)\"
            ,TO_CHAR(\"Blocks Receive Time\"/DECODE(\"Blocks Received\",0,1,\"Blocks Received\"),'FM999,999,999.9') \"Interconnect Latency(ms)\"
            ,TO_CHAR(\"Blocks Served Time\"/DECODE(\"Blocks Served\",0,1,\"Blocks Served\"),'FM999,999,999.9') \"Prepare Latency(ms)\"
            ,TO_CHAR((\"Blocks Receive Time\"/DECODE(\"Blocks Received\",0,1,\"Blocks Received\"))-(\"Blocks Served Time\"/DECODE(\"Blocks Served\",0,1,\"Blocks Served\")),'FM999,999,999.9') \"Transfer Latency(ms)\"
            ,TO_CHAR(\"gc cr blocks served\",'FM999,999,999.9') \"gc cr blocks served\"
            ,TO_CHAR(\"gc current blocks served\",'FM999,999,999.9') \"gc current blocks served\"
            ,TO_CHAR(\"gc cr blocks received\",'FM999,999,999.9') \"gc cr blocks received\"
            ,TO_CHAR(\"gc current blocks received\",'FM999,999,999.9') \"gc current blocks received\"
            ,TO_CHAR(\"gcs messages sent\",'FM999,999,999.9') \"gcs messages sent\"
            ,TO_CHAR(\"ges messages sent\",'FM999,999,999.9') \"ges messages sent\"
            ,TO_CHAR(\"gcs msgs received\",'FM999,999,999.9') \"gcs msgs received\"
            ,TO_CHAR(\"ges msgs received\" ,'FM999,999,999.9') \"ges msgs received\"
            ,TO_CHAR(\"gc blocks lost\",'FM999,999,999.9') \"gc blocks lost\"
            ,TO_CHAR(\"gc cr block build time\",'FM999,999,999.99') \"gc cr block build tm(ms)\"
            ,TO_CHAR(\"gc cr block flush time\",'FM999,999,999.99') \"gc cr block flush tm(ms)\"
            ,TO_CHAR(\"gc current block flush time\",'FM999,999,999.99') \"gc current block flush tm(ms)\"
            ,TO_CHAR(\"gc cr block receive time\",'FM999,999,999.99') \"gc cr block receive tm(ms)\"
            ,TO_CHAR(\"gc current block receive time\",'FM999,999,999.99') \"gc current block receiv tm(ms)\"
            ,TO_CHAR(\"gc cr block send time\",'FM999,999,999.99') \"gc cr block send tm(ms)\"
            ,TO_CHAR(\"gc current block send time\",'FM999,999,999.99') \"gc current block send tm(ms)\"
            ,TO_CHAR(\"gc current block pin time\",'FM999,999,999.99') \"gc current block pin tm(ms)\"
            ,TO_CHAR(\"Blocks Served Time\",'FM999,999,999.99') \"Blocks Served tm(ms)\"
            ,TO_CHAR(\"Blocks Served\",'FM999,999,999.9') \"Blocks Served\"
            ,TO_CHAR(\"Blocks Receive Time\",'FM999,999,999.99') \"Blocks Receive tm(ms)\"
            ,TO_CHAR(\"Blocks Received\",'FM999,999,999.9') \"Blocks Received\"
        FROM
            (
            SELECT
                SNAP_TIME
                ,SNAP_TIME_RANGE
                ,\"gc cr blocks served\"
                ,\"gc current blocks served\"
                ,\"gc cr blocks received\"
                ,\"gc current blocks received\"
                ,\"gcs messages sent\"
                ,\"ges messages sent\"
                ,\"gcs msgs received\"
                ,\"ges msgs received\"
                ,\"gc blocks lost\"
                ,\"gc cr block build time\"*10 \"gc cr block build time\"
                ,\"gc cr block flush time\"*10 \"gc cr block flush time\"
                ,\"gc current block flush time\"*10 \"gc current block flush time\"
                ,\"gc cr block send time\"*10 \"gc cr block send time\"
                ,\"gc current block send time\"*10 \"gc current block send time\"
                ,\"gc current block pin time\"*10 \"gc current block pin time\"
                ,((\"gc cr blocks served\"+\"gc current blocks served\"+\"gc cr blocks received\"+\"gc current blocks received\")*DB_BLOCK_SIZE + (\"gcs msgs received\"+\"ges msgs received\"+\"gcs messages sent\"+\"ges messages sent\")*200)/1024 \"Estd.Interconnect Traffic(KB)\"
                ,(\"gc cr block build time\"+\"gc cr block flush time\"+\"gc current block flush time\"+\"gc cr block send time\"+\"gc current block send time\"+\"gc current block pin time\")*10 \"Blocks Served Time\"
                ,\"gc current blocks served\"+\"gc cr blocks served\" \"Blocks Served\"
                ,(\"gc cr block receive time\"+\"gc current block receive time\")*10 \"Blocks Receive Time\"
                ,\"gc cr blocks received\"+\"gc current blocks received\" \"Blocks Received\"
                ,\"gc cr block receive time\" \"gc cr block receive time\"
                ,\"gc current block receive time\" \"gc current block receive time\"
            FROM
                (
                SELECT SNAP_TIME
                  ,SNAP_TIME_RANGE
                  ,SUM(DECODE(STAT_NAME,'gc cr blocks served',VALUE)) \"gc cr blocks served\"
                  ,SUM(DECODE(STAT_NAME,'gc current blocks served',VALUE)) \"gc current blocks served\"
                  ,SUM(DECODE(STAT_NAME,'gc cr blocks received',VALUE)) \"gc cr blocks received\"
                  ,SUM(DECODE(STAT_NAME,'gc current blocks received',VALUE)) \"gc current blocks received\"
                  ,SUM(DECODE(STAT_NAME,'gcs messages sent',VALUE)) \"gcs messages sent\"
                  ,SUM(DECODE(STAT_NAME,'ges messages sent',VALUE)) \"ges messages sent\"
                  ,SUM(DECODE(STAT_NAME,'gcs msgs received',VALUE)) \"gcs msgs received\"
                  ,SUM(DECODE(STAT_NAME,'ges msgs received',VALUE)) \"ges msgs received\"
                  ,SUM(DECODE(STAT_NAME,'gc blocks lost',VALUE)) \"gc blocks lost\"
                  ,SUM(DECODE(STAT_NAME,'gc cr block build time',VALUE)) \"gc cr block build time\"
                  ,SUM(DECODE(STAT_NAME,'gc cr block flush time',VALUE)) \"gc cr block flush time\"
                  ,SUM(DECODE(STAT_NAME,'gc current block flush time',VALUE)) \"gc current block flush time\"
                  ,SUM(DECODE(STAT_NAME,'gc cr block send time',VALUE)) \"gc cr block send time\"
                  ,SUM(DECODE(STAT_NAME,'gc current block send time',VALUE)) \"gc current block send time\"
                  ,SUM(DECODE(STAT_NAME,'gc current block pin time',VALUE)) \"gc current block pin time\"
                  ,SUM(DECODE(STAT_NAME,'gc cr block receive time',VALUE)) \"gc cr block receive time\"
                  ,SUM(DECODE(STAT_NAME,'gc current block receive time',VALUE)) \"gc current block receive time\"
                  ,MIN(P.PVALUE) DB_BLOCK_SIZE
                FROM
                    (
                    SELECT STAT_NAME
                      ,DECODE(G1,1,'SUB AVG',SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1)) SNAP_TIME
                      ,SNAP_TIME SNAP_TIME_RANGE
                      ,VALUE
                      ,VALUE_DIFF
                    FROM
                        (
                        SELECT STAT_NAME
                          ,START_TIME || '-' || END_TIME SNAP_TIME
                          ,ROUND(AVG(NVL(VALUE,0)),1) VALUE
                          ,ROUND(AVG(NVL(VALUE_DIFF,0)),1) VALUE_DIFF
                          ,GROUPING(START_TIME || '-' || END_TIME) G1
                          ,GROUPING(STAT_NAME) G2
                        FROM
                            (
                            SELECT STAT_NAME
                              ,TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                              ,TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                              ,DECODE(SNAP_TIME_C2
                                    ,NULL,0
                                    ,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)))
                                VALUE
                              ,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END) VALUE_DIFF
                              ,ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) RNUM
                              ,SNAP_ID
                              ,INSTANCE_NUMBER
                            FROM
                                (
                                SELECT SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                                  ,LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                                  ,STAT.STAT_NAME
                                  ,STAT.VALUE VALUE_1
                                  ,LEAD(STAT.VALUE) OVER (PARTITION BY DBI.INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                                  ,SNAP.SNAP_ID
                                  ,DBI.INSTANCE_NUMBER
                                FROM
                                    (
                                    SELECT DI.DBID,DI.INSTANCE_NUMBER,MAX(DI.STARTUP_TIME) STARTUP_TIME
                                    FROM DBA_HIST_DATABASE_INSTANCE DI
                                    WHERE DI.DBID = DBID AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                    GROUP BY DI.DBID,DI.INSTANCE_NUMBER
                                    ) DBI
                                   ,DBA_HIST_SNAPSHOT SNAP
                                   ,DBA_HIST_SYSSTAT STAT
                                WHERE DBI.DBID = SNAP.DBID
                                    AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                                    AND DBI.DBID = SNAP.DBID
                                    AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                                    AND SNAP.SNAP_ID<=:END_SNAP_ID
                                    AND SNAP.DBID = STAT.DBID
                                    AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                                    AND SNAP.SNAP_ID = STAT.SNAP_ID
                                    AND STAT.STAT_NAME IN ('gc blocks lost','gc cr block build time','gc cr block flush time','gc current block flush time','gc cr block send time','gc current block send time','gc current block pin time','gc cr blocks served','gc current blocks served','gc cr blocks received','gc current blocks received','messages sent','messages received','gcs messages sent','ges messages sent','gc cr block receive time','gc current block receive time')
                                ORDER BY SNAP.SNAP_ID
                                )
                            WHERE SNAP_TIME_C2 <> SNAP_TIME_C1
                            )
                        WHERE
                            START_TIME IS NOT NULL
                            AND END_TIME IS NOT NULL
                        GROUP BY
                            ROLLUP(STAT_NAME,START_TIME || '-' || END_TIME)
                        )
                    WHERE NOT (G1=1 AND G2=1)
                    UNION ALL
                    SELECT STAT_NAME
                      ,DECODE(G1,1,'SUB AVG',SUBSTR(SNAP_TIME,1,INSTR(SNAP_TIME,'-')-1)) SNAP_TIME
                      ,SNAP_TIME SNAP_TIME_RANGE
                      ,VALUE
                      ,VALUE_DIFF
                    FROM
                        (
                        SELECT STAT_NAME
                          ,START_TIME || '-' || END_TIME SNAP_TIME
                          ,ROUND(AVG(NVL(VALUE,0)),1) VALUE
                          ,ROUND(AVG(NVL(VALUE_DIFF,0)),1) VALUE_DIFF
                          ,GROUPING(START_TIME || '-' || END_TIME) G1
                          ,GROUPING(STAT_NAME) G2
                        FROM
                            (
                            SELECT STAT_NAME
                              ,TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                              ,TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                              ,DECODE(SNAP_TIME_C2 ,NULL,0,ROUND((CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)),1)) VALUE
                              ,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END) VALUE_DIFF
                              ,ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) RNUM
                              ,SNAP_ID
                              ,INSTANCE_NUMBER
                            FROM
                                (
                                SELECT SNAP.END_INTERVAL_TIME SNAP_TIME_C1
                                  ,LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER,NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2
                                  ,STAT.NAME STAT_NAME
                                  ,STAT.VALUE VALUE_1
                                  ,LEAD(STAT.VALUE) OVER (PARTITION BY DBI.INSTANCE_NUMBER,NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                                  ,SNAP.SNAP_ID
                                  ,DBI.INSTANCE_NUMBER
                                FROM
                                    (
                                    SELECT DI.DBID,DI.INSTANCE_NUMBER,MAX(DI.STARTUP_TIME) STARTUP_TIME
                                    FROM DBA_HIST_DATABASE_INSTANCE DI
                                    WHERE DI.DBID = :DBID AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                                    GROUP BY DI.DBID,DI.INSTANCE_NUMBER
                                    ) DBI
                                   ,DBA_HIST_SNAPSHOT SNAP
                                   ,DBA_HIST_DLM_MISC STAT
                                WHERE DBI.DBID = SNAP.DBID
                                    AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                                    AND DBI.DBID = SNAP.DBID
                                    AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                                    AND SNAP.SNAP_ID<=:END_SNAP_ID
                                    AND SNAP.DBID = STAT.DBID
                                    AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                                    AND SNAP.SNAP_ID = STAT.SNAP_ID
                                    AND STAT.NAME IN ('gcs msgs received','ges msgs received')
                                ORDER BY SNAP.SNAP_ID
                                )
                            WHERE SNAP_TIME_C2 <> SNAP_TIME_C1
                            )
                        WHERE START_TIME IS NOT NULL
                            AND END_TIME IS NOT NULL
                        GROUP BY ROLLUP(STAT_NAME,START_TIME || '-' || END_TIME)
                        )
                    WHERE NOT (G1=1 AND G2=1)
                    ORDER BY STAT_NAME,SNAP_TIME
                    ) V
                    ,(
                     SELECT /*+ NO_MERGE */ VALUE PVALUE
                     FROM DBA_HIST_PARAMETER DHP,DBA_HIST_SNAPSHOT SNAP
                     WHERE SNAP.DBID = :DBID
                     AND SNAP.INSTANCE_NUMBER=:INSTANCE_NUMBER
                     AND SNAP.SNAP_ID=:END_SNAP_ID
                     AND SNAP.DBID=DHP.DBID
                     AND SNAP.INSTANCE_NUMBER=DHP.INSTANCE_NUMBER
                     AND SNAP.SNAP_ID=DHP.SNAP_ID
                     AND DHP.PARAMETER_NAME = 'db_block_size'
                     ) P
                GROUP BY SNAP_TIME
                  ,SNAP_TIME_RANGE
                )
            )
        ORDER BY SNAP_TIME
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 10:
        sqlcommand = """
        SELECT
            SNAP_TIME
            ,SUM(CASE STAT_NAME WHEN 'USER_TIME' THEN VALUE END) USER_TIME
            ,SUM(CASE STAT_NAME WHEN 'SYS_TIME' THEN VALUE END) SYS_TIME
            ,SUM(CASE STAT_NAME WHEN 'BUSY_TIME' THEN VALUE END) BUSY_TIME
            ,SUM(CASE STAT_NAME WHEN 'IOWAIT_TIME' THEN VALUE END) IOWAIT_TIME
            ,SUM(CASE STAT_NAME WHEN 'IDLE_TIME' THEN VALUE END) IDLE_TIME
            ,SUM(CASE STAT_NAME WHEN 'AVG_USER_TIME' THEN VALUE END) AVG_USER_TIME
            ,SUM(CASE STAT_NAME WHEN 'AVG_SYS_TIME' THEN VALUE END) AVG_SYS_TIME
            ,SUM(CASE STAT_NAME WHEN 'AVG_BUSY_TIME' THEN VALUE END) AVG_BUSY_TIME
            ,SUM(CASE STAT_NAME WHEN 'AVG_IOWAIT_TIME' THEN VALUE END) AVG_IOWAIT_TIME
            ,SUM(CASE STAT_NAME WHEN 'AVG_IDLE_TIME' THEN VALUE END) AVG_IDLE_TIME
            ,SUM(CASE STAT_NAME WHEN 'LOAD' THEN VALUE END) LOAD
            ,SUM(CASE STAT_NAME WHEN 'NUM_CPUS' THEN VALUE END) NUM_CPUS
            ,SUM(CASE STAT_NAME WHEN 'NUM_CPU_SOCKETS' THEN VALUE END) NUM_CPU_SOCKETS
            ,SUM(CASE STAT_NAME WHEN 'OS_CPU_WAIT_TIME' THEN VALUE END) OS_CPU_WAIT_TIME
            ,SUM(CASE STAT_NAME WHEN 'PHYSICAL_MEMORY_BYTES' THEN VALUE END) PHYSICAL_MEMORY_BYTES
            ,SUM(CASE STAT_NAME WHEN 'RSRC_MGR_CPU_WAIT_TIME' THEN VALUE END) RSRC_MGR_CPU_WAIT_TIME
            ,SUM(CASE STAT_NAME WHEN 'VM_IN_BYTES' THEN VALUE END) VM_IN_BYTES
            ,SUM(CASE STAT_NAME WHEN 'VM_OUT_BYTES' THEN VALUE END) VM_OUT_BYTES
        FROM
            (
            SELECT
                STAT_NAME
                ,START_TIME SNAP_TIME
                ,VALUE
                ,VALUE_DIFF
            FROM
                (
                SELECT
                    STAT_NAME
                    ,TO_CHAR(SNAP_TIME_C1,'MM.DD HH24:MI') START_TIME
                    ,TO_CHAR(SNAP_TIME_C2,'MM.DD HH24:MI') END_TIME
                    ,DECODE(SNAP_TIME_C2,NULL,0,ROUND((CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1)),1)) VALUE
                    ,(CASE WHEN VALUE_2<VALUE_1 THEN 0 ELSE VALUE_2-VALUE_1 END) VALUE_DIFF
                    ,ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) RNUM
                    ,SNAP_ID
                    ,INSTANCE_NUMBER
                FROM
                    (
                    SELECT
                        END_TIME SNAP_TIME_C1
                        ,LEAD(END_TIME) OVER (PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) SNAP_TIME_C2
                        ,VALUE VALUE_1
                        ,LEAD(VALUE) OVER (PARTITION BY INSTANCE_NUMBER,STAT_NAME ORDER BY SNAP_ID) VALUE_2
                        ,STAT_NAME
                        ,SNAP_ID
                        ,INSTANCE_NUMBER
                    FROM
                        (
                        SELECT
                            SNAP.END_INTERVAL_TIME END_TIME
                            ,SNAP.SNAP_ID
                            ,DHO.STAT_NAME
                            ,DHO.VALUE
                            ,DHO.INSTANCE_NUMBER
                        FROM
                            DBA_HIST_OSSTAT DHO
                            ,DBA_HIST_SNAPSHOT SNAP
                        WHERE
                            DHO.SNAP_ID=SNAP.SNAP_ID
                            AND DHO.DBID=SNAP.DBID
                            AND DHO.INSTANCE_NUMBER=SNAP.INSTANCE_NUMBER
                            AND DHO.DBID=:DBID
                            AND DHO.INSTANCE_NUMBER=:INSTANCE_NUMBER
                            AND DHO.SNAP_ID >=:BEGIN_SNAP_ID
                            AND DHO.SNAP_ID <=:END_SNAP_ID
                        )
                    )
                )
            )
        GROUP BY SNAP_TIME
        ORDER BY SNAP_TIME
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 11:
        sqlcommand = """
        SELECT SDATE,
             MAX (SLR_MAX_VALUE) SLR_MAX_VALUE,
             MAX (SLR_AVG_VALUE) SLR_AVG_VALUE,
             MAX (SLR_MIN_VALUE) SLR_MIN_VALUE,
             MAX (PR_MAX_VALUE) PR_MAX_VALUE,
             MAX (PR_AVG_VALUE) PR_AVG_VALUE,
             MAX (PR_MIN_VALUE) PR_MIN_VALUE,
             MAX (EC_MAX_VALUE) EC_MAX_VALUE,
             MAX (EC_AVG_VALUE) EC_AVG_VALUE,
             MAX (EC_MIN_VALUE) EC_MIN_VALUE,
             MAX (UC_MAX_VALUE) UC_MAX_VALUE,
             MAX (UC_AVG_VALUE) UC_AVG_VALUE,
             MAX (UC_MIN_VALUE) UC_MIN_VALUE,
             MAX (RE_MAX_VALUE) RE_MAX_VALUE,
             MAX (RE_AVG_VALUE) RE_AVG_VALUE,
             MAX (RE_MIN_VALUE) RE_MIN_VALUE,
             MAX (CPU_MAX_VALUE) CPU_MAX_VALUE,
             MAX (CPU_AVG_VALUE) CPU_AVG_VALUE,
             MAX (CPU_MIN_VALUE) CPU_MIN_VALUE,
             MAX (SLR_STD_VALUE) SLR_STD_VALUE,
             MAX (PR_STD_VALUE) PR_STD_VALUE,
             MAX (EC_STD_VALUE) EC_STD_VALUE,
             MAX (UC_STD_VALUE) UC_STD_VALUE,
             MAX (RE_STD_VALUE) RE_STD_VALUE,
             MAX (CPU_STD_VALUE) CPU_STD_VALUE
        FROM (  SELECT TO_CHAR (START_TIME, 'YYYY.MM.DD')
                           SDATE,
                       ROUND (MAX (DECODE (STAT_NAME, 'session logical reads', VALUE)), 1) SLR_MAX_VALUE,
                       ROUND (AVG (DECODE (STAT_NAME, 'session logical reads', VALUE)), 1) SLR_AVG_VALUE,
                       ROUND (MIN (DECODE (STAT_NAME, 'session logical reads', VALUE)), 1) SLR_MIN_VALUE,
                       ROUND (MAX (DECODE (STAT_NAME, 'physical reads', VALUE)), 1) PR_MAX_VALUE,
                       ROUND (AVG (DECODE (STAT_NAME, 'physical reads', VALUE)), 1) PR_AVG_VALUE,
                       ROUND (MIN (DECODE (STAT_NAME, 'physical reads', VALUE)), 1) PR_MIN_VALUE,
                       ROUND (MAX (DECODE (STAT_NAME, 'execute count', VALUE)), 1) EC_MAX_VALUE,
                       ROUND (AVG (DECODE (STAT_NAME, 'execute count', VALUE)), 1) EC_AVG_VALUE,
                       ROUND (MIN (DECODE (STAT_NAME, 'execute count', VALUE)), 1) EC_MIN_VALUE,
                       ROUND (MAX (DECODE (STAT_NAME, 'user commits', VALUE)), 1) UC_MAX_VALUE,
                       ROUND (AVG (DECODE (STAT_NAME, 'user commits', VALUE)), 1) UC_AVG_VALUE,
                       ROUND (MIN (DECODE (STAT_NAME, 'user commits', VALUE)), 1) UC_MIN_VALUE,
                       ROUND (MAX (DECODE (STAT_NAME, 'redo entries', VALUE)), 1) RE_MAX_VALUE,
                       ROUND (AVG (DECODE (STAT_NAME, 'redo entries', VALUE)), 1) RE_AVG_VALUE,
                       ROUND (MIN (DECODE (STAT_NAME, 'redo entries', VALUE)), 1) RE_MIN_VALUE,
                       NULL CPU_MAX_VALUE,
                       NULL CPU_AVG_VALUE,
                       NULL CPU_MIN_VALUE,
                       ROUND (STDDEV (DECODE (STAT_NAME, 'session logical reads', VALUE)), 1) SLR_STD_VALUE,
                       ROUND (STDDEV (DECODE (STAT_NAME, 'physical reads', VALUE)), 1) PR_STD_VALUE,
                       ROUND (STDDEV (DECODE (STAT_NAME, 'execute count', VALUE)), 1) EC_STD_VALUE,
                       ROUND (STDDEV (DECODE (STAT_NAME, 'user commits', VALUE)), 1) UC_STD_VALUE,
                       ROUND (STDDEV (DECODE (STAT_NAME, 'redo entries', VALUE)), 1) RE_STD_VALUE,
                       NULL CPU_STD_VALUE
                  FROM (SELECT STAT_NAME,
                               SNAP_TIME_C1 START_TIME,
                               SNAP_TIME_C2 END_TIME,
                               GREATEST (DECODE (SNAP_TIME_C2, NULL, 0, (VALUE_2 - VALUE_1) / (EXTRACT (DAY FROM   SNAP_TIME_C2 - SNAP_TIME_C1) * 86400
                                          +   EXTRACT (HOUR FROM  SNAP_TIME_C2 - SNAP_TIME_C1) * 3600
                                          +   EXTRACT (MINUTE FROM   SNAP_TIME_C2 - SNAP_TIME_C1) * 60
                                          + EXTRACT (SECOND FROM   SNAP_TIME_C2 - SNAP_TIME_C1))), 0) VALUE,
                               GREATEST (VALUE_2 - VALUE_1, 0) VALUE_DIFF,
                               ROW_NUMBER () OVER (PARTITION BY INSTANCE_NUMBER, STAT_NAME ORDER BY SNAP_ID) RNUM,
                               SNAP_ID,
                               INSTANCE_NUMBER
                          FROM (  SELECT SNAP.END_INTERVAL_TIME
                                             SNAP_TIME_C1,
                                         LEAD (SNAP.END_INTERVAL_TIME) OVER (PARTITION BY DBI.INSTANCE_NUMBER, STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2,
                                         STAT.STAT_NAME,
                                         STAT.VALUE VALUE_1,
                                         LEAD (STAT.VALUE) OVER (PARTITION BY DBI.INSTANCE_NUMBER, STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2,
                                         SNAP.SNAP_ID,
                                         DBI.INSTANCE_NUMBER
                                    FROM (SELECT DI.DBID,
                                                 DI.INSTANCE_NUMBER,
                                                 DI.STARTUP_TIME
                                            FROM DBA_HIST_DATABASE_INSTANCE DI
                                           WHERE     DI.DBID = :DBID
                                                 AND INSTANCE_NUMBER = :INSTANCE_NUMBER)
                                         DBI,
                                         DBA_HIST_SNAPSHOT SNAP,
                                         DBA_HIST_SYSSTAT STAT
                                   WHERE     DBI.DBID = SNAP.DBID
                                         AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                                         AND SNAP.SNAP_ID >= :BEGIN_SNAP_ID
                                         AND SNAP.SNAP_ID <= :END_SNAP_ID
                                         AND SNAP.DBID = STAT.DBID
                                         AND SNAP.INSTANCE_NUMBER =
                                             STAT.INSTANCE_NUMBER
                                         AND SNAP.SNAP_ID = STAT.SNAP_ID
                                         AND STAT.STAT_NAME IN
                                                 ('session logical reads',
                                                  'physical reads',
                                                  'execute count',
                                                  'user commits',
                                                  'redo entries')
                                ORDER BY SNAP.SNAP_ID)
                         WHERE SNAP_TIME_C2 > SNAP_TIME_C1)
                 WHERE START_TIME IS NOT NULL AND END_TIME IS NOT NULL
              GROUP BY TO_CHAR (START_TIME, 'YYYY.MM.DD')
              UNION ALL
                SELECT TO_CHAR (END_TIME, 'YYYY.MM.DD') SDATE,
                       NULL                         SLR_MAX_VALUE,
                       NULL                         SLR_AVG_VALUE,
                       NULL                         SLR_MIN_VALUE,
                       NULL                         PR_MAX_VALUE,
                       NULL                         PR_AVG_VALUE,
                       NULL                         PR_MIN_VALUE,
                       NULL                         EC_MAX_VALUE,
                       NULL                         EC_AVG_VALUE,
                       NULL                         EC_MIN_VALUE,
                       NULL                         UC_MAX_VALUE,
                       NULL                         UC_AVG_VALUE,
                       NULL                         UC_MIN_VALUE,
                       NULL                         RE_MAX_VALUE,
                       NULL                         RE_AVG_VALUE,
                       NULL                         RE_MIN_VALUE,
                       ROUND (MAX (AVERAGE), 1)     CPU_MAX_VALUE,
                       ROUND (AVG (AVERAGE), 1)     CPU_AVG_VALUE,
                       ROUND (MIN (AVERAGE), 1)     CPU_MIN_VALUE,
                       NULL                         SLR_STD_VALUE,
                       NULL                         PR_STD_VALUE,
                       NULL                         EC_STD_VALUE,
                       NULL                         UC_STD_VALUE,
                       NULL                         RE_STD_VALUE,
                       ROUND (STDDEV (AVERAGE), 1)  CPU_STD_VALUE
                  FROM DBA_HIST_SYSMETRIC_SUMMARY DHSS
                 WHERE     DHSS.DBID = :DBID
                       AND DHSS.INSTANCE_NUMBER = :INSTANCE_NUMBER
                       AND DHSS.SNAP_ID > :BEGIN_SNAP_ID
                       AND DHSS.SNAP_ID < :END_SNAP_ID
                       AND DHSS.METRIC_NAME = 'Host CPU Utilization (%)'
              GROUP BY TO_CHAR (END_TIME, 'YYYY.MM.DD'))
        GROUP BY SDATE
        ORDER BY SDATE
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 12:
        sqlcommand = """
        WITH A AS
            (
            SELECT /*+ MATERIALIZE LEADING(DBI SNAP) USE_HASH(SQLS TXT) NO_MERGE(SQLS) NO_MERGE(TXT) */ SQLS.DBID,SQLS.SQL_ID,SQLS.OPTIMIZER_MODE,SQLS.MODULE,SUM(SQLS.FETCHES_DELTA) FETCHES,SUM(SQLS.EXECUTIONS_DELTA) EXECUTIONS,SUM(SQLS.SORTS_DELTA) SORTS,SUM(SQLS.DISK_READS_DELTA) DISK_READS,SUM(SQLS.BUFFER_GETS_DELTA) BUFFER_GETS,SUM(SQLS.ROWS_PROCESSED_DELTA) ROWS_PROCESSED,SUM(SQLS.CPU_TIME_DELTA)/1000000 CPU_TIME,SUM(SQLS.ELAPSED_TIME_DELTA)/1000000 ELAPSED_TIME,SUM(SQLS.IOWAIT_DELTA)/1000000 IOWAIT,SUM(SQLS.CLWAIT_DELTA)/1000000 CLWAIT,SUM(SQLS.APWAIT_DELTA)/1000000 APWAIT,SUM(SQLS.CCWAIT_DELTA)/1000000 CCWAIT,SUM(DIRECT_WRITES_DELTA) DIRECT_WRITES,NULL RNUM
            FROM
               (SELECT DI.DBID,DI.INSTANCE_NUMBER,DI.STARTUP_TIME FROM DBA_HIST_DATABASE_INSTANCE DI WHERE DI.DBID=:DBID AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER AND ROWNUM<=1) DBI
              ,DBA_HIST_SNAPSHOT SNAP
              ,DBA_HIST_SQLSTAT SQLS
            WHERE DBI.DBID = SNAP.DBID AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID+1 AND SNAP.SNAP_ID<=:END_SNAP_ID AND SNAP.DBID = SQLS.DBID AND SNAP.INSTANCE_NUMBER = SQLS.INSTANCE_NUMBER AND SNAP.SNAP_ID = SQLS.SNAP_ID
            GROUP BY SQLS.DBID,SQLS.SQL_ID,SQLS.OPTIMIZER_MODE,SQLS.MODULE
            HAVING SUM(SQLS.EXECUTIONS_DELTA)>0
            )
          ,A_TOT AS
            (
            SELECT 0 DBID,'TOT' SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,MAX(STAT.EXECUTIONS) EXECUTIONS,SUM(A.FETCHES) FETCHES,(MAX(STAT.SORTS_D)+MAX(STAT.SORTS_M)) SORTS,MAX(STAT.BUFFER_GETS) BUFFER_GETS,MAX(STAT.DISK_READS) DISK_READS,SUM(A.ROWS_PROCESSED) ROWS_PROCESSED,MAX(TIME.DB_CPU) CPU_TIME,MAX(TIME.DB_TIME) ELAPSED_TIME,MAX(STAT.BUFFER_GETS)/MAX(STAT.EXECUTIONS) BUF_EXEC,MAX(STAT.DISK_READS)/MAX(STAT.EXECUTIONS) DISK_EXEC,SUM(A.ROWS_PROCESSED)/SUM(A.EXECUTIONS) ROWS_EXEC,MAX(TIME.DB_CPU)/MAX(STAT.EXECUTIONS) CPU_EXEC,MAX(TIME.DB_TIME)/MAX(STAT.EXECUTIONS) ELAP_EXEC,MAX(STAT.IOWAIT) IOWAIT,MAX(STAT.CLWAIT) CLWAIT,MAX(STAT.APWAIT) APWAIT,MAX(STAT.CCWAIT) CCWAIT,COUNT(DISTINCT SQL_ID) RNUM
            FROM A
              ,(
                SELECT
                  MAX(DECODE(STAT_NAME,'execute count',TOT_VALUE)) EXECUTIONS
                  ,MAX(DECODE(STAT_NAME,'session logical reads',TOT_VALUE)) BUFFER_GETS
                  ,MAX(DECODE(STAT_NAME,'physical reads',TOT_VALUE)) DISK_READS
                  ,MAX(DECODE(STAT_NAME,'sorts (disk)',TOT_VALUE)) SORTS_D
                  ,MAX(DECODE(STAT_NAME,'sorts (memory)',TOT_VALUE)) SORTS_M
                  ,MAX(DECODE(STAT_NAME,'cluster wait time',TOT_VALUE/100)) CLWAIT
                  ,MAX(DECODE(STAT_NAME,'application wait time',TOT_VALUE/100)) APWAIT
                  ,MAX(DECODE(STAT_NAME,'concurrency wait time',TOT_VALUE/100)) CCWAIT
                  ,MAX(DECODE(STAT_NAME,'user I/O wait time',TOT_VALUE/100)) IOWAIT
                FROM
                  (
                  SELECT STAT_NAME,SUM(TOT_VALUE) TOT_VALUE
                  FROM
                      (SELECT STAT_NAME
                        ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
                        ,DECODE(SNAP_TIME_2,NULL,0 ,ROUND((VALUE_1-VALUE_2))) TOT_VALUE
                        ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
                      FROM
                          (SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) NO_MERGE(SNAP) NO_MERGE(STAT) */
                              SNAP.END_INTERVAL_TIME SNAP_TIME_1
                            ,STAT.STAT_NAME
                            ,STAT.VALUE VALUE_1
                            ,LAG(STAT.VALUE) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                            ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                            ,SNAP.SNAP_ID
                          FROM
                              (SELECT /*+ NO_MERGE */
                                  DI.DBID
                                ,DI.INSTANCE_NUMBER
                                ,DI.STARTUP_TIME
                              FROM DBA_HIST_DATABASE_INSTANCE DI
                              WHERE DI.DBID = :DBID
                              AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                              AND ROWNUM<=1
                              ) DBI
                            ,DBA_HIST_SNAPSHOT SNAP
                            ,DBA_HIST_SYSSTAT STAT
                          WHERE DBI.DBID = SNAP.DBID
                          AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                          AND SNAP.SNAP_ID<=:END_SNAP_ID
                          AND SNAP.DBID = STAT.DBID
                          AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID = STAT.SNAP_ID
                          AND STAT.STAT_NAME IN ('execute count','session logical reads','sorts (disk)','sorts (memory)','physical reads','cluster wait time','application wait time','user I/O wait time','concurrency wait time')
                          ORDER BY SNAP.SNAP_ID
                          )
                      )
                   GROUP BY STAT_NAME
                )
              ) STAT
              ,(
                SELECT
                  MAX(DECODE(STAT_NAME,'DB CPU',TOT_VALUE/1000000)) DB_CPU
                  ,MAX(DECODE(STAT_NAME,'DB time',TOT_VALUE/1000000)) DB_TIME
                FROM
                  (
                  SELECT
                     STAT_NAME
                    ,SUM(TOT_VALUE) TOT_VALUE
                  FROM
                      ( SELECT STAT_NAME
                        ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
                        ,DECODE(SNAP_TIME_2
                              ,NULL,0
                              ,ROUND((VALUE_1-VALUE_2)))
                          TOT_VALUE
                        ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
                      FROM
                          ( SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) NO_MERGE(SNAP) NO_MERGE(STAT) */ SNAP.END_INTERVAL_TIME SNAP_TIME_1
                            ,STAT.STAT_NAME
                            ,STAT.VALUE VALUE_1
                            ,LAG(STAT.VALUE) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                            ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                            ,SNAP.SNAP_ID
                          FROM
                              (
                              SELECT /*+ NO_MERGE */ DI.DBID
                                ,DI.INSTANCE_NUMBER
                                ,DI.STARTUP_TIME
                              FROM
                                DBA_HIST_DATABASE_INSTANCE DI
                              WHERE
                              DI.DBID = :DBID
                              AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                              AND ROWNUM<=1
                              ) DBI
                            ,DBA_HIST_SNAPSHOT SNAP
                            ,DBA_HIST_SYS_TIME_MODEL STAT
                          WHERE DBI.DBID = SNAP.DBID
                          AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                          AND SNAP.SNAP_ID<=:END_SNAP_ID
                          AND SNAP.DBID = STAT.DBID
                          AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID = STAT.SNAP_ID
                          AND STAT.STAT_NAME IN ('DB CPU','DB time')
                          ORDER BY SNAP.SNAP_ID
                          )
                      )
                  GROUP BY STAT_NAME
                )
              ) TIME            
            )
          ,A_ET AS
            (
            SELECT *
            FROM
                (SELECT 'ELAPSED TIME ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY ELAPSED_TIME DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'ELAPSED TIME ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_CT AS
            (
            SELECT *
            FROM
                (SELECT 'CPU TIME ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CPU_TIME DESC, BUFFER_GETS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CPU TIME ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_BG AS
            (
            SELECT *
            FROM
                (SELECT 'BUFFER GETS ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY BUFFER_GETS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'BUFFER GETS ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_DR AS
            (
            SELECT *
            FROM
                (
                SELECT 'DISK READS ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY DISK_READS DESC, IOWAIT DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'DISK READS ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_CL AS
            (
            SELECT *
            FROM
                (
                SELECT 'CLUSTER WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CLWAIT DESC, DISK_READS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CLUSTER WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_CC AS
            (
            SELECT *
            FROM
                (
                SELECT 'CONSISTENT WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CCWAIT DESC, BUFFER_GETS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CONSISTENT WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_IW AS
            (
            SELECT *
            FROM
                (
                SELECT 'IO WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY IOWAIT DESC, DISK_READS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'IO WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_AW AS
            (
            SELECT *
            FROM
                (
                SELECT 'LOCK WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY APWAIT DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'LOCK WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_EC AS
            (
            SELECT *
            FROM
                (
                SELECT 'EXEC CNT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY EXECUTIONS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'EXEC CNT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
        SELECT /*+ OPT_PARAM('_gby_hash_aggregation_enabled','TRUE') OPT_PARAM('_optimizer_distinct_agg_transform','FALSE') BUG_9002336 */
            SQLSTAT.VIEWTYPE,SQLSTAT.SQL_ID,SQLSTAT.OPTIMIZER_MODE,SQLSTAT.MODULE,TO_CHAR(SQLSTAT.EXECUTIONS,'FM999,999,999,999,999,999') EXECUTIONS,TO_CHAR(SQLSTAT.FETCHES,'FM999,999,999,999,999,999') FETCHES,TO_CHAR(SQLSTAT.SORTS,'FM999,999,999,999,999,999') SORTS,TO_CHAR(SQLSTAT.BUFFER_GETS,'FM999,999,999,999,999,999') BUFFER_GETS,TO_CHAR(SQLSTAT.DISK_READS,'FM999,999,999,999,999,999') DISK_READS,TO_CHAR(SQLSTAT.ROWS_PROCESSED,'FM999,999,999,999,999,999') ROWS_PROCESSED,TO_CHAR(SQLSTAT.CPU_TIME,'FM999,999,999,999,999,999.9') CPU_TIME,TO_CHAR(SQLSTAT.ELAPSED_TIME,'FM999,999,999,999,999,999.9') ELAPSED_TIME,TO_CHAR(SQLSTAT.BUF_EXEC,'FM999,999,999,999,999,999.9') BUF_EXEC,TO_CHAR(SQLSTAT.DISK_EXEC,'FM999,999,999,999,999,999.9') DISK_EXEC,TO_CHAR(SQLSTAT.ROWS_EXEC,'FM999,999,999,999,999,999.9') ROWS_EXEC,TO_CHAR(SQLSTAT.CPU_EXEC,'FM999,999,999,999,999.999') CPU_EXEC,TO_CHAR(SQLSTAT.ELAP_EXEC,'FM999,999,999,999,999.999') ELAP_EXEC,TO_CHAR(SQLSTAT.IOWAIT,'FM999,999,999,999,999,999.9') IOWAIT
            ,TO_CHAR(SQLSTAT.CLWAIT,'FM999,999,999,999,999,999.9') CLWAIT,TO_CHAR(SQLSTAT.APWAIT,'FM999,999,999,999,999,999.9') APWAIT,TO_CHAR(SQLSTAT.CCWAIT,'FM999,999,999,999,999,999.9') CCWAIT,SQLSTAT.RNUM,DBMS_LOB.SUBSTR(SQLT.SQL_TEXT,3000,1) SQL_TEXT
            ,NULL SQL_PLAN
            ,(SELECT 'Table Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION NOT IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'View Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'Window Func Count:' || COUNT( CASE WHEN OPERATION IN ('WINDOW') THEN OPTIONS END ) || CHR(10) || 'Query Block Count:' || COUNT(DISTINCT QBLOCK_NAME ) || CHR(10) || 'TIME:' || MAX(TIME) || CHR(10) || 'COST:' || TO_CHAR(MAX(COST),'FM999,999,999') || CHR(10) || 'CPU COST:' || TO_CHAR(MAX(CPU_COST),'FM999,999,999,999,999') || CHR(10) || 'IO COST:' || TO_CHAR(MAX(IO_COST),'FM999,999,999,999') FROM V$SQL_PLAN WHERE SQL_ID = SQLSTAT.SQL_ID) PLAN_COST1
            ,(SELECT 'Table Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION NOT IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'View Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'Window Func Count:' || COUNT( CASE WHEN OPERATION IN ('WINDOW') THEN OPTIONS END ) || CHR(10) || 'Query Block Count:' || COUNT(DISTINCT QBLOCK_NAME ) || CHR(10) || 'TIME:' || MAX(TIME) || CHR(10) || 'COST:' || TO_CHAR(MAX(COST),'FM999,999,999') || CHR(10) || 'CPU COST:' || TO_CHAR(MAX(CPU_COST),'FM999,999,999,999,999') || CHR(10) || 'IO COST:' || TO_CHAR(MAX(IO_COST),'FM999,999,999,999') FROM DBA_HIST_SQL_PLAN WHERE SQL_ID = SQLSTAT.SQL_ID AND DBID = SQLSTAT.DBID AND TIMESTAMP IN (SELECT MAX(TIMESTAMP) FROM DBA_HIST_SQL_PLAN WHERE SQL_ID = SQLSTAT.SQL_ID AND DBID=SQLSTAT.DBID )) PLAN_COST2
            ,NULL BIND_VALUE
            ,NULL SQL_PROFILE
        FROM
            (
            SELECT * FROM A_ET
            UNION ALL
            SELECT 'ELAPSED TIME ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_ET) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'ELAPSED TIME ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY ELAPSED_TIME/EXECUTIONS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'ELAPSED TIME ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_CT
            UNION ALL
            SELECT 'CPU TIME ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_CT) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'CPU TIME ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CPU_TIME/EXECUTIONS DESC, BUFFER_GETS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CPU TIME ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_BG
            UNION ALL
            SELECT 'BUFFER GETS ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_BG) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'BUFFER GETS ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY BUFFER_GETS/EXECUTIONS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'BUFFER GETS ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_DR
            UNION ALL
            SELECT 'DISK READS ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_DR) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'DISK READS ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY DISK_READS/EXECUTIONS DESC, CLWAIT DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'DISK READS ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_CL
            UNION ALL
            SELECT 'CLUSTER WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_CL) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_CC
            UNION ALL
            SELECT 'CONSISTENT WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_CC) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_IW
            UNION ALL
            SELECT 'IO WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_IW) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_AW
            UNION ALL
            SELECT 'LOCK WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_AW) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_EC
            UNION ALL
            SELECT 'EXEC CNT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_EC) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT 'TOTAL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUF_EXEC,DISK_EXEC,ROWS_EXEC,CPU_EXEC,ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,RNUM FROM A_TOT
            ) SQLSTAT
          ,DBA_HIST_SQLTEXT SQLT
        WHERE SQLSTAT.SQL_ID = SQLT.SQL_ID(+) AND SQLSTAT.DBID=SQLT.DBID(+)
        ORDER BY VIEWTYPE,RNUM
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID'],
                  'RNUM': args['RNUM']}

    elif ind == 13:
        sqlcommand = """
        WITH A AS
            (
            SELECT /*+ LEADING(DBI SNAP) USE_HASH(SQLS TXT) NO_MERGE(SQLS TXT) */ SQLS.DBID,SQLS.SQL_ID,SQLS.OPTIMIZER_MODE,SQLS.MODULE,SUM(SQLS.FETCHES_DELTA) FETCHES,SUM(SQLS.EXECUTIONS_DELTA) EXECUTIONS,SUM(SQLS.SORTS_DELTA) SORTS,SUM(SQLS.DISK_READS_DELTA) DISK_READS,SUM(SQLS.BUFFER_GETS_DELTA) BUFFER_GETS,SUM(SQLS.ROWS_PROCESSED_DELTA) ROWS_PROCESSED,SUM(SQLS.CPU_TIME_DELTA)/1000000 CPU_TIME,SUM(SQLS.ELAPSED_TIME_DELTA)/1000000 ELAPSED_TIME,SUM(SQLS.IOWAIT_DELTA)/1000000 IOWAIT,SUM(SQLS.CLWAIT_DELTA)/1000000 CLWAIT,SUM(SQLS.APWAIT_DELTA)/1000000 APWAIT,SUM(SQLS.CCWAIT_DELTA)/1000000 CCWAIT,SUM(DIRECT_WRITES_DELTA) DIRECT_WRITES,NULL RNUM
            FROM
               (SELECT DI.DBID,DI.INSTANCE_NUMBER,DI.STARTUP_TIME FROM DBA_HIST_DATABASE_INSTANCE DI WHERE DI.DBID=:DBID AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER AND ROWNUM<=1) DBI
              ,DBA_HIST_SNAPSHOT SNAP
              ,DBA_HIST_SQLSTAT SQLS
              ,DBA_HIST_SQLTEXT SQLT
            WHERE DBI.DBID = SNAP.DBID AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID+1 AND SNAP.SNAP_ID<=:END_SNAP_ID AND SNAP.DBID = SQLS.DBID AND SNAP.INSTANCE_NUMBER = SQLS.INSTANCE_NUMBER AND SNAP.SNAP_ID = SQLS.SNAP_ID AND SQLS.SQL_ID = SQLT.SQL_ID AND SQLS.DBID = SQLT.DBID AND SQLT.COMMAND_TYPE NOT IN (47,170)
            GROUP BY SQLS.DBID,SQLS.SQL_ID,SQLS.OPTIMIZER_MODE,SQLS.MODULE
            HAVING SUM(SQLS.EXECUTIONS_DELTA)>0
            )
          ,A_TOT AS
            (
            SELECT 0 DBID,'TOT' SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,MAX(STAT.EXECUTIONS) EXECUTIONS,SUM(A.FETCHES) FETCHES,(MAX(STAT.SORTS_D)+MAX(STAT.SORTS_M)) SORTS,MAX(STAT.BUFFER_GETS) BUFFER_GETS,MAX(STAT.DISK_READS) DISK_READS,SUM(A.ROWS_PROCESSED) ROWS_PROCESSED,MAX(TIME.DB_CPU) CPU_TIME,MAX(TIME.DB_TIME) ELAPSED_TIME,MAX(STAT.BUFFER_GETS)/MAX(STAT.EXECUTIONS) BUF_EXEC,MAX(STAT.DISK_READS)/MAX(STAT.EXECUTIONS) DISK_EXEC,SUM(A.ROWS_PROCESSED)/SUM(A.EXECUTIONS) ROWS_EXEC,MAX(TIME.DB_CPU)/MAX(STAT.EXECUTIONS) CPU_EXEC,MAX(TIME.DB_TIME)/MAX(STAT.EXECUTIONS) ELAP_EXEC,MAX(STAT.IOWAIT) IOWAIT,MAX(STAT.CLWAIT) CLWAIT,MAX(STAT.APWAIT) APWAIT,MAX(STAT.CCWAIT) CCWAIT,COUNT(DISTINCT SQL_ID) RNUM
            FROM A
              ,(
                SELECT
                  MAX(DECODE(STAT_NAME,'execute count',TOT_VALUE)) EXECUTIONS
                  ,MAX(DECODE(STAT_NAME,'session logical reads',TOT_VALUE)) BUFFER_GETS
                  ,MAX(DECODE(STAT_NAME,'physical reads',TOT_VALUE)) DISK_READS
                  ,MAX(DECODE(STAT_NAME,'sorts (disk)',TOT_VALUE)) SORTS_D
                  ,MAX(DECODE(STAT_NAME,'sorts (memory)',TOT_VALUE)) SORTS_M
                  ,MAX(DECODE(STAT_NAME,'cluster wait time',TOT_VALUE/100)) CLWAIT
                  ,MAX(DECODE(STAT_NAME,'application wait time',TOT_VALUE/100)) APWAIT
                  ,MAX(DECODE(STAT_NAME,'concurrency wait time',TOT_VALUE/100)) CCWAIT
                  ,MAX(DECODE(STAT_NAME,'user I/O wait time',TOT_VALUE/100)) IOWAIT
                FROM
                  (
                  SELECT STAT_NAME,SUM(TOT_VALUE) TOT_VALUE
                  FROM
                      (SELECT STAT_NAME
                        ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
                        ,DECODE(SNAP_TIME_2,NULL,0 ,ROUND((VALUE_1-VALUE_2))) TOT_VALUE
                        ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
                      FROM
                          (SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) NO_MERGE(SNAP) NO_MERGE(STAT) */
                              SNAP.END_INTERVAL_TIME SNAP_TIME_1
                            ,STAT.STAT_NAME
                            ,STAT.VALUE VALUE_1
                            ,LAG(STAT.VALUE) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                            ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                            ,SNAP.SNAP_ID
                          FROM
                              (SELECT /*+ NO_MERGE */
                                  DI.DBID
                                ,DI.INSTANCE_NUMBER
                                ,DI.STARTUP_TIME
                              FROM DBA_HIST_DATABASE_INSTANCE DI
                              WHERE DI.DBID = :DBID
                              AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                              AND ROWNUM<=1
                              ) DBI
                            ,DBA_HIST_SNAPSHOT SNAP
                            ,DBA_HIST_SYSSTAT STAT
                          WHERE DBI.DBID = SNAP.DBID
                          AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                          AND SNAP.SNAP_ID<=:END_SNAP_ID
                          AND SNAP.DBID = STAT.DBID
                          AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID = STAT.SNAP_ID
                          AND STAT.STAT_NAME IN ('execute count','session logical reads','sorts (disk)','sorts (memory)','physical reads','cluster wait time','application wait time','user I/O wait time','concurrency wait time')
                          ORDER BY SNAP.SNAP_ID
                          )
                      )
                   GROUP BY STAT_NAME
                )
              ) STAT
              ,(
                SELECT
                  MAX(DECODE(STAT_NAME,'DB CPU',TOT_VALUE/1000000)) DB_CPU
                  ,MAX(DECODE(STAT_NAME,'DB time',TOT_VALUE/1000000)) DB_TIME
                FROM
                  (
                  SELECT
                     STAT_NAME
                    ,SUM(TOT_VALUE) TOT_VALUE
                  FROM
                      ( SELECT STAT_NAME
                        ,TO_CHAR(SNAP_TIME_1,'HH24:MI:SS') SNAP_TIME
                        ,DECODE(SNAP_TIME_2
                              ,NULL,0
                              ,ROUND((VALUE_1-VALUE_2)))
                          TOT_VALUE
                        ,(EXTRACT(DAY FROM SNAP_TIME_1 - SNAP_TIME_2) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_1 - SNAP_TIME_2) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_1 - SNAP_TIME_2) * 60 + EXTRACT(SECOND FROM SNAP_TIME_1 - SNAP_TIME_2)) TOT_TIME_SEC
                      FROM
                          ( SELECT /*+ LEADING(DBI) USE_HASH(SNAP STAT) NO_MERGE(SNAP) NO_MERGE(STAT) */ SNAP.END_INTERVAL_TIME SNAP_TIME_1
                            ,STAT.STAT_NAME
                            ,STAT.VALUE VALUE_1
                            ,LAG(STAT.VALUE) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) VALUE_2
                            ,LAG(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY STAT_NAME ORDER BY SNAP.SNAP_ID) SNAP_TIME_2
                            ,SNAP.SNAP_ID
                          FROM
                              (
                              SELECT /*+ NO_MERGE */ DI.DBID
                                ,DI.INSTANCE_NUMBER
                                ,DI.STARTUP_TIME
                              FROM
                                DBA_HIST_DATABASE_INSTANCE DI
                              WHERE
                              DI.DBID = :DBID
                              AND DI.INSTANCE_NUMBER=:INSTANCE_NUMBER
                              AND ROWNUM<=1
                              ) DBI
                            ,DBA_HIST_SNAPSHOT SNAP
                            ,DBA_HIST_SYS_TIME_MODEL STAT
                          WHERE DBI.DBID = SNAP.DBID
                          AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                          AND SNAP.SNAP_ID<=:END_SNAP_ID
                          AND SNAP.DBID = STAT.DBID
                          AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER
                          AND SNAP.SNAP_ID = STAT.SNAP_ID
                          AND STAT.STAT_NAME IN ('DB CPU','DB time')
                          ORDER BY SNAP.SNAP_ID
                          )
                      )
                  GROUP BY STAT_NAME
                )
              ) TIME
            )
          ,A_ET AS
            (
            SELECT *
            FROM
                (SELECT 'ELAPSED TIME ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY ELAPSED_TIME DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'ELAPSED TIME ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_CT AS
            (
            SELECT *
            FROM
                (SELECT 'CPU TIME ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CPU_TIME DESC, BUFFER_GETS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CPU TIME ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_BG AS
            (
            SELECT *
            FROM
                (SELECT 'BUFFER GETS ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY BUFFER_GETS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'BUFFER GETS ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_DR AS
            (
            SELECT *
            FROM
                (
                SELECT 'DISK READS ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY DISK_READS DESC, IOWAIT DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'DISK READS ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_CL AS
            (
            SELECT *
            FROM
                (
                SELECT 'CLUSTER WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CLWAIT DESC, DISK_READS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CLUSTER WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_CC AS
            (
            SELECT *
            FROM
                (
                SELECT 'CONSISTENT WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CCWAIT DESC, BUFFER_GETS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CONSISTENT WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_IW AS
            (
            SELECT *
            FROM
                (
                SELECT 'IO WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY IOWAIT DESC, DISK_READS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'IO WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_AW AS
            (
            SELECT *
            FROM
                (
                SELECT 'LOCK WAIT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY APWAIT DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'LOCK WAIT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
          ,A_EC AS
            (
            SELECT *
            FROM
                (
                SELECT 'EXEC CNT ALL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY EXECUTIONS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'EXEC CNT ALL' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM
                )
            WHERE RNUM<=:RNUM AND ROWNUM<=:RNUM
            )
            
        SELECT /*+ OPT_PARAM('_gby_hash_aggregation_enabled','TRUE') OPT_PARAM('_optimizer_distinct_agg_transform','FALSE') BUG_9002336 */
            SQLSTAT.VIEWTYPE,SQLSTAT.SQL_ID,SQLSTAT.OPTIMIZER_MODE,SQLSTAT.MODULE,TO_CHAR(SQLSTAT.EXECUTIONS,'FM999,999,999,999,999,999') EXECUTIONS,TO_CHAR(SQLSTAT.FETCHES,'FM999,999,999,999,999,999') FETCHES,TO_CHAR(SQLSTAT.SORTS,'FM999,999,999,999,999,999') SORTS,TO_CHAR(SQLSTAT.BUFFER_GETS,'FM999,999,999,999,999,999') BUFFER_GETS,TO_CHAR(SQLSTAT.DISK_READS,'FM999,999,999,999,999,999') DISK_READS,TO_CHAR(SQLSTAT.ROWS_PROCESSED,'FM999,999,999,999,999,999') ROWS_PROCESSED,TO_CHAR(SQLSTAT.CPU_TIME,'FM999,999,999,999,999,999.9') CPU_TIME,TO_CHAR(SQLSTAT.ELAPSED_TIME,'FM999,999,999,999,999,999.9') ELAPSED_TIME,TO_CHAR(SQLSTAT.BUF_EXEC,'FM999,999,999,999,999,999.9') BUF_EXEC,TO_CHAR(SQLSTAT.DISK_EXEC,'FM999,999,999,999,999,999.9') DISK_EXEC,TO_CHAR(SQLSTAT.ROWS_EXEC,'FM999,999,999,999,999,999.9') ROWS_EXEC,TO_CHAR(SQLSTAT.CPU_EXEC,'FM999,999,999,999,999.999') CPU_EXEC,TO_CHAR(SQLSTAT.ELAP_EXEC,'FM999,999,999,999,999.999') ELAP_EXEC,TO_CHAR(SQLSTAT.IOWAIT,'FM999,999,999,999,999,999.9') IOWAIT
            ,TO_CHAR(SQLSTAT.CLWAIT,'FM999,999,999,999,999,999.9') CLWAIT,TO_CHAR(SQLSTAT.APWAIT,'FM999,999,999,999,999,999.9') APWAIT,TO_CHAR(SQLSTAT.CCWAIT,'FM999,999,999,999,999,999.9') CCWAIT,SQLSTAT.RNUM,DBMS_LOB.SUBSTR(SQLT.SQL_TEXT,3000,1) SQL_TEXT
            ,NULL SQL_PLAN
            ,(SELECT 'Table Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION NOT IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'View Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'Window Func Count:' || COUNT( CASE WHEN OPERATION IN ('WINDOW') THEN OPTIONS END ) || CHR(10) || 'Query Block Count:' || COUNT(DISTINCT QBLOCK_NAME ) || CHR(10) || 'TIME:' || MAX(TIME) || CHR(10) || 'COST:' || TO_CHAR(MAX(COST),'FM999,999,999') || CHR(10) || 'CPU COST:' || TO_CHAR(MAX(CPU_COST),'FM999,999,999,999,999') || CHR(10) || 'IO COST:' || TO_CHAR(MAX(IO_COST),'FM999,999,999,999') FROM V$SQL_PLAN WHERE SQL_ID = SQLSTAT.SQL_ID) PLAN_COST1
            ,(SELECT 'Table Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION NOT IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'View Count:' || COUNT(DISTINCT CASE WHEN OBJECT_ALIAS IS NOT NULL AND OPERATION IN ('VIEW') THEN OBJECT_ALIAS END ) || CHR(10) || 'Window Func Count:' || COUNT( CASE WHEN OPERATION IN ('WINDOW') THEN OPTIONS END ) || CHR(10) || 'Query Block Count:' || COUNT(DISTINCT QBLOCK_NAME ) || CHR(10) || 'TIME:' || MAX(TIME) || CHR(10) || 'COST:' || TO_CHAR(MAX(COST),'FM999,999,999') || CHR(10) || 'CPU COST:' || TO_CHAR(MAX(CPU_COST),'FM999,999,999,999,999') || CHR(10) || 'IO COST:' || TO_CHAR(MAX(IO_COST),'FM999,999,999,999') FROM DBA_HIST_SQL_PLAN WHERE SQL_ID = SQLSTAT.SQL_ID AND DBID = SQLSTAT.DBID AND TIMESTAMP IN (SELECT MAX(TIMESTAMP) FROM DBA_HIST_SQL_PLAN WHERE SQL_ID = SQLSTAT.SQL_ID AND DBID=SQLSTAT.DBID )) PLAN_COST2
            ,NULL BIND_VALUE
            ,NULL SQL_PROFILE
        FROM
            (
            SELECT * FROM A_ET
            UNION ALL
            SELECT 'ELAPSED TIME ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_ET) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'ELAPSED TIME ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY ELAPSED_TIME/EXECUTIONS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'ELAPSED TIME ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_CT
            UNION ALL
            SELECT 'CPU TIME ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_CT) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'CPU TIME ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY CPU_TIME/EXECUTIONS DESC, BUFFER_GETS DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'CPU TIME ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_BG
            UNION ALL
            SELECT 'BUFFER GETS ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_BG) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'BUFFER GETS ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY BUFFER_GETS/EXECUTIONS DESC, CPU_TIME DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'BUFFER GETS ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_DR
            UNION ALL
            SELECT 'DISK READS ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_DR) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT *
            FROM
                (
                SELECT 'DISK READS ONE' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUFFER_GETS/EXECUTIONS BUF_EXEC,DISK_READS/EXECUTIONS DISK_EXEC,ROWS_PROCESSED/EXECUTIONS ROWS_EXEC,CPU_TIME/EXECUTIONS CPU_EXEC,ELAPSED_TIME/EXECUTIONS ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,ROW_NUMBER() OVER(ORDER BY DISK_READS/EXECUTIONS DESC, CLWAIT DESC,ROWNUM) RNUM
                FROM A
                UNION ALL
                SELECT 'DISK READS ONE' VIEWTYPE,NULL DBID,NULL SQL_ID,NULL OPTIMIZER_MODE,NULL MODULE,NULL EXECUTIONS,NULL FETCHES,NULL SORTS,NULL BUFFER_GETS,NULL DISK_READS,NULL ROWS_PROCESSED,NULL CPU_TIME,NULL ELAPSED_TIME,NULL BUF_EXEC,NULL DISK_EXEC,NULL ROWS_EXEC,NULL CPU_EXEC,NULL ELAP_EXEC,NULL IOWAIT,NULL CLWAIT,NULL APWAIT,NULL CCWAIT,NULL RNUM
                FROM A
                WHERE ROWNUM<=:RNUM+1
                )
            WHERE RNUM<=:RNUM+1 AND ROWNUM<=:RNUM+1
            UNION ALL
            SELECT * FROM A_CL
            UNION ALL
            SELECT 'CLUSTER WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_CL) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_CC
            UNION ALL
            SELECT 'CONSISTENT WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_CC) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_IW
            UNION ALL
            SELECT 'IO WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_IW) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_AW
            UNION ALL
            SELECT 'LOCK WAIT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_AW) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT * FROM A_EC
            UNION ALL
            SELECT 'EXEC CNT ALL' VIEWTYPE,0 DBID,'ETC' SQL_ID,NULL OPTIMIZER_MODE,'ETC' MODULE,V2.EXECUTIONS-V1.EXECUTIONS EXECUTIONS,V2.FETCHES-V1.FETCHES FETCHES,V2.SORTS-V1.SORTS SORTS,V2.BUFFER_GETS-V1.BUFFER_GETS BUFFER_GETS,V2.DISK_READS-V1.DISK_READS DISK_READS,V2.ROWS_PROCESSED-V1.ROWS_PROCESSED ROWS_PROCESSED,V2.CPU_TIME-V1.CPU_TIME CPU_TIME,V2.ELAPSED_TIME-V1.ELAPSED_TIME ELAPSED_TIME,0 BUF_EXEC,0 DISK_EXEC,0 ROWS_EXEC,0 CPU_EXEC,0 ELAP_EXEC,V2.IOWAIT-V1.IOWAIT IOWAIT,V2.CLWAIT-V1.CLWAIT CLWAIT,V2.APWAIT-V1.APWAIT APWAIT,V2.CCWAIT-V1.CCWAIT CCWAIT,NULL RNUM
            FROM
                (SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_EC) V1
                ,(SELECT SUM(EXECUTIONS) EXECUTIONS,SUM(FETCHES) FETCHES,SUM(SORTS) SORTS,SUM(BUFFER_GETS) BUFFER_GETS,SUM(DISK_READS) DISK_READS,SUM(ROWS_PROCESSED) ROWS_PROCESSED,SUM(CPU_TIME) CPU_TIME,SUM(ELAPSED_TIME) ELAPSED_TIME,SUM(IOWAIT) IOWAIT,SUM(CLWAIT) CLWAIT,SUM(APWAIT) APWAIT,SUM(CCWAIT) CCWAIT FROM A_TOT) V2
            UNION ALL
            SELECT 'TOTAL' VIEWTYPE,DBID,SQL_ID,OPTIMIZER_MODE,MODULE,EXECUTIONS,FETCHES,SORTS,BUFFER_GETS,DISK_READS,ROWS_PROCESSED,CPU_TIME,ELAPSED_TIME,BUF_EXEC,DISK_EXEC,ROWS_EXEC,CPU_EXEC,ELAP_EXEC,IOWAIT,CLWAIT,APWAIT,CCWAIT,RNUM FROM A_TOT
            ) SQLSTAT
          ,DBA_HIST_SQLTEXT SQLT
        WHERE SQLSTAT.SQL_ID = SQLT.SQL_ID(+) AND SQLSTAT.DBID=SQLT.DBID(+)
        ORDER BY VIEWTYPE,RNUM
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID'],
                  'RNUM': args['RNUM']}

    elif ind == 14:
        sqlcommand = """
        WITH CV_A AS
            (
            SELECT /*+ MATERIALIZE LEADING(DBI SNAP) USE_HASH(SQLS TXT) */ SQLS.DBID,SNAP.INSTANCE_NUMBER,SQLS.SQL_ID,SQLS.PLAN_HASH_VALUE,SQLS.MODULE,SUM(SQLS.FETCHES_DELTA) FETCHES,SUM(SQLS.EXECUTIONS_DELTA) EXECUTIONS,SUM(SQLS.SORTS_DELTA) SORTS,SUM(SQLS.DISK_READS_DELTA) DISK_READS,SUM(SQLS.BUFFER_GETS_DELTA) BUFFER_GETS,SUM(SQLS.ROWS_PROCESSED_DELTA) ROWS_PROCESSED,SUM(SQLS.CPU_TIME_DELTA)/1000000 CPU_TIME,SUM(SQLS.ELAPSED_TIME_DELTA)/1000000 ELAPSED_TIME,SUM(SQLS.IOWAIT_DELTA)/1000000 IOWAIT,SUM(SQLS.CLWAIT_DELTA)/1000000 CLWAIT,SUM(SQLS.APWAIT_DELTA)/1000000 APWAIT,SUM(SQLS.CCWAIT_DELTA)/1000000 CCWAIT,SUM(DIRECT_WRITES_DELTA) DIRECT_WRITES,TO_CHAR(MIN(END_INTERVAL_TIME),'YYYY.MM.DD HH24:MI') MIN_SNAP_TIME,TO_CHAR(MAX(END_INTERVAL_TIME),'YYYY.MM.DD HH24:MI') MAX_SNAP_TIME,MIN(SNAP.SNAP_ID) MIN_SNAP_ID,MAX(SNAP.SNAP_ID) MAX_SNAP_ID
            FROM
               (SELECT DI.DBID,DI.INSTANCE_NUMBER,DI.STARTUP_TIME FROM DBA_HIST_DATABASE_INSTANCE DI WHERE DI.DBID=:DBID AND DI.INSTANCE_NUMBER=INSTANCE_NUMBER AND ROWNUM<=1) DBI
              ,DBA_HIST_SNAPSHOT SNAP
              ,DBA_HIST_SQLSTAT SQLS
            WHERE DBI.DBID = SNAP.DBID AND DBI.INSTANCE_NUMBER = SNAP.INSTANCE_NUMBER AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID+1 AND SNAP.SNAP_ID<=:END_SNAP_ID AND SNAP.DBID = SQLS.DBID AND SNAP.INSTANCE_NUMBER = SQLS.INSTANCE_NUMBER AND SNAP.SNAP_ID = SQLS.SNAP_ID AND SQLS.PLAN_HASH_VALUE>0
            GROUP BY SQLS.DBID,SNAP.INSTANCE_NUMBER,SQLS.SQL_ID,SQLS.PLAN_HASH_VALUE,SQLS.OPTIMIZER_MODE,SQLS.MODULE
            HAVING SUM(SQLS.EXECUTIONS_DELTA)>0
            )
        SELECT
            DBID
            ,SQL_ID
            ,PLAN_HASH_VALUE
            ,MODULE
            ,TO_CHAR(FETCHES,'FM999,999,999,999') FETCHES
            ,TO_CHAR(EXECUTIONS,'FM999,999,999,999') EXECUTIONS
            ,TO_CHAR(SORTS,'FM999,999,999,999') SORTS
            ,TO_CHAR(DISK_READS,'FM999,999,999,999') DISK_READS
            ,TO_CHAR(BUFFER_GETS,'FM999,999,999,999') BUFFER_GETS
            ,TO_CHAR(ROWS_PROCESSED,'FM999,999,999,999') ROWS_PROCESSED
            ,TO_CHAR(CPU_TIME,'FM999,999,999,999.99') CPU_TIME
            ,TO_CHAR(ELAPSED_TIME,'FM999,999,999,999.99') ELAPSED_TIME
            ,TO_CHAR(IOWAIT,'FM999,999,999,999.99') IOWAIT
            ,TO_CHAR(CLWAIT,'FM999,999,999,999.99') CLWAIT
            ,TO_CHAR(APWAIT,'FM999,999,999,999.99') APWAIT
            ,TO_CHAR(CCWAIT,'FM999,999,999,999.99') CCWAIT
            ,TO_CHAR(DIRECT_WRITES,'FM999,999,999,999.99') DIRECT_WRITES
            ,TO_CHAR(BUFFER_GETS_EXEC,'FM999,999,999,999.99') BUFFER_GETS_EXEC
            ,TO_CHAR(DISK_READS_EXEC,'FM999,999,999,999.99') DISK_READS_EXEC
            ,TO_CHAR(CPU_TIME_EXEC,'FM999,999,999,999.99') CPU_TIME_EXEC
            ,TO_CHAR(ELAPSED_TIME_EXEC,'FM999,999,999,999.99') ELAPSED_TIME_EXEC
            ,TO_CHAR(ROWS_PROCESSED_EXEC,'FM999,999,999,999.99') ROWS_PROCESSED_EXEC
            ,MIN_SNAP_TIME
            ,MAX_SNAP_TIME
            ,MIN_SNAP_ID
            ,MAX_SNAP_ID
            ,SQL_TEXT
            ,NULL SQL_PLAN
            ,NULL SQL_BIND
        FROM
            (
            SELECT
                A.DBID
                ,A.SQL_ID
                ,A.PLAN_HASH_VALUE
                ,A.MODULE
                ,A.FETCHES
                ,A.EXECUTIONS
                ,A.SORTS
                ,A.DISK_READS
                ,A.BUFFER_GETS
                ,A.ROWS_PROCESSED
                ,A.CPU_TIME
                ,A.ELAPSED_TIME
                ,A.IOWAIT
                ,A.CLWAIT
                ,A.APWAIT
                ,A.CCWAIT
                ,A.DIRECT_WRITES
                ,BUFFER_GETS/DECODE(EXECUTIONS,0,1,EXECUTIONS) BUFFER_GETS_EXEC
                ,DISK_READS/DECODE(EXECUTIONS,0,1,EXECUTIONS) DISK_READS_EXEC
                ,CPU_TIME/DECODE(EXECUTIONS,0,1,EXECUTIONS) CPU_TIME_EXEC
                ,ELAPSED_TIME/DECODE(EXECUTIONS,0,1,EXECUTIONS) ELAPSED_TIME_EXEC
                ,ROWS_PROCESSED/DECODE(EXECUTIONS,0,1,EXECUTIONS) ROWS_PROCESSED_EXEC
                ,MIN_SNAP_TIME
                ,MAX_SNAP_TIME
                ,MIN_SNAP_ID
                ,MAX_SNAP_ID
                ,DBMS_LOB.SUBSTR(DHS.SQL_TEXT,3000,1) SQL_TEXT
            FROM
                CV_A A
                ,DBA_HIST_SQLTEXT DHS
            WHERE
                (A.DBID,A.SQL_ID) IN
                    (
                    SELECT DBID,SQL_ID
                    FROM CV_A
                    GROUP BY DBID,SQL_ID
                    HAVING COUNT(DISTINCT PLAN_HASH_VALUE)>1
                    )
                AND A.SQL_ID = DHS.SQL_ID(+) AND A.DBID=DHS.DBID(+)
            ORDER BY A.SQL_ID,BUFFER_GETS_EXEC
            )
        """
        params = {'DBID': args['DBID'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    elif ind == 15:
        sqlcommand = """
        SELECT /*+ NO_MERGE(DAFND) NO_MERGE(DARAT) NO_MERGE(DAACT) */
            DAFND.FINDING_ID_M FINDING_ID
            ,DAFND.FINDING_NAME
            ,DAREC.BENEFIT_RATIO \"BENEFIT RATIO(%)\"
            ,ROUND(DAREC.BENEFIT/DECODE(DAFND.IMPACT,0,1,DAFND.IMPACT)*100,2) \"IMPACT RATIO(%)\"
            ,DAREC.TYPE \"RECOMMEND TYPE\"
            ,DAACT.MESSAGE \"ACTION\"
            ,DARAT.MESSAGE \"RATIONALE\"
            ,DAFND.IMPACT_TYPE
            ,DAREC.BENEFIT_TYPE
        FROM
            (
            SELECT
                LPAD('>',2*(LEVEL-1)) || '[' || FINDING_ID  || ']' FINDING_ID_M
                ,FINDING_ID
                ,FINDING_NAME
                ,IMPACT
                ,MESSAGE
                ,IMPACT_TYPE
                ,OWNER
                ,TASK_NAME
                ,TASK_ID
                ,EXECUTION_NAME
                ,TYPE
                ,TYPE_ID
                ,OBJECT_ID
                ,MORE_INFO
                ,FILTERED
                ,FLAGS
                ,ROWNUM RNUM
            FROM DBA_ADVISOR_FINDINGS
            START WITH PARENT=0 AND TASK_ID = :TASK_ID
            CONNECT BY PRIOR FINDING_ID=PARENT AND TASK_ID = :TASK_ID
            ) DAFND
            ,(
            SELECT
                TASK_ID
                ,OWNER
                ,EXECUTION_NAME
                ,REC_ID
                ,FINDING_ID
                ,BENEFIT
                ,TYPE
                ,RANK
                ,BENEFIT_TYPE
                ,ROUND(BENEFIT/SUM(BENEFIT) OVER()*100,2) BENEFIT_RATIO
            FROM DBA_ADVISOR_RECOMMENDATIONS DAREC
            WHERE TASK_ID=:TASK_ID
            ) DAREC
            ,(
            SELECT
                TASK_ID
                ,OWNER
                ,EXECUTION_NAME
                ,REC_ID
                ,REPLACE(MESSAGE,'~',CHR(13)||CHR(10)) MESSAGE
            FROM
                (
                SELECT
                    TASK_ID
                    ,OWNER
                    ,EXECUTION_NAME
                    ,REC_ID
                    ,SUBSTR(SYS_CONNECT_BY_PATH(MESSAGE, '~'),2) MESSAGE
                    ,REC_RANK
                    ,REC_COUNT
                    ,REC_RANK_O
                FROM
                    (
                    SELECT
                        TASK_ID
                        ,OWNER
                        ,EXECUTION_NAME
                        ,REC_ID
                        ,'*'||MESSAGE MESSAGE
                        ,REC_RANK   REC_RANK_O
                        ,REC_ID * 1000000 + REC_RANK REC_RANK
                        ,REC_ID * 1000000 + REC_RANK-1 REC_RANK_P
                        ,REC_COUNT
                    FROM
                        (
                        SELECT
                            TASK_ID,OWNER,EXECUTION_NAME,REC_ID,MESSAGE,RANK() OVER(PARTITION BY REC_ID ORDER BY MESSAGE) REC_RANK,COUNT(*) OVER(PARTITION BY REC_ID) REC_COUNT
                        FROM DBA_ADVISOR_ACTIONS DAACT
                        WHERE TASK_ID = :TASK_ID
                        )
                    )
                START WITH REC_RANK_O = 1
                CONNECT BY PRIOR REC_RANK=REC_RANK_P
                )
            WHERE REC_COUNT=REC_RANK_O
            ) DAACT
            ,(
            SELECT
                TASK_ID
                ,OWNER
                ,EXECUTION_NAME
                ,REC_ID
                ,REPLACE(MESSAGE,'~',CHR(13)||CHR(10)) MESSAGE
            FROM
                (
                SELECT
                    TASK_ID
                    ,OWNER
                    ,EXECUTION_NAME
                    ,REC_ID
                    ,SUBSTR(SYS_CONNECT_BY_PATH(MESSAGE, '~'),2) MESSAGE
                    ,REC_RANK
                    ,REC_COUNT
                    ,REC_RANK_O
                FROM
                    (
                    SELECT
                        TASK_ID
                        ,OWNER
                        ,EXECUTION_NAME
                        ,REC_ID
                        ,'*'||MESSAGE MESSAGE
                        ,REC_RANK   REC_RANK_O
                        ,REC_ID * 1000000 + REC_RANK REC_RANK
                        ,REC_ID * 1000000 + REC_RANK-1 REC_RANK_P
                        ,REC_COUNT
                    FROM
                        (
                        SELECT
                            TASK_ID,OWNER,EXECUTION_NAME,REC_ID,MESSAGE,RANK() OVER(PARTITION BY REC_ID ORDER BY MESSAGE) REC_RANK,COUNT(*) OVER(PARTITION BY REC_ID) REC_COUNT
                        FROM DBA_ADVISOR_RATIONALE DARAT
                        WHERE TASK_ID = :TASK_ID
                        )
                    )
                START WITH REC_RANK_O = 1
                CONNECT BY PRIOR REC_RANK=REC_RANK_P
                )
            WHERE REC_COUNT=REC_RANK_O
            ) DARAT
        WHERE
            DAFND.TASK_ID=DAREC.TASK_ID(+)
            AND DAFND.OWNER=DAREC.OWNER(+)
            AND DAFND.EXECUTION_NAME=DAREC.EXECUTION_NAME(+)
            AND DAFND.FINDING_ID=DAREC.FINDING_ID(+)
            AND DAREC.TASK_ID=DAACT.TASK_ID(+)
            AND DAREC.OWNER=DAACT.OWNER(+)
            AND DAREC.EXECUTION_NAME=DAACT.EXECUTION_NAME(+)
            AND DAREC.REC_ID=DAACT.REC_ID(+)
            AND DAREC.TASK_ID=DARAT.TASK_ID(+)
            AND DAREC.OWNER=DARAT.OWNER(+)
            AND DAREC.EXECUTION_NAME=DARAT.EXECUTION_NAME(+)
            AND DAREC.REC_ID=DARAT.REC_ID(+)
        ORDER BY DAFND.RNUM,DAREC.RANK,DAREC.BENEFIT DESC
        """
        params = {'TASK_ID': args['TASK_ID']}


    elif ind == 16:
        sqlcommand = """
        WITH A AS
          (
          SELECT
            DBID
            ,OBJ#
            ,SUM(LOGICAL_READS_DELTA) LOGICAL_READS
            ,SUM(BUFFER_BUSY_WAITS_DELTA) BUFFER_BUSY_WAITS
            ,SUM(DB_BLOCK_CHANGES_DELTA) DB_BLOCK_CHANGES
            ,SUM(PHYSICAL_READS_DELTA) PHYSICAL_READS
            ,SUM(PHYSICAL_WRITES_DELTA) PHYSICAL_WRITES
            ,SUM(PHYSICAL_READS_DIRECT_DELTA) PHYSICAL_READS_DIRECT
            ,SUM(PHYSICAL_WRITES_DIRECT_DELTA) PHYSICAL_WRITES_DIRECT
            ,SUM(ITL_WAITS_DELTA) ITL_WAITS
            ,SUM(ROW_LOCK_WAITS_DELTA) ROW_LOCK_WAITS
            ,SUM(GC_CR_BLOCKS_SERVED_DELTA) GC_CR_BLOCKS_SERVED
            ,SUM(GC_CU_BLOCKS_SERVED_DELTA) GC_CU_BLOCKS_SERVED
            ,SUM(SPACE_USED_DELTA) SPACE_USED
            ,SUM(SPACE_ALLOCATED_DELTA) SPACE_ALLOCATED
            ,SUM(TABLE_SCANS_DELTA) TABLE_SCANS
          FROM DBA_HIST_SEG_STAT
          WHERE
            DBID=:DBID
            AND INSTANCE_NUMBER=:INSTANCE_NUMBER
            AND SNAP_ID BETWEEN :BEGIN_SNAP_ID AND :END_SNAP_ID
          GROUP BY
            DBID
            ,OBJ#
          )
          ,A_TOT AS
          (
          SELECT
            NULL DBID
            ,NULL OBJ#
            ,SUM(LOGICAL_READS) LOGICAL_READS
            ,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS
            ,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES
            ,SUM(PHYSICAL_READS) PHYSICAL_READS
            ,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES
            ,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT
            ,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT
            ,SUM(ITL_WAITS) ITL_WAITS
            ,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS
            ,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED
            ,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED
            ,SUM(SPACE_USED) SPACE_USED
            ,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED
            ,SUM(TABLE_SCANS) TABLE_SCANS
            ,COUNT(DISTINCT OBJ#) OBJ_CNT
          FROM A
          )
          ,A_LR AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'LOGICAL READS' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY LOGICAL_READS DESC, PHYSICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_BBW AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'BUFFER BUSY WAITS' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY BUFFER_BUSY_WAITS DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_DBC AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'DB_BLOCK CHANGES' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY DB_BLOCK_CHANGES DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_PR AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'PHYSICAL READS' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY PHYSICAL_READS DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_PW AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'PHYSICAL WRITES' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY PHYSICAL_WRITES DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_PRD AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'PHYSICAL READS DIRECT' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY PHYSICAL_READS_DIRECT DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_IW AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'ITL WAITS' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY ITL_WAITS DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_RLW AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'ROW LOCK WAITS' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY ROW_LOCK_WAITS DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_GCRBS AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'GC CR BLOCKS SERVED' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY GC_CR_BLOCKS_SERVED DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_GCUBS AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'GC CU BLOCKS SERVED' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY GC_CU_BLOCKS_SERVED DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_SU AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'SPACE USED' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY SPACE_USED DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_SA AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'SPACE ALLOCATED' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY SPACE_ALLOCATED DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
          ,A_TS AS
          (
          SELECT
            V.VIEWTYPE,V.RNUM,V.DBID,DO.OWNER,NVL(DO.OBJECT_NAME,TO_CHAR(V.OBJ#)) OBJECT_NAME,DO.SUBOBJECT_NAME,DO.OBJECT_TYPE,V.LOGICAL_READS,V.BUFFER_BUSY_WAITS,V.DB_BLOCK_CHANGES,V.PHYSICAL_READS,V.PHYSICAL_WRITES,V.PHYSICAL_READS_DIRECT,V.PHYSICAL_WRITES_DIRECT,V.ITL_WAITS,V.ROW_LOCK_WAITS,V.GC_CR_BLOCKS_SERVED,V.GC_CU_BLOCKS_SERVED,V.SPACE_USED,V.SPACE_ALLOCATED,V.TABLE_SCANS
          FROM
            (
            SELECT 'TABLE SCANS' VIEWTYPE,DBID,OBJ#,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,ROW_NUMBER() OVER(ORDER BY TABLE_SCANS DESC, LOGICAL_READS DESC,ROWNUM) RNUM
            FROM A
            ) V
            ,DBA_OBJECTS DO
          WHERE
            V.OBJ#=DO.OBJECT_ID(+)
            AND RNUM<=:RNUM AND ROWNUM<=:RNUM
          )
        SELECT /*+ OPT_PARAM('_gby_hash_aggregation_enabled','TRUE') OPT_PARAM('_optimizer_distinct_agg_transform','FALSE') BUG_9002336 */
          A.VIEWTYPE,A.TOR,A.RNUM,A.OWNER,DECODE(A.SUBOBJECT_NAME,NULL,A.OBJECT_NAME,A.OBJECT_NAME || '(' || A.SUBOBJECT_NAME || ')') OBJECT_NAME,A.OBJECT_TYPE,TO_CHAR(A.LOGICAL_READS,'FM999,999,999,999,999,999,999,999,999,999,999') LOGICAL_READS,TO_CHAR(A.BUFFER_BUSY_WAITS,'FM999,999,999,999,999,999,999,999,999,999,999') BUFFER_BUSY_WAITS,TO_CHAR(A.DB_BLOCK_CHANGES,'FM999,999,999,999,999,999,999,999,999,999,999') DB_BLOCK_CHANGES,TO_CHAR(A.PHYSICAL_READS,'FM999,999,999,999,999,999,999,999,999,999,999') PHYSICAL_READS,TO_CHAR(A.PHYSICAL_WRITES,'FM999,999,999,999,999,999,999,999,999,999,999') PHYSICAL_WRITES,TO_CHAR(A.PHYSICAL_READS_DIRECT,'FM999,999,999,999,999,999,999,999,999,999,999') PHYSICAL_READS_DIRECT,TO_CHAR(A.PHYSICAL_WRITES_DIRECT,'FM999,999,999,999,999,999,999,999,999,999,999') PHYSICAL_WRITES_DIRECT,TO_CHAR(A.ITL_WAITS,'FM999,999,999,999,999,999,999,999,999,999,999') ITL_WAITS
          ,TO_CHAR(A.ROW_LOCK_WAITS,'FM999,999,999,999,999,999,999,999,999,999,999') ROW_LOCK_WAITS,TO_CHAR(A.GC_CR_BLOCKS_SERVED,'FM999,999,999,999,999,999,999,999,999,999,999') GC_CR_BLOCKS_SERVED,TO_CHAR(A.GC_CU_BLOCKS_SERVED,'FM999,999,999,999,999,999,999,999,999,999,999') GC_CU_BLOCKS_SERVED,TO_CHAR(A.SPACE_USED,'FM999,999,999,999,999,999,999,999,999,999,999') SPACE_USED,TO_CHAR(A.SPACE_ALLOCATED,'FM999,999,999,999,999,999,999,999,999,999,999') SPACE_ALLOCATED,TO_CHAR(A.TABLE_SCANS,'FM999,999,999,999,999,999,999,999,999,999,999') TABLE_SCANS
        FROM
          (
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,1 TOR
          FROM A_LR
          UNION ALL
          SELECT 'LOGICAL READS' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,1 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_LR) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,3 TOR
          FROM A_BBW
          UNION ALL
          SELECT 'BUFFER BUSY WAITS' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,3 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_BBW) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,4 TOR
          FROM A_DBC
          UNION ALL
          SELECT 'DB_BLOCK CHANGES' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,4 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_DBC) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,2 TOR
          FROM A_PR
          UNION ALL
          SELECT 'PHYSICAL READS' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,2 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_PR) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,5 TOR
          FROM A_PW
          UNION ALL
          SELECT 'PHYSICAL WRITES' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,5 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_PW) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,6 TOR
          FROM A_PRD
          UNION ALL
          SELECT 'PHYSICAL READS DIRECT' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,6 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_PRD) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,7 TOR
          FROM A_IW
          UNION ALL
          SELECT 'ITL WAITS' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,7 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_IW) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,8 TOR
          FROM A_RLW
          UNION ALL
          SELECT 'ROW LOCK WAITS' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,8 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_RLW) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,9 TOR
          FROM A_GCRBS
          UNION ALL
          SELECT 'GC CR BLOCKS SERVED' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,9 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_GCRBS) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,10 TOR
          FROM A_GCUBS
          UNION ALL
          SELECT 'GC CU BLOCKS SERVED' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,10 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_GCUBS) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,11 TOR
          FROM A_SU
          UNION ALL
          SELECT 'SPACE USED' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,11 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_SU) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,12 TOR
          FROM A_SA
          UNION ALL
          SELECT 'SPACE ALLOCATED' VIEWTYPE,NULL RNUM,0 DBID,NULL OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,12 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_SA) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TOT) V2
          UNION ALL
          SELECT VIEWTYPE,RNUM,DBID,OWNER,OBJECT_NAME,SUBOBJECT_NAME,OBJECT_TYPE,LOGICAL_READS,BUFFER_BUSY_WAITS,DB_BLOCK_CHANGES,PHYSICAL_READS,PHYSICAL_WRITES,PHYSICAL_READS_DIRECT,PHYSICAL_WRITES_DIRECT,ITL_WAITS,ROW_LOCK_WAITS,GC_CR_BLOCKS_SERVED,GC_CU_BLOCKS_SERVED,SPACE_USED,SPACE_ALLOCATED,TABLE_SCANS,13 TOR
          FROM A_TS
          UNION ALL
          SELECT 'TABLE SCANS' VIEWTYPE,NULL RNUM,0 DBID,TO_CHAR(V2.OBJ_CNT) OWNER,'ETC' OBJECT_NAME,NULL SUBOBJECT_NAME,NULL OBJECT_TYPE,V2.LOGICAL_READS-V1.LOGICAL_READS LOGICAL_READS,V2.BUFFER_BUSY_WAITS-V1.BUFFER_BUSY_WAITS BUFFER_BUSY_WAITS,V2.DB_BLOCK_CHANGES-V1.DB_BLOCK_CHANGES DB_BLOCK_CHANGES,V2.PHYSICAL_READS-V1.PHYSICAL_READS PHYSICAL_READS,V2.PHYSICAL_WRITES-V1.PHYSICAL_WRITES PHYSICAL_WRITES,V2.PHYSICAL_READS_DIRECT-V1.PHYSICAL_READS_DIRECT PHYSICAL_READS_DIRECT,V2.PHYSICAL_WRITES_DIRECT-V1.PHYSICAL_WRITES_DIRECT PHYSICAL_WRITES_DIRECT,V2.ITL_WAITS-V1.ITL_WAITS ITL_WAITS,V2.ROW_LOCK_WAITS-V1.ROW_LOCK_WAITS ROW_LOCK_WAITS,V2.GC_CR_BLOCKS_SERVED-V1.GC_CR_BLOCKS_SERVED GC_CR_BLOCKS_SERVED,V2.GC_CU_BLOCKS_SERVED-V1.GC_CU_BLOCKS_SERVED GC_CU_BLOCKS_SERVED,V2.SPACE_USED-V1.SPACE_USED SPACE_USED,V2.SPACE_ALLOCATED-V1.SPACE_ALLOCATED SPACE_ALLOCATED,V2.TABLE_SCANS-V1.TABLE_SCANS TABLE_SCANS,13 TOR
          FROM
            (SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS FROM A_TS) V1
            ,(SELECT SUM(LOGICAL_READS) LOGICAL_READS,SUM(BUFFER_BUSY_WAITS) BUFFER_BUSY_WAITS,SUM(DB_BLOCK_CHANGES) DB_BLOCK_CHANGES,SUM(PHYSICAL_READS) PHYSICAL_READS,SUM(PHYSICAL_WRITES) PHYSICAL_WRITES,SUM(PHYSICAL_READS_DIRECT) PHYSICAL_READS_DIRECT,SUM(PHYSICAL_WRITES_DIRECT) PHYSICAL_WRITES_DIRECT,SUM(ITL_WAITS) ITL_WAITS,SUM(ROW_LOCK_WAITS) ROW_LOCK_WAITS,SUM(GC_CR_BLOCKS_SERVED) GC_CR_BLOCKS_SERVED,SUM(GC_CU_BLOCKS_SERVED) GC_CU_BLOCKS_SERVED,SUM(SPACE_USED) SPACE_USED,SUM(SPACE_ALLOCATED) SPACE_ALLOCATED,SUM(TABLE_SCANS) TABLE_SCANS,SUM(OBJ_CNT) OBJ_CNT FROM A_TOT) V2
          ) A
        ORDER BY TOR,VIEWTYPE,RNUM
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID'],
                  'RNUM': args['RNUM']}

    elif ind == 17:
        sqlcommand = """ 
         WITH CV_A AS 
             ( 
             SELECT 
                START_TIME SNAP_TIME 
               ,INSTANCE_NUMBER 
               ,MUTEX_TYPE 
               ,LOCATION 
               ,ROUND(AVG(NVL(WAIT_TIME,0)),3) AVG_WAIT_TIME 
               ,ROUND(AVG(NVL(WAIT_TIME_DIFF,0)),3) AVG_WAIT_TIME_DIFF 
               ,ROUND(MAX(NVL(WAIT_TIME,0)),3) MAX_WAIT_TIME 
               ,ROUND(MAX(NVL(WAIT_TIME_DIFF,0)),3) MAX_WAIT_TIME_DIFF 
               ,ROUND(AVG(NVL(SLEEPS,0)),3) AVG_SLEEPS 
               ,ROUND(AVG(NVL(SLEEPS_DIFF,0)),3) AVG_SLEEPS_DIFF 
               ,ROUND(MAX(NVL(SLEEPS,0)),3) MAX_SLEEPS 
               ,ROUND(MAX(NVL(SLEEPS_DIFF,0)),3) MAX_SLEEPS_DIFF 
               ,SUM(WAIT_TIME_DIFF) WAIT_TIME_DIFF 
               ,SUM(SLEEPS_DIFF) SLEEPS_DIFF 
             FROM 
                 (SELECT 
                    MUTEX_TYPE 
                   ,LOCATION 
                   ,INSTANCE_NUMBER 
                   ,TO_CHAR(SNAP_TIME_C1,'RR.MM.DD HH24:MI') START_TIME 
                   ,TO_CHAR(SNAP_TIME_C2,'RR.MM.DD HH24:MI') END_TIME 
                   ,ROUND(DECODE(SNAP_TIME_C2 ,NULL,0 ,( 
                     CASE 
                         WHEN WAIT_TIME_2<WAIT_TIME_1 THEN 0 ELSE WAIT_TIME_2-WAIT_TIME_1 
                     END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1))) / 1000,3) WAIT_TIME 
                   ,ROUND(( 
                     CASE 
                         WHEN WAIT_TIME_2<WAIT_TIME_1 THEN 0 ELSE WAIT_TIME_2-WAIT_TIME_1 
                     END)/1000,1) WAIT_TIME_DIFF 
                   ,ROUND(DECODE(SNAP_TIME_C2 ,NULL,0 ,( 
                     CASE 
                         WHEN SLEEPS_2<=SLEEPS_1 THEN 0 ELSE SLEEPS_2-SLEEPS_1 
                     END)/(EXTRACT(DAY FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 86400 + EXTRACT(HOUR FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 3600 + EXTRACT(MINUTE FROM SNAP_TIME_C2 - SNAP_TIME_C1) * 60 + EXTRACT(SECOND FROM SNAP_TIME_C2 - SNAP_TIME_C1))) / 1000,3) SLEEPS 
                   ,ROUND(( 
                     CASE 
                         WHEN SLEEPS_2<=SLEEPS_1 THEN 0 ELSE SLEEPS_2-SLEEPS_1 
                     END),1) SLEEPS_DIFF 
                   ,ROW_NUMBER() OVER(PARTITION BY INSTANCE_NUMBER,MUTEX_TYPE, LOCATION ORDER BY SNAP_ID) RNUM 
                   ,SNAP_ID 
                 FROM 
                     (SELECT 
                         SNAP.END_INTERVAL_TIME SNAP_TIME_C1 
                       ,LEAD(SNAP.END_INTERVAL_TIME) OVER (PARTITION BY SNAP.INSTANCE_NUMBER,MUTEX_TYPE,LOCATION ORDER BY SNAP.SNAP_ID) SNAP_TIME_C2 
                       ,STAT.MUTEX_TYPE 
                       ,STAT.LOCATION 
                       ,STAT.WAIT_TIME WAIT_TIME_1 
                       ,STAT.SLEEPS SLEEPS_1 
                       ,LEAD(STAT.WAIT_TIME) OVER (PARTITION BY SNAP.INSTANCE_NUMBER,MUTEX_TYPE,LOCATION ORDER BY SNAP.SNAP_ID) WAIT_TIME_2 
                       ,LEAD(STAT.SLEEPS) OVER (PARTITION BY SNAP.INSTANCE_NUMBER,MUTEX_TYPE,LOCATION ORDER BY SNAP.SNAP_ID) SLEEPS_2 
                       ,SNAP.SNAP_ID 
                       ,SNAP.INSTANCE_NUMBER 
                     FROM 
                        DBA_HIST_SNAPSHOT SNAP 
                       ,DBA_HIST_MUTEX_SLEEP STAT 
                     WHERE 
                         SNAP.DBID = :DBID
                     AND SNAP.INSTANCE_NUMBER=:INSTANCE_NUMBER 
                     AND SNAP.SNAP_ID>=:BEGIN_SNAP_ID
                     AND SNAP.SNAP_ID<=:END_SNAP_ID
                     AND SNAP.DBID = STAT.DBID 
                     AND SNAP.INSTANCE_NUMBER = STAT.INSTANCE_NUMBER 
                     AND SNAP.SNAP_ID = STAT.SNAP_ID 
                     ) 
                 WHERE 
                     SNAP_TIME_C2 <> SNAP_TIME_C1 
                 AND SLEEPS_2 <> SLEEPS_1 
                 ) 
             WHERE 
                 START_TIME IS NOT NULL 
             AND END_TIME IS NOT NULL 
             GROUP BY 
                 MUTEX_TYPE 
               , LOCATION 
               , INSTANCE_NUMBER 
               , START_TIME 
             ORDER BY MUTEX_TYPE, LOCATION, INSTANCE_NUMBER, SNAP_TIME 
             ) 
         SELECT 
             SNAP_TIME 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Parent' THEN AVG_WAIT_TIME END),0) \"AVG Wait(ms) Cursor Parent\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' THEN AVG_WAIT_TIME END),0) \"AVG Wait(ms) Cursor Pin\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Stat' THEN AVG_WAIT_TIME END),0) \"AVG Wait(ms) Cursor Stat\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' THEN AVG_WAIT_TIME END),0) \"AVG Wait(ms) Library Cache\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='hash table' THEN AVG_WAIT_TIME END),0) \"AVG Wait(ms) hash table\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Parent' THEN AVG_SLEEPS END),0) \"AVG SLEEPS Cursor Parent\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' THEN AVG_SLEEPS END),0) \"AVG SLEEPS Cursor Pin\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Stat' THEN AVG_SLEEPS END),0) \"AVG SLEEPS Cursor Stat\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' THEN AVG_SLEEPS END),0) \"AVG SLEEPS Library Cache\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='hash table' THEN AVG_SLEEPS END),0) \"AVG SLEEPS hash table\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Parent' THEN WAIT_TIME_DIFF END),0) \"TOTAL W.TIME(ms) Cursor Parent\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' THEN WAIT_TIME_DIFF END),0) \"TOTAL W.TIME(ms) Cursor Pin\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Stat' THEN WAIT_TIME_DIFF END),0) \"TOTAL W.TIME(ms) Cursor Stat\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' THEN WAIT_TIME_DIFF END),0) \"TOTAL W.TIME(ms) Library Cache\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='hash table' THEN WAIT_TIME_DIFF END),0) \"TOTAL W.TIME(ms) hash table\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Parent' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS Cursor Parent\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS Cursor Pin\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Stat' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS Cursor Stat\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS Library Cache\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='hash table' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS hash table\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglget2   2' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpin1   4' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 4\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglini1   32' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 32\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtld1  40' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 40\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtin1  42' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 42\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglati1   45' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 45\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglnti1   46' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 46\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglic1    49' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 49\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkc1   57' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 57\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn1  62' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 62\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhbh1   63' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 63\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgh1  64' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 64\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobpn1  71' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 71\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobld1  75' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 75\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglrfcl1  79' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 79\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal1  80' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 80\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal3  82' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 82\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkdl1  85' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 85\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnck1  88' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 88\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkck1  89' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 89\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal1  90' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 90\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal2  91' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 91\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpndl1  95' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 95\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn2 106' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 106\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal1 109' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 109\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal3 111' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 111\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllldl2 112' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 112\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglIsOwnerVersionable 121' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) 121\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksCheckCursor [KKSCHLBRKN]' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) KKSCHLBRKN\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksLockDelete [KKSCHLPIN6]' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) KKSCHLPIN6\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLFSP2]' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) KKSCHLFSP2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLPIN1]' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) KKSCHLPIN1\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kkslce [KKSCHLPIN2]' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) KKSCHLPIN2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksxsccmp [KKSCHLPIN5]' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) KKSCHLPIN5\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kqlfgx [KKSCHLRED1]' THEN AVG_WAIT_TIME END),0) \"AVG Wait Time(ms) KKSCHLRED1\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglget2   2' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpin1   4' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 4\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglini1   32' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 32\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtld1  40' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 40\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtin1  42' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 42\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglati1   45' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 45\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglnti1   46' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 46\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglic1    49' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 49\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkc1   57' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 57\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn1  62' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 62\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhbh1   63' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 63\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgh1  64' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 64\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobpn1  71' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 71\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobld1  75' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 75\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglrfcl1  79' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 79\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal1  80' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 80\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal3  82' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 82\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkdl1  85' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 85\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnck1  88' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 88\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkck1  89' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 89\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal1  90' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 90\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal2  91' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 91\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpndl1  95' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 95\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn2 106' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 106\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal1 109' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 109\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal3 111' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 111\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllldl2 112' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 112\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglIsOwnerVersionable 121' THEN AVG_SLEEPS END),0) \"AVG SLEEPS 121\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksCheckCursor [KKSCHLBRKN]' THEN AVG_SLEEPS END),0) \"AVG SLEEPS KKSCHLBRKN\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksLockDelete [KKSCHLPIN6]' THEN AVG_SLEEPS END),0) \"AVG SLEEPS KKSCHLPIN6\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLFSP2]' THEN AVG_SLEEPS END),0) \"AVG SLEEPS KKSCHLFSP2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLPIN1]' THEN AVG_SLEEPS END),0) \"AVG SLEEPS KKSCHLPIN1\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kkslce [KKSCHLPIN2]' THEN AVG_SLEEPS END),0) \"AVG SLEEPS KKSCHLPIN2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksxsccmp [KKSCHLPIN5]' THEN AVG_SLEEPS END),0) \"AVG SLEEPS KKSCHLPIN5\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kqlfgx [KKSCHLRED1]' THEN AVG_SLEEPS END),0) \"AVG SLEEPS KKSCHLRED1\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglget2   2' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpin1   4' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 4\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglini1   32' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 32\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtld1  40' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 40\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtin1  42' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 42\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglati1   45' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 45\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglnti1   46' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 46\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglic1    49' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 49\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkc1   57' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 57\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn1  62' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 62\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhbh1   63' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 63\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgh1  64' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 64\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobpn1  71' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 71\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobld1  75' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 75\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglrfcl1  79' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 79\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal1  80' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 80\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal3  82' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 82\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkdl1  85' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 85\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnck1  88' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 88\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkck1  89' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 89\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal1  90' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 90\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal2  91' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 91\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpndl1  95' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 95\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn2 106' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 106\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal1 109' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 109\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal3 111' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 111\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllldl2 112' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 112\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglIsOwnerVersionable 121' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) 121\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksCheckCursor [KKSCHLBRKN]' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) KKSCHLBRKN\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksLockDelete [KKSCHLPIN6]' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) KKSCHLPIN6\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLFSP2]' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) KKSCHLFSP2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLPIN1]' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) KKSCHLPIN1\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kkslce [KKSCHLPIN2]' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) KKSCHLPIN2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksxsccmp [KKSCHLPIN5]' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) KKSCHLPIN5\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kqlfgx [KKSCHLRED1]' THEN WAIT_TIME_DIFF END),0) \"TOTAL WAIT TIME(ms) KKSCHLRED1\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglget2   2' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpin1   4' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 4\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglini1   32' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 32\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtld1  40' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 40\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgldtin1  42' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 42\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglati1   45' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 45\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglnti1   46' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 46\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglic1    49' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 49\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkc1   57' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 57\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn1  62' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 62\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhbh1   63' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 63\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgh1  64' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 64\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobpn1  71' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 71\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglobld1  75' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 75\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglrfcl1  79' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 79\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal1  80' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 80\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkal3  82' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 82\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkdl1  85' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 85\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnck1  88' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 88\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllkck1  89' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 89\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal1  90' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 90\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpnal2  91' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 91\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglpndl1  95' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 95\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglhdgn2 106' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 106\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal1 109' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 109\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglllal3 111' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 111\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kgllldl2 112' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 112\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Library Cache' AND LOCATION='kglIsOwnerVersionable 121' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS 121\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksCheckCursor [KKSCHLBRKN]' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS KKSCHLBRKN\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksLockDelete [KKSCHLPIN6]' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS KKSCHLPIN6\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLFSP2]' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS KKSCHLFSP2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksfbc [KKSCHLPIN1]' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS KKSCHLPIN1\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kkslce [KKSCHLPIN2]' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS KKSCHLPIN2\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kksxsccmp [KKSCHLPIN5]' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS KKSCHLPIN5\" 
             ,NVL(MAX(CASE WHEN MUTEX_TYPE='Cursor Pin' AND LOCATION='kqlfgx [KKSCHLRED1]' THEN SLEEPS_DIFF END),0) \"TOTAL SLEEPS KKSCHLRED1\" 
         FROM 
             CV_A 
         GROUP BY 
             SNAP_TIME 
             ,INSTANCE_NUMBER 
         ORDER BY 2,1 
        """
        params = {'DBID': args['DBID'], 'INSTANCE_NUMBER': args['INSTANCE_NUMBER'],
                  'END_SNAP_ID': args['END_SNAP_ID'], 'BEGIN_SNAP_ID': args['BEGIN_SNAP_ID']}

    return (sqlcommand, params)

