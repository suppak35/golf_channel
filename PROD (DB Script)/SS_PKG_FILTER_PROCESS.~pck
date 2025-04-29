CREATE OR REPLACE PACKAGE BODY SS_PKG_FILTER_PROCESS IS

P_BATCH_NO     SS_FILE_LOG_DETAIL.BATCH_NO%TYPE;
P_REFILTER_DTM SS_FILE_LOG_DETAIL.REFILTER_DTM%TYPE;
P_SERVICE_REF  SS_FILE_LOG_DETAIL.SERVICE_REF%TYPE;
P_EVENT_DATE   SS_FILE_LOG_DETAIL.EVENT_DATE%TYPE;
P_USER_ID      SS_MASTER_CDR.USER_ID%TYPE;


FUNCTION FNC_GET_TABLE(P_CDR_TYPE IN VARCHAR2,
                       P_SERVICE_TYPE  IN VARCHAR2) RETURN VARCHAR2 IS
  V_SQL_TEXT   VARCHAR2(32767);
  V_TABLE_NAME VARCHAR2(1000);
BEGIN
  -- Get table name
  SELECT DISTINCT M.TABLE_DESTINATION
    INTO V_TABLE_NAME
    FROM SS_CFG_LOAD_MAPPING M
   WHERE CDR_TYPE = P_CDR_TYPE;

  -- Prepare SQL statement
  V_SQL_TEXT := 'SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE ' ||
                'FROM ALL_TAB_COLS WHERE TABLE_NAME = ''' || V_TABLE_NAME || '''';

  RETURN V_SQL_TEXT;

EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END;

  FUNCTION FNC_INS_TABLE(P_TABLE_INS    IN VARCHAR2,
                         P_TABLE_SOURCE IN VARCHAR2,
                         P_ROWID        IN VARCHAR2,
                         P_CDR_ID       IN VARCHAR2,
                         P_REASON_TYPE  IN VARCHAR2,
                         P_REASON_CODE  IN VARCHAR2,
                         P_CDR_STATUS   IN VARCHAR2) RETURN NUMBER IS
  
    --V_TABLE_NAME VARCHAR2(1000);
    V_SQL CLOB;
    -------------------------------
    CURSOR CUR_TABLE IS
      SELECT *
        FROM ALL_TAB_COLS
       WHERE TABLE_NAME = P_TABLE_INS
       ORDER BY COLUMN_ID;
    REC_TABLE CUR_TABLE%ROWTYPE;
    -------------------------------
  BEGIN
    --GET TABLE NAME
    OPEN CUR_TABLE;
    LOOP
      FETCH CUR_TABLE
        INTO REC_TABLE;
      EXIT WHEN CUR_TABLE%NOTFOUND;
    
      IF CUR_TABLE%ROWCOUNT > 1 THEN
        V_SQL := V_SQL || ',';
      END IF;
    
      IF REC_TABLE.COLUMN_NAME = 'REASON_TYPE' THEN
        V_SQL := V_SQL || '''' || P_REASON_TYPE || ''''; --NVL(P_REASON_TYPE,'''');
      ELSIF REC_TABLE.COLUMN_NAME = 'REASON_CODE' THEN
        V_SQL := V_SQL || '''' || P_REASON_CODE || '''';
      ELSIF REC_TABLE.COLUMN_NAME = 'CDR_STATUS' THEN
        V_SQL := V_SQL || '''' || P_CDR_STATUS || '''';
      ELSIF REC_TABLE.COLUMN_NAME = 'USER_DTM' THEN
        V_SQL := V_SQL || 'SYSDATE';
      ELSIF REC_TABLE.COLUMN_NAME = 'USER_ID' THEN
        V_SQL := V_SQL || '''' || P_USER_ID || ''''; 
      ELSE
        V_SQL := V_SQL || REC_TABLE.COLUMN_NAME;
      END IF;
    
    END LOOP;
    CLOSE CUR_TABLE;
    V_SQL := 'INSERT INTO ' || P_TABLE_INS || ' SELECT ' || V_SQL ||
             ' FROM ' || P_TABLE_SOURCE || ' WHERE CDR_ID ='''||P_CDR_ID||'''';--ROWID =''' || P_ROWID || '''';
    
    EXECUTE IMMEDIATE TO_CHAR(V_SQL);
  
    RETURN 0;
  
  END;
  
  FUNCTION FNC_CONVERT_SATANG_TO_BATH (P_AMT IN NUMBER,
                                       P_DIGITS IN NUMBER) RETURN NUMBER IS
  V_RESULT NUMBER;
    BEGIN
      IF P_DIGITS > 0 THEN
       V_RESULT :=1;
       FOR i IN 1..P_DIGITS
       LOOP
           V_RESULT := V_RESULT * 10;
       END LOOP;
       V_RESULT := P_AMT/V_RESULT;
      ELSE
       V_RESULT := P_AMT;
      END IF;
       RETURN V_RESULT;
    END;

  PROCEDURE PRC_FILTER_DUPLICATE(P_PROCESS_NAME     IN VARCHAR2,
                                 P_CDR_TYPE         IN VARCHAR2,
                                 P_SERVICE_TYPE     IN VARCHAR2,
                                 IN_SEQ NUMBER
                                 ) IS
  
    V_COUNT NUMBER;
    ----------------------------------
    --V_SQL       CLOB;
    V_SQL_VALID CLOB;
  
    -------------------------------------
  
    V_FILE_NAME     VARCHAR2(200);
    CUR_SOURCE_FILE SYS_REFCURSOR;
    CUR_SOURCE_DATA SYS_REFCURSOR;
    V_ROWID         VARCHAR2(1000);
    V_CDR_ID        VARCHAR2(1000);
    V_SEQ_NO        NUMBER := IN_SEQ;
    -------------------------------------
    ERR_MSG             VARCHAR2(1000);
    V_CNT_CDR_IN        NUMBER;
    V_CNT_CDR_OUT       NUMBER;
    V_TABLE_NAME_VALID  VARCHAR2(1000);
    V_TABLE_NAME_SOURCE VARCHAR2(1000);
    V_TABLE_NAME_REJECT VARCHAR2(1000);
    V_TABLE_INS         VARCHAR2(1000);
    V_SQL_TEMP          CLOB := NULL;
    V_CDR_STATUS        VARCHAR2(1000);
    V_QUERY_FILE        CLOB;
    V_QUERY_DATA        CLOB;
    RET_CODE            NUMBER := 0;
    V_REASON_TYPE       VARCHAR2(1000) := NULL;
    V_REASON_CODE       VARCHAR2(1000) := NULL;
    V_EVENT_TIME        DATE;
    V_PROCESS           VARCHAR2(100);
    -------------------------------------
    V_CNT_REJ           NUMBER:=0;
    V_CNT_COMP          NUMBER:=0;
    V_CNT_TMP           NUMBER:=0;
    -------------------------------------
    CURSOR CUR_FILTER_CONDITION IS
      SELECT *
        FROM SS_CFG_FILTER_CONDITION c
       WHERE c.cdr_type = P_CDR_TYPE
         AND c.service_type = P_SERVICE_TYPE
         AND EFF_DATE <= SYSDATE
         AND (EXP_DATE > SYSDATE - 1 OR EXP_DATE IS NULL)
       ORDER BY PRIORITY;
    REC_FILTER_CONDITION CUR_FILTER_CONDITION%ROWTYPE;
    -------------------------------------
    CURSOR CUR_FILTER_RULE IS
      SELECT R.SERVICE_TYPE, R.RULE_TYPE, V.MATCH_TYPE_CODE, V.ATTR_VALUE
        FROM SS_CFG_FILTER_RULE R, SS_CFG_FILTER_VALUE V
       WHERE R.RULE_ID = V.RULE_ID
         AND R.CDR_TYPE = V.CDR_TYPE
         AND R.SERVICE_TYPE = V.SERVICE_TYPE
         AND R.CDR_TYPE = P_CDR_TYPE
         AND R.SERVICE_TYPE = P_SERVICE_TYPE
         AND R.RULE_ID = REC_FILTER_CONDITION.RULE_ID;
    REC_FILTER_RULE CUR_FILTER_RULE%ROWTYPE;
    -------------------------------------
  
  BEGIN
    --UPDATE STATUS MAPPED TO FILTERING
    SELECT O.TABLE_SOURCE
      INTO V_TABLE_NAME_SOURCE
      FROM SS_CFG_FILTER_OUTPUT O
     WHERE CDR_TYPE = P_CDR_TYPE
       AND SERVICE_TYPE = P_SERVICE_TYPE
       AND PROCESS = 'FILTER'
     GROUP BY O.TABLE_SOURCE;
     
     IF P_PROCESS_NAME = 'REFILTER' THEN
       V_PROCESS := 'RE';
       
     END IF;
  
    BEGIN
    
      EXECUTE IMMEDIATE 'UPDATE ' || V_TABLE_NAME_SOURCE ||
                        ' SET CDR_STATUS ='''||V_PROCESS||'FILTERING'' WHERE CDR_STATUS ='''||V_PROCESS||'MAPPED''';
    
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;
        ERR_MSG := 'ERROR : UPDATE FILTERING - ' || SUBSTR(SQLERRM, 0, 255);
    END;
  
    --ERR_MSG IS NULL --> NOT ERROR      
    IF ERR_MSG IS NULL THEN
      --//GET FILE NAME
      V_QUERY_FILE := 'SELECT CDR_FILENAME ,COUNT(*)
                      FROM ' || V_TABLE_NAME_SOURCE || '
                      WHERE CDR_STATUS ='''||V_PROCESS||'FILTERING''
                      GROUP BY CDR_FILENAME
                      ORDER BY MAX(USER_DTM)';
      
      
                      
      OPEN CUR_SOURCE_FILE FOR TO_CHAR(V_QUERY_FILE);
      LOOP
        FETCH CUR_SOURCE_FILE
          INTO V_FILE_NAME, V_CNT_CDR_IN;
        EXIT WHEN CUR_SOURCE_FILE%NOTFOUND;
        --V_SEQ_NO := V_SEQ_NO + 1;
        BEGIN
          V_CNT_TMP := 0;
        
            V_CNT_REJ :=0;
            V_CNT_COMP:=0;
            V_CNT_CDR_OUT := 0;
          --//GET DATA         
          V_QUERY_DATA := 'SELECT ROWID ,EVENT_TIME,CDR_ID
                      FROM ' ||
                          V_TABLE_NAME_SOURCE || '
                      WHERE CDR_FILENAME = ''' ||
                          V_FILE_NAME || '''
                      AND CDR_STATUS ='''||V_PROCESS||'FILTERING''
                      AND CDR_ID is not null
                      ORDER BY USER_DTM';
        
          OPEN CUR_SOURCE_DATA FOR TO_CHAR(V_QUERY_DATA);
          LOOP
            FETCH CUR_SOURCE_DATA
              INTO V_ROWID,V_EVENT_TIME,V_CDR_ID;
            EXIT WHEN CUR_SOURCE_DATA%NOTFOUND;
            --// GET FILTER CONDITION
            
          if CUR_FILTER_CONDITION%isopen then
              CLOSE CUR_FILTER_CONDITION;
            end if;
            OPEN CUR_FILTER_CONDITION;
            LOOP
              FETCH CUR_FILTER_CONDITION
                INTO REC_FILTER_CONDITION;
              EXIT WHEN CUR_FILTER_CONDITION%NOTFOUND;
            
              --DBMS_OUTPUT.PUT_LINE(REC_FILTER_CONDITION.RULE_ID);
            
              SELECT O.TABLE_REJECT, O.TABLE_VALID
                INTO V_TABLE_NAME_REJECT, V_TABLE_NAME_VALID
                FROM SS_CFG_FILTER_OUTPUT O
               WHERE CDR_TYPE = P_CDR_TYPE
                 AND SERVICE_TYPE = P_SERVICE_TYPE
                 AND PROCESS = 'FILTER'
                 AND O.OUTPUT_CODE = REC_FILTER_CONDITION.OUTPUT_CODE;
            
            if CUR_FILTER_RULE%isopen then
              CLOSE CUR_FILTER_RULE;
            end if;
              OPEN CUR_FILTER_RULE;
              LOOP
                FETCH CUR_FILTER_RULE
                  INTO REC_FILTER_RULE;
                EXIT WHEN CUR_FILTER_RULE%NOTFOUND;
              
                --/////////////
                V_SQL_TEMP := NULL;
                IF TO_CHAR(V_SQL_TEMP) IS NULL THEN
                  V_SQL_TEMP := V_SQL_TEMP || ' WHERE 1=1 ';
                
                END IF;
                --DBMS_OUTPUT.PUT_LINE(V_SQL_TEMP);
                IF REC_FILTER_RULE.RULE_TYPE = 'STATEMENT' THEN
                  V_SQL_TEMP := V_SQL_TEMP || ' AND ' ||
                                REC_FILTER_RULE.ATTR_VALUE;
                END IF;
              
                --V_SQL_TEMP := V_SQL_TEMP || ' AND A.ROWID =''' || V_ROWID || '''';
                V_SQL_TEMP := V_SQL_TEMP || ' AND A.CDR_ID =''' || V_CDR_ID || '''';
                
                IF REC_FILTER_CONDITION.REASON_CODE = 1 THEN
                V_SQL_VALID := 'SELECT COUNT(*) FROM ' ||
                               V_TABLE_NAME_VALID || ' C,' ||
                               V_TABLE_NAME_SOURCE || ' A';
                ELSE
                V_SQL_VALID := 'SELECT COUNT(*) FROM ' ||
                               V_TABLE_NAME_SOURCE || ' A';
                END IF;
                
                --/////////////
                /*insert into chanutso_log_script (script,created_by,created_dtm)values(TO_CHAR(V_SQL_VALID) ||TO_CHAR(V_SQL_TEMP),'CHANUTSO',sysdate);
                   commit; */
              
                EXECUTE IMMEDIATE TO_CHAR(V_SQL_VALID) ||
                                  TO_CHAR(V_SQL_TEMP)
                  INTO V_COUNT;
              
                IF V_COUNT > 0 THEN
                
                  V_CDR_STATUS := 'REJECTED';
                  EXECUTE IMMEDIATE 'UPDATE ' || V_TABLE_NAME_SOURCE ||
                                    ' SET CDR_STATUS ='''||V_CDR_STATUS||'''
                                    WHERE CDR_ID =''' || V_CDR_ID ||
                                    /*''' WHERE ROWID =''' || V_ROWID ||*/
                                    ''' AND CDR_STATUS = '''||V_PROCESS||'FILTERING''';
                                    
                  V_REASON_TYPE := REC_FILTER_CONDITION.REASON_TYPE;
                  V_REASON_CODE := REC_FILTER_CONDITION.REASON_CODE;
                  V_TABLE_INS   := V_TABLE_NAME_REJECT;
                  --V_CNT_CDR_OUT := V_CNT_CDR_OUT + 1;
                  GOTO INS_TABLE;
                ELSE
                  V_TABLE_INS := V_TABLE_NAME_VALID;
                END IF;
              
              END LOOP;
              CLOSE CUR_FILTER_RULE;
            
            END LOOP;
            CLOSE CUR_FILTER_CONDITION;
            
            V_CDR_STATUS  := V_PROCESS||'FILTERED';
            V_CNT_CDR_OUT := V_CNT_CDR_OUT + 1;
            <<INS_TABLE>>
            RET_CODE := FNC_INS_TABLE(V_TABLE_INS,
                                      V_TABLE_NAME_SOURCE,
                                      V_ROWID,
                                      V_CDR_ID,
                                      V_REASON_TYPE,
                                      V_REASON_CODE,
                                      V_CDR_STATUS);
            EXECUTE IMMEDIATE 'UPDATE ' || V_TABLE_NAME_SOURCE ||
                              ' SET CDR_STATUS ='''||V_PROCESS||'FILTERED''
                               WHERE CDR_ID ='''||V_CDR_ID||
                              /* '''AND ROWID =''' ||V_ROWID ||*/
                              ''' AND CDR_STATUS = '''||V_PROCESS||'FILTERING''';
          
           
             
             IF V_CDR_STATUS = 'REJECTED' THEN
               /*UPDATE SS_MASTER_CDR_TEMP SET CDR_STATUS ='R' ,MASTER_STATUS = V_PROCESS||'FILTERED' , USER_DTM = SYSDATE
               WHERE CDR_ID = V_CDR_ID;*/
               EXECUTE IMMEDIATE 'DELETE FROM ' || V_TABLE_NAME_SOURCE ||
                                ' WHERE CDR_ID ='''||V_CDR_ID||
                                ''' AND CDR_STATUS =''' || V_CDR_STATUS || '''';
               DELETE FROM SS_MASTER_CDR_TEMP WHERE CDR_ID = V_CDR_ID;                 
                                
               V_CNT_REJ := V_CNT_REJ+1;
             ELSE
               UPDATE SS_MASTER_CDR_TEMP SET CDR_STATUS ='V' ,MASTER_STATUS = V_PROCESS||'FILTERED' , USER_DTM = SYSDATE
               WHERE CDR_ID = V_CDR_ID;
               V_CNT_COMP := V_CNT_COMP+1;
             END IF;
               
               V_CNT_TMP := V_CNT_TMP+1;
          
          END LOOP;
    CLOSE CUR_SOURCE_DATA;
    
    IF P_PROCESS_NAME = 'FILTER' THEN
    
      UPDATE SS_FILE_LOG SET REC_COMP = NVL(REC_COMP, 0)+V_CNT_COMP ,INS_COMP = NVL(INS_COMP, 0)+V_CNT_COMP,
                             REC_REJ  = NVL(REC_REJ, 0)+V_CNT_REJ   ,INS_REJ  = NVL(INS_REJ, 0)+V_CNT_REJ,
                             REC_TMP  = NVL(REC_TMP, 0)-V_CNT_TMP   ,INS_TMP  = NVL(INS_TMP, 0)-V_CNT_TMP,
                             FILE_STATUS = 'COMP'  ,USER_ID  = USER,
                             USER_DTM    = SYSDATE
      WHERE FILE_NAME = V_FILE_NAME AND FILE_STATUS ='COMPLOAD';
      
    ELSE
      
      P_EVENT_DATE := TRUNC(V_EVENT_TIME);
      
       --#Update Log
      UPDATE SS_FILE_LOG_DETAIL
         SET REC_COMP = NVL(REC_COMP, 0)+V_CNT_COMP ,INS_COMP = NVL(INS_COMP, 0)+V_CNT_COMP,
             REC_REJ  = NVL(REC_REJ, 0)+V_CNT_REJ   ,INS_REJ  = NVL(INS_REJ, 0)+V_CNT_REJ
       WHERE CDR_TYPE = P_CDR_TYPE
         AND PROC_TYPE = 'RE-FILTER'
         AND BATCH_NO = P_BATCH_NO
         AND NVL(SERVICE_REF, 'X') = NVL(P_SERVICE_REF, 'X')
         AND FILE_NAME = V_FILE_NAME
         AND EVENT_DATE = TRUNC(P_EVENT_DATE);
      
      IF SQL%ROWCOUNT = 0 THEN
        
      
      INSERT INTO SS_FILE_LOG_DETAIL
        (CDR_TYPE,
         FILE_NAME,
         FILE_STATUS,
         FILE_TOTAL,
         FILE_READ,
         REC_COMP,
         INS_COMP,
         REC_REJ,
         INS_REJ,
         REC_TMP,
         INS_TMP,
         REC_ERR,
         USER_ID,
         USER_DTM,
         REC_TST,
         INS_TST,
         PROC_TYPE,
         BATCH_NO,
         REFILTER_DTM,
         EVENT_DATE,
         SERVICE_REF
         )
      VALUES
        (P_CDR_TYPE,
         V_FILE_NAME,
         'COMP',
         0,
         0,
         V_CNT_COMP,
         V_CNT_COMP,
         V_CNT_REJ,
         V_CNT_REJ,
         0,--V_CNT_TMP,
         0,--V_CNT_TMP,
         0,
         'RE-FILTER',
         SYSDATE,
         0,
         0,
         'RE-FILTER',
         P_BATCH_NO,
         P_REFILTER_DTM,
         P_EVENT_DATE,
         P_SERVICE_REF
         );
         END IF;
    
    END IF;
      
          COMMIT;
          
          RET_CODE := FNC_INS_FILTER_LOG(P_PROCESS_NAME,
                                         P_SERVICE_TYPE,
                                         P_CDR_TYPE,
                                         v_file_name,
                                         V_SEQ_NO,
                                         V_CNT_CDR_IN,
                                         V_CNT_CDR_OUT,
                                         'COMPLETED',
                                         '');
        EXCEPTION
          WHEN OTHERS THEN
            ROLLBACK;
          
            RET_CODE := FNC_INS_FILTER_LOG(P_PROCESS_NAME,
                                           P_SERVICE_TYPE,
                                           P_CDR_TYPE,
                                           v_file_name,
                                           V_SEQ_NO,
                                           V_CNT_CDR_IN,
                                           0,
                                           'FAILED',
                                           SUBSTR(SQLERRM, 0, 1000));
            RET_CODE := 1;
            --DBMS_OUTPUT.PUT_LINE(SQLERRM);
        END;
      END LOOP;
    CLOSE CUR_SOURCE_FILE;
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RET_CODE := 1;
      DBMS_OUTPUT.PUT_LINE(SQLERRM);
  END PRC_FILTER_DUPLICATE;

  FUNCTION FNC_INS_FILTER_LOG(P_TASK_NAME          IN VARCHAR2,
                              P_SERVICE_TYPE       IN VARCHAR2,
                              P_CDR_TYPE           IN VARCHAR2,
                              P_ORIGINAL_FILE_NAME IN VARCHAR2,
                              P_SEQ_NO             IN NUMBER,
                              P_CDR_NUM_IN         IN NUMBER,
                              P_CDR_NUM_OUT        IN NUMBER,
                              P_STATUS             IN VARCHAR2,
                              P_REMARK             IN VARCHAR2) RETURN NUMBER IS
  
  BEGIN
  
    INSERT INTO SS_MTX_FILTER_PROCESS_LOG
      (TASK_NAME,
       SERVICE_TYPE,
       CDR_TYPE,
       ORIGINAL_FILE_NAME,
       SEQ_NO,
       CDR_NUM_IN,
       CDR_NUM_OUT,
       STATUS,
       REMARK,
       CREATED_BY,
       CREATED_DTM,
       UPDATED_BY,
       UPDATED_DTM)
    VALUES
      (P_TASK_NAME,
       P_SERVICE_TYPE,
       P_CDR_TYPE,
       P_ORIGINAL_FILE_NAME,
       P_SEQ_NO,
       P_CDR_NUM_IN,
       P_CDR_NUM_OUT,
       P_STATUS,
       P_REMARK,
       USER,
       SYSDATE,
       USER,
       SYSDATE);
  
    COMMIT;
    return 0;
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      return 1;
  END;
  
  /********************************************************************************************************/
 PROCEDURE SS_PRC_VALIDATEFILE(IN_SERVICE_TYPE IN VARCHAR2, IN_FILE_NAME IN VARCHAR2,IN_CDR_TYPE IN VARCHAR2,OUT_ERROR_CODE OUT NUMBER ) IS
  PROCESS_ERROR EXCEPTION;
  V_ERR_DESC     VARCHAR2(2000):='';
  V_LOG_ERROR    VARCHAR2(2000);
  V_ERR_SHOW_LOG VARCHAR2(1) := 'N';
  V_PROC_NAME VARCHAR2(20) := 'SS_PRC_VALIDATEFILE';
  V_LOG_NAME    VARCHAR2(100);
  V_DATA_SQL  LONG ;
  O_TEMP_SQL SYS_REFCURSOR;
  V_DATA  VARCHAR2(1000);
  V_ROWID  VARCHAR2(20);
  V_QUERY_TEMP  LONG  :='';
  V_UPDATE LONG  :='';
  V_ERROR_CODE VARCHAR2(3); 
  V_COLUMN_NAME  VARCHAR2(1000);
  V_DATA_TYPE    VARCHAR2(1000);
  V_DATA_LENGTH  NUMBER;
  V_TABLE_NAME   VARCHAR2(1000);
  CUR_TABLE_DESC SYS_REFCURSOR;
  V_NULLABLE  VARCHAR2(1);
  V_SQL CLOB;
  V_COUNT_CHK NUMBER;

  -------------------------------------------------------------
FUNCTION IS_DIGIT(I_STR VARCHAR2) RETURN NUMBER IS
  
    V_RESULT NUMBER(1) := 0;
    --> 0 : TRUE
    --> 1 : FAIL
    I NUMBER := 0;
  
  BEGIN
    IF NVL(I_STR, 'X') <> 'X' THEN
      FOR I IN 1 .. LENGTH(I_STR) LOOP
        IF SUBSTR(I_STR, I, 1) >= CHR(48) AND
           SUBSTR(I_STR, I, 1) <= CHR(57) THEN
          V_RESULT := 0;
        ELSE
          IF ((SUBSTR(I_STR, I, 1) = '-' OR SUBSTR(I_STR, I, 1) = '+') AND
             I = 1) OR (SUBSTR(I_STR, I, 1) = ',' AND I > 1) OR
             (SUBSTR(I_STR, I, 1) = '.') THEN
            V_RESULT := 0;
          ELSE
            V_RESULT := 1;
            EXIT;
          END IF;
        END IF;
      END LOOP;
    END IF;
  
    RETURN V_RESULT;
  END IS_DIGIT;
  ---------------------------------------------------------------------------
   PROCEDURE WRITE_ERROR(I_ERRCODE   VARCHAR2,
                        I_ROWNUM    NUMBER,
                        I_FIELDNAME VARCHAR2,
                        I_CDR_TYPE  VARCHAR2
                        ) IS
    V_ORA_TEXT VARCHAR2(100);
    LINE_TXT   VARCHAR2(200);
  BEGIN

    
    SELECT  A.ERROR_ID||A.DESCRIPTION INTO V_ORA_TEXT 
     FROM SS_ERROR_CODE A 
     WHERE A.CDR_TYPE= I_CDR_TYPE 
     AND A.ERROR_GROUP='FILTER'
     AND A.ERROR_ID like '%'||I_ERRCODE||'%'
     ;
    
    LINE_TXT := '***'||REPLACE(REPLACE(V_ORA_TEXT,'xx',I_ROWNUM),'yy',I_FIELDNAME);
                       

    DBMS_OUTPUT.PUT_LINE(LINE_TXT);
  
  END WRITE_ERROR;
  -------------------------------------------------------------
  FUNCTION CHECK_DATE(IVAL VARCHAR2) RETURN NUMBER IS
    RETURN_CODE NUMBER := 0;
    VDTM        DATE;
  BEGIN
    BEGIN
      SELECT TO_DATE(IVAL, 'YYYY/MM/DD HH24:MI:SS') INTO VDTM FROM DUAL;
    EXCEPTION
      WHEN OTHERS THEN
        RETURN_CODE := -1;
    END;
    RETURN RETURN_CODE;
  END CHECK_DATE;


  /* =============================================================*/
BEGIN
  -- *** MAIN ***
  OUT_ERROR_CODE := 0;
  --V_COUNT_CHK := 0 ;
     --DBMS_OUTPUT.PUT_LINE( 'SS_PRC_VALIDATEFILE - FILE NAME : ' || IN_FILE_NAME);   
      V_SQL := FNC_GET_TABLE(IN_CDR_TYPE,IN_SERVICE_TYPE);           
      OPEN CUR_TABLE_DESC FOR TO_CHAR(V_SQL);
       LOOP FETCH CUR_TABLE_DESC
          INTO V_TABLE_NAME,V_COLUMN_NAME, V_DATA_TYPE, V_DATA_LENGTH,V_NULLABLE;
             EXIT WHEN CUR_TABLE_DESC%NOTFOUND;
                IF V_COLUMN_NAME in('USER_ID','USER_DTM') THEN
                GOTO NEXT_RECORD;
                ELSE
                  IF V_DATA_TYPE = 'DATE' THEN
                    V_DATA_SQL := ' SELECT TO_CHAR('||V_COLUMN_NAME||',''YYYY/MM/DD HH24:MI:SS''),ROWID AS ROW_ID FROM '||V_TABLE_NAME||' WHERE CDR_STATUS =''MAPPING'' AND CDR_FILENAME = '''||IN_FILE_NAME||''' '; 
                  ELSE
                     V_DATA_SQL := ' SELECT '||V_COLUMN_NAME||',ROWID AS ROW_ID FROM '||V_TABLE_NAME||' WHERE CDR_STATUS =''MAPPING'' AND CDR_FILENAME = '''||IN_FILE_NAME||''' '; --STATUS   
                  END IF;
         OPEN O_TEMP_SQL FOR V_DATA_SQL ;
         --LOOP BY COLUMN_NAME
            LOOP
               FETCH O_TEMP_SQL 
               INTO V_DATA,V_ROWID   ;
               EXIT WHEN O_TEMP_SQL%NOTFOUND;
                  V_ERROR_CODE := '0';
                  --1) CHK NULL
                 IF V_NULLABLE = 'N' THEN --'N' is not null
                   IF NVL(V_DATA, 'X') = 'X' THEN
                     V_ERROR_CODE := '203';
                   END IF;
                 END IF;
                 
                 --2) CHK LENGTH
                IF V_ERROR_CODE = '0' THEN
                 IF NVL(V_DATA_LENGTH, 0) > 0  AND V_DATA_TYPE <> 'DATE' THEN
                  IF LENGTH(V_DATA) > V_DATA_LENGTH THEN
                     V_ERROR_CODE := '201';
                  END IF;
                END IF;
               END IF;
  
               --3) CHK TYPE
               IF V_ERROR_CODE = '0' THEN
                 IF INSTR(UPPER(V_DATA_TYPE), 'NUMBER') > 0 THEN
                  IF IS_DIGIT(V_DATA) <> 0 THEN
                     V_ERROR_CODE := '202';
                  END IF;
                 ELSIF INSTR(UPPER(V_DATA_TYPE), 'DATE') > 0 THEN
                    IF CHECK_DATE(V_DATA) <> 0 THEN
                        V_ERROR_CODE := '202';
                   END IF;
                END IF;
               END IF;
               
               IF V_ERROR_CODE <> '0' THEN
               WRITE_ERROR(V_ERROR_CODE,V_ROWID,V_DATA,IN_CDR_TYPE);
               V_UPDATE := 'UPDATE '||V_TABLE_NAME||'
                            SET CDR_STATUS = ''FAILED''
                            WHERE '||V_COLUMN_NAME||' = '''||V_DATA||'''
                            AND ROWID = '''||V_ROWID||''' ';
                 EXECUTE IMMEDIATE V_UPDATE ;
                  COMMIT;
               END IF;
           END LOOP;
              CLOSE O_TEMP_SQL;                             
       END IF;    
       <<NEXT_RECORD>> NULL;    
      END LOOP;
      CLOSE CUR_TABLE_DESC;
EXCEPTION
  WHEN PROCESS_ERROR THEN
    V_ERR_DESC := SQLERRM;
    DBMS_OUTPUT.PUT_LINE('EXCEPTION IN ' ||V_PROC_NAME || ' : '|| V_ERR_DESC|| ' : ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
    --COMMIT;
    OUT_ERROR_CODE := 1;
    DBMS_OUTPUT.PUT_LINE( 'V_DATA_SQL : ' || V_DATA_SQL);  
  WHEN OTHERS THEN
    V_ERR_DESC := SQLERRM;
    DBMS_OUTPUT.PUT_LINE('EXCEPTION IN ' ||V_PROC_NAME || ' : '|| V_ERR_DESC|| ' : ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
    --COMMIT;
    OUT_ERROR_CODE := 1;
    DBMS_OUTPUT.PUT_LINE( 'V_DATA_SQL : ' || V_DATA_SQL);  
END SS_PRC_VALIDATEFILE;
 
/********************************************************************************************************/
PROCEDURE SS_PORC_INS_MASTER_CRD_TEMP( IN_ROWID VARCHAR2,
                            IN_CDR_ID VARCHAR2,
                            IN_CDR_TYPE VARCHAR2,
                            IN_EVENT_TIME VARCHAR2,
                            IN_EE_ID VARCHAR2,
                            IN_VAS_NO VARCHAR2,
                            IN_TOTAL_AMT NUMBER,  
                            IN_NETWORK_TYPE VARCHAR2,
                            IN_INVOICING_CO_ID VARCHAR2,
                            IN_CDR_FILENAME VARCHAR2,
                            IN_SHARING_FLAG VARCHAR2,
                            IN_TARIFF_ID VARCHAR2,
                            IN_SHARE_BASIS VARCHAR2,
                            IN_USAGE_RATE NUMBER,
                            IN_TABLE_LOAD VARCHAR2,
                            IN_TASK_NAME  VARCHAR2,
                            IN_MOBILE_NO  VARCHAR2,
                            IN_RECORD_TYPE NUMBER,
                            IN_BEFORE_SHARING  NUMBER,
                            IN_TOTAL_TRANS NUMBER,
                            OUT_ERROR_CODE OUT NUMBER
                            ) IS
 
V_STATUS VARCHAR2(10);
SQL_TMP VARCHAR2(32767);


VTMP_ACCOUNT_NO SS_MASTER_CDR.ACCOUNT_NO%TYPE;
VTMP_NETWORK_TYPE SS_MASTER_CDR.NETWORK_TYPE%TYPE;

VTMP_EVENT_SEQ SS_MASTER_CDR.EVENT_SEQ%TYPE;
VTMP_COST_BAND SS_MASTER_CDR.COST_BAND%TYPE;
VTMP_ZONE_NAME SS_MASTER_CDR.ZONE_NAME%TYPE;
VTMP_ADD_INFO SS_MASTER_CDR.ADD_INFO%TYPE;
VTMP_TOTAL_AMT SS_MASTER_CDR.TOTAL_AMT%TYPE;

VTMP_INV_NO SS_MASTER_CDR.INV_NO%TYPE;
VTMP_INV_DATE SS_MASTER_CDR.INV_DATE%TYPE;

BEGIN
      IF IN_TASK_NAME = 'FILTER' THEN 
          
           V_STATUS := 'MAPPED';
ELSIF IN_TASK_NAME = 'REFILTER' THEN 
            
           V_STATUS := 'REMAPPED';
END IF;

  --# for aisok
  BEGIN
  FOR FT_COLUMN IN (SELECT * FROM SS_CFG_FILTER_COLUMN C WHERE C.CDR_TYPE = IN_CDR_TYPE ORDER BY C.SEQ ASC)
  LOOP
     IF SQL_TMP IS NOT NULL THEN
        SQL_TMP := SQL_TMP || ',';
     ELSE
         SQL_TMP := SQL_TMP || 'SELECT ' ;
     END IF;
     SQL_TMP := SQL_TMP || FT_COLUMN.COLUMN_TMP;
  END LOOP;
  SQL_TMP := SQL_TMP || ' FROM ' || IN_TABLE_LOAD || ' WHERE ROWID = ''' ||IN_ROWID || '''';
  
  --DBMS_OUTPUT.PUT_LINE('SQL TMP : ' || SQL_TMP);
  
  IF IN_CDR_TYPE IN ('126','128') THEN -- max 23/04/2025
     EXECUTE IMMEDIATE SQL_TMP INTO VTMP_ACCOUNT_NO,VTMP_EVENT_SEQ,VTMP_ZONE_NAME,VTMP_INV_NO,VTMP_INV_DATE;
  ELSE
     EXECUTE IMMEDIATE SQL_TMP INTO VTMP_ACCOUNT_NO,VTMP_EVENT_SEQ,VTMP_COST_BAND,VTMP_ZONE_NAME,VTMP_ADD_INFO;
  END IF;
  
  EXCEPTION
  WHEN OTHERS THEN
    NULL;
  END;

  INSERT INTO SS_MASTER_CDR_TEMP
    (REF_ROWID,
     CDR_ID,
     CDR_TYPE,
     EVENT_DTM,
     EE_ID,
     VAS_NO,
     TARIFF_ID,
     TOTAL_AMT,
     TRANSACTION_UNITS,
     SHARING_FLAG,
     NETWORK_TYPE,
     SAP_STATUS ,
     SHARING_STATUS,
     INVOICING_CO_ID,
     CDR_FILENAME,
     SHARE_BASIS,
     FILTER_DTM,
     MOBILE_NO,
     RECORD_TYPE,
     BEFORE_SHARING,
     USER_ID,
     USER_DTM,
     CDR_STATUS,
     MASTER_STATUS,
     ACCOUNT_NO,
     EVENT_SEQ,
     COST_BAND,
     ZONE_NAME,
     ADD_INFO,
     INV_NO,
     INV_DATE
     )
  VALUES
    (IN_ROWID,
     IN_CDR_ID,
     IN_CDR_TYPE,
     TO_DATE(IN_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS'),
     IN_EE_ID,
     IN_VAS_NO,
     IN_TARIFF_ID,
     IN_TOTAL_AMT,
     IN_TOTAL_TRANS,
     IN_SHARING_FLAG,     
     IN_NETWORK_TYPE,
     'N',-- SAP STATUS
     'N',-- SHARING STATUS
     IN_INVOICING_CO_ID,
     IN_CDR_FILENAME,
     IN_SHARE_BASIS,
     --IN_USAGE_RATE,
     SYSDATE,
     IN_MOBILE_NO,
     IN_RECORD_TYPE,
     IN_BEFORE_SHARING,
     P_USER_ID,
     SYSDATE,
     'T',
     V_STATUS,
     VTMP_ACCOUNT_NO,
     VTMP_EVENT_SEQ,
     VTMP_COST_BAND,
     VTMP_ZONE_NAME,
     VTMP_ADD_INFO,
     VTMP_INV_NO,
     VTMP_INV_DATE
     );
     
     IF SQL%ROWCOUNT >= 1 THEN
      execute immediate 'UPDATE '||IN_TABLE_LOAD||'
                            SET CDR_STATUS = '''||V_STATUS||'''
                            ,CDR_ID = '''||IN_CDR_ID||'''
                            WHERE CDR_FILENAME = '''||IN_CDR_FILENAME||'''
                            AND ROWID = '''||IN_ROWID||''''
                             ;
                            
       COMMIT;
       
      END IF;
     
    OUT_ERROR_CODE := 0;
  
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('ERROR [INSERT_MASTER_CDR_TEMP] : ' || SQLERRM);
    OUT_ERROR_CODE := 1;
    ROLLBACK;
END SS_PORC_INS_MASTER_CRD_TEMP;

/********************************************************************************************************/

/********************************************************************************************************/
PROCEDURE SS_PORC_INS_MASTER( IN_TASK_NAME VARCHAR2,
                              --IN_CDR_ID VARCHAR2,
                              IN_SERVICE_TYPE   VARCHAR2,
                              IN_CDR_TYPE VARCHAR2,
                              IN_CDR_FILENAME VARCHAR2,
                              IN_TABLE_NAME VARCHAR2,
                              IN_SEQ NUMBER
                            ) IS
 
V_DELETE_SQL LONG ;
V_CNT_CDR_IN NUMBER := 0;
V_CNT_CDR_OUT NUMBER := 0 ;
RET_CODE NUMBER := 0 ;
V_STATUS VARCHAR2(10) := '' ;
V_LOG_TASK VARCHAR2(10) := '' ;

 CURSOR GET_DATE_MASTER_TEMP IS     
      SELECT * FROM SS_MASTER_CDR_TEMP 
      WHERE  CDR_FILENAME= IN_CDR_FILENAME
      AND MASTER_STATUS = V_STATUS
      AND CDR_TYPE = IN_CDR_TYPE ;
  G_DATE GET_DATE_MASTER_TEMP%ROWTYPE;

  
BEGIN
  IF IN_TASK_NAME = 'FILTER' THEN
     V_STATUS := 'FILTERED';
     V_LOG_TASK := 'MASTER';
  ELSIF IN_TASK_NAME = 'REFILTER' THEN 
     V_STATUS := 'REFILTERED';
     V_LOG_TASK := 'REMASTER';
  END IF;
  
  EXECUTE IMMEDIATE ' SELECT COUNT(*) FROM '||IN_TABLE_NAME||' WHERE CDR_STATUS = '''||V_STATUS||''' AND CDR_FILENAME = '''||IN_CDR_FILENAME||''' ' INTO V_CNT_CDR_IN ;
  
  --DBMS_OUTPUT.PUT_LINE( ' SS_PORC_INS_MASTER - FILE NAME : ' || IN_CDR_FILENAME);                                        
  OPEN GET_DATE_MASTER_TEMP;
          LOOP FETCH GET_DATE_MASTER_TEMP
            INTO G_DATE;
            EXIT WHEN GET_DATE_MASTER_TEMP%NOTFOUND;
            BEGIN
  INSERT INTO SS_MASTER_CDR
    (CDR_ID,
     CDR_TYPE,
     EE_ID,
     VAS_NO,
     TARIFF_ID,
     TOTAL_AMT,
     AIS_AMT,
     EE_AMT,
     BC_AMT,
     BD_AMT,
     ADJ_AMT,
     JAVA_AMT,
     MARKUP_AMT1,
     MARKUP_AMT2,
     DURATION,
     EVENT_DTM,
     REVENUE_CODE,
     REGION_ID,
     COST_CTR,
     DIAL_NUMBER,
     SHARING_FLAG,
     NETWORK_TYPE,
     SAP_ID,
     SAP_STATUS,
     SHARING_STATUS,
     CDR_STATUS,
     ZONE_NAME,
     COST_BAND,
     INVOICING_CO_ID,
     MOBILE_NO,
     CDR_FILENAME,
     FILTER_DTM,
     MGR_AMT,
     COMM_AMT,
     USER_ID,
     USER_DTM,
     TRANSACTION_UNITS,
     SHARE_BASIS,
     REASON_CODE,
     INV_DATE,
     INV_NO,
     EVENT_SEQ,
     ACCOUNT_NO,
     TARIFF_DETAIL_CODE,
     RECORD_TYPE,
     DISCOUNT_ID,
     PRE_SHARING_FLAG,
     TOTAL_AMT_FLAG,
     BEFORE_SHARING,
     ADD_INFO,
     REPORT_REFERENCE,
     MULTI_BUNDLE_FLAG,
     REF_CDR_ID)
  VALUES
    ( G_DATE.CDR_ID,
     G_DATE.CDR_TYPE,
     G_DATE.EE_ID,
     G_DATE.VAS_NO,
     G_DATE.TARIFF_ID,
     G_DATE.TOTAL_AMT,
     G_DATE.AIS_AMT,
     G_DATE.EE_AMT,
     G_DATE.BC_AMT,
     G_DATE.BD_AMT,
     G_DATE.ADJ_AMT,
     G_DATE.JAVA_AMT,
     G_DATE.MARKUP_AMT1,
     G_DATE.MARKUP_AMT2,
     G_DATE.DURATION,
     G_DATE.EVENT_DTM,
     G_DATE.REVENUE_CODE,
     G_DATE.REGION_ID,
     G_DATE.COST_CTR,
     G_DATE.DIAL_NUMBER,
     G_DATE.SHARING_FLAG,
     G_DATE.NETWORK_TYPE,
     G_DATE.SAP_ID,
     G_DATE.SAP_STATUS,
     G_DATE.SHARING_STATUS,
     G_DATE.CDR_STATUS,
     G_DATE.ZONE_NAME,
     G_DATE.COST_BAND,
     G_DATE.INVOICING_CO_ID,
     G_DATE.MOBILE_NO,
     G_DATE.CDR_FILENAME,
     G_DATE.FILTER_DTM,
     G_DATE.MGR_AMT,
     G_DATE.COMM_AMT,
     G_DATE.USER_ID,
     G_DATE.USER_DTM,
     G_DATE.TRANSACTION_UNITS,
     G_DATE.SHARE_BASIS,
     G_DATE.REASON_CODE,
     G_DATE.INV_DATE,
     G_DATE.INV_NO,
     G_DATE.EVENT_SEQ,
     G_DATE.ACCOUNT_NO,
     G_DATE.TARIFF_DETAIL_CODE,
     G_DATE.RECORD_TYPE,
     G_DATE.DISCOUNT_ID,
     G_DATE.PRE_SHARING_FLAG,
     G_DATE.TOTAL_AMT_FLAG,
     G_DATE.BEFORE_SHARING,
     G_DATE.ADD_INFO,
     G_DATE.REPORT_REFERENCE,
     G_DATE.MULTI_BUNDLE_FLAG,
     G_DATE.REF_CDR_ID);

     IF SQL%ROWCOUNT >= 1 THEN
        
        V_DELETE_SQL :=' DELETE '||IN_TABLE_NAME||' WHERE ROWID= '''||G_DATE.REF_ROWID||''' AND CDR_FILENAME='''||IN_CDR_FILENAME||''' ' ;
        EXECUTE IMMEDIATE V_DELETE_SQL ;
        
        DELETE SS_MASTER_CDR_TEMP WHERE REF_ROWID = G_DATE.REF_ROWID;
        V_CNT_CDR_OUT := V_CNT_CDR_OUT+1;
        COMMIT;
        
      END IF;
      EXCEPTION
          WHEN OTHERS THEN
          ROLLBACK;
          RET_CODE := FNC_INS_FILTER_LOG(V_LOG_TASK,
                                      IN_SERVICE_TYPE,
                                      IN_CDR_TYPE,
                                      IN_CDR_FILENAME,
                                      0,
                                      V_CNT_CDR_IN,
                                      IN_SEQ,
                                      'COMPLETED',
                                      SUBSTR(SQLERRM,0,1000));
      END;
   END LOOP;
   
CLOSE GET_DATE_MASTER_TEMP;
       RET_CODE := FNC_INS_FILTER_LOG (V_LOG_TASK,
                                       IN_SERVICE_TYPE,
                                       IN_CDR_TYPE,
                                       IN_CDR_FILENAME,
                                       IN_SEQ,--SEQ
                                       V_CNT_CDR_IN,
                                       V_CNT_CDR_OUT,
                                       'COMPLETED',
                                       ''); 
    
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
         
          RET_CODE := FNC_INS_FILTER_LOG(V_LOG_TASK,
                                      IN_SERVICE_TYPE,
                                      IN_CDR_TYPE,
                                      IN_CDR_FILENAME,
                                      0,
                                      V_CNT_CDR_IN,
                                      IN_SEQ,
                                      'FAILED',
                                      SUBSTR(SQLERRM,0,1000));
                                     
END SS_PORC_INS_MASTER;

/********************************************************************************************************/


 PROCEDURE SS_PRC_FILTER( IN_TASK_NAME IN VARCHAR2,IN_SERVICE_TYPE IN VARCHAR2,IN_CDR_TYPE IN VARCHAR2 ,OUT_ERROR_CODE OUT NUMBER) IS
   
  PROCESS_ERROR EXCEPTION;
  V_ERR_DESC     VARCHAR2(2000):='';
  V_LOG_ERROR    VARCHAR2(2000);
  V_ERR_SHOW_LOG VARCHAR2(1) := 'N';
  V_PROC_NAME VARCHAR2(20) := 'SS_PRC_FILTER';
  V_LOG_PATH    VARCHAR2(100);
  V_LOG_NAME    VARCHAR2(100);
  V_DATA_SQL  LONG ;
  O_TEMP_SQL SYS_REFCURSOR;
  V_FILE_NAME  VARCHAR2(1000);
  V_DATE_TIME  VARCHAR2(20);
  V_USER_DTM DATE;
  V_QUERY_FILE_NAME  LONG  :='';
  V_QUERY  LONG  :='';
  V_UPDATE LONG  :='';
  V_ERROR_CODE VARCHAR2(3);
  V_COUNT NUMBER(10);
  V_SQL_TABLE_NAME CLOB;
  V_TABLE_NAME VARCHAR2(1000);
  V_DETAIL_CFG_VAS LONG  :='';
  O_TEMP_DETAIL_CFG SYS_REFCURSOR;
  O_MAP_DATA SYS_REFCURSOR;
  V_PREFIX_VALUE VARCHAR2(50);
  V_CDR_COLUMN VARCHAR2(50);
  V_VAS_COLUMN VARCHAR2(50);
  V_QUERY_MAP LONG  :=' ';
  V_ROWID VARCHAR2(20);
  V_CDR_ID VARCHAR2(50);
  V_EVENT_TIME VARCHAR2(50);
  V_EE_ID VARCHAR2(10);
  V_VAS_NO VARCHAR2(10);
  V_TOTAL_AMT NUMBER  ;
  V_TOTAL_TRANS NUMBER  ;
  V_NETWORK_TYPE VARCHAR2(5);
  V_INVOICING_CO_ID VARCHAR2(5);

  V_CHECK_CFG VARCHAR2(1);
  V_SHARING_FLAG VARCHAR2(5);
  V_TARIFF_ID VARCHAR2(10);
  V_SHARE_BASIS VARCHAR2(5);
  V_USAGE_RATE NUMBER;    
  V_CHECK_TARIFF VARCHAR2(1);
  
  V_COUNT_LOAD_TEMP NUMBER;
  V_COUNT_MASTER_TEMP NUMBER;
  RET_CODE      NUMBER := 0;
  V_CNT_CDR_IN  NUMBER := 0;
  V_CNT_CDR_OUT NUMBER := 0;
  V_SEQ  NUMBER := 0;
  
  V_UNMAPPED_SQL LONG  :='';
  V_STATUS VARCHAR2(10);
  V_LOG_TASK VARCHAR2(10);    

  V_CDR_TYPE VARCHAR2(5);
  V_VAS_SERVICE_TYPE VARCHAR2(15);
  V_SERVICE_TYPE VARCHAR2(30);
  ------------------------------
  V_QUERY_SRC_DATA  LONG  :='';
  O_SRC_DATA SYS_REFCURSOR;
  V_MOBILE_NO       SS_MASTER_CDR.MOBILE_NO%TYPE;
  V_BEFORE_SHARING  SS_MASTER_CDR.BEFORE_SHARING%TYPE;
  V_RECORD_TYPE     SS_MASTER_CDR.RECORD_TYPE%TYPE;
  V_REC_TEMP        NUMBER;
  V_INC_VAT_FLAG    SS_CFG_VAS_TYPE.INC_VAT_FLAG%TYPE;
  V_CURRENCY_TYPE   SS_CFG_VAS_TYPE.CURRENCY_TYPE%TYPE;
  V_SATANG_DIGITS   SS_CFG_VAS_TYPE.SATANG_DIGITS%TYPE;
  V_AMOUNT_COLUMN   SS_CFG_VAS_TYPE.AMOUNT_COLUMN%TYPE;
  V_TRANS_COLUMN    SS_CFG_VAS_TYPE.AMOUNT_COLUMN%TYPE;
  V_NO_CONVERT      NUMBER;
  
  V_PREFIX_ADD      SS_CFG_VAS_TYPE_DETAIL.ADD_PREFIX%TYPE;
  V_PREFIX_SUB      SS_CFG_VAS_TYPE_DETAIL.SUB_PREFIX%TYPE;
  
  V_VAT             SS_TAX.TAX_VALUE%TYPE;
  ------------------------------
        
/*  CURSOR GET_CFG_MASTER_TYPE IS     
      SELECT M.SERVICE_TYPE,M.CDR_TYPE,M.VAS_SERVICE_TYPE FROM SS_CFG_VAS_TYPE M
      WHERE M.SERVICE_TYPE = IN_SERVICE_TYPE
      AND M.CDR_TYPE = IN_CDR_TYPE
      AND M.EFF_DATE <= TO_DATE(V_DATE_TIME,'DD/MM/YYYY HH24:MI:SS')
      AND ( M.EXP_DATE > TO_DATE(V_DATE_TIME,'DD/MM/YYYY HH24:MI:SS')-1 OR M.EXP_DATE IS NULL );   
  C_CFG_MAS GET_CFG_MASTER_TYPE%ROWTYPE;*/

  ---------------------------------------------------------------------------
   PROCEDURE WRITE_ERROR(I_ERRCODE   VARCHAR2,
                        I_ROWNUM    NUMBER,
                        I_FIELDNAME VARCHAR2,
                        I_CDR_TYPE  VARCHAR2
                        ) IS
    V_ORA_TEXT VARCHAR2(100);
    LINE_TXT   VARCHAR2(200);
  BEGIN
    SELECT  A.ERROR_ID||A.DESCRIPTION INTO V_ORA_TEXT 
     FROM SS_ERROR_CODE A 
     WHERE A.CDR_TYPE=I_CDR_TYPE 
     AND A.ERROR_GROUP='FILTER'
     AND A.ERROR_ID like '%'||I_ERRCODE||'%'
     ;
    
    LINE_TXT := '***'||REPLACE(REPLACE(V_ORA_TEXT,'xx',I_ROWNUM),'yy',I_FIELDNAME);
                       
    DBMS_OUTPUT.PUT_LINE(LINE_TXT);

  END WRITE_ERROR;
  
  -------------------------------------------------------------

BEGIN
  -- *** MAIN ***
  OUT_ERROR_CODE := 0;
  --Get Table Name
  V_SQL_TABLE_NAME := FNC_GET_TABLE(IN_CDR_TYPE,IN_SERVICE_TYPE);   
                                      
  execute immediate 'SELECT DISTINCT TABLE_NAME FROM ('||TO_CHAR(V_SQL_TABLE_NAME)||')' into V_TABLE_NAME;
  
  --Count Transection From Temp
  IF IN_TASK_NAME = 'FILTER' THEN 
   V_STATUS := 'LOADED';
      
  ELSIF IN_TASK_NAME = 'REFILTER' THEN 
  V_STATUS := 'REMAPPING';
   --V_STATUS := 'UNMAPPED'; --edit by chanutso
   
  END IF;  
  P_USER_ID := USER;
  EXECUTE IMMEDIATE 'SELECT  COUNT(*) FROM '||V_TABLE_NAME||' WHERE CDR_STATUS= '''||V_STATUS||''' ' INTO V_COUNT;
  
  IF V_COUNT > 0 THEN
      DBMS_OUTPUT.PUT_LINE( 'DATA  : ' || V_COUNT);
      

       IF IN_TASK_NAME = 'FILTER' THEN 
           V_UPDATE :=' UPDATE '||V_TABLE_NAME||' SET CDR_STATUS = ''MAPPING'' WHERE CDR_STATUS = ''LOADED''  '; 
           EXECUTE IMMEDIATE V_UPDATE ;
           COMMIT;
           V_STATUS := 'MAPPING';
           V_LOG_TASK := 'MAP';
       ELSIF IN_TASK_NAME = 'REFILTER' THEN 
           /*V_UPDATE :=' UPDATE '||V_TABLE_NAME||' SET CDR_STATUS = ''REMAPPING'' WHERE CDR_STATUS = ''UNMAPPED''  '; 
           EXECUTE IMMEDIATE V_UPDATE ; 
           COMMIT;   */--edit by chanutso
           P_BATCH_NO := TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
           P_REFILTER_DTM := TRUNC(TO_DATE(P_BATCH_NO, 'YYYYMMDDHH24MISS'));
           P_SERVICE_REF := NULL;
           P_USER_ID := 'RE-FILTER';
           
           V_STATUS := 'REMAPPING';
           V_LOG_TASK := 'REMAP';
       END IF;
       V_QUERY_FILE_NAME := 'SELECT CDR_FILENAME
                      FROM ' || V_TABLE_NAME || '
                      WHERE CDR_STATUS ='''||V_STATUS||'''
                      GROUP BY CDR_FILENAME
                      ORDER BY MAX(USER_DTM)';
       OPEN O_TEMP_SQL FOR V_QUERY_FILE_NAME;
       LOOP
         FETCH O_TEMP_SQL
         INTO V_FILE_NAME;
         EXIT WHEN O_TEMP_SQL%NOTFOUND;
              
         --Validate  --FOR Filter Only
              V_SEQ := V_SEQ+1;
              V_REC_TEMP := 0;
              
              --START MAPPING
              /*EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM '||V_TABLE_NAME||' 
                                 WHERE CDR_STATUS = '''||V_STATUS||''' 
                                 AND CDR_FILENAME = '''||V_FILE_NAME||''' ' INTO V_CNT_CDR_IN ;
              V_CNT_CDR_OUT:= 0;*/
               
              IF IN_TASK_NAME = 'FILTER' THEN 
                    SS_PRC_VALIDATEFILE(IN_SERVICE_TYPE,V_FILE_NAME,IN_CDR_TYPE,OUT_ERROR_CODE);        
                    V_STATUS := 'MAPPING';--add by chanutso
              ELSIF IN_TASK_NAME = 'REFILTER' THEN 
                    V_STATUS := 'REMAPPING';--add by chanutso
              END IF;
              EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM '||V_TABLE_NAME||' 
                                 WHERE CDR_STATUS = '''||V_STATUS||''' 
                                 AND CDR_FILENAME = '''||V_FILE_NAME||''' ' INTO V_CNT_CDR_IN ;
              V_CNT_CDR_OUT:= 0;
              V_QUERY_SRC_DATA :='SELECT ROWID,TO_CHAR(A.EVENT_TIME,''DD/MM/YYYY HH24:MI:SS'')
                                  FROM '||V_TABLE_NAME||' A 
                                  WHERE CDR_STATUS ='''||V_STATUS||'''
                                  AND CDR_FILENAME = '''||V_FILE_NAME||'''
                                  ORDER BY  A.USER_DTM';
              DBMS_OUTPUT.PUT_LINE( 'BATCH NO : ' || P_BATCH_NO);                     
              DBMS_OUTPUT.PUT_LINE( 'MAPPING DATA - FILE NAME : ' || V_FILE_NAME); 
              BEGIN
                                    
              OPEN O_SRC_DATA FOR V_QUERY_SRC_DATA;
              LOOP
                FETCH O_SRC_DATA
                INTO V_ROWID,V_DATE_TIME;
                EXIT WHEN O_SRC_DATA%NOTFOUND;
                
                --BEGIN
                   V_REC_TEMP := V_REC_TEMP+1;
                   BEGIN
                     SELECT M.VAS_SERVICE_TYPE,M.INC_VAT_FLAG,M.CURRENCY_TYPE,M.SATANG_DIGITS,M.AMOUNT_COLUMN,M.TRANS_COLUMN
                       INTO V_VAS_SERVICE_TYPE,V_INC_VAT_FLAG,V_CURRENCY_TYPE,V_SATANG_DIGITS,V_AMOUNT_COLUMN,V_TRANS_COLUMN
                       FROM SS_CFG_VAS_TYPE M
                      WHERE M.SERVICE_TYPE = IN_SERVICE_TYPE
                        AND M.CDR_TYPE = IN_CDR_TYPE
                        AND M.EFF_DATE <=TO_DATE(V_DATE_TIME, 'DD/MM/YYYY HH24:MI:SS')
                        AND (M.EXP_DATE >TO_DATE(V_DATE_TIME, 'DD/MM/YYYY HH24:MI:SS')-1 OR M.EXP_DATE IS NULL);
                   EXCEPTION
                     WHEN OTHERS THEN
                       V_VAS_SERVICE_TYPE:= null;
                   END;
                   
                   IF V_VAS_SERVICE_TYPE is null THEN
                      GOTO NEXT_UNMAPPED;
                   END IF;
                   
                  
                           
                  V_DETAIL_CFG_VAS := 'SELECT D.ADD_PREFIX,D.SUB_PREFIX,D.CDR_COLUMN,D.VAS_COLUMN
                  FROM SS_CFG_VAS_TYPE_DETAIL D WHERE D.SERVICE_TYPE = '''||IN_SERVICE_TYPE||''' 
                  AND D.CDR_TYPE = '''||IN_CDR_TYPE||''' 
                  AND D.VAS_SERVICE_TYPE = '''||V_VAS_SERVICE_TYPE||'''
                  AND D.EFF_DATE <= TO_DATE('''||V_DATE_TIME||''',''DD/MM/YYYY HH24:MI:SS'')
                  AND (D.EXP_DATE >TO_DATE('''||V_DATE_TIME||''',''DD/MM/YYYY HH24:MI:SS'')-1 OR D.EXP_DATE IS NULL)';
                   
                  V_QUERY_MAP:='';
                     
                  /**** edit by jaran - 20240705 ****/    
                  IF IN_CDR_TYPE = '126' THEN
                    V_QUERY_MAP := 'SELECT /*+ INDEX(V , INX_SS_EE_VAS_4)*/ (TO_CHAR(SYSDATE, ''YYYYMM'') || TO_CHAR(SEQ_SS_CDR_ID.NEXTVAL) ) AS CDR_ID,
                                  TO_CHAR(C.EVENT_TIME,''DD/MM/YYYY HH24:MI:SS''),
                                  V.EE_ID,
                                  V.VAS_NO,
                                  C.CHARGE_AMT TOTAL_AMT,
                                  V.NETWORK_TYPE, 
                                  V.INVOICING_CO_ID,
                                  C.CALLING_NUMBER'; 
                   ELSIF IN_CDR_TYPE = '128' THEN -- max 22/04/2025
                     V_QUERY_MAP := 'SELECT /*+ INDEX(V , INX_SS_EE_VAS_4)*/ (TO_CHAR(SYSDATE, ''YYYYMM'') || TO_CHAR(SEQ_SS_CDR_ID.NEXTVAL) ) AS CDR_ID,
                                    TO_CHAR(C.EVENT_TIME,''DD/MM/YYYY HH24:MI:SS''),
                                    V.EE_ID,
                                    V.VAS_NO,
                                    NULL TOTAL_AMT,
                                    V.NETWORK_TYPE, 
                                    V.INVOICING_CO_ID,
                                    NULL AS CALLING_NUMBER'; 
                   
                  ELSE
                    V_QUERY_MAP := 'SELECT /*+ INDEX(V , INX_SS_EE_VAS_4)*/ (TO_CHAR(SYSDATE, ''YYYYMM'') || TO_CHAR(SEQ_SS_CDR_ID.NEXTVAL) ) AS CDR_ID,
                                  TO_CHAR(C.EVENT_TIME,''DD/MM/YYYY HH24:MI:SS''),
                                  V.EE_ID,
                                  V.VAS_NO,
                                  NULL TOTAL_AMT,
                                  V.NETWORK_TYPE, 
                                  V.INVOICING_CO_ID,
                                  C.CALLING_NUMBER'; 
                  END IF;
                   
                  /**** edit by jaran - 20240705 ****/   
                  IF IN_CDR_TYPE = '126' THEN
                    V_QUERY_MAP := V_QUERY_MAP||',C.CHARGE_AMT BEFORE_SHARING';
                  ELSE
                    /**** edit by chanutso - 20230628 ****/                
                    IF V_AMOUNT_COLUMN IS NOT NULL THEN                
                       V_QUERY_MAP := V_QUERY_MAP||',C.'||V_AMOUNT_COLUMN;
                    ELSE
                       V_QUERY_MAP := V_QUERY_MAP||',0 BEFORE_SHARING';
                    END IF;
                  END IF;
                  /**** edit by jaran - 20240705 ****/   
                  IF IN_CDR_TYPE = '126' THEN
                    V_QUERY_MAP := V_QUERY_MAP||',C.UNIT';
                  ELSE
                    IF V_TRANS_COLUMN IS NOT NULL THEN
                       V_QUERY_MAP := V_QUERY_MAP||',C.'||V_TRANS_COLUMN;
                    ELSE
                       V_QUERY_MAP := V_QUERY_MAP||',1 TOT_TRANS';   
                    END IF; 
                  END IF;
                   /**** edit by max - 04/04/2024 ****/   
                  IF IN_CDR_TYPE = '127' THEN
                     V_QUERY_MAP := V_QUERY_MAP||' FROM '||V_TABLE_NAME||' C, SS_EE_VAS V,SS_EE_VAS_TARIFF T  WHERE V.SERVICE_TYPE='''||V_VAS_SERVICE_TYPE||''' 
                                  AND C.CDR_FILENAME='''||V_FILE_NAME||''' AND C.CDR_STATUS='''||V_STATUS||'''
                                  AND C.ROWID ='''||V_ROWID||''' '  ;
                  ELSE
                      /**** end edit by chanutso - 20230628 ****/ 
                      V_QUERY_MAP := V_QUERY_MAP||' FROM '||V_TABLE_NAME||' C, SS_EE_VAS V  WHERE V.SERVICE_TYPE='''||V_VAS_SERVICE_TYPE||''' 
                                  AND C.CDR_FILENAME='''||V_FILE_NAME||''' AND C.CDR_STATUS='''||V_STATUS||'''
                                  AND C.ROWID ='''||V_ROWID||''' '  ;              
                  END IF;

                  
                  -- LOOP FOR GET CONDITION                          
                  OPEN O_TEMP_DETAIL_CFG FOR V_DETAIL_CFG_VAS ; 
                  LOOP
                    FETCH O_TEMP_DETAIL_CFG
                    INTO V_PREFIX_ADD,V_PREFIX_SUB,V_CDR_COLUMN,V_VAS_COLUMN   ;
                    EXIT WHEN O_TEMP_DETAIL_CFG%NOTFOUND;
                    
                    IF V_PREFIX_ADD IS NOT NULL THEN
                       V_QUERY_MAP := V_QUERY_MAP ||' AND '''|| V_PREFIX_ADD ||''' || nvl(C.'||V_CDR_COLUMN||',''x'') = nvl(V.'||V_VAS_COLUMN||',''x'')' ;
                    ELSIF V_PREFIX_SUB IS NOT NULL THEN
                       V_QUERY_MAP := V_QUERY_MAP ||' AND ((SUBSTR(C.'||V_CDR_COLUMN||',0,LENGTH('''|| V_PREFIX_SUB ||'''))='''|| V_PREFIX_SUB ||''' AND SUBSTR(C.'||V_CDR_COLUMN||',LENGTH('''|| V_PREFIX_SUB ||''')+1) = V.'||V_VAS_COLUMN||')' ;
                       V_QUERY_MAP := V_QUERY_MAP ||' OR  (SUBSTR(C.'||V_CDR_COLUMN||',0,LENGTH('''|| V_PREFIX_SUB ||'''))<>'''|| V_PREFIX_SUB ||''' AND C.'||V_CDR_COLUMN||' = V.'||V_VAS_COLUMN||'))';
                    ELSE   
                      /* V_QUERY_MAP := V_QUERY_MAP ||' AND C.'||V_CDR_COLUMN||' = V.'||V_VAS_COLUMN ;*/
                         IF IN_CDR_TYPE = '107' THEN
                             V_QUERY_MAP := V_QUERY_MAP ||' AND C.'||V_CDR_COLUMN||' = V.'||V_VAS_COLUMN ;
                        ELSE 
                             V_QUERY_MAP := V_QUERY_MAP ||' AND  nvl(C.'||V_CDR_COLUMN||',''x'') = nvl(V.'||V_VAS_COLUMN||',''x'')' ; 
                       END IF;
                    END IF;
                    
                  END LOOP;
                  CLOSE O_TEMP_DETAIL_CFG;
                  
                 /* insert into chanutso_log_script (script,created_by,created_dtm)values(V_QUERY_MAP,user,sysdate);
                  commit;*/
                  
                  /**** edit by max - 04/04/2024 ****/   
                  IF IN_CDR_TYPE = '127' THEN
                    V_QUERY_MAP := V_QUERY_MAP||' AND V.VAS_NO = T.VAS_NO AND (T.EXPIRED_DATE IS NULL OR T.EXPIRED_DATE <= C.START_DT )';
                  END IF;
                  
                                     
                  --V_CNT_CDR_OUT := 0;       
                  
                  OPEN O_MAP_DATA FOR V_QUERY_MAP ;
                     LOOP
                      FETCH O_MAP_DATA 
                      INTO V_CDR_ID,V_EVENT_TIME,V_EE_ID,V_VAS_NO,V_TOTAL_AMT,V_NETWORK_TYPE,V_INVOICING_CO_ID,V_MOBILE_NO,V_BEFORE_SHARING,V_TOTAL_TRANS;
                      EXIT WHEN O_MAP_DATA%NOTFOUND;
                      
                      V_RECORD_TYPE:=0;
                      
                      --CHACK CONFIG AT TABLE FROM SERVICE_TYPE
                      BEGIN
                        IF V_VAS_SERVICE_TYPE = 'BNO' THEN
                          V_CHECK_CFG:=''; 
                          
                          SELECT 'Y' INTO V_CHECK_CFG 
                          FROM SS_BNO_VAS WHERE VAS_NO = V_VAS_NO
                          AND EFFECTIVE_DATE <= TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')
                          AND (EXPIRED_DATE > TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')-1 OR EXPIRED_DATE IS NULL );
                          
                       ELSIF V_VAS_SERVICE_TYPE = 'PRO' THEN
                          V_CHECK_CFG:=''; 
                          
                          SELECT 'Y' INTO V_CHECK_CFG 
                          FROM SS_VAS_GENEVA_PRODUCT WHERE VAS_NO = ''||V_VAS_NO||'' 
                          AND EFFECTIVE_DATE <= TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')
                          AND (EXPIRED_DATE > TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')-1 OR EXPIRED_DATE IS NULL );
                          
                          
                        ELSIF V_VAS_SERVICE_TYPE = 'USC' THEN 
                          V_CHECK_CFG:=''; 
                          
                          SELECT 'Y' INTO V_CHECK_CFG 
                          FROM SS_USC_VAS WHERE VAS_NO = ''||V_VAS_NO||'' 
                          AND EFFECTIVE_DATE <= TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')
                          AND (EXPIRED_DATE > TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')-1 OR EXPIRED_DATE IS NULL );
                         
                       END IF;
                     EXCEPTION WHEN OTHERS THEN
                        V_CHECK_CFG:='N'; 
                     END ;
                     
                     IF V_CHECK_CFG = 'Y' THEN
                           V_CHECK_TARIFF :=''; 
                           
                           
                            --GET VAT 
                           BEGIN
                             
                           SELECT S.TAX_VALUE
                           INTO V_VAT
                           FROM SS_TAX S
                           WHERE EFFECTIVE_DATE <= TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')
                           AND (EXPIRED_DATE > TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')-1 OR EXPIRED_DATE IS NULL )
                           AND S.TAX_CODE ='VT';
                           
                           EXCEPTION WHEN OTHERS THEN
                              V_VAT:= NULL;
                            END;
                           
                           IF V_CURRENCY_TYPE = 'SATANG' THEN
                              IF V_SATANG_DIGITS > 0 THEN
                                --V_NO_CONVERT := V
                                V_BEFORE_SHARING := FNC_CONVERT_SATANG_TO_BATH(V_BEFORE_SHARING,V_SATANG_DIGITS);
                                --V_BEFORE_SHARING := V_BEFORE_SHARING/(RPAD('1', V_SATANG_DIGITS, '0'));
                              END IF;
                           ELSIF V_CURRENCY_TYPE = 'TRANS' THEN
                              V_TOTAL_AMT:=0;
                              V_BEFORE_SHARING := 0;
                              GOTO NEXT_STEP;   
                           END IF;
                           
                           
                           IF V_INC_VAT_FLAG = 'Y' AND V_VAT IS NOT NULL THEN
                           --CONVERT Inc VAT TO Exc VAT
                              V_TOTAL_AMT := V_BEFORE_SHARING*(100/(100+V_VAT));
                              
                           ELSIF V_INC_VAT_FLAG = 'Y' AND V_VAT IS NULL THEN
                              DBMS_OUTPUT.PUT_LINE('NOT FOUND CONFIG Vat. IN SS_TAX');
                              GOTO NEXT_LOOP;
                           ELSE
                             V_TOTAL_AMT := V_BEFORE_SHARING;
                           END IF;
                           
                           <<NEXT_STEP>>
                           
                           BEGIN 
                           --CHACK CONFIG AT SS_EE_VAS_TARIFF
                           SELECT SHARING_FLAG,TARIFF_ID,SHARE_BASIS,USAGE_RATE,'Y'
                           INTO V_SHARING_FLAG,V_TARIFF_ID,V_SHARE_BASIS,V_USAGE_RATE,V_CHECK_TARIFF 
                           FROM SS_EE_VAS_TARIFF WHERE VAS_NO = V_VAS_NO 
                            AND EFFECTIVE_DATE <= TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')
                            AND (EXPIRED_DATE > TO_DATE(V_EVENT_TIME,'DD/MM/YYYY HH24:MI:SS')-1 OR EXPIRED_DATE IS NULL );
                            
                            EXCEPTION WHEN OTHERS THEN
                              V_CHECK_TARIFF:= 'N';
                            END;
                           
                               IF V_CHECK_TARIFF = 'Y' THEN
                               --INSERT MASTER_TEMP
                                        SS_PORC_INS_MASTER_CRD_TEMP( V_ROWID,
                                                                     V_CDR_ID,
                                                                     IN_CDR_TYPE,
                                                                     V_EVENT_TIME,
                                                                     V_EE_ID,
                                                                     V_VAS_NO,
                                                                     V_TOTAL_AMT,
                                                                     V_NETWORK_TYPE,
                                                                     V_INVOICING_CO_ID,
                                                                     V_FILE_NAME,
                                                                     V_SHARING_FLAG,
                                                                     V_TARIFF_ID,
                                                                     V_SHARE_BASIS,
                                                                     V_USAGE_RATE,
                                                                     V_TABLE_NAME,
                                                                     IN_TASK_NAME,
                                                                     V_MOBILE_NO,
                                                                     V_RECORD_TYPE,
                                                                     V_BEFORE_SHARING,
                                                                     V_TOTAL_TRANS,
                                                                     OUT_ERROR_CODE
                                                                    -- V_CNT_CDR_OUT
                                                                     );
                                        IF OUT_ERROR_CODE = 0 THEN
                                        V_CNT_CDR_OUT := V_CNT_CDR_OUT+1;
                                       END IF;
                                      
                               ELSIF NVL(V_CHECK_TARIFF,'N') = 'N' THEN  
                               
                                 DBMS_OUTPUT.PUT_LINE('NOT FOUND CONFIG IN SS_EE_VAS_TARIFF');
                                 
                               END IF;
                               <<NEXT_LOOP>>
                               NULL;
                   ELSIF NVL(V_CHECK_CFG,'N') = 'N' THEN  
                    DBMS_OUTPUT.PUT_LINE('NOT FOUND CONFIG IN VAS SERVICE TYPE '||SQLERRM);
                   END IF;
                   
                 END LOOP;
                 CLOSE O_MAP_DATA;
                 
                 <<NEXT_UNMAPPED>>
                 BEGIN --- UPDATE STATUS FOR UNMAPPED CDR
                   
                  EXECUTE IMMEDIATE 'UPDATE '||V_TABLE_NAME||'
                            SET CDR_STATUS = ''UNMAPPED''
                            WHERE CDR_FILENAME = '''||V_FILE_NAME||'''
                            AND CDR_STATUS = '''||V_STATUS||'''
                            AND ROWID ='''||v_ROWID||'''';
                            COMMIT;
                 
                 EXCEPTION
                 WHEN OTHERS THEN
                 ROLLBACK;  
                       DBMS_OUTPUT.PUT_LINE('ERROR UPDATE STATUS TO UNMAPPED');      
                 END;       
                     
                
                /*EXCEPTION 
                  WHEN OTHERS THEN
                    ROLLBACK;
                    DBMS_OUTPUT.PUT_LINE('ERROR MAPPING : '||SQLERRM);
                       
                END;*/
                
              END LOOP;
              CLOSE O_SRC_DATA;
              
              RET_CODE := FNC_INS_FILTER_LOG (V_LOG_TASK,
                                                  IN_SERVICE_TYPE,
                                                  IN_CDR_TYPE,
                                                  V_FILE_NAME,
                                                  V_SEQ,
                                                  V_CNT_CDR_IN,
                                                  V_CNT_CDR_OUT,
                                                  'COMPLETED',
                                                  '');
              EXCEPTION 
                  WHEN OTHERS THEN
                    ROLLBACK;
                    RET_CODE := FNC_INS_FILTER_LOG(V_LOG_TASK,
                                           IN_SERVICE_TYPE,
                                           IN_CDR_TYPE,
                                           V_FILE_NAME,
                                           V_SEQ,
                                           V_CNT_CDR_IN,
                                           0,
                                           'FAILED',
                                           SUBSTR(SQLERRM, 0, 1000));
                            
                  RET_CODE := 1;
                       
              END;
              --------END MAPPING BY FILE---------                                    
              --FILTER_DUP
              PRC_FILTER_DUPLICATE(IN_TASK_NAME,IN_CDR_TYPE,IN_SERVICE_TYPE,V_SEQ)  ;
              
                    
              IF IN_TASK_NAME = 'FILTER' THEN 
                 V_STATUS := 'FILTERED';
              ELSIF IN_TASK_NAME = 'REFILTER' THEN    
                 V_STATUS := 'REFILTERED';
              END IF;
              --INS SS_MASTER_CDR
              SS_PORC_INS_MASTER(IN_TASK_NAME, IN_SERVICE_TYPE,IN_CDR_TYPE, V_FILE_NAME ,V_TABLE_NAME,V_SEQ);
                                           
         
       END LOOP;
       CLOSE O_TEMP_SQL;
         
                              
                            
     
    ELSE
      DBMS_OUTPUT.PUT_LINE('NO DATA FOUND: '|| V_COUNT);
    END IF;
    
EXCEPTION
  WHEN PROCESS_ERROR THEN
    V_ERR_DESC := SQLERRM;
    --WRITE_LOG(V_ERR_DESC);
    ROLLBACK;
    OUT_ERROR_CODE := 1;
  WHEN OTHERS THEN
    V_ERR_DESC := SQLERRM;
    --WRITE_LOG(V_ERR_DESC);
    ROLLBACK;
    OUT_ERROR_CODE := 1;
END SS_PRC_FILTER;

END SS_PKG_FILTER_PROCESS;
/
