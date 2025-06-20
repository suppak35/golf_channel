/*----------------------------------------------------------------------------------------------------------------------*/
/* CREATE DATE : 25/05/2015 <Samak T.>                                                                                  */
/* PROGRAM DESCRIPTION : Load Master CDR from SS_COPYM                                                                  */
/* VERSION: 1.0 - 25/05/2015   By Samak T.   - CREATE NEW PROGRAM                                                       */
/* VERSION: 1.2 - 02/12/2016   By Karn T.    - ADD CALLING PLUS SERVICE                                                 */
/* VERSION: 1.3 - 20/02/2017   By Samak T.   - Add Service Type CPAC_INVOICE                                            */
/* VERSION: 1.4 - 06/03/2018   By Nutsiri S. - Add Service Type M2M                                                     */
/* VERSION: 1.4a - 12/03/2018  By Satithch   - Add Service Type MOBIEL_CARE_WAIVE                                       */
/* VERSION: 1.4b - 04/04/2018  By Satithch   - Add Exception Error for FUNC GENERATE_FIELD_TXT                          */
/* VERSION: 1.5  - 23/05/2018  By Nutsiri S. - Support PJ. KARAOKE (KARAOKE_ACTIVATE)                                   */
/* VERSION: 1.6  - 21/08/2019  By Nutsiri S. - Support PJ. CopyD                                                        */
/* VERSION: 1.7  - 20/09/2018  By Pitchsiree - Add Service Type MOBILE_CARE_WAIVE_RBM                                   */
/* VERSION: 1.8  - 25/09/2019  By Nutsiri S. - Support Other Project just config                                        */
/*               - 21/10/2019  By Nutsiri S. - Add Function Check Duplicate for RBM                                     */
/* VERSION: 1.9  - 02/04/2020  By Satithch   - Modify Function Skip Record                                              */
/* VERSION: 2.0  - 13/07/2020  By Nutsiri    - Support PJ. Sim Myanmar					                */
/* VERSION: 2.1  - 17/12/2020  By Nutsiri    - Support PJ. NAFA Sumsunf TV					        */
/* VERSION: 2.2  - 23/02/2021  By Nutsiri    - Support PJ. Auto Reconcile usage for interrim                            */
/* VERSION: 2.3  - 28/04/2021  By Nutsiri    - Support PJ. DOWNLOAD_SONGID						*/
/* VERSION: 2.4  - 17/11/2021  By Pongsatorn - Support PJ. Apple Care								*/
/* VERSION: 2.5  - 17/07/2022  By Chanut - Support PJ. VRBT (VDO_CALLING)							*/
/* VERSION: 2.6  - 05/10/2022  By Chanut - Support PJ. backup data APPLE_MUSIC_OUR 					*/
/* VERSION: 2.7  - 07/08/2023 By Supakit - Support PJ. YOUTUBE_PARTNER 					*/
/* VERSION: 2.8  - 23/08/2023 By Chanut S. - Support APPLE CARE PARTNER (@,#,$,%,!) in column imei_number		*/
/* VERSION: 2.9  - 09/10/2023 By Supakit K. - Replace "^m" when read line data 					*/
/* VERSION: 3.0  - 23/11/2023 By Supakit K. - Support RSME_PARTNER get date from filename into COLUMN INVOICE_PERIOD	*/
/* VERSION: 3.1  - 23/11/2023 By Supakit K. - Support load RSME cancel split handset_obj into band,model,color		*/
/* VERSION: 3.2  - 20/05/2024 By Chanutso. - Support load get Month in file name		*/
/* VERSION: 3.3  - 02/07/2024 By Jaran R. - Support load payment add PERIOD_DATE (3bb cloud IP camera)		*/
/* VERSION: 3.4  - 12/07/2024 BY Jaran R. - Add check PAID_DATE = 'C' to set FULL_PAID_DATE */
/* VERSION: 3.5  - 24/04/2025 By Suppachai K. - Support load payment add PERIOD_DATE (3BB_VDO_PAYMENT)		*/

/*----------------------------------------------------------------------------------------------------------------------*/

@/users/oper/pwd/connect_sst.sql

SET SERVEROUTPUT ON SIZE 1000000
VAR RETCODE  NUMBER
ALTER SESSION SET NLS_DATE_LANGUAGE='AMERICAN';
EXEC :RETCODE := 2;

DECLARE
  P_SERVICE_TYPE SS_LOAD_CONFIG.SERVICE_TYPE%TYPE := '&1'; --# PARAMETER

  V_DATPATH    VARCHAR2(100) := '/users/oper/ssdata';
  V_DATNAME    VARCHAR2(100) := 'report_param.txt';
  P_AS_OF      VARCHAR2(6);
  P_REPORT_REF VARCHAR2(8); --#GET REPORT_REFERENCE FROM report_param.txt

  V_TABLE_NAME       VARCHAR2(255);
  V_FILE_NAME        VARCHAR2(255);
  V_FILE_PATH        VARCHAR2(255);
  V_CDR_FILE_NAME    VARCHAR2(255);
  V_SUB_GROUP        VARCHAR2(20);
  V_REPORT_REFERENCE VARCHAR2(20) := NULL;
  UTL_DATFILE        UTL_FILE.FILE_TYPE;
  V_STATUS           VARCHAR2(10); /*Modify V1.6*/
  V_SEPARATE         SS_LOAD_CONFIG.SEPARATE%TYPE;
  V_RECORD_TYPE      SS_LOAD_CONFIG.REMARK%TYPE;
  V_INVOICE_CO_ID    SS_CHK_FULL_PAID.INVOICING_CO_ID%TYPE;
  V_FOOTER_LINE      VARCHAR2(20); /*Add V1.8*/

  V_CDR_TYPE SS_RECONCILE_COPYD.CDR_TYPE%TYPE; /*Add v1.6*/

  V_PRIV_CP_ID VARCHAR2(20); /*V2.2*/

  SQL_INS_COL VARCHAR2(12000);
  SQL_INS_VAL VARCHAR2(12000);
  SQL_INSERT  VARCHAR2(12000);

  MAX_LINE_SIZE NUMBER := 30000;
  SZ_LINE_BUFF  VARCHAR2(30000);
  SZ_LINE_TMP   VARCHAR2(30000);

  CNT_REC_TOT  NUMBER;
  CNT_REC_INS  NUMBER;
  CNT_REC_ERR  NUMBER;
  CNT_REC_COM  NUMBER;
  CNT_REC_REJ  NUMBER;
  CNT_REC_SKIP NUMBER;

  /*V1.4 Add*/
  V_SEQ_EXCHANGE_RATE SS_EXCHANGE_RATE_M2M.SEQ_EXCHANGE_RATE%TYPE;
  V_EVENT_MONTHS      SS_EXCHANGE_RATE_M2M.EVENT_MONTHS%TYPE;
  V_EXCHANGE_RATE     SS_EXCHANGE_RATE_M2M.EXCHANGE_RATE%TYPE;
  V_RUNNING_SEQ       SS_EXCHANGE_RATE_M2M.RUNNING_SEQ%TYPE;

  /*V1.8 21/10/2019*/
  V_CHK_DUP_SUM       VARCHAR2(1);
  V_WAIVE_AMT         SS_MOBILE_CARE_WAIVE_RBM.WAIVE_AMT%TYPE;
  V_MOBILE_NO         SS_MOBILE_CARE_WAIVE_RBM.MOBILE_NO%TYPE;
  V_PACKAGE_NAME      SS_MOBILE_CARE_WAIVE_RBM.PACKAGE_NAME%TYPE;
  V_CANCEL_ORDER_DATE VARCHAR2(50);
  V_PRODUCT_SEQ       SS_MOBILE_CARE_WAIVE_RBM.PRODUCT_SEQ%TYPE;

  TYPE STRUCT_CONFIG IS RECORD(
    CDR_POSITION   SS_LOAD_CONFIG.CDR_POSITION%TYPE,
    COLUMN_NAME    SS_LOAD_CONFIG.COLUMN_NAME%TYPE,
    DATA_TYPE      SS_LOAD_CONFIG.DATA_TYPE%TYPE,
    DATA_LENGTH    SS_LOAD_CONFIG.DATA_LENGTH%TYPE,
    DATA_FORMAT    SS_LOAD_CONFIG.DATA_FORMAT%TYPE,
    MANDATORY_FLAG SS_LOAD_CONFIG.MANDATORY_FLAG%TYPE,
    CONVERT_FLAG   SS_LOAD_CONFIG.CONVERT_FLAG%TYPE,
    REMARK         SS_LOAD_CONFIG.REMARK%TYPE); /*Add 1.8*/
  TYPE STRUCT_LIST IS TABLE OF STRUCT_CONFIG INDEX BY VARCHAR2(7); --#CDR_POSITION
  LIST_CONFIG STRUCT_LIST;

  /*Add V1.8*/
  TYPE STRUCT_FORMAT IS RECORD(
    COLUMN_NAME SS_LOAD_CONFIG.COLUMN_NAME%TYPE,
    DATA_FORMAT SS_LOAD_CONFIG.DATA_FORMAT%TYPE);
  TYPE STRUCT_DATA IS TABLE OF STRUCT_FORMAT INDEX BY VARCHAR2(7);
  LIST_DATA STRUCT_DATA;

  /*Add V1.8*/
  TYPE STRUCT_HEADER IS RECORD(
    COLUMN_NAME SS_LOAD_CONFIG.COLUMN_NAME%TYPE,
    DATA_FORMAT SS_LOAD_CONFIG.DATA_FORMAT%TYPE);
  TYPE STR_HEADER IS TABLE OF STRUCT_HEADER INDEX BY VARCHAR2(7);
  LIST_HEADER STR_HEADER;
  
  V_CNT_FILE_APPLE_MUSIC_OUR NUMBER :=0;

  HD NUMBER := 0; /*Add V1.8*/

  CURSOR COPYM_CUR IS
    SELECT T.FILE_NAME,
           TO_CHAR(T.FILE_DTM, 'YYYYMMDD') FILE_DTM,
           T.PROCESS,
           SUBSTR(T.FILE_NAME, INSTR(T.FILE_NAME, '_', -1, 2) + 1, INSTR(T.FILE_NAME, '_', -1, 1) -
                   INSTR(T.FILE_NAME, '_', -1, 2) - 1) SUB_GROUP
      FROM SS_COPYM T
     WHERE T.PROCESS = P_SERVICE_TYPE
       AND T.STATUS = 'NEW'
     ORDER BY T.USER_DTM;
  COPYM_REC COPYM_CUR%ROWTYPE;

  ---------------------------------------------------------------------------------------
  /*Add V1.8*/
  PROCEDURE LOAD_HEADER IS
    LH NUMBER;
  BEGIN
    LH := 1;
    LIST_HEADER.DELETE;
    FOR H1 IN (SELECT C.*
                 FROM SS_LOAD_CONFIG C
                WHERE C.SERVICE_TYPE = P_SERVICE_TYPE
                  AND C.CDR_POSITION = -1
                  AND C.DATA_TYPE = 'CONFIG'
                  AND C.LOAD_USED = 'Y') LOOP
      LIST_HEADER(TO_CHAR(LH)).COLUMN_NAME := H1.COLUMN_NAME;
      LIST_HEADER(TO_CHAR(LH)).DATA_FORMAT := H1.DATA_FORMAT;
    
      LH := LH + 1;
    
    /*IF H1.DATA_FORMAT = 'SKIP_FIRST_LINE' AND HD < 1 THEN
                                                                                V_RECORD_TYPE := 'SKIP_FIRST_LINE';
                                                                                HD            := HD + 1;
                                                                              ELSIF H1.DATA_FORMAT = 'SKIP_FIRST_LINE' AND HD >= 1 THEN
                                                                                V_RECORD_TYPE := NULL;
                                                                              END IF;
                                                                            
                                                                              IF H1.DATA_FORMAT = 'SKIP_FOOTER_LINE' THEN
                                                                                V_FOOTER_LINE := H1.COLUMN_NAME;
                                                                              END IF;*/
    
    END LOOP;
  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END LOAD_HEADER;

  ---------------------------------------------------------------------------------------
  /*Add V1.8*/
  PROCEDURE LOAD_FORMAT IS
    LC NUMBER;
  BEGIN
    LC := 1;
    LIST_DATA.DELETE;
    FOR D1 IN (SELECT C.*
                 FROM SS_LOAD_CONFIG C
                WHERE C.SERVICE_TYPE = P_SERVICE_TYPE
                  AND C.CDR_POSITION = 0
                  AND C.DATA_TYPE = 'FIELD_NAME'
                  AND C.LOAD_USED = 'Y'
                ORDER BY C.COLUMN_NAME) LOOP
      LIST_DATA(TO_CHAR(LC)).COLUMN_NAME := D1.COLUMN_NAME;
      LIST_DATA(TO_CHAR(LC)).DATA_FORMAT := D1.DATA_FORMAT;
    
      LC := LC + 1;
    
    END LOOP;
  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END LOAD_FORMAT;

  ---------------------------------------------------------------------------------------
  PROCEDURE LOAD_CONFIG IS
    IDX NUMBER;
  BEGIN
    IDX := 1;
    LIST_CONFIG.DELETE;
    FOR CF IN (SELECT T.*
                 FROM SS_LOAD_CONFIG T
                WHERE T.SERVICE_TYPE = P_SERVICE_TYPE
                  AND T.CDR_POSITION > 0
                  AND T.LOAD_USED = 'Y'
                ORDER BY T.CDR_POSITION) LOOP
      LIST_CONFIG(TO_CHAR(IDX)).CDR_POSITION := CF.CDR_POSITION;
      LIST_CONFIG(TO_CHAR(IDX)).COLUMN_NAME := CF.COLUMN_NAME;
      LIST_CONFIG(TO_CHAR(IDX)).DATA_TYPE := CF.DATA_TYPE;
      LIST_CONFIG(TO_CHAR(IDX)).DATA_LENGTH := CF.DATA_LENGTH;
      LIST_CONFIG(TO_CHAR(IDX)).DATA_FORMAT := CF.DATA_FORMAT;
      LIST_CONFIG(TO_CHAR(IDX)).MANDATORY_FLAG := CF.MANDATORY_FLAG;
      LIST_CONFIG(TO_CHAR(IDX)).CONVERT_FLAG := CF.CONVERT_FLAG;
      LIST_CONFIG(TO_CHAR(IDX)).REMARK := CF.REMARK; /*V1.8*/
    
      IDX := IDX + 1;
    END LOOP;
  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END LOAD_CONFIG;

  ---------------------------------------------------------------------------------------
  PROCEDURE UPD_COPYM(IN_STATUS VARCHAR2) IS
  BEGIN
    UPDATE SS_COPYM T
       SET T.STATUS     = IN_STATUS,
           T.FILE_TOTAL = CNT_REC_TOT,
           T.FILE_READ  = CNT_REC_TOT,
           T.REC_COMP   = CNT_REC_COM,
           T.INS_COMP   = CNT_REC_INS,
           T.REC_REJ    = CNT_REC_REJ, /*Add V1.8 21/10/2019*/
           T.INS_REJ    = CNT_REC_REJ, /*Add V1.8 21/10/2019*/
           T.REC_ERR    = CNT_REC_ERR,
           T.USER_ID    = USER,
           T.USER_DTM   = SYSDATE
     WHERE T.FILE_NAME = V_CDR_FILE_NAME
       AND T.PROCESS = P_SERVICE_TYPE;
  
  EXCEPTION
    WHEN OTHERS THEN
      :RETCODE := -1;
      DBMS_OUTPUT.PUT_LINE('Error Update SS_COPYM : ' || SUBSTR(SQLERRM, 1, 150));
  END UPD_COPYM;

  ---------------------------------------------------------------------------------------
  PROCEDURE OPEN_FILE IS
  BEGIN
    V_FILE_NAME     := NULL;
    V_FILE_PATH     := NULL;
    V_CDR_FILE_NAME := NULL;
    V_SUB_GROUP     := NULL;
  
    V_FILE_NAME := SUBSTR(COPYM_REC.FILE_NAME, INSTR(COPYM_REC.FILE_NAME, '/', -1) + 1, LENGTH(COPYM_REC.FILE_NAME) -
                           INSTR(COPYM_REC.FILE_NAME, '/', -1));
    V_FILE_PATH := SUBSTR(COPYM_REC.FILE_NAME, 1, INSTR(COPYM_REC.FILE_NAME, '/', -1));
    BEGIN
      :RETCODE         := 0;
      UTL_DATFILE     := UTL_FILE.FOPEN(V_FILE_PATH, V_FILE_NAME, 'R', MAX_LINE_SIZE);
      V_CDR_FILE_NAME := V_FILE_PATH || V_FILE_NAME;
    
    EXCEPTION
      WHEN UTL_FILE.INVALID_PATH THEN
        DBMS_OUTPUT.PUT_LINE(COPYM_REC.FILE_NAME);
        DBMS_OUTPUT.PUT_LINE('*** ERROR: FILE LOCATION OR NAME WAS INVALID.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_MODE THEN
        DBMS_OUTPUT.PUT_LINE(COPYM_REC.FILE_NAME);
        DBMS_OUTPUT.PUT_LINE('*** ERROR: THE OPEN_MODE STRING WAS INVALID.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_OPERATION THEN
        DBMS_OUTPUT.PUT_LINE(COPYM_REC.FILE_NAME);
        DBMS_OUTPUT.PUT_LINE('*** ERROR: FILE COULD NOT BE OPENED AS REQUESTED.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_MAXLINESIZE THEN
        DBMS_OUTPUT.PUT_LINE(COPYM_REC.FILE_NAME);
        DBMS_OUTPUT.PUT_LINE('*** ERROR: SPECIFIED MAX_LINESIZE IS TOO LARGE OR TOO SMALL.');
        :RETCODE := 2;
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(COPYM_REC.FILE_NAME);
        DBMS_OUTPUT.PUT_LINE('*** ERROR: CANNOT OPEN FILE.');
        :RETCODE := 2;
    END;
  END OPEN_FILE;

  ---------------------------------------------------------------------------------------
  FUNCTION CHECK_LOADED RETURN VARCHAR2 IS
    V_RESULT VARCHAR2(1) := 'N';
  BEGIN
    BEGIN
      EXECUTE IMMEDIATE 'SELECT ''Y'' FROM DUAL WHERE EXISTS (SELECT 1 FROM ' || V_TABLE_NAME ||
                        ' T WHERE T.FILE_NAME = ''' || V_CDR_FILE_NAME || ''')'
        INTO V_RESULT;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_RESULT := 'N';
    END;
    RETURN V_RESULT;
  END CHECK_LOADED;

  ---------------------------------------------------------------------------------------
  FUNCTION GET_TABLE_NAME RETURN VARCHAR2 IS
    V_RESULT VARCHAR2(100) := NULL;
  BEGIN
    BEGIN
      SELECT T.COLUMN_NAME, T.SEPARATE, T.REMARK
        INTO V_RESULT, V_SEPARATE, V_RECORD_TYPE
        FROM SS_LOAD_CONFIG T
       WHERE T.SERVICE_TYPE = P_SERVICE_TYPE
         AND T.CDR_POSITION = 0
         AND (T.DATA_TYPE IS NULL OR T.DATA_TYPE = 'TABLE_NAME'); /*Add 1.8*/
      --AND T.DATA_TYPE = 'TABLE_NAME'; /*Add 1.8*/
    
    EXCEPTION
      WHEN OTHERS THEN
        V_RESULT := NULL;
    END;
  
    RETURN V_RESULT;
  END GET_TABLE_NAME;

  ---------------------------------------------------------------------------------------
  FUNCTION GET_ATTR(IN_TEXT VARCHAR2, IN_DELIM VARCHAR2, IN_POS NUMBER) RETURN VARCHAR2 IS
    V_TEXT   VARCHAR2(10000) := IN_TEXT || IN_DELIM;
    V_RESULT VARCHAR2(10000);
  BEGIN
    V_RESULT := '';
    IF NVL(IN_TEXT, 'X') <> 'X' AND NVL(IN_DELIM, 'X') <> 'X' AND NVL(IN_POS, 0) <> 0 THEN
      IF INSTR(IN_TEXT, IN_DELIM) > 0 THEN
        IF IN_POS = 1 THEN
          V_RESULT := TRIM(SUBSTR(V_TEXT, 1, INSTR(IN_TEXT, IN_DELIM, 1, IN_POS) - 1));
        ELSE
          V_RESULT := TRIM(SUBSTR(V_TEXT, INSTR(V_TEXT, IN_DELIM, 1, IN_POS - 1) + 1, INSTR(V_TEXT, IN_DELIM, 1, IN_POS) -
                                   (INSTR(V_TEXT, IN_DELIM, 1, IN_POS - 1) + 1)));
        END IF;
      ELSE
        V_RESULT := IN_TEXT;
      END IF;
    END IF;
    RETURN V_RESULT;
  END GET_ATTR;

  ---------------------------------------------------------------------------------------
  FUNCTION IS_DIGIT(I_STR VARCHAR2) RETURN NUMBER IS
  
    V_RESULT NUMBER(1) := 0;
    --> 0 : TRUE
    --> 1 : FAIL
    I NUMBER := 0;
  
  BEGIN
    IF NVL(I_STR, 'X') <> 'X' THEN
      FOR I IN 1 .. LENGTH(I_STR) LOOP
        IF SUBSTR(I_STR, I, 1) >= CHR(48) AND SUBSTR(I_STR, I, 1) <= CHR(57) THEN
          V_RESULT := 0;
        ELSE
          IF ((SUBSTR(I_STR, I, 1) = '-' OR SUBSTR(I_STR, I, 1) = '+') AND I = 1) OR
             (SUBSTR(I_STR, I, 1) = ',' AND I > 1) OR (SUBSTR(I_STR, I, 1) = '.') THEN
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

  ---------------------------------------------------------------------------------------
  FUNCTION CHECK_DATE(IVAL VARCHAR2, IFORMAT VARCHAR2) RETURN NUMBER IS
    RETURN_CODE NUMBER := 0;
    VDTM        DATE;
  BEGIN
    BEGIN
      SELECT TO_DATE(IVAL, IFORMAT) INTO VDTM FROM DUAL;
    EXCEPTION
      WHEN OTHERS THEN
        RETURN_CODE := -1;
    END;
    RETURN RETURN_CODE;
  END CHECK_DATE;

  ---------------------------------------------------------------------------------------
  FUNCTION VALIDATE_FIELD(V_MANDATORY_FLAG SS_BOS_CONFIG_MASTER.MANDATORY_FLAG%TYPE,
                          V_DATA_LENGTH    SS_BOS_CONFIG_MASTER.DATA_LENGTH%TYPE,
                          V_DATA_TYPE      SS_BOS_CONFIG_MASTER.DATA_TYPE%TYPE,
                          V_DATA_FORMAT    SS_BOS_CONFIG_MASTER.DATA_FORMAT%TYPE,
                          I_DATA           VARCHAR2) RETURN NUMBER IS
  
    V_RET_CODE NUMBER(3) := 0;
  
    --# Step check : NULL -->> LENGTH -->> TYPE
    --> 201 : eror LENGTH (Check case LOAD_USED='N', FILTER_USED='Y')
    --> 202 : eror TYPE
    --> 203 : error NULL
    --> 0   : not found error
  
  BEGIN
  
    --1) CHK NULL
    IF V_MANDATORY_FLAG = 'Y' THEN
      IF NVL(I_DATA, 'X') = 'X' THEN
        V_RET_CODE := 203;
      END IF;
    END IF;
  
    --2) CHK LENGTH
    IF V_RET_CODE = 0 THEN
      IF NVL(V_DATA_LENGTH, 0) > 0 THEN
        IF LENGTH(I_DATA) > V_DATA_LENGTH THEN
          V_RET_CODE := 201;
        END IF;
      END IF;
    END IF;
  
    --3) CHK TYPE
    IF V_RET_CODE = 0 THEN
      IF INSTR(UPPER(V_DATA_TYPE), 'NUMBER') > 0 THEN
        IF IS_DIGIT(I_DATA) <> 0 THEN
          V_RET_CODE := 202;
        END IF;
      
      ELSIF INSTR(UPPER(V_DATA_TYPE), 'DATE') > 0 THEN
        --IF SS_FNC_CHK_DATE(I_DATA, V_DATA_FORMAT) <> 0 THEN
        IF CHECK_DATE(I_DATA, V_DATA_FORMAT) <> 0 THEN
          V_RET_CODE := 202;
        END IF;
      END IF;
    END IF;
  
    RETURN V_RET_CODE;
  END VALIDATE_FIELD;

  ---------------------------------------------------------------------------------------
  /*V1.4a Add*/
  FUNCTION GET_DATA_WITH_FORMAT(I_DATA        IN VARCHAR2,
                                I_DATA_FORMAT IN SS_BOS_CONFIG_MASTER.DATA_FORMAT%TYPE)
    RETURN VARCHAR2 IS
    L_DATA   VARCHAR2(200);
    L_LENGTH NUMBER;
  BEGIN
  
    L_LENGTH := LENGTH(I_DATA);
    IF UPPER(I_DATA_FORMAT) = 'MOBILE_NO:660-0000-0000' THEN
      IF L_LENGTH = 9 THEN
        --#EX. 8-9123-45678
        L_DATA := '66' || SUBSTR(I_DATA, 1, 1) || SUBSTR(I_DATA, 2);
      
      ELSIF L_LENGTH = 10 THEN
        --#EX. 08-9123-45678
        L_DATA := '66' || SUBSTR(I_DATA, 2, 1) || SUBSTR(I_DATA, 3);
      
      ELSIF L_LENGTH = 11 THEN
        --#EX. 668-9123-45678
        L_DATA := I_DATA;
      
      ELSE
        L_DATA := I_DATA;
      END IF;
    
    ELSE
      NULL;
    END IF;
    RETURN L_DATA;
  
  EXCEPTION
    WHEN OTHERS THEN
      RETURN I_DATA;
  END;

  ---------------------------------------------------------------------------------------
  FUNCTION GENERATE_FIELD_TXT(V_DATA_TYPE    SS_BOS_CONFIG_MASTER.DATA_TYPE%TYPE,
                              V_DATA_FORMAT  SS_BOS_CONFIG_MASTER.DATA_FORMAT%TYPE,
                              V_CONVERT_FLAG SS_LOAD_CONFIG.CONVERT_FLAG%TYPE,
                              I_DATA         VARCHAR2) RETURN VARCHAR2 IS
    V_FORMAT  VARCHAR2(200);
    V_RET_TXT VARCHAR2(1000);
  BEGIN
  
    IF INSTR(UPPER(V_DATA_TYPE), 'VARCHAR2') > 0 THEN
      IF V_CONVERT_FLAG = 'Y' THEN
        SELECT CONVERT(I_DATA, 'TH8TISASCII', 'UTF8') INTO V_RET_TXT FROM DUAL;
      ELSE
        V_RET_TXT := I_DATA;
      END IF;
    
      IF NVL(V_DATA_FORMAT, 'X') = 'X' THEN
        V_RET_TXT := '''' || V_RET_TXT || '''';
      ELSE
        V_RET_TXT := '''' || GET_DATA_WITH_FORMAT(V_RET_TXT, V_DATA_FORMAT) || '''';
      END IF;
    
    ELSIF INSTR(UPPER(V_DATA_TYPE), 'NUMBER') > 0 THEN
      V_RET_TXT := NVL(I_DATA, 'NULL');
      V_FORMAT  := NVL(V_DATA_FORMAT, 'NULL');
      IF V_RET_TXT <> 'NULL' THEN
        IF V_DATA_FORMAT <> 'NULL' THEN
          V_RET_TXT := TO_CHAR(TO_NUMBER(V_RET_TXT) / POWER(10, TO_NUMBER(V_DATA_FORMAT)));
        ELSE
          V_RET_TXT := TO_CHAR(TO_NUMBER(V_RET_TXT));
        END IF;
      END IF;
    
    ELSIF INSTR(UPPER(V_DATA_TYPE), 'DATE') > 0 THEN
      V_RET_TXT := 'TO_DATE(''' || I_DATA || ''',''' || V_DATA_FORMAT || ''')';
    
    ELSE
      V_RET_TXT := '''' || I_DATA || '''';
    END IF;
  
    RETURN V_RET_TXT;
  
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('ERROR ON FUNC GENERATE_FIELD_TXT: ' || SUBSTR(I_DATA, 1, 60));
      RETURN I_DATA;
  END GENERATE_FIELD_TXT;

  ---------------------------------------------------------------------------------------
  PROCEDURE WRITE_ERROR(I_ERRCODE   VARCHAR2,
                        I_ROWNUM    NUMBER,
                        I_FIELDNAME VARCHAR2,
                        I_FIELDDATA VARCHAR2,
                        I_ORACODE   VARCHAR2) IS
    V_ORA_TEXT VARCHAR2(50);
    LINE_TXT   VARCHAR2(200);
  BEGIN
    V_ORA_TEXT := '';
    IF NVL(I_ORACODE, 'X') <> 'X' THEN
      V_ORA_TEXT := '[ORA-' || I_ORACODE || ']';
    END IF;
  
    LINE_TXT := SUBSTR(I_ERRCODE || '[ROW-' || TO_CHAR(I_ROWNUM) || '][' || I_FIELDNAME || '-' ||
                       I_FIELDDATA || ']' || V_ORA_TEXT, 1, 200);
    DBMS_OUTPUT.PUT_LINE(LINE_TXT);
  
  END WRITE_ERROR;

  ---------------------------------------------------------------------------------------
  PROCEDURE GET_PARAM IS
  BEGIN
    :RETCODE      := 0;
    P_REPORT_REF := NULL;
    UTL_DATFILE  := UTL_FILE.FOPEN(V_DATPATH, V_DATNAME, 'R');
  
    BEGIN
      UTL_FILE.GET_LINE(UTL_DATFILE, P_AS_OF);
      P_REPORT_REF := TO_CHAR(LAST_DAY(TO_DATE(P_AS_OF, 'YYYYMM')), 'YYYYMMDD');
    EXCEPTION
      WHEN UTL_FILE.INVALID_PATH THEN
        RAISE_APPLICATION_ERROR(-20001, '*** ERROR: FILE LOCATION OR NAME WAS INVALID.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_MODE THEN
        RAISE_APPLICATION_ERROR(-20001, '*** ERROR: THE OPEN_MODE STRING WAS INVALID.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_OPERATION THEN
        RAISE_APPLICATION_ERROR(-20001, '*** ERROR: FILE COULD NOT BE OPENED AS REQUESTED.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_MAXLINESIZE THEN
        RAISE_APPLICATION_ERROR(-20001, '*** ERROR: SPECIFIED MAX_LINESIZE IS TOO LARGE OR TOO SMALL.');
        :RETCODE := 2;
      WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, '*** ERROR: CANNOT OPEN FILE.');
        :RETCODE := 2;
    END;
  END GET_PARAM;

  ---------------------------------------------------------------------------------------
  /*Add 1.8 21/10/2019*/
  PROCEDURE CHECK_DUP_RBM(IN_SZ_LINE_BUFF IN VARCHAR2) IS
  BEGIN
    V_CHK_DUP_SUM       := NULL;
    V_MOBILE_NO         := NULL;
    V_PACKAGE_NAME      := NULL;
    V_CANCEL_ORDER_DATE := NULL;
    V_PRODUCT_SEQ       := NULL;
    V_WAIVE_AMT         := NULL;
  
    BEGIN
    
      V_MOBILE_NO := GET_DATA_WITH_FORMAT(TRIM(SUBSTR(IN_SZ_LINE_BUFF, INSTR(IN_SZ_LINE_BUFF, '|', 1, 1) + 1, (INSTR(IN_SZ_LINE_BUFF, '|', 1, 2)) -
                                                       (INSTR(IN_SZ_LINE_BUFF, '|', 1) + 1))), 'MOBILE_NO:660-0000-0000');
    
      V_PACKAGE_NAME := TRIM(SUBSTR(IN_SZ_LINE_BUFF, INSTR(IN_SZ_LINE_BUFF, '|', 1, 2) + 1, (INSTR(IN_SZ_LINE_BUFF, '|', 1, 3)) -
                                     (INSTR(IN_SZ_LINE_BUFF, '|', 1, 2) + 1)));
    
      V_CANCEL_ORDER_DATE := TRIM(SUBSTR(IN_SZ_LINE_BUFF, INSTR(IN_SZ_LINE_BUFF, '|', 1, 4) + 1, (INSTR(IN_SZ_LINE_BUFF, '|', 1, 5)) -
                                          (INSTR(IN_SZ_LINE_BUFF, '|', 1, 4) + 1)));
    
      V_PRODUCT_SEQ := TRIM(SUBSTR(IN_SZ_LINE_BUFF, INSTR(IN_SZ_LINE_BUFF, '|', 1, 8) + 1, (INSTR(IN_SZ_LINE_BUFF, '|', 1, 9)) -
                                    (INSTR(IN_SZ_LINE_BUFF, '|', 1, 8) + 1)));
    
      V_WAIVE_AMT := TRIM(SUBSTR(IN_SZ_LINE_BUFF, INSTR(IN_SZ_LINE_BUFF, '|', 1, 13) + 1, (INSTR(IN_SZ_LINE_BUFF, '|', 1, 14)) -
                                  (INSTR(IN_SZ_LINE_BUFF, '|', 1, 13) + 1)));
    
      SELECT 'Y'
        INTO V_CHK_DUP_SUM
        FROM SS_MOBILE_CARE_WAIVE_RBM G
       WHERE G.WAIVE_AMT = V_WAIVE_AMT
         AND G.MOBILE_NO = V_MOBILE_NO
         AND G.PACKAGE_NAME = V_PACKAGE_NAME
         AND G.CANCEL_ORDER_DATE = TO_DATE(V_CANCEL_ORDER_DATE, 'DD/MM/YYYY HH24:MI:SS')
         AND G.PRODUCT_SEQ = V_PRODUCT_SEQ
         AND ROWNUM <= 1;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_CHK_DUP_SUM := 'N';
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR IN CHECK DUP MOBILE_CARE_WAIVE_RBM IN REPORT_REF: ' ||
                             P_REPORT_REF);
        RETURN;
    END;
  END CHECK_DUP_RBM;
  ---------------------------------------------------------------------------------------
  PROCEDURE INSERT_DATA IS
  BEGIN
    SQL_INSERT := NULL;
    SQL_INSERT := 'INSERT INTO ' || V_TABLE_NAME || '(' ||
                  SUBSTR(SQL_INS_COL, 1, LENGTH(SQL_INS_COL) - 1) || ')VALUES(' ||
                  SUBSTR(SQL_INS_VAL, 1, LENGTH(SQL_INS_VAL) - 1) || ')';
  
    --##DBMS_OUTPUT.PUT_LINE(SQL_INS_COL);    
    --##DBMS_OUTPUT.PUT_LINE(SUBSTR(SQL_INS_VAL, 1, 255));
  
    BEGIN
      EXECUTE IMMEDIATE SQL_INSERT;
      CNT_REC_INS := CNT_REC_INS + 1;
      CNT_REC_COM := CNT_REC_COM + 1;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('[Line - ' || TO_CHAR(CNT_REC_TOT) || '] Error Insert : ' ||
                             SUBSTR(SQLERRM, 1, 150));
        CNT_REC_ERR := CNT_REC_ERR + 1;
    END;
  
  END INSERT_DATA;

  --#
  /*V1.9 Add*/
  FUNCTION CHK_SKIP_RECORD(IN_VALUE IN VARCHAR2) RETURN BOOLEAN IS
    T_RET BOOLEAN;
  BEGIN
    T_RET := FALSE;
    FOR I IN 1 .. LIST_HEADER.COUNT LOOP
    
      IF LIST_HEADER(I).COLUMN_NAME = SUBSTR(IN_VALUE, 1, LENGTH(LIST_HEADER(I).COLUMN_NAME)) THEN
        IF LIST_HEADER(I).DATA_FORMAT LIKE 'SKIP%' THEN
          T_RET        := TRUE;
          CNT_REC_SKIP := CNT_REC_SKIP + 1;
          DBMS_OUTPUT.PUT_LINE('++' || LIST_HEADER(I).DATA_FORMAT || '..[' || IN_VALUE || ']');
          GOTO EXIT_FUNC;
        END IF;
      END IF;
    
    END LOOP;
  
    <<EXIT_FUNC>>
    RETURN T_RET;
  END;

  ---------------------------------------------------------------------------------------
  PROCEDURE LOAD_DATA IS
    FLAG_INS     VARCHAR2(1);
    I_ERROR_CODE NUMBER := 0;
    V_DATA_FIELD VARCHAR2(1000);
	V_PAID_STATUS_C VARCHAR2(1) := 'N';
  BEGIN
    --## Load Structure Line CDR
    LOAD_CONFIG();
    LOAD_FORMAT(); /*Add V1.8*/
    LOAD_HEADER(); /*V1.9 Add*/
  
    BEGIN
      LOOP
      
        --#LOAD_HEADER(); /*Add V1.8*/
        SZ_LINE_BUFF := NULL;
        --## Read LINE
        UTL_FILE.GET_LINE(UTL_DATFILE, SZ_LINE_TMP, MAX_LINE_SIZE);
      
        --## Replace Quate
        	--SZ_LINE_BUFF := REPLACE(SZ_LINE_TMP, '''', '''''');
	SZ_LINE_BUFF := REPLACE(REPLACE(SZ_LINE_TMP, '''', ''''''),CHR(13), ' '); /* V2.9 */
	/*
	--## RESOLVE ^M ON RAW FILE YOUTUBE PARTNER
        IF P_SERVICE_TYPE = 'YOUTUBE_PARTNER' THEN
          SZ_LINE_BUFF := REPLACE(REPLACE(SZ_LINE_TMP, '''', ''''''),CHR(13), ' ');
        ELSE
          SZ_LINE_BUFF := REPLACE(SZ_LINE_TMP, '''', '''''');
        END IF;
		*/
      
        --## REMARK = '02' -> SKIP HEADER        
        /*V1.9 Add*/
        IF CHK_SKIP_RECORD(SZ_LINE_BUFF) = TRUE THEN
          GOTO NEXT_RECORD;
        END IF;
      
        IF V_RECORD_TYPE IS NULL OR --## Record_type is not exists
           INSTR(V_RECORD_TYPE, GET_ATTR(SZ_LINE_BUFF, V_SEPARATE, 1)) > 0 --## Record_type is exists 
         THEN
        
          --# SERVICE_NAME -> Name of Service
          --# SERVICE_SUB_GROUP -> Group of PORTION
        
          FLAG_INS    := 'Y';
          CNT_REC_TOT := CNT_REC_TOT + 1;
        
          -------## Generate Script Insert Data [Start]
          --# (A) CPAC_INVOICE
          IF P_SERVICE_TYPE = 'CPAC_INVOICE' THEN
            IF SUBSTR(TRIM(GET_ATTR(SZ_LINE_BUFF, V_SEPARATE, 2)), 1, 1) = 'A' THEN
              V_INVOICE_CO_ID := 'AIS';
            ELSIF SUBSTR(TRIM(GET_ATTR(SZ_LINE_BUFF, V_SEPARATE, 2)), 1, 1) = 'D' THEN
              V_INVOICE_CO_ID := 'DPC';
            ELSIF SUBSTR(TRIM(GET_ATTR(SZ_LINE_BUFF, V_SEPARATE, 2)), 1, 1) = 'W' THEN
              V_INVOICE_CO_ID := 'AWN';
            ELSE
              V_INVOICE_CO_ID := 'NULL';
            END IF;
          
            SQL_INS_COL := 'USER_ID,USER_DTM, PERIOD_DATE, FILE_NAME, INVOICING_CO_ID, SHARE_BASIS, ';
            SQL_INS_VAL := 'USER,SYSDATE,TO_DATE(''' || P_REPORT_REF || ''',''YYYYMMDD''),''' ||
                           COPYM_REC.FILE_NAME || ''',''' || V_INVOICE_CO_ID || ''',''' ||
                           UPPER(SUBSTR(COPYM_REC.FILE_NAME, INSTR(COPYM_REC.FILE_NAME, '/', -1) + 15, 3)) ||
                           ''',';
          
            --# (B) PRIVILEGE    AXA                          
          ELSIF P_SERVICE_TYPE = 'IOT_AXA' OR P_SERVICE_TYPE = 'IOT_RESULT' THEN
            SQL_INS_COL := 'SERVICE_NAME,FILE_DTM,FILE_NAME, USER_ID,USER_DTM,';
            SQL_INS_VAL := '''' || P_SERVICE_TYPE || ''',TO_DATE(''' || COPYM_REC.FILE_DTM ||
                           ''',''YYYYMMDD''),''' || COPYM_REC.FILE_NAME || ''',USER,SYSDATE,';
          
            --# (C) Service M2M /*V1.4 Add*/
          ELSIF P_SERVICE_TYPE = 'M2M' THEN
          
            SELECT NVL(MAX(SEQ_EXCHANGE_RATE), 0) + 1
              INTO V_SEQ_EXCHANGE_RATE
              FROM SS_EXCHANGE_RATE_M2M;
          
            V_EVENT_MONTHS  := SUBSTR(COPYM_REC.FILE_NAME, INSTR(COPYM_REC.FILE_NAME, '_', -1, 2) + 1, 6);
            V_RUNNING_SEQ   := SUBSTR(COPYM_REC.FILE_NAME, INSTR(COPYM_REC.FILE_NAME, '_', -1, 1) + 1, 2);
            V_EXCHANGE_RATE := SZ_LINE_TMP;
          
            SQL_INS_COL := 'SEQ_EXCHANGE_RATE,EVENT_MONTHS,EXCHANGE_RATE,FILE_NAME, USER_ID,USER_DTM,RUNNING_SEQ,';
            SQL_INS_VAL := '''' || V_SEQ_EXCHANGE_RATE || ''',''' || V_EVENT_MONTHS || ''',''' ||
                           V_EXCHANGE_RATE || ''',''' || COPYM_REC.FILE_NAME || ''',USER,SYSDATE,' ||
                           V_RUNNING_SEQ || ',';
          
            --# (D) PORTION (KARAOKE, VDO, CALLING_MAX, CALLING_PLUS)   
          ELSIF P_SERVICE_TYPE IN ('KARAOKE', 'VDO', 'CALLING_MAX', 'CALLING_PLUS','VDO_CALLING') THEN
            SQL_INS_COL := 'SERVICE_NAME,SERVICE_SUB_GROUP,FILE_DTM,FILE_NAME, USER_ID,USER_DTM,REPORT_REFERENCE,';
            IF V_REPORT_REFERENCE IS NULL THEN
              V_REPORT_REFERENCE := 'NULL';
            END IF;
          
            SQL_INS_VAL := '''' || P_SERVICE_TYPE || ''',''' || COPYM_REC.SUB_GROUP ||
                           ''',TO_DATE(''' || COPYM_REC.FILE_DTM || ''',''YYYYMMDD''),''' ||
                           COPYM_REC.FILE_NAME || ''',USER,SYSDATE,' || V_REPORT_REFERENCE || ',';
             --# (D) PORTION (VDO_CALLING_PLUS,VDO_CALLING_MAO_MAO)                  
          ELSIF P_SERVICE_TYPE IN ('VDO_CALLING_PLUS','VDO_CALLING_MAO_MAO') THEN
            SQL_INS_COL := 'SERVICE_NAME,SERVICE_SUB_GROUP,FILE_DTM,FILE_NAME, USER_ID,USER_DTM,REPORT_REFERENCE,';
            IF V_REPORT_REFERENCE IS NULL THEN
              V_REPORT_REFERENCE := 'NULL';
            END IF;
          
            SQL_INS_VAL := '''' || P_SERVICE_TYPE || ''',''' || COPYM_REC.SUB_GROUP ||
                           ''',TO_DATE(''' || COPYM_REC.FILE_DTM || ''',''YYYYMMDD''),''' ||
                           COPYM_REC.FILE_NAME || ''',USER,SYSDATE,' || V_REPORT_REFERENCE || ',';
            --# (E) MOBILE_CARE WAIVE /*V1.4a Add*/
          ELSIF P_SERVICE_TYPE = 'MOBILE_CARE_WAIVE' THEN
            SQL_INS_COL := 'FILE_NAME,LINE_NO,STATUS,USER_ID,USER_DTM,REFERENCE_CDR_ID,REPORT_REFERENCE,';
            SQL_INS_VAL := '''' || COPYM_REC.FILE_NAME || ''',''' || CNT_REC_TOT || ''',''' ||
                           'NEW' || ''',USER,SYSDATE,NULL,' || P_REPORT_REF || ',';
            NULL;
          
            --# (E) MOBILE_CARE_WAIVE_RBM /*V1.7*/
          ELSIF P_SERVICE_TYPE = 'MOBILE_CARE_WAIVE_RBM' THEN
            /*Modify 1.8 21/10/2019*/
            CHECK_DUP_RBM(SZ_LINE_BUFF);
            IF V_CHK_DUP_SUM = 'N' THEN
              SQL_INS_COL := 'FILE_NAME,STATUS,USER_ID,USER_DTM,REFERENCE_CDR_ID,REPORT_REFERENCE,';
              SQL_INS_VAL := '''' || COPYM_REC.FILE_NAME || ''',''' || 'NEW' ||
                             ''',USER,SYSDATE,NULL,' || P_REPORT_REF || ',';
              NULL;
            ELSE
              CNT_REC_REJ := CNT_REC_REJ + 1;
              GOTO NEXT_RECORD;
            END IF;
          
            /*Add V1.6*/
          ELSIF P_SERVICE_TYPE LIKE 'COPYD%' THEN
            V_CDR_TYPE  := SUBSTR(P_SERVICE_TYPE, INSTR(P_SERVICE_TYPE, '_', 1) + 1);
            SQL_INS_COL := 'CDR_TYPE,SUMMARY_FILENAME,USER_ID,USER_DTM,';
            SQL_INS_VAL := '''' || V_CDR_TYPE || ''',''' || COPYM_REC.FILE_NAME ||
                           ''',USER,SYSDATE,';
          
            IF SZ_LINE_BUFF LIKE 'TOTAL%' THEN
              CNT_REC_TOT := CNT_REC_TOT - 1;
              GOTO NEXT_RECORD;
            END IF;

	   /*Add V2.1*/
	   ELSIF P_SERVICE_TYPE = 'NAFA_PACKAGE_NONAIS' THEN
          
		    IF (SUBSTR(SZ_LINE_BUFF,
			       INSTR(SZ_LINE_BUFF, '|', -1, 4) + 1,
			       INSTR(SUBSTR(SZ_LINE_BUFF,
					    INSTR(SZ_LINE_BUFF, '|', -1, 4) + 1),
				     '|',
				     -1,
				     3) - 1) =
		       'INS_NF NG PLAY PREMIUM Free Trial 3M_SS') AND
		       (SUBSTR(SZ_LINE_BUFF,
			       INSTR(SZ_LINE_BUFF, '|', -1, 10) + 1,
			       INSTR(SUBSTR(SZ_LINE_BUFF,
					    INSTR(SZ_LINE_BUFF, '|', -1, 10) + 1),
				     '|',
				     -1,
				     9) - 1) <> 1 ) THEN
		      SQL_INS_COL := NULL;
		      SQL_INS_VAL := NULL;
		    
		      IF SZ_LINE_BUFF LIKE NVL(V_FOOTER_LINE, 'FOOTER_LINE') || '%' THEN
			CNT_REC_TOT := CNT_REC_TOT - 1;
			GOTO NEXT_RECORD;
		      END IF;
		    
		      FOR I IN 1 .. LIST_DATA.COUNT LOOP
			SQL_INS_COL := SQL_INS_COL || LIST_DATA(I).COLUMN_NAME || ',';
			SQL_INS_VAL := SQL_INS_VAL || '''' || LIST_DATA(I)
				      .DATA_FORMAT || '''' || ',';
		      END LOOP; --# Config FORMAT
		    
		      SQL_INS_VAL := REPLACE(SQL_INS_VAL,
					     '{%FILE_NAME%}',
					     COPYM_REC.FILE_NAME);
		      SQL_INS_VAL := REPLACE(SQL_INS_VAL,
					     '{%LINE_NO%}',
					     CNT_REC_TOT);
		      SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%USER_ID%}', USER);
		      SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%USER_DTM%}', SYSDATE);
		      SQL_INS_VAL := REPLACE(SQL_INS_VAL,
					     '{%REPORT_REFERENCE%}',
					     P_REPORT_REF);
		      SQL_INS_VAL := REPLACE(SQL_INS_VAL,
					     '{%SERVICE_TYPE%}',
					     P_SERVICE_TYPE);

	 	ELSE
		      CNT_REC_REJ := CNT_REC_REJ + 1;
		      GOTO NEXT_RECORD;
		    END IF;
          
          ELSE
            /*Add V1.5*/
            /*SQL_INS_COL := 'FILE_NAME,USER_ID,USER_DTM,REPORT_REFERENCE,SERVICE_TYPE,';
            SQL_INS_VAL := '''' || COPYM_REC.FILE_NAME || ''',USER,SYSDATE,' || P_REPORT_REF || ',''' || P_SERVICE_TYPE || ''',';*/
          
            /*Add V1.8*/
            SQL_INS_COL := NULL;
            SQL_INS_VAL := NULL;

	    /*Add V2.2*/
	    IF P_SERVICE_TYPE = 'PRIV_RECONCILE' THEN
              V_PRIV_CP_ID := NULL;
              V_PRIV_CP_ID := SUBSTR(COPYM_REC.FILE_NAME,
                                    INSTR(COPYM_REC.FILE_NAME, '_', 1, 4) + 1,
                                    5);
                                                  
              SQL_INS_COL := 'CP_ID,';
              SQL_INS_VAL := '''' || V_PRIV_CP_ID || ''',';
	    ELSIF P_SERVICE_TYPE = 'DOWNLOAD_SONGID' THEN  /*Add V2.3*/
              IF (SUBSTR(SZ_LINE_BUFF,
                         INSTR(SZ_LINE_BUFF, '|', 1, 4) + 1,
                         (INSTR(SZ_LINE_BUFF, '|', 1, 5) -
                         INSTR(SZ_LINE_BUFF, '|', 1, 4)) - 1) <> 3) THEN
                         DBMS_OUTPUT.PUT_LINE('*** EVENT_TYPE <> 3');
                CNT_REC_ERR := CNT_REC_ERR + 1;
                GOTO NEXT_RECORD;
              END IF;
	     ELSIF P_SERVICE_TYPE = 'APPLE_CARE_PARTNER' THEN
              sz_line_buff := replace(sz_line_buff,'"','');
            END IF;
          
            IF SZ_LINE_BUFF LIKE NVL(V_FOOTER_LINE, 'FOOTER_LINE') || '%' THEN
              CNT_REC_TOT := CNT_REC_TOT - 1;
              GOTO NEXT_RECORD;
            END IF;
          
            FOR I IN 1 .. LIST_DATA.COUNT LOOP
              SQL_INS_COL := SQL_INS_COL || LIST_DATA(I).COLUMN_NAME || ',';
              SQL_INS_VAL := SQL_INS_VAL || '''' || LIST_DATA(I).DATA_FORMAT || '''' || ',';
            END LOOP; --# Config FORMAT
          
            SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%FILE_NAME%}', COPYM_REC.FILE_NAME);
            SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%LINE_NO%}', CNT_REC_TOT);
            SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%USER_ID%}', USER);
            SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%USER_DTM%}', SYSDATE);
            SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%REPORT_REFERENCE%}', P_REPORT_REF);
            SQL_INS_VAL := REPLACE(SQL_INS_VAL, '{%SERVICE_TYPE%}', P_SERVICE_TYPE);
          END IF;
        

	/*Add V3.0*/
      	IF V_FILE_NAME IS NOT NULL AND P_SERVICE_TYPE = 'RSME_PARTNER' THEN
            SQL_INS_VAL := REPLACE(SQL_INS_VAL,'{%INVOICE_PERIOD%}',
            TO_DATE(TO_CHAR(LAST_DAY( to_date(substr(V_FILE_NAME,23,6),'YYYYMM')), 'DDMMYYYY'),'DDMMYYYY'));
      	END IF;
		SQL_INS_VAL := REPLACE(SQL_INS_VAL,'{%REPORT_REF_CDR%}',TO_CHAR(LAST_DAY(ADD_MONTHS(TO_DATE(COPYM_REC.FILE_DTM,'YYYYMMDD'),-1)),'YYYYMMDD'));
		IF V_FILE_NAME IS NOT NULL AND P_SERVICE_TYPE IN ('3BB_CM_PAYMENT','3BB_VDO_PAYMENT') THEN --# add by jaran -- max 24/04/2025
			SQL_INS_VAL := REPLACE(SQL_INS_VAL,'{%PERIOD_DATE%}',TO_DATE(TO_CHAR(TO_DATE(P_REPORT_REF, 'YYYYMMDD'),'DD/MM/YYYY'),'DD/MM/YYYY'));
      	END IF;
		V_PAID_STATUS_C := 'N';
          --## List Column Config
          FOR I IN 1 .. LIST_CONFIG.COUNT LOOP
            V_DATA_FIELD := GET_ATTR(SZ_LINE_BUFF, V_SEPARATE, LIST_CONFIG(I).CDR_POSITION);
            


            IF P_SERVICE_TYPE = 'YOUTUBE_PARTNER' AND LIST_CONFIG(I).DATA_TYPE = 'DATE' THEN
              IF LIST_CONFIG(I).COLUMN_NAME = 'SIGNUP_TIME' OR LIST_CONFIG(I).COLUMN_NAME = 'START_TIME' THEN
                 V_DATA_FIELD := substr(V_DATA_FIELD, 0, 19);
              ELSE 
                 V_DATA_FIELD := substr(V_DATA_FIELD, 0, 10);
              END IF;
            ELSIF P_SERVICE_TYPE ='APPLE_CARE_PARTNER' THEN
                  IF LIST_CONFIG(I).COLUMN_NAME ='IMEI_NUMBER' THEN
                    IF SUBSTR(V_DATA_FIELD,0,1) IN ('$','@','#','!','%')THEN 
                     V_DATA_FIELD := NULL;
                    END IF;
                  END IF;
            END IF;
          
            I_ERROR_CODE := 0;
            I_ERROR_CODE := VALIDATE_FIELD(LIST_CONFIG(I).MANDATORY_FLAG, LIST_CONFIG(I).DATA_LENGTH, LIST_CONFIG(I)
                                            .DATA_TYPE, LIST_CONFIG(I).DATA_FORMAT, V_DATA_FIELD);
            IF I_ERROR_CODE <> 0 THEN
              WRITE_ERROR('*** E0' || I_ERROR_CODE, CNT_REC_TOT, LIST_CONFIG(I).COLUMN_NAME, V_DATA_FIELD, '');
              CNT_REC_ERR := CNT_REC_ERR + 1;
              FLAG_INS    := 'N';
              EXIT;
            ELSE
		
		--# v3.1 added rsme cancel 
	      IF LIST_CONFIG(I).COLUMN_NAME = 'HANDSET_MODEL_OBJ' AND P_SERVICE_TYPE = 'AISOK_CANCEL' THEN
                SQL_INS_VAL := REPLACE(SQL_INS_VAL,'{%BRAND%}',substr(V_DATA_FIELD,0,instr(V_DATA_FIELD,' ',1)-1)); 
                SQL_INS_VAL := REPLACE(SQL_INS_VAL,'{%MODEL%}',substr(substr(V_DATA_FIELD,instr(V_DATA_FIELD,' ',1)+1),0,instr(substr(V_DATA_FIELD,instr(V_DATA_FIELD,' ',1)+1),' ',1)-1)); 
                SQL_INS_VAL := REPLACE(SQL_INS_VAL,'{%COLOR%}',substr(V_DATA_FIELD,instr(V_DATA_FIELD,' ',-1)+1)); 
              END IF;

              SQL_INS_COL := SQL_INS_COL || LIST_CONFIG(I).COLUMN_NAME || ',';
              SQL_INS_VAL := SQL_INS_VAL ||
                             GENERATE_FIELD_TXT(LIST_CONFIG(I).DATA_TYPE, LIST_CONFIG(I).DATA_FORMAT, LIST_CONFIG(I)
                                                 .CONVERT_FLAG, V_DATA_FIELD) || ',';
            
            END IF;
          END LOOP; --# Config
          -------## Generate Script Insert Data [End]
		  
          --# Insert Data
          IF FLAG_INS = 'Y' THEN
            INSERT_DATA;
          END IF;
        
        ELSE
          IF SUBSTR(TRIM(GET_ATTR(SZ_LINE_BUFF, V_SEPARATE, 1)), 1, 2) = '01' THEN
            BEGIN
              V_REPORT_REFERENCE := TO_CHAR(LAST_DAY(TO_DATE(SUBSTR(GET_ATTR(SZ_LINE_BUFF, V_SEPARATE, 2), 1, 6), 'YYYYMM')), 'YYYYMMDD');
            EXCEPTION
              WHEN OTHERS THEN
                NULL;
            END;
            --#DBMS_OUTPUT.PUT_LINE(V_REPORT_REFERENCE);
          
          END IF;
        END IF; --# _02
      
        /*Add V1.4a*/
        IF V_RECORD_TYPE = 'SKIP_HEADER' THEN
          V_RECORD_TYPE := NULL;
        END IF;
      
        <<NEXT_RECORD>> /*Add 1.6*/
        NULL; /*Add 1.6*/
      END LOOP; --## Loop CDR Rows
    
    EXCEPTION
      WHEN UTL_FILE.INVALID_PATH THEN
        DBMS_OUTPUT.PUT_LINE('File location is invalid');
        V_STATUS := 'FAIL';
      WHEN NO_DATA_FOUND THEN
        UTL_FILE.FCLOSE(UTL_DATFILE);
        V_STATUS := 'COMP';
      
        /*Add 1.4*/
        IF P_SERVICE_TYPE = 'M2M' AND V_EXCHANGE_RATE IS NULL THEN
          V_STATUS    := 'FAIL';
          CNT_REC_TOT := CNT_REC_TOT + 1;
          WRITE_ERROR('*** E0' || '203', CNT_REC_TOT, 'EXCHANGE_RATE', '', '');
          CNT_REC_ERR := CNT_REC_ERR + 1;
        ELSIF P_SERVICE_TYPE LIKE 'COPYD%' THEN
          /*Add 1.6*/
          V_STATUS := 'LOADED';
        END IF;
      
        NULL;
    END;
  END LOAD_DATA;

  ------------------------------------MAIN PROCESS---------------------------------------
BEGIN

  DBMS_OUTPUT.PUT_LINE('***********************************************************');
  DBMS_OUTPUT.PUT_LINE('START PROCESS     : ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
  DBMS_OUTPUT.PUT_LINE('SERVICE TYPE      : ' || P_SERVICE_TYPE);

  GET_PARAM;
  --##P_REPORT_REF := '20180131'; --#FOR MANUAL
  DBMS_OUTPUT.PUT_LINE('REPORT REFERENCE  : ' || P_REPORT_REF);

  OPEN COPYM_CUR;
  LOOP
    FETCH COPYM_CUR
      INTO COPYM_REC;
    EXIT WHEN COPYM_CUR%NOTFOUND;
  
    V_STATUS := NULL;
    :RETCODE     := 0;
    CNT_REC_TOT  := 0;
    CNT_REC_COM  := 0;
    CNT_REC_INS  := 0;
    CNT_REC_ERR  := 0;
    CNT_REC_REJ  := 0; /*Add 1.8 21/10/2019*/
    CNT_REC_SKIP := 0; /*V1.9 Add*/
    DBMS_OUTPUT.PUT_LINE('FILE LOAD         : ' || COPYM_REC.FILE_NAME);
  
    OPEN_FILE;
  
    V_TABLE_NAME := GET_TABLE_NAME;
    IF CHECK_LOADED = 'N' THEN
      IF V_TABLE_NAME IS NOT NULL THEN
        
         IF P_SERVICE_TYPE ='APPLE_MUSIC_OUR' THEN/*V2.6*/
             
            V_CNT_FILE_APPLE_MUSIC_OUR := V_CNT_FILE_APPLE_MUSIC_OUR +1;
            IF V_CNT_FILE_APPLE_MUSIC_OUR = 1 THEN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE SS_APPLE_MUSIC_OUR_HIST';
            BEGIN
              
             EXECUTE IMMEDIATE'INSERT INTO SS_APPLE_MUSIC_OUR_HIST
              SELECT * FROM '||V_TABLE_NAME;
              
              COMMIT;
                            
              EXECUTE IMMEDIATE 'TRUNCATE TABLE '||V_TABLE_NAME;
            
            EXCEPTION WHEN OTHERS THEN
              ROLLBACK;
              DBMS_OUTPUT.PUT_LINE('Backup Data APPLE_MUSIC_OUR FAIL');
            END;
            END IF;
            
          END IF;
        LOAD_DATA();
      ELSE
        DBMS_OUTPUT.PUT_LINE('*** ERROR: NOT FOUND TABLE NAME.');
        V_STATUS := 'FAIL';
      END IF;
    ELSE
      DBMS_OUTPUT.PUT_LINE('*** ERROR: FILE DUPLICATE.');
      V_STATUS := 'FAIL';
    END IF;
  
    UPD_COPYM(V_STATUS);
    COMMIT;
  
    DBMS_OUTPUT.PUT_LINE('TOTAL RECORD      : ' || TO_CHAR(CNT_REC_TOT));
    DBMS_OUTPUT.PUT_LINE('TOTAL INSERT      : ' || TO_CHAR(CNT_REC_INS));
    DBMS_OUTPUT.PUT_LINE('TOTAL REJECT      : ' || TO_CHAR(CNT_REC_REJ));
    DBMS_OUTPUT.PUT_LINE('TOTAL ERROR       : ' || TO_CHAR(CNT_REC_ERR));
    DBMS_OUTPUT.PUT_LINE('TOTAL SKIP        : ' || TO_CHAR(CNT_REC_SKIP)); /*V1.9 Add*/
  
    --## UPDATE SERVICE_SUB_GROUP = PACKAGE_NAME (CASE VDO SERVICE)
    IF COPYM_REC.PROCESS = 'VDO' THEN
      BEGIN
        UPDATE SS_LOAD_PORTION T
           SET T.SERVICE_SUB_GROUP = T.PACKAGE_NAME
         WHERE T.SERVICE_NAME = COPYM_REC.PROCESS
           AND T.FILE_NAME = COPYM_REC.FILE_NAME;
      
      EXCEPTION
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('*** ERROR: UPDATE SUB_GROUP [VDO]');
      END;
    END IF;
  
  END LOOP;
  CLOSE COPYM_CUR;

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('END PROCESS       : ' || TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
  DBMS_OUTPUT.PUT_LINE('***********************************************************');
END;

/
SHOW SQLCODE
EXIT :RETCODE
