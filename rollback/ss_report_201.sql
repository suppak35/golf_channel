/*-----------------------------------------------------------------------------------------------------------------------------*/
/* Author Name : Jaran R.                                                                                                     */
/* Create Date : 20/06/2024                                                                                                    */
/* Program Description : Run process summary 3bb cloud ip camera SSRSR201                                                        */
/* Version 1.0 : 20/06/2024 - Jaranr35 - Create program                                                                       */
/* Version 1.1 : 08/08/2024 - Jaranr35 - add SHARE_BASIS to CURSOR MAIN_CUR                                                    */
/* Version 1.2 : 25/09/2024 - Saenn571 - add Condition AND BNO.VAS_NO = E.VAS_NO                                                   */
/*-----------------------------------------------------------------------------------------------------------------------------*/
@/users/oper/pwd/connect_ss.sql

SET SERVEROUTPUT ON SIZE 1000000
VAR RETCODE  NUMBER
EXEC :RETCODE := 2;

DECLARE
  V_DATPATH    VARCHAR2(100) := '/users/oper/ssdata'; --#'/users/oper/ssdata';
  V_DATNAME    VARCHAR2(100) := 'report_param.txt'; --#'report_param.txt';
  P_REPORT_REF VARCHAR2(8);
  UTL_DATFILE  UTL_FILE.FILE_TYPE;
  P_AS_OF      VARCHAR2(6);
  C_REPORT_CODE CONSTANT VARCHAR2(8) := 'SSRSR201';
  V_TOT_COMMIT NUMBER := 0;
  -------------------------------------------------
  V_CHK_DUP_SUM VARCHAR2(1);
  V_CHK_DUP_SAP VARCHAR2(1);
  -------------------------------------------------
  V_INVOICING_CO_ID SS_SUMMARY_REPORT.INVOICING_CO_ID%TYPE;
  V_NETWORK_TYPE    SS_SUMMARY_REPORT.NETWORK_TYPE%TYPE;
  V_EE_ID           SS_SUMMARY_REPORT.EE_ID%TYPE;
  V_REPORT_TYPE     SS_SUMMARY_REPORT.REPORT_TYPE%TYPE;
  V_EVENT_MONTH     SS_SUMMARY_REPORT.EVENT_MONTHS%TYPE;
  V_EVENT_DTM       SS_SUMMARY_REPORT.EVENT_DTM%TYPE;
  V_VAS_NO          SS_SUMMARY_REPORT.VAS_NO%TYPE;
  V_USAGE_RATE      SS_SUMMARY_REPORT.USAGE_RATE%TYPE;
  V_PROVIDER_RATE   SS_SUMMARY_REPORT.PROVIDER_RATE%TYPE;
  V_TOT_TRANS       SS_SUMMARY_REPORT.TOT_TRANS%TYPE;
  V_TOT_AMT         SS_SUMMARY_REPORT.TOT_AMT%TYPE;
  V_TOT_AIS_AMT     SS_SUMMARY_REPORT.TOT_AIS_AMT%TYPE;
  V_TOT_EE_AMT      SS_SUMMARY_REPORT.TOT_EE_AMT%TYPE;
  V_AIS_REVENUE     SS_SUMMARY_REPORT.AIS_REVENUE%TYPE;
  V_EE_REVENUE      SS_SUMMARY_REPORT.EE_REVENUE%TYPE;
  V_PRODUCT_DESC    SS_SUMMARY_REPORT.PRODUCT_DESC%TYPE;
  V_SUB_GROUP_ID    SS_SUMMARY_REPORT.SUB_GROUP_ID%TYPE;
  V_DATA_TYPE       SS_SUMMARY_REPORT.DATA_TYPE%TYPE;
  V_SETTLE_CO_ID    SS_SUMMARY_REPORT.SETTLE_CO_ID%TYPE;
  V_GENEVA_PRICE    SS_SUMMARY_REPORT.GENEVA_PRICE%TYPE;
  V_SHARE_BASIS     SS_SUMMARY_REPORT.SHARE_BASIS%TYPE;
  -------------------------------------------------
  V_TARIFF_ID    SS_TARIFF_MASTER.TARIFF_ID%TYPE;
  V_TARIFF_MODEL SS_TARIFF_MASTER.TARIFF_MODEL%TYPE;
  V_TARIFF_MARKUP1 SS_TARIFF_DETAIL.TARIFF_MARKUP1%TYPE;

  V_TARIFF_RATE_TYPE        SS_TARIFF_DETAIL.TARIFF_RATE_TYPE%TYPE;
  V_TARIFF_ICO_REVENUE_FLAG SS_TARIFF_DETAIL.TARIFF_ICO_REVENUE_FLAG%TYPE;
  V_TARIFF_RATE             SS_TARIFF_DETAIL.TARIFF_RATE%TYPE;
  V_TARIFF_MARKUP4          SS_TARIFF_DETAIL.TARIFF_MARKUP4%TYPE;
  V_TARIFF_MARKUP4_TYPE     SS_TARIFF_DETAIL.TARIFF_MARKUP4_TYPE%TYPE;
  V_TARIFF_VOL_END          SS_TARIFF_DETAIL.TARIFF_VOL_END%TYPE;
  V_TARIFF_VOL_START        SS_TARIFF_DETAIL.TARIFF_VOL_START%TYPE;


  V_TARIFF_ICO_MAX          NUMBER(15, 3);
   V_TARIFF_MARKUP1_TYPE VARCHAR2(3);
   V_PARTY_ID                SS_TARIFF_DETAIL.PARTY_ID%TYPE;
  --#----------------------------------------------------------------------
  CURSOR MAIN_CUR IS
     SELECT REPORT_REFERENCE,
           INVOICING_CO_ID,
           NETWORK_TYPE,
           EE_ID,
           VAS_NO,
           REPORT_CODE,
           DATA_TYPE,
           REPORT_TYPE,
           TARIFF_ID,
           CASE
             WHEN GROUPING(EVENT_MONTH) = 1 THEN
              '000000'
             ELSE
              EVENT_MONTH
           END EVENT_MONTH,
          P_REPORT_REF EVENT_DATE,
           USAGE_RATE,
           NULL PROVIDER_RATE,
           SETTLE_CO_ID,
           ABS(SUM(CASE
                 WHEN REPORT_TYPE = 'ADJUST' THEN
                  TOT_TRANS * (-1)
                 ELSE
                  TOT_TRANS
               END)) TOT_TRANS,
           ABS(SUM(CASE
                 WHEN REPORT_TYPE = 'ADJUST' THEN
                  TOT_AMT * (-1)
                 ELSE
                  TOT_AMT
               END)) TOT_AMT,
           SUM(AIS_AMT) TOT_AIS_AMT,
           ABS(SUM(CASE
                 WHEN REPORT_TYPE = 'ADJUST' THEN
                  (TARIFF_RATE/100)*TOT_AMT * (-1)
                 ELSE
                  (TARIFF_RATE/100)*TOT_AMT
               END)) TOT_EE_AMT,
           TARIFF_RATE,
           STATUS,
           SHARE_BASIS
      FROM (SELECT P_REPORT_REF REPORT_REFERENCE,
                   R.INVOICING_CO_ID,
                   R.NETWORK_TYPE,
                   E.EE_ID,
                   E.VAS_NO,
                   C_REPORT_CODE REPORT_CODE,
                   'VAS_DATA'DATA_TYPE,
                   CASE  WHEN B.STATUS = 'C' THEN 'ADJUST' ELSE 'NORMAL' END REPORT_TYPE,
                   TO_CHAR(B.CASH_INV_DATE,'YYYYMM') EVENT_MONTH,
                   VT.USAGE_RATE ,
                  -- R.TARIFF_ID,
                   VT.Tariff_Id,
                   R.SETTLE_CO_ID,
                   SUM(R.TOTAL_TRANSACTION) TOT_TRANS,
                   SUM(R.TOTAL_AMT) TOT_AMT,
                   SUM(R.AIS_AMT) AIS_AMT,
                   SUM(R.EE_AMT) EE_AMT,
                   (SELECT TARIFF_RATE FROM SS_TARIFF_DETAIL WHERE TARIFF_ID = VT.TARIFF_ID)  TARIFF_RATE,
                   B.STATUS,
                   'CSH' SHARE_BASIS
              FROM SS_CASH_BASIS R, SS_CASH_BASIS_BALANCE B,SS_EE_VAS_TARIFF VT, SS_BNO_VAS BNO , SS_EE_VAS E
             WHERE 1=1
             AND R.VAS_NO = BNO.REF_VAS_NO
             AND VT.VAS_NO = BNO.VAS_NO
             AND BNO.VAS_NO = E.VAS_NO /* V1.2 */
             --check payment--
               AND R.INVOICING_CO_ID = B.INVOICING_CO_ID
               AND R.NETWORK_TYPE = B.NETWORK_TYPE
               AND R.ACCOUNT_NO = B.ACCOUNT_NO
               AND R.EVENT_SEQ = B.EVENT_SEQ
               AND R.EE_ID = B.EE_ID
               AND R.VAS_NO = B.VAS_NO
               AND R.CASH_INV_DATE = B.CASH_INV_DATE
               AND B.STATUS IN ('P','C')
               AND B.CASH_PAID_DATE = TO_DATE(P_REPORT_REF, 'YYYYMMDD')
             GROUP BY R.INVOICING_CO_ID,
                   R.SETTLE_CO_ID,
                   R.NETWORK_TYPE,
                   E.EE_ID,
                   E.VAS_NO,
                  -- R.TARIFF_ID,
                   VT.TARIFF_ID,
                   TO_CHAR(B.CASH_INV_DATE, 'YYYYMM'),
                   VT.USAGE_RATE , B.STATUS )
                   GROUP BY GROUPING SETS((REPORT_REFERENCE, INVOICING_CO_ID, NETWORK_TYPE,SETTLE_CO_ID, EE_ID, 
                   REPORT_CODE, DATA_TYPE,TARIFF_ID, REPORT_TYPE, EVENT_MONTH, USAGE_RATE, VAS_NO, TARIFF_RATE,
                    STATUS, SHARE_BASIS),(REPORT_REFERENCE, INVOICING_CO_ID,SETTLE_CO_ID, NETWORK_TYPE, 
                    EE_ID, REPORT_CODE, DATA_TYPE, TARIFF_RATE));
                    
	 
  M1 MAIN_CUR%ROWTYPE;
  --#----------------------------------------------------------------------

  PROCEDURE GET_PARAM IS
  BEGIN
    :RETCODE      := 0;
    P_REPORT_REF := NULL;
    UTL_DATFILE  := UTL_FILE.FOPEN(V_DATPATH, V_DATNAME, 'R');
  
    BEGIN
      UTL_FILE.GET_LINE(UTL_DATFILE, P_AS_OF);
      P_REPORT_REF := TO_CHAR(LAST_DAY(TO_DATE(P_AS_OF, 'YYYYMM')),
                              'YYYYMMDD');
    EXCEPTION
      WHEN UTL_FILE.INVALID_PATH THEN
        RAISE_APPLICATION_ERROR(-20001,
                                '*** ERROR: FILE LOCATION OR NAME WAS INVALID.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_MODE THEN
        RAISE_APPLICATION_ERROR(-20001,
                                '*** ERROR: THE OPEN_MODE STRING WAS INVALID.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_OPERATION THEN
        RAISE_APPLICATION_ERROR(-20001,
                                '*** ERROR: FILE COULD NOT BE OPENED AS REQUESTED.');
        :RETCODE := 2;
      WHEN UTL_FILE.INVALID_MAXLINESIZE THEN
        RAISE_APPLICATION_ERROR(-20001,
                                '*** ERROR: SPECIFIED MAX_LINESIZE IS TOO LARGE OR TOO SMALL.');
        :RETCODE := 2;
      WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, '*** ERROR: CANNOT OPEN FILE.');
        :RETCODE := 2;
    END;
  END GET_PARAM;
  --------------------------------------------------------------------------------------------------
  PROCEDURE CHK_DUP_SUMMARY IS
  BEGIN
    V_CHK_DUP_SUM := NULL;
    V_CHK_DUP_SAP := NULL;
  
    BEGIN
      SELECT 'Y'
        INTO V_CHK_DUP_SUM
        FROM SS_SUMMARY_REPORT
       WHERE REPORT_REFERENCE = P_REPORT_REF
         AND REPORT_CODE = C_REPORT_CODE
         AND ROWNUM <= 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_CHK_DUP_SUM := 'N';
       /* BEGIN
          SELECT 'Y'
            INTO V_CHK_DUP_SUM
            FROM SS_SAP_EXTRACT_CONTROL
           WHERE REPORT_REFERENCE = P_REPORT_REF
             AND REPORT_CODE = C_REPORT_CODE
             AND ROWNUM <= 1;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            V_CHK_DUP_SUM := 'N';
          WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Duplicate in SAP_EXT Report_Ref: ' ||
                                 P_REPORT_REF);
            RETURN;
        END;*/
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Duplicate in Summary Report_Ref: ' ||
                             P_REPORT_REF);
        RETURN;
    END;
  END CHK_DUP_SUMMARY;
  --#----------------------------------------------------------------------

  PROCEDURE CHK_PROVIDER_RATE(IN_SHARE_TYPE IN VARCHAR2 := 'NORMAL') IS
    TMP_TARIFF_DETAIL_CODE    SS_TARIFF_DETAIL.TARIFF_DETAIL_CODE%TYPE;
    V_TARIFF_ID               SS_TARIFF_DETAIL.TARIFF_ID%TYPE;
    V_TARIFF_RATE_TYPE        SS_TARIFF_DETAIL.TARIFF_RATE_TYPE%TYPE;
    V_TARIFF_ICO_REVENUE_FLAG SS_TARIFF_DETAIL.TARIFF_ICO_REVENUE_FLAG%TYPE;
    V_TARIFF_RATE             SS_TARIFF_DETAIL.TARIFF_RATE%TYPE;
    V_TARIFF_ICO_MAX          SS_TARIFF_DETAIL.TARIFF_ICO_MAX%TYPE;
    V_TARIFF_MARKUP1          SS_TARIFF_DETAIL.TARIFF_MARKUP1%TYPE;
    V_TARIFF_MARKUP1_TYPE     SS_TARIFF_DETAIL.TARIFF_MARKUP1_TYPE%TYPE;
    --#----------------------------------------------   
    /*         --M1.VAS_NO := NULL;
    IF M1.VAS_NO IS NOT NULL THEN
       CHK_PROVIDER_RATE('NORMAL');
    END IF;*/
    --#----------------------------------------------  
  BEGIN
    V_TARIFF_ID := NULL;
    IF IN_SHARE_TYPE = 'NORMAL' THEN
      TMP_TARIFF_DETAIL_CODE := TO_NUMBER(TRIM(TO_CHAR(M1.TARIFF_ID)) || '1');
    END IF;
  
    BEGIN
      SELECT DISTINCT TARIFF_ID,
                      TARIFF_RATE_TYPE,
                      TARIFF_ICO_REVENUE_FLAG,
                      TARIFF_RATE,
                      TARIFF_MARKUP1,
                      TARIFF_ICO_MAX,
                      TARIFF_MARKUP1_TYPE 
        INTO V_TARIFF_ID,
             V_TARIFF_RATE_TYPE,
             V_TARIFF_ICO_REVENUE_FLAG,
             V_TARIFF_RATE,
             V_TARIFF_MARKUP1,
             V_TARIFF_ICO_MAX,
             V_TARIFF_MARKUP1_TYPE
        FROM SS_TARIFF_DETAIL
       WHERE TARIFF_ID = M1.TARIFF_ID
         AND TARIFF_DETAIL_CODE = TMP_TARIFF_DETAIL_CODE;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_TARIFF_ID := NULL;
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR GET TARIFF_DETAIL : ' ||
                             SUBSTR(SQLERRM, 1, 80));
    END;
  
    IF V_TARIFF_ID IS NOT NULL THEN
      IF V_TARIFF_RATE_TYPE = 'PER' THEN
        IF V_TARIFF_ICO_REVENUE_FLAG = '1' THEN
          IF IN_SHARE_TYPE = 'MULTI' THEN
            V_PROVIDER_RATE := ROUND(V_TARIFF_RATE * M1.USAGE_RATE / 100,
                                     2);
          ELSE
            V_PROVIDER_RATE := ROUND((100 - V_TARIFF_RATE) *
                                     M1.USAGE_RATE / 100,
                                     2);
          END IF;
        ELSIF V_TARIFF_ICO_REVENUE_FLAG = '0' THEN
          V_PROVIDER_RATE := ROUND(V_TARIFF_RATE * M1.USAGE_RATE / 100,
                                   2);
        END IF;
      ELSIF V_TARIFF_RATE_TYPE = 'RIO' THEN
        IF V_TARIFF_ICO_REVENUE_FLAG = '1' THEN
          V_PROVIDER_RATE := ROUND((V_TARIFF_ICO_MAX - V_TARIFF_RATE) *
                                   M1.USAGE_RATE / V_TARIFF_ICO_MAX,
                                   2);
        ELSIF V_TARIFF_ICO_REVENUE_FLAG = '0' THEN
          V_PROVIDER_RATE := ROUND(V_TARIFF_RATE * M1.USAGE_RATE /
                                   V_TARIFF_ICO_MAX,
                                   2);
        END IF;
      
      ELSE
        IF V_TARIFF_ICO_REVENUE_FLAG = '1' THEN
          V_PROVIDER_RATE := M1.USAGE_RATE - V_TARIFF_RATE;
        ELSIF V_TARIFF_ICO_REVENUE_FLAG = '0' THEN
          V_PROVIDER_RATE := V_TARIFF_RATE;
        END IF;
      END IF;
    END IF;
  
  END CHK_PROVIDER_RATE;
  --#----------------------------------------------------------------------
  
  PROCEDURE INSERT_DATA IS
  BEGIN
    /*IF NETWORK_REC.NETWORK_TYPE = 'PPS' THEN*/
    IF V_NETWORK_TYPE IN ('PPS', '3PP') THEN
      V_DATA_TYPE := 'VAS';
    ELSE
      V_DATA_TYPE := 'VAS_DATA';
    END IF;
    CHK_PROVIDER_RATE('NORMAL');
    INSERT INTO SS_SUMMARY_REPORT
      (REPORT_REFERENCE,
       INVOICING_CO_ID,
       NETWORK_TYPE,
       EE_ID,
       REPORT_CODE,
       DATA_TYPE,
       REPORT_TYPE,
       EVENT_MONTHS,
       EVENT_DTM,
       VAS_NO,
       TARIFF_ID,
       PROVIDER_RATE,
       USAGE_RATE,
       TOT_TRANS,
       TOT_AMT,
       TOT_AIS_AMT,
       TOT_EE_AMT,
       AIS_REVENUE,
       EE_REVENUE,
       JAVA_AMT,
       BC_AMT,
       BD_AMT,
       COMM_AMT,
       MARKUP_AMT1,
       MARKUP_AMT2,
       USER_ID,
       USER_DATE,
       SETTLE_CO_ID,
	   GENEVA_PRICE,
	   SHARE_BASIS)
    VALUES
      (P_REPORT_REF,
       V_INVOICING_CO_ID,
       V_NETWORK_TYPE,
       V_EE_ID,
       C_REPORT_CODE,
       V_DATA_TYPE,
       V_REPORT_TYPE,
       V_EVENT_MONTH,
       V_EVENT_DTM,
       V_VAS_NO,
       V_TARIFF_ID,
       V_PROVIDER_RATE,
       V_USAGE_RATE,
       V_TOT_TRANS,
       V_TOT_AMT,
       V_TOT_AIS_AMT,
       V_TOT_EE_AMT,
       V_AIS_REVENUE,
       V_TOT_EE_AMT,
       0,
       0,
       0,
       0,
       0,
       0,
       USER,
       SYSDATE,
       V_SETTLE_CO_ID,
	   V_GENEVA_PRICE,
	   V_SHARE_BASIS);
  
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('CAN NOT INSERT :' || V_EE_ID || '|' ||
                           V_REPORT_TYPE || '|' || V_EVENT_MONTH || '|' ||
                           V_VAS_NO || '|' || V_TARIFF_ID || ':' ||
                           SQLERRM);
  END INSERT_DATA;
  --#----------------------------------------------------------------------
  PROCEDURE INSERT_EXT_CONTROL IS
  BEGIN
    INSERT INTO SS_SAP_EXTRACT_CONTROL
      (REPORT_REFERENCE,
       INVOICING_CO_ID,
       NETWORK_TYPE,
       REPORT_CODE,
       EXTRACT_TYPE,
       DATA_TYPE,
       SAP_INTERFACE_FLAG,
       USER_DTM,
       USER_ID)
    VALUES
      (P_REPORT_REF,
       V_INVOICING_CO_ID,
       V_NETWORK_TYPE,
       C_REPORT_CODE,
       'AR',
       '3BB Play box Package',
       'REP',
       SYSDATE,
       USER);
  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('CAN NOT INSERT2 :' || P_REPORT_REF || '|' ||
                           V_INVOICING_CO_ID || '|' || V_NETWORK_TYPE || ':' ||
                           SQLERRM);
  END INSERT_EXT_CONTROL;
  --#----------------------------------------------------------------------
  PROCEDURE INITIALIZE_VARIABLE IS
  BEGIN
     V_TARIFF_ID               := NULL;
    V_TARIFF_MODEL            := NULL;
    V_TARIFF_RATE_TYPE        := NULL;
    V_TARIFF_ICO_REVENUE_FLAG := NULL;
    V_TARIFF_RATE             := NULL;
    V_TARIFF_VOL_END          := NULL;
    V_TARIFF_VOL_START        := NULL;
    V_TARIFF_MARKUP4          := NULL;
    V_INVOICING_CO_ID := NULL;
    V_NETWORK_TYPE    := NULL;
    V_EE_ID           := NULL;
    V_REPORT_TYPE     := NULL;
    V_EVENT_MONTH     := NULL;
    V_EVENT_DTM       := NULL;
    V_VAS_NO          := NULL;
    V_PROVIDER_RATE   := NULL;
    V_USAGE_RATE      := NULL;
    V_TOT_TRANS       := NULL;
    V_TOT_AMT         := NULL;
    V_TOT_AIS_AMT     := NULL;
    V_TOT_EE_AMT      := NULL;
    V_AIS_REVENUE     := NULL;
    V_EE_REVENUE      := NULL;
    V_SETTLE_CO_ID    := NULL;
    --V_MINIMUM_GUARANTEE  := NULL;
    --V_TOT_MO             := NULL;
    -- V_TOT_MT             := NULL;
    V_PRODUCT_DESC := NULL;
    V_SUB_GROUP_ID := NULL;
  
  END INITIALIZE_VARIABLE;
  --#----------------------------------------------------------------------

  --#----------------------------------------------------------------------
  PROCEDURE MAIN_CURSOR IS
  BEGIN
    OPEN MAIN_CUR;
    LOOP
      FETCH MAIN_CUR
        INTO M1;
      EXIT WHEN MAIN_CUR%NOTFOUND;
      V_TOT_COMMIT := NVL(V_TOT_COMMIT, 0) + 1;
    
      INITIALIZE_VARIABLE;
    
      V_INVOICING_CO_ID := M1.INVOICING_CO_ID;
      V_NETWORK_TYPE    := M1.NETWORK_TYPE;
      V_EE_ID           := M1.EE_ID;
      V_REPORT_TYPE     := M1.REPORT_TYPE;
      V_EVENT_MONTH     := M1.EVENT_MONTH;
      V_TARIFF_ID       := M1.TARIFF_ID;
      V_EVENT_DTM       := TO_DATE(M1.EVENT_DATE,'YYYYMMDD');
      V_VAS_NO          := M1.VAS_NO;
      V_USAGE_RATE      := M1.USAGE_RATE;
      V_TOT_TRANS       := M1.TOT_TRANS;
      V_TOT_AMT         := M1.TOT_AMT;
      V_TOT_AIS_AMT     := M1.TOT_AIS_AMT;
      V_TOT_EE_AMT      := M1.TOT_EE_AMT;
      V_AIS_REVENUE     := M1.TOT_AIS_AMT;
      V_EE_REVENUE      := M1.TOT_EE_AMT;
      V_SETTLE_CO_ID    := M1.SETTLE_CO_ID;
      V_GENEVA_PRICE    := M1.TARIFF_RATE;
      V_SHARE_BASIS     := M1.SHARE_BASIS;
    
      INSERT_DATA; --CALL PROCEDURE
    
      IF V_TOT_COMMIT >= 100 THEN
        COMMIT;
        V_TOT_COMMIT := 0;
      END IF;
    END LOOP;
    CLOSE MAIN_CUR;
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE('ERROR : ' || SQLERRM);
    
  END MAIN_CURSOR;

  /******************************************** START PROCESS *************************************/
BEGIN
  DBMS_OUTPUT.PUT_LINE('***********************************************************');
  DBMS_OUTPUT.PUT_LINE('START PROCESS     : ' ||
                       TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
  :RETCODE := 0;
  GET_PARAM;
  SS_PKG_PROC.SET_PROCESS_INI(I_PROC_NAME => 'ss_report_201.sql');
  SS_PKG_PROC.SET_FILE_NAME('/users/settoper/ssprocess/');
  SS_PKG_PROC.SET_REPORT_REFERENCE(P_REPORT_REF);
  SS_PKG_PROC.SET_REPORT_CODE(C_REPORT_CODE);
  SS_PKG_PROC.SET_PROCESS_GROUP('SUMMARY_REPORT');
  SS_PKG_PROC.PROCESS_START;

  DBMS_OUTPUT.PUT_LINE('REPORT_REFERENCE  : ' || P_REPORT_REF);

    CHK_DUP_SUMMARY;
    IF V_CHK_DUP_SUM = 'N' THEN
      --SUM
      MAIN_CURSOR;
      /*INSERT_EXT_CONTROL;*/
    
      UTL_FILE.FCLOSE_ALL;
    ELSE
      DBMS_OUTPUT.PUT_LINE(' "CDR Duplicate in SUMMARY REPORT SSRSR201 :' ||
                           P_REPORT_REF || ' "');
      ROLLBACK;
    END IF;
  

  DBMS_OUTPUT.PUT_LINE('END PROCESS       : ' ||
                       TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS'));
  DBMS_OUTPUT.PUT_LINE('***********************************************************');

  SS_PKG_PROC.PROCESS_STOP(SS_PKG_PROC.GC_STATUS_COMP);
EXCEPTION
  WHEN OTHERS THEN
    :RETCODE := 2;
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('ERR:' || SUBSTR(SQLERRM, 1, 80));
  
    SS_PKG_PROC.PUT_LINE('ERR:' || SUBSTR(SQLERRM, 1, 80));
    SS_PKG_PROC.PROCESS_STOP(SS_PKG_PROC.GC_STATUS_FAIL);
END;

/
SHOW SQLCODE
EXIT :RETCODE
