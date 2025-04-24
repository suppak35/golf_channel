#!/usr/bin/ksh

CUR_DATE=`date +"%Y%m%d"`

#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql KARAOKE >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log 
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql KARAOKE_ACTIVATE >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql VDO >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql CALLING_PLUS >> /users/settoper/ssdata/ss_load_master_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql CALLING_MAX >> /users/settoper/ssdata/ss_load_master_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql CPAC_INVOICE >> /users/settoper/ssdata/ss_load_master_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql IOT_AXA >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql IOT_RESULT >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql M2M >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql MOBILE_CARE_WAIVE >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql MOBILE_CARE_WAIVE_RBM >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master_nonais.sql NAFA_PACKAGE_NONAIS >> /users/settoper/ssdata/ss_load_master_nonais_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql DOWNLOAD_SONGID >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql APPLE_CARE_OUR_INV >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql APPLE_CARE_OUR_CANCEL >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql APPLE_CARE_PARTNER >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql APPLE_MUSIC_PARTNER >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql APPLE_MUSIC_OUR >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql VDO_CALLING_PLUS >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql VDO_CALLING_MAO_MAO >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql APPLE_MUSIC_SFF >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql YOUTUBE_PARTNER >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql YOUTUBE_SFF >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql NETFLIX_PARTNER >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql NETFLIX_INTERNAL >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql RSME_PARTNER >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql CPAAS_SFF >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql CPAAS_PARTNER >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql 3BB_CM_PAYMENT >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql onspot >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql WAIVE_ACCOUNT >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql WAIVE_INVOICE >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql WAIVE_DETAIL >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
#sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql BP_PARTNER >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql BP_ADDRESS >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
sqlplus /nolog @/users/settoper/ssprocess/ss_load_master.sql 3BB_VDO_PAYMENT >> /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log

########### Function _mail()  Start process send E-mail    ###############
function send_mail
{
        if [ -z ${FILE_PATH} ]
        then
                echo "Path Not found <${FILE_PATH}>"
                exit
        fi
        
        Detail_email="${FILE_PATH}${FILE_NAME1}"
        MAIL_LIST="${EMAIL_PATH}mail_list.txt"
        Mail_subject="[LOAD MASTER MONTHLY] Process Load master monthly Log"

        ####  Send Mail ####
        name_list=`cat $MAIL_LIST`
        (cat $Detail_email; uuencode ${FILE_PATH}${FILE_NAME1} ${FILE_NAME1})| mailx -s "$Mail_subject" $name_list
        #echo "Send mail to $name_list complete "   
}

EMAIL_PATH=/users/settoper/ssdata/alert_mail/upd_mobile_test/Email/
FILE_PATH=/users/settoper/ssdata/
FILE_NAME1=ss_load_master_monthly_${CUR_DATE}.log

#------ Send Mail
if [[ `ls -lrt ${FILE_PATH}${FILE_NAME1} 2</dev/null | wc -l` -ne 0 ]]
then
        send_mail
fi
#rm -f /users/settoper/ssdata/ss_load_master_monthly_${CUR_DATE}.log
