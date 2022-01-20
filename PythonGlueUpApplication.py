from __future__ import print_function
from datetime import date, datetime, timedelta
from mysql.connector import errorcode
import mysql.connector
from move_procedures import *

global source_database 
global source_host
global source_port
global source_login
global source_password

global target_database 
global target_host
global target_port
global target_login
global target_password

def dispatch(self, value):
    method_name = 'move_' + str(value)
    method = getattr(self, method_name)
    return method()

# основная программа 
source_database = "ebdb_migration"
source_host = "localhost"
source_port = 3304
source_login = "etl"
source_password = "etletl"

target_database = "stagearea"
target_host = "localhost"
target_port = 3306
target_login = "etl"
target_password = "etletl"


try:
    try:
        source_conn = mysql.connector.connect(user=source_login, password=source_password, host=source_host, port = source_port, database=source_database)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name={} and password for source database={}".format(source_login, source_database))
            exit()
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Source database does not exist:"+source_database)
            exit()
        else:
            print("Error dusing opening the target database={} Message={}".format(source_database, err))
            exit()
    try:
        target_conn = mysql.connector.connect(user=target_login, password=target_password, host=target_host, port =target_port, database=target_database)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name={} and password for target database={}".format(target_login, target_database))
            exit()
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Target database does not exist:"+target_database)
            exit()
        else:
            print("Error dusing opening the target database={} Message={}".format(target_database, err))
            exit()
    delta = 10
    today_delta = datetime.now().date() - timedelta(minutes=10)
  ## Add the journal log migration
    curs = source_conn.cursor(buffered=True)
    row = curs.execute("SELECT target_name, source_table_name, source_database, stg_table_name, vw_name, vwz_name, dwh_table_name, dwh_database FROM v_tables_confirmed_migration ")
    for row in curs:
        target_name = row[0]
        source_table_name = row[1]
        source_database = row[2]
        stg_table_name = row[3]
        vw_name = row[4]
        vwz_name = row[5]
        dwh_table_name = row[6]
        dwh_database = row[7]
        curs.close()
        # =============== begin of if =====================
        if target_name=='accreditation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_accreditation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_accreditation(source_conn, target_conn)
        elif target_name=='accreditation_credit':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_accreditation_credit")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_accreditation_credit(source_conn, target_conn)
        elif target_name=='accreditation_credit_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_accreditation_credit_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_accreditation_credit_document(source_conn, target_conn)
        elif target_name=='adapay_autosplit_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_adapay_autosplit_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_adapay_autosplit_track(source_conn, target_conn)
        elif target_name=='adapay_refund':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_adapay_refund")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_adapay_refund(source_conn, target_conn)
        elif target_name=='adapay_split':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_adapay_split")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_adapay_split(source_conn, target_conn)
        elif target_name=='analytics_company_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_analytics_company_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_analytics_company_track(source_conn, target_conn)
        elif target_name=='analytics_contact_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_analytics_contact_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_analytics_contact_track(source_conn, target_conn)
        elif target_name=='analytics_request':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_analytics_request")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_analytics_request(source_conn, target_conn)
        elif target_name=='analytics_user_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_analytics_user_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_analytics_user_track(source_conn, target_conn)
        elif target_name=='app':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_app")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_app(source_conn, target_conn)
        elif target_name=='app_store':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_app_store")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_app_store(source_conn, target_conn)
        elif target_name=='app_version_log':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_app_version_log")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_app_versionlog(source_conn, target_conn)
        elif target_name=='attendee_notification':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_attendee_notification")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_attendee_notification(source_conn, target_conn)
        elif target_name=='billing_address':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_billing_address")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_billing_address(source_conn, target_conn)
        elif target_name=='checkin_point':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_checkin_point")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_checkin_point(source_conn, target_conn)
        elif target_name=='checkin_point_ticket_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_checkin_point_ticket_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_checkin_point_ticket_relation(source_conn, target_conn)
        elif target_name=='client_key':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_client_key")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_client_key(source_conn, target_conn)
        elif target_name=='community':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community(source_conn, target_conn)
        elif target_name=='community_comment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community_comment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community_comment(source_conn, target_conn)
        elif target_name=='community_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community_document(source_conn, target_conn)
        elif target_name=='community_group':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community_group")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community_group(source_conn, target_conn)
        elif target_name=='community_group_join_request':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community_group_join_request")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community_group_join_request(source_conn, target_conn)
        elif target_name=='community_group_member_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community_group_member_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community_group_member_relation(source_conn, target_conn)
        elif target_name=='community_post':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community_post")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community_post(source_conn, target_conn)
        elif target_name=='community_reaction':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_community_reaction")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_community_reaction(source_conn, target_conn)
        elif target_name=='contact_bak':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_contact_bak")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_contact_bak(source_conn, target_conn)
        elif target_name=='contact_delta':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_contact_delta")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_contact_delta(source_conn, target_conn)
        elif target_name=='coupon':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_coupon")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_coupon(source_conn, target_conn)
        elif target_name=='coupon_ticket_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_coupon_ticket_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_coupon_ticket_relation(source_conn, target_conn)
        elif target_name=='crm_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_crm_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_crm_document(source_conn, target_conn)
        elif target_name=='crm_opportunity':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_crm_opportunity")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_crm_opportunity(source_conn, target_conn)
        elif target_name=='crm_opportunity_company_contacts':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_crm_opportunity_company_contacts")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_crm_opportunity_company_contacts(source_conn, target_conn)
        elif target_name=='crm_opportunity_products':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_crm_opportunity_products")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_crm_opportunity_products(source_conn, target_conn)
        elif target_name=='crm_product':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_crm_product")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_crm_product(source_conn, target_conn)
        elif target_name=='currency_exchange_rate':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_currency_exchange_rate")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_currency_exchange_rate(source_conn, target_conn)
        elif target_name=='custom_email_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_custom_email_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_custom_email_document(source_conn, target_conn)
        elif target_name=='custom_email_template_abstract':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_custom_email_template_abstract")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_custom_email_template_abstract(source_conn, target_conn)
        elif target_name=='custom_email_template_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_custom_email_template_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_custom_email_template_ctx(source_conn, target_conn)
        elif target_name=='data_update_setting':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_data_update_setting")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_data_update_setting(source_conn, target_conn)
        elif target_name=='dm_user_block_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_dm_user_block_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_dm_user_block_list(source_conn, target_conn)
        elif target_name=='dm_user_contact_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_dm_user_contact_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_dm_user_contact_list(source_conn, target_conn)
        elif target_name=='document_bucket':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_document_bucket")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_document_bucket(source_conn, target_conn)
        elif target_name=='edition_purchase_transaction':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edition_purchase_transaction")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edition_purchase_transaction(source_conn, target_conn)
        elif target_name=='edition_purchase_transaction_addon':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edition_purchase_transaction_addon")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edition_purchase_transaction_addon(source_conn, target_conn)
        elif target_name=='edition_purchase_transaction_status':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edition_purchase_transaction_status")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edition_purchase_transaction_status(source_conn, target_conn)
        elif target_name=='edm_campaign_failure':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_campaign_failure")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_campaign_failure(source_conn, target_conn)
        elif target_name=='edm_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_document(source_conn, target_conn)
        elif target_name=='edm_email_trace':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_email_trace")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_email_trace(source_conn, target_conn)
        elif target_name=='edm_email_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_email_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_email_track(source_conn, target_conn)
        elif target_name=='edm_event_invitation_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_event_invitation_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_event_invitation_track(source_conn, target_conn)
        elif target_name=='edm_notification_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_notification_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_notification_track(source_conn, target_conn)
        elif target_name=='edm_preview_email_trace':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_preview_email_trace")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_preview_email_trace(source_conn, target_conn)
        elif target_name=='edm_preview_email_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_preview_email_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_preview_email_track(source_conn, target_conn)
        elif target_name=='edm_template':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_template")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_template(source_conn, target_conn)
        elif target_name=='edm_template_receiver_contact_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_template_receiver_contact_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_template_receiver_contact_list(source_conn, target_conn)
        elif target_name=='edm_template_setting':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_template_setting")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_template_setting(source_conn, target_conn)
        elif target_name=='edm_trace_summary':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_edm_trace_summary")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_edm_trace_summary(source_conn, target_conn)
        elif target_name=='email':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_email")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_email(source_conn, target_conn)
        elif target_name=='email_merged':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_email_merged")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_email_merged(source_conn, target_conn)
        elif target_name=='email_template':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_email_template")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_email_template(source_conn, target_conn)
        elif target_name=='email_template_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_email_template_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_email_template_ctx(source_conn, target_conn)
        elif target_name=='event_abstract':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_abstract")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_abstract(source_conn, target_conn)
        elif target_name=='event_agenda':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_agenda")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_agenda(source_conn, target_conn)
        elif target_name=='event_attendee':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_attendee")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_attendee(source_conn, target_conn)
        elif target_name=='event_attendee_category':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_attendee_category")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_attendee_category(source_conn, target_conn)
        elif target_name=='event_collaborator':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_collaborator")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_collaborator(source_conn, target_conn)
        elif target_name=='event_collaborator_category':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_collaborator_category")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_collaborator_category(source_conn, target_conn)
        elif target_name=='event_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_ctx(source_conn, target_conn)
        elif target_name=='event_currency':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_currency")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_currency(source_conn, target_conn)
        elif target_name=='event_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_document(source_conn, target_conn)
        elif target_name=='event_document_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_document_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_document_list(source_conn, target_conn)
        elif target_name=='event_document_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_document_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_document_relation(source_conn, target_conn)
        elif target_name=='event_eanbar_code_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_eanbar_code_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_eanbar_code_list(source_conn, target_conn)
        elif target_name=='event_expense_revenue':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_expense_revenue")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_expense_revenue(source_conn, target_conn)
        elif target_name=='event_invitee':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_invitee")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_invitee(source_conn, target_conn)
        elif target_name=='event_member':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_member")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_member(source_conn, target_conn)
        elif target_name=='event_member_team_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_member_team_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_member_team_relation(source_conn, target_conn)
        elif target_name=='event_multi_checkin':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_multi_checkin")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_multi_checkin(source_conn, target_conn)
        elif target_name=='event_reminder_setting':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_reminder_setting")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_reminder_setting(source_conn, target_conn)
        elif target_name=='event_tag_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_tag_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_tag_list(source_conn, target_conn)
        elif target_name=='event_tag_list_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_tag_list_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_tag_list_ctx(source_conn, target_conn)
        elif target_name=='event_tag_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_tag_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_tag_relation(source_conn, target_conn)
        elif target_name=='event_task':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_task")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_task(source_conn, target_conn)
        elif target_name=='event_task_assignee':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_task_assignee")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_task_assignee(source_conn, target_conn)
        elif target_name=='event_task_category':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_task_category")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_task_category(source_conn, target_conn)
        elif target_name=='event_task_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_task_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_task_document(source_conn, target_conn)
        elif target_name=='event_task_note':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_task_note")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_task_note(source_conn, target_conn)
        elif target_name=='event_translated_language':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_translated_language")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_translated_language(source_conn, target_conn)
        elif target_name=='event_url':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_url")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_url(source_conn, target_conn)
        elif target_name=='event_working_group':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_event_working_group")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_event_working_group(source_conn, target_conn)
        elif target_name=='export_request':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_export_request")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_export_request(source_conn, target_conn)
        elif target_name=='fapiao_delivery_method':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_fapiao_delivery_method")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_fapiao_delivery_method(source_conn, target_conn)
        elif target_name=='feedback':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_feedback")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_feedback(source_conn, target_conn)
        elif target_name=='finance_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_finance_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_finance_document(source_conn, target_conn)
        elif target_name=='finance_fapiao':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_finance_fapiao")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_finance_fapiao(source_conn, target_conn)
        elif target_name=='finance_invoice':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_finance_invoice")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_finance_invoice(source_conn, target_conn)
        elif target_name=='finance_invoice_item':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_finance_invoice_item")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_finance_invoice_item(source_conn, target_conn)
        elif target_name=='finance_invoice_itemextracharge':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_finance_invoice_itemextracharge")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_finance_invoice_itemextracharge(source_conn, target_conn)
        elif target_name=='finance_item_paymentrelation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_finance_item_paymentrelation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_finance_item_paymentrelation(source_conn, target_conn)
        elif target_name=='finance_payment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_finance_payment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_finance_payment(source_conn, target_conn)
        elif target_name=='gateway_balance':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_balance")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_balance(source_conn, target_conn)
        elif target_name=='gateway_customer':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_customer")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_customer(source_conn, target_conn)
        elif target_name=='gateway_fee_detail':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_fee_detail")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_fee_detail(source_conn, target_conn)
        elif target_name=='gateway_key':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_key")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_key(source_conn, target_conn)
        elif target_name=='gateway_merchant':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_merchant")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_merchant(source_conn, target_conn)
        elif target_name=='gateway_payment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_payment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_payment(source_conn, target_conn)
        elif target_name=='gateway_payment_method':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_payment_method")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_payment_method(source_conn, target_conn)
        elif target_name=='gateway_payout':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_payout")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_payout(source_conn, target_conn)
        elif target_name=='gateway_transaction':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_transaction")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_transaction(source_conn, target_conn)
        elif target_name=='gateway_transaction_payout_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gateway_transaction_payout_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gateway_transaction_payout_relation(source_conn, target_conn)
        elif target_name=='gun_badge':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gun_badge")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gun_badge(source_conn, target_conn)
        elif target_name=='gun_level':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gun_level")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gun_level(source_conn, target_conn)
        elif target_name=='gun_points':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gun_points")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gun_points(source_conn, target_conn)
        elif target_name=='gun_referral':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_gun_referral")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_gun_referral(source_conn, target_conn)
        elif target_name=='meeting':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_meeting")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_meeting(source_conn, target_conn)
        elif target_name=='member':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_member")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_member(source_conn, target_conn)
        elif target_name=='membership':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership(source_conn, target_conn)
        elif target_name=='membership_amendment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_amendment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_amendment(source_conn, target_conn)
        elif target_name=='membership_custom_email_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_custom_email_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_custom_email_track(source_conn, target_conn)
        elif target_name=='membership_extra_member_purchase':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_extra_member_purchase")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_extra_member_purchase(source_conn, target_conn)
        elif target_name=='membership_extra_member_purchase_status_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_extra_member_purchase_status_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_extra_member_purchase_status_track(source_conn, target_conn)
        elif target_name=='membership_extra_member_purchase_tax':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_extra_member_purchase_tax")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_extra_member_purchase_tax(source_conn, target_conn)
        elif target_name=='membership_group_join_request':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_group_join_request")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_group_join_request(source_conn, target_conn)
        elif target_name=='membership_group_member':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_group_member")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_group_member(source_conn, target_conn)
        elif target_name=='membership_history':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_history")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_history(source_conn, target_conn)
        elif target_name=='membership_invoice':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_invoice")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_invoice(source_conn, target_conn)
        elif target_name=='membership_note':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_note")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_note(source_conn, target_conn)
        elif target_name=='membership_payment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_payment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_payment(source_conn, target_conn)
        elif target_name=='membership_renewal_email_trace':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_renewal_email_trace")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_renewal_email_trace(source_conn, target_conn)
        elif target_name=='membership_renewal_status_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_renewal_status_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_renewal_status_track(source_conn, target_conn)
        elif target_name=='membership_renewal_tax':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_renewal_tax")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_renewal_tax(source_conn, target_conn)
        elif target_name=='membership_status_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_status_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_status_track(source_conn, target_conn)
        elif target_name=='membership_tax':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_tax")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_tax(source_conn, target_conn)
        elif target_name=='membership_type':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type(source_conn, target_conn)
        elif target_name=='membership_type_additional_price':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_additional_price")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_additional_price(source_conn, target_conn)
        elif target_name=='membership_type_discount':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_discount")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_discount(source_conn, target_conn)
        elif target_name=='membership_type_email_config':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_email_config")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_email_config(source_conn, target_conn)
        elif target_name=='membership_type_email_config_staff_recipients':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_email_config_staff_recipients")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_email_config_staff_recipients(source_conn, target_conn)
        elif target_name=='membership_type_extra_fee':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_extra_fee")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_extra_fee(source_conn, target_conn)
        elif target_name=='membership_type_extra_price':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_extra_price")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_extra_price(source_conn, target_conn)
        elif target_name=='membership_type_invoice':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_invoice")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_invoice(source_conn, target_conn)
        elif target_name=='membership_type_price':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_price")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_price(source_conn, target_conn)
        elif target_name=='membership_type_version':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_membership_type_version")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_membership_type_version(source_conn, target_conn)
        elif target_name=='member_tag':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_member_tag")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_member_tag(source_conn, target_conn)
        elif target_name=='migration_task':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_migration_task")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_migration_task(source_conn, target_conn)
        elif target_name=='notification_message':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_notification_message")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_notification_message(source_conn, target_conn)
        elif target_name=='opportunity_note':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_opportunity_note")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_opportunity_note(source_conn, target_conn)
        elif target_name=='opportunity_proposal':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_opportunity_proposal")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_opportunity_proposal(source_conn, target_conn)
        elif target_name=='opportunity_proposal_document':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_opportunity_proposal_document")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_opportunity_proposal_document(source_conn, target_conn)
        elif target_name=='opportunity_stage':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_opportunity_stage")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_opportunity_stage(source_conn, target_conn)
        elif target_name=='opportunity_stagectx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_opportunity_stagectx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_opportunity_stagectx(source_conn, target_conn)
        elif target_name=='organization_abstract':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_abstract")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_abstract(source_conn, target_conn)
        elif target_name=='organization_analytics_settings':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_analytics_settings")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_analytics_settings(source_conn, target_conn)
        elif target_name=='organization_bankinfo':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_bankinfo")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_bankinfo(source_conn, target_conn)
        elif target_name=='organization_company':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_company")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_company(source_conn, target_conn)
        elif target_name=='organization_company_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_company_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_company_list(source_conn, target_conn)
        elif target_name=='organization_company_note':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_company_note")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_company_note(source_conn, target_conn)
        elif target_name=='organization_company_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_company_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_company_relation(source_conn, target_conn)
        elif target_name=='organization_contact':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_contact")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_contact(source_conn, target_conn)
        elif target_name=='organization_contact_list':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_contact_list")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_contact_list(source_conn, target_conn)
        elif target_name=='organization_contact_note':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_contact_note")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_contact_note(source_conn, target_conn)
        elif target_name=='organization_contact_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_contact_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_contact_relation(source_conn, target_conn)
        elif target_name=='organization_contract_email_config':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_contract_email_config")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_contract_email_config(source_conn, target_conn)
        elif target_name=='organization_contract_email_trace':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_contract_email_trace")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_contract_email_trace(source_conn, target_conn)
        elif target_name=='organization_crm_merge':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_crm_merge")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_crm_merge(source_conn, target_conn)
        elif target_name=='organization_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_ctx(source_conn, target_conn)
        elif target_name=='organization_domain':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_domain")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_domain(source_conn, target_conn)
        elif target_name=='organization_domain_record':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_domain_record")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_domain_record(source_conn, target_conn)
        elif target_name=='organization_edition':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_edition")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_edition(source_conn, target_conn)
        elif target_name=='organization_feed':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_feed")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_feed(source_conn, target_conn)
        elif target_name=='organization_gateway_config':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_gateway_config")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_gateway_config(source_conn, target_conn)
        elif target_name=='organization_member':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_member")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_member(source_conn, target_conn)
        elif target_name=='organization_memberteam':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_memberteam")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_memberteam(source_conn, target_conn)
        elif target_name=='organization_memberteam_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_memberteam_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_memberteam_relation(source_conn, target_conn)
        elif target_name=='organization_member_invitation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_member_invitation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_member_invitation(source_conn, target_conn)
        elif target_name=='organization_smartlist':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_smartlist")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_smartlist(source_conn, target_conn)
        elif target_name=='organization_socialmedia_account_abstract':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_socialmedia_account_abstract")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_socialmedia_account_abstract(source_conn, target_conn)
        elif target_name=='organization_socialmedia_account_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_socialmedia_account_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_socialmedia_account_ctx(source_conn, target_conn)
        elif target_name=='organization_subscription':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_subscription")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_subscription(source_conn, target_conn)
        elif target_name=='organization_taxes':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_taxes")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_taxes(source_conn, target_conn)
        elif target_name=='organization_venue':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_venue")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_venue(source_conn, target_conn)
        elif target_name=='organization_venue_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_organization_venue_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_organization_venue_ctx(source_conn, target_conn)
        elif target_name=='passcode':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_passcode")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_passcode(source_conn, target_conn)
        elif target_name=='payment_detail':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_payment_detail")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_payment_detail(source_conn, target_conn)
        elif target_name=='payment_external_log':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_payment_external_log")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_payment_external_log(source_conn, target_conn)
        elif target_name=='payment_order_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_payment_order_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_payment_order_relation(source_conn, target_conn)
        elif target_name=='person_abstract':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_person_abstract")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_person_abstract(source_conn, target_conn)
        elif target_name=='person_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_person_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_person_ctx(source_conn, target_conn)
        elif target_name=='phone':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_phone")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_phone(source_conn, target_conn)
        elif target_name=='quickbooks_attachments':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_attachments")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_attachments(source_conn, target_conn)
        elif target_name=='quickbooks_auth':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_auth")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_auth(source_conn, target_conn)
        elif target_name=='quickbooks_authtrack':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_authtrack")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_authtrack(source_conn, target_conn)
        elif target_name=='quickbooks_company':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_company")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_company(source_conn, target_conn)
        elif target_name=='quickbooks_contact':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_contact")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_contact(source_conn, target_conn)
        elif target_name=='quickbooks_invoice':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_invoice")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_invoice(source_conn, target_conn)
        elif target_name=='quickbooks_limiter':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_limiter")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_limiter(source_conn, target_conn)
        elif target_name=='quickbooks_mapping':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_mapping")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_mapping(source_conn, target_conn)
        elif target_name=='quickbooks_payment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_payment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_payment(source_conn, target_conn)
        elif target_name=='quickbooks_paymentmethod':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_paymentmethod")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_paymentmethod(source_conn, target_conn)
        elif target_name=='quickbooks_synctrack':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_quickbooks_synctrack")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_quickbooks_synctrack(source_conn, target_conn)
        elif target_name=='resource_descriptor':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_resource_descriptor")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_resource_descriptor(source_conn, target_conn)
        elif target_name=='send_grid_edm_response':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_send_grid_edm_response")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_send_grid_edm_response(source_conn, target_conn)
        elif target_name=='server_node':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_server_node")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_server_node(source_conn, target_conn)
        elif target_name=='server_node_user_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_server_node_user_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_server_node_user_relation(source_conn, target_conn)
        elif target_name=='session':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_session")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_session(source_conn, target_conn)
        elif target_name=='session_tag':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_session_tag")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_session_tag(source_conn, target_conn)
        elif target_name=='session_tag_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_session_tag_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_session_tag_ctx(source_conn, target_conn)
        elif target_name=='sms_platform_log':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_sms_platform_log")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_sms_platform_log(source_conn, target_conn)
        elif target_name=='sms_template':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_sms_template")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_sms_template(source_conn, target_conn)
        elif target_name=='sns_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_sns_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_sns_relation(source_conn, target_conn)
        elif target_name=='sns_relation_merged':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_sns_relation_merged")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_sns_relation_merged(source_conn, target_conn)
        elif target_name=='stripe_currency':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_stripe_currency")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_stripe_currency(source_conn, target_conn)
        elif target_name=='subject_subscription':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_subject_subscription")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_subject_subscription(source_conn, target_conn)
        elif target_name=='subscribed_campaign':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_subscribed_campaign")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_subscribed_campaign(source_conn, target_conn)
        elif target_name=='subscribed_event':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_subscribed_event")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_subscribed_event(source_conn, target_conn)
        elif target_name=='subscription_enterprise_request':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_subscription_enterprise_request")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_subscription_enterprise_request(source_conn, target_conn)
        elif target_name=='suppression':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_suppression")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_suppression(source_conn, target_conn)
        elif target_name=='survey_recipient_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_survey_recipient_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_survey_recipient_relation(source_conn, target_conn)
        elif target_name=='system_email_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_system_email_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_system_email_track(source_conn, target_conn)
        elif target_name=='task':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_task")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_task(source_conn, target_conn)
        elif target_name=='task_assignee':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_task_assignee")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_task_assignee(source_conn, target_conn)
        elif target_name=='task_update':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_task_update")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_task_update(source_conn, target_conn)
        elif target_name=='temporary_user':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_temporary_user")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_temporary_user(source_conn, target_conn)
        elif target_name=='tenant_banner':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_tenant_banner")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_tenant_banner(source_conn, target_conn)
        elif target_name=='tenant_branding':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_tenant_branding")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_tenant_branding(source_conn, target_conn)
        elif target_name=='tenant_controlled_feature':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_tenant_controlled_feature")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_tenant_controlled_feature(source_conn, target_conn)
        elif target_name=='ticket':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket(source_conn, target_conn)
        elif target_name=='ticket_payment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_payment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_payment(source_conn, target_conn)
        elif target_name=='ticket_price_currency':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_price_currency")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_price_currency(source_conn, target_conn)
        elif target_name=='ticket_price_membership_type_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_price_membership_type_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_price_membership_type_relation(source_conn, target_conn)
        elif target_name=='ticket_sale':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_sale")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_sale(source_conn, target_conn)
        elif target_name=='ticket_sales_group':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_sales_group")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_sales_group(source_conn, target_conn)
        elif target_name=='ticket_sales_transaction':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_sales_transaction")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_sales_transaction(source_conn, target_conn)
        elif target_name=='ticket_sales_transaction_invoice':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_sales_transaction_invoice")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_sales_transaction_invoice(source_conn, target_conn)
        elif target_name=='ticket_sales_transaction_status':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_ticket_sales_transaction_status")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_ticket_sales_transaction_status(source_conn, target_conn)
        elif target_name=='translation_language_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_translation_language_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_translation_language_relation(source_conn, target_conn)
        elif target_name=='user':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user(source_conn, target_conn)
        elif target_name=='user_account':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_account")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_account(source_conn, target_conn)
        elif target_name=='user_ctx':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_ctx")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_ctx(source_conn, target_conn)
        elif target_name=='user_device':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_device")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_device(source_conn, target_conn)
        elif target_name=='user_event_agenda':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_event_agenda")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_event_agenda(source_conn, target_conn)
        elif target_name=='user_fapiao':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_fapiao")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_fapiao(source_conn, target_conn)
        elif target_name=='user_feed':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_feed")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_feed(source_conn, target_conn)
        elif target_name=='user_highlighted_event':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_highlighted_event")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_highlighted_event(source_conn, target_conn)
        elif target_name=='user_like_devent':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_like_devent")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_like_devent(source_conn, target_conn)
        elif target_name=='user_login_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_login_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_login_track(source_conn, target_conn)
        elif target_name=='user_merged':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_merged")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_merged(source_conn, target_conn)
        elif target_name=='user_notification_log':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_notification_log")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_notification_log(source_conn, target_conn)
        elif target_name=='user_profile':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_profile")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_profile(source_conn, target_conn)
        elif target_name=='user_profile_business_interest':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_profile_business_interest")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_profile_business_interest(source_conn, target_conn)
        elif target_name=='user_profile_experience':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_profile_experience")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_profile_experience(source_conn, target_conn)
        elif target_name=='user_profile_language_proficiency':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_profile_language_proficiency")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_profile_language_proficiency(source_conn, target_conn)
        elif target_name=='user_profile_picture':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_profile_picture")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_profile_picture(source_conn, target_conn)
        elif target_name=='user_profile_skill':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_profile_skill")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_profile_skill(source_conn, target_conn)
        elif target_name=='user_profile_summary':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_profile_summary")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_profile_summary(source_conn, target_conn)
        elif target_name=='user_relation':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_relation")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_relation(source_conn, target_conn)
        elif target_name=='user_saved_event':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_saved_event")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_saved_event(source_conn, target_conn)
        elif target_name=='user_sync_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_user_sync_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_user_sync_track(source_conn, target_conn)
        elif target_name=='xero_account_mapping':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_account_mapping")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_account_mapping(source_conn, target_conn)
        elif target_name=='xero_auth':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_auth")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_auth(source_conn, target_conn)
        elif target_name=='xero_auth2':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_auth2")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_auth2(source_conn, target_conn)
        elif target_name=='xero_auth_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_auth_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_auth_track(source_conn, target_conn)
        elif target_name=='xero_company':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_company")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_company(source_conn, target_conn)
        elif target_name=='xero_contact':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_contact")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_contact(source_conn, target_conn)
        elif target_name=='xero_currency_mapping':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_currency_mapping")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_currency_mapping(source_conn, target_conn)
        elif target_name=='xero_invoice':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_invoice")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_invoice(source_conn, target_conn)
        elif target_name=='xero_invoice_attachments':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_invoice_attachments")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_invoice_attachments(source_conn, target_conn)
        elif target_name=='xero_limiter':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_limiter")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_limiter(source_conn, target_conn)
        elif target_name=='xero_payment':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_payment")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_payment(source_conn, target_conn)
        elif target_name=='xero_pull_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_pull_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_pull_track(source_conn, target_conn)
        elif target_name=='xero_sync_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_xero_sync_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_xero_sync_track(source_conn, target_conn)
        elif target_name=='zoom_sync_track':
            print("{}".format(target_name))
            curs2 = source_conn.cursor(buffered=True)
            curs2.execute("select count(*) as cntr FROM  vwz_zoom_sync_track")
            rows2 = curs2.fetchone()
            for row2 in rows2:
                row2=rows2[0]
                if row2>0 : move_vwz_zoom_sync_track(source_conn, target_conn)
        else: print("Unknown table, check the name:{}".format(target_name))
        # ===============  end of if  =====================

    curs2.close()
finally: source_conn.close()
