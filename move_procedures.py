from datetime import date, datetime, timedelta

## ================= begin move_vwz_accreditation =====================
def move_vwz_accreditation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, name, deadline, deadline_timezone, abs_deadline, total_hours, completed_hours, classification, created_on, created_by, last_modified, last_modified_by from vwz_accreditation")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        name = source_row[2]
        deadline = source_row[3]
        deadline_timezone = source_row[4]
        abs_deadline = source_row[5]
        total_hours = source_row[6]
        completed_hours = source_row[7]
        classification = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_accreditation(id, user_id, name, deadline, deadline_timezone, abs_deadline, total_hours, completed_hours, classification, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(name)s,%(deadline)s,%(deadline_timezone)s,%(abs_deadline)s,%(total_hours)s,%(completed_hours)s,%(classification)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'name':name,
                'deadline':deadline,
                'deadline_timezone':deadline_timezone,
                'abs_deadline':abs_deadline,
                'total_hours':total_hours,
                'completed_hours':completed_hours,
                'classification':classification,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_accreditation =====================
## ================= begin move_vwz_accreditation_credit =====================
def move_vwz_accreditation_credit(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, accreditation_id, user_id, name, description, attended_on, attended_timezone, abs_attended_on, hours, created_on, created_by, last_modified, last_modified_by from vwz_accreditation_credit")
    for source_row in source_cursor:
        id = source_row[0]
        accreditation_id = source_row[1]
        user_id = source_row[2]
        name = source_row[3]
        description = source_row[4]
        attended_on = source_row[5]
        attended_timezone = source_row[6]
        abs_attended_on = source_row[7]
        hours = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_accreditation_credit(id, accreditation_id, user_id, name, description, attended_on, attended_timezone, abs_attended_on, hours, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(accreditation_id)s,%(user_id)s,%(name)s,%(description)s,%(attended_on)s,%(attended_timezone)s,%(abs_attended_on)s,%(hours)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'accreditation_id':accreditation_id,
                'user_id':user_id,
                'name':name,
                'description':description,
                'attended_on':attended_on,
                'attended_timezone':attended_timezone,
                'abs_attended_on':abs_attended_on,
                'hours':hours,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_accreditation_credit =====================
## ================= begin move_vwz_accreditation_credit_document =====================
def move_vwz_accreditation_credit_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, credit_id, bucket_id, created_on from vwz_accreditation_credit_document")
    for source_row in source_cursor:
        id = source_row[0]
        credit_id = source_row[1]
        bucket_id = source_row[2]
        created_on = source_row[3]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_accreditation_credit_document(id, credit_id, bucket_id, created_on) "
              "values(%(id)s,%(credit_id)s,%(bucket_id)s,%(created_on)s)")
        data = {  
                'id':id,
                'credit_id':credit_id,
                'bucket_id':bucket_id,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_accreditation_credit_document =====================
## ================= begin move_vwz_adapay_autosplit_track =====================
def move_vwz_adapay_autosplit_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, split_time, status, failure_reason, created_on, last_modified from vwz_adapay_autosplit_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        split_time = source_row[2]
        status = source_row[3]
        failure_reason = source_row[4]
        created_on = source_row[5]
        last_modified = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_adapay_autosplit_track(id, organization_id, split_time, status, failure_reason, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(split_time)s,%(status)s,%(failure_reason)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'split_time':split_time,
                'status':status,
                'failure_reason':failure_reason,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_adapay_autosplit_track =====================
## ================= begin move_vwz_adapay_refund =====================
def move_vwz_adapay_refund(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, adapay_payment_id, order_id, pay_channel, amount, username, status, request, response, refund_id, callback, created_on, created_by from vwz_adapay_refund")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        adapay_payment_id = source_row[2]
        order_id = source_row[3]
        pay_channel = source_row[4]
        amount = source_row[5]
        username = source_row[6]
        status = source_row[7]
        request = source_row[8]
        response = source_row[9]
        refund_id = source_row[10]
        callback = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_adapay_refund(id, organization_id, adapay_payment_id, order_id, pay_channel, amount, username, status, request, response, refund_id, callback, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(adapay_payment_id)s,%(order_id)s,%(pay_channel)s,%(amount)s,%(username)s,%(status)s,%(request)s,%(response)s,%(refund_id)s,%(callback)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'adapay_payment_id':adapay_payment_id,
                'order_id':order_id,
                'pay_channel':pay_channel,
                'amount':amount,
                'username':username,
                'status':status,
                'request':request,
                'response':response,
                'refund_id':refund_id,
                'callback':callback,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_adapay_refund =====================
## ================= begin move_vwz_adapay_split =====================
def move_vwz_adapay_split(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, adapay_payment_id, order_id, pay_channel, amount, username, status, request, response, callback, process_end_on, split_track_id, created_on, created_by from vwz_adapay_split")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        adapay_payment_id = source_row[2]
        order_id = source_row[3]
        pay_channel = source_row[4]
        amount = source_row[5]
        username = source_row[6]
        status = source_row[7]
        request = source_row[8]
        response = source_row[9]
        callback = source_row[10]
        process_end_on = source_row[11]
        split_track_id = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_adapay_split(id, organization_id, adapay_payment_id, order_id, pay_channel, amount, username, status, request, response, callback, process_end_on, split_track_id, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(adapay_payment_id)s,%(order_id)s,%(pay_channel)s,%(amount)s,%(username)s,%(status)s,%(request)s,%(response)s,%(callback)s,%(process_end_on)s,%(split_track_id)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'adapay_payment_id':adapay_payment_id,
                'order_id':order_id,
                'pay_channel':pay_channel,
                'amount':amount,
                'username':username,
                'status':status,
                'request':request,
                'response':response,
                'callback':callback,
                'process_end_on':process_end_on,
                'split_track_id':split_track_id,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_adapay_split =====================
## ================= begin move_vwz_analytics_company_track =====================
def move_vwz_analytics_company_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, company_id, company_name, first_relation_id, second_relation_id, activity_time, activity_type, positive, created_on, created_by, last_modified, last_modified_by from vwz_analytics_company_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        company_id = source_row[2]
        company_name = source_row[3]
        first_relation_id = source_row[4]
        second_relation_id = source_row[5]
        activity_time = source_row[6]
        activity_type = source_row[7]
        positive = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_analytics_company_track(id, organization_id, company_id, company_name, first_relation_id, second_relation_id, activity_time, activity_type, positive, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(company_id)s,%(company_name)s,%(first_relation_id)s,%(second_relation_id)s,%(activity_time)s,%(activity_type)s,%(positive)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'company_id':company_id,
                'company_name':company_name,
                'first_relation_id':first_relation_id,
                'second_relation_id':second_relation_id,
                'activity_time':activity_time,
                'activity_type':activity_type,
                'positive':positive,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_analytics_company_track =====================
## ================= begin move_vwz_analytics_contact_track =====================
def move_vwz_analytics_contact_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, contact_id, email, company_id, company_name, first_relation_id, second_relation_id, activity_time, activity_type, positive, created_on, created_by, last_modified, last_modified_by from vwz_analytics_contact_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        contact_id = source_row[2]
        email = source_row[3]
        company_id = source_row[4]
        company_name = source_row[5]
        first_relation_id = source_row[6]
        second_relation_id = source_row[7]
        activity_time = source_row[8]
        activity_type = source_row[9]
        positive = source_row[10]
        created_on = source_row[11]
        created_by = source_row[12]
        last_modified = source_row[13]
        last_modified_by = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_analytics_contact_track(id, organization_id, contact_id, email, company_id, company_name, first_relation_id, second_relation_id, activity_time, activity_type, positive, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(contact_id)s,%(email)s,%(company_id)s,%(company_name)s,%(first_relation_id)s,%(second_relation_id)s,%(activity_time)s,%(activity_type)s,%(positive)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'contact_id':contact_id,
                'email':email,
                'company_id':company_id,
                'company_name':company_name,
                'first_relation_id':first_relation_id,
                'second_relation_id':second_relation_id,
                'activity_time':activity_time,
                'activity_type':activity_type,
                'positive':positive,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_analytics_contact_track =====================
## ================= begin move_vwz_analytics_request =====================
def move_vwz_analytics_request(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, type, state, start_process_time, end_process_time, end_export_time, message, retry_times, created_on, created_by, last_modified, last_modified_by from vwz_analytics_request")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        type = source_row[2]
        state = source_row[3]
        start_process_time = source_row[4]
        end_process_time = source_row[5]
        end_export_time = source_row[6]
        message = source_row[7]
        retry_times = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_analytics_request(id, organization_id, type, state, start_process_time, end_process_time, end_export_time, message, retry_times, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(type)s,%(state)s,%(start_process_time)s,%(end_process_time)s,%(end_export_time)s,%(message)s,%(retry_times)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'type':type,
                'state':state,
                'start_process_time':start_process_time,
                'end_process_time':end_process_time,
                'end_export_time':end_export_time,
                'message':message,
                'retry_times':retry_times,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_analytics_request =====================
## ================= begin move_vwz_analytics_user_track =====================
def move_vwz_analytics_user_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, first_relation_id, second_relation_id, activity_time, activity_type, positive, created_on, created_by, last_modified, last_modified_by from vwz_analytics_user_track")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        first_relation_id = source_row[2]
        second_relation_id = source_row[3]
        activity_time = source_row[4]
        activity_type = source_row[5]
        positive = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_analytics_user_track(id, user_id, first_relation_id, second_relation_id, activity_time, activity_type, positive, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(first_relation_id)s,%(second_relation_id)s,%(activity_time)s,%(activity_type)s,%(positive)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'first_relation_id':first_relation_id,
                'second_relation_id':second_relation_id,
                'activity_time':activity_time,
                'activity_type':activity_type,
                'positive':positive,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_analytics_user_track =====================
## ================= begin move_vwz_app =====================
def move_vwz_app(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, app_id, app_type, app_platform, app_name, pushy_key, created_on from vwz_app")
    for source_row in source_cursor:
        id = source_row[0]
        app_id = source_row[1]
        app_type = source_row[2]
        app_platform = source_row[3]
        app_name = source_row[4]
        pushy_key = source_row[5]
        created_on = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_app(id, app_id, app_type, app_platform, app_name, pushy_key, created_on) "
              "values(%(id)s,%(app_id)s,%(app_type)s,%(app_platform)s,%(app_name)s,%(pushy_key)s,%(created_on)s)")
        data = {  
                'id':id,
                'app_id':app_id,
                'app_type':app_type,
                'app_platform':app_platform,
                'app_name':app_name,
                'pushy_key':pushy_key,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_app =====================
## ================= begin move_vwz_app_store =====================
def move_vwz_app_store(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, app_id, store, store_url, created_on from vwz_app_store")
    for source_row in source_cursor:
        id = source_row[0]
        app_id = source_row[1]
        store = source_row[2]
        store_url = source_row[3]
        created_on = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_app_store(id, app_id, store, store_url, created_on) "
              "values(%(id)s,%(app_id)s,%(store)s,%(store_url)s,%(created_on)s)")
        data = {  
                'id':id,
                'app_id':app_id,
                'store':store,
                'store_url':store_url,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_app_store =====================
## ================= begin move_vwz_app_versionlog =====================
def move_vwz_app_versionlog(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, app_id, version, mandatory, platform, store, valid, description, created_on from vwz_app_versionlog")
    for source_row in source_cursor:
        id = source_row[0]
        app_id = source_row[1]
        version = source_row[2]
        mandatory = source_row[3]
        platform = source_row[4]
        store = source_row[5]
        valid = source_row[6]
        description = source_row[7]
        created_on = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_app_versionlog(id, app_id, version, mandatory, platform, store, valid, description, created_on) "
              "values(%(id)s,%(app_id)s,%(version)s,%(mandatory)s,%(platform)s,%(store)s,%(valid)s,%(description)s,%(created_on)s)")
        data = {  
                'id':id,
                'app_id':app_id,
                'version':version,
                'mandatory':mandatory,
                'platform':platform,
                'store':store,
                'valid':valid,
                'description':description,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_app_versionlog =====================
## ================= begin move_vwz_attendee_notification =====================
def move_vwz_attendee_notification(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, event_id, reminder_id, attendee_id, given_name, family_name, language_code, email, sent_count, message_id, attachment_file_url, status, failure_reason, created_on from vwz_attendee_notification")
    for source_row in source_cursor:
        organization_id = source_row[0]
        event_id = source_row[1]
        reminder_id = source_row[2]
        attendee_id = source_row[3]
        given_name = source_row[4]
        family_name = source_row[5]
        language_code = source_row[6]
        email = source_row[7]
        sent_count = source_row[8]
        message_id = source_row[9]
        attachment_file_url = source_row[10]
        status = source_row[11]
        failure_reason = source_row[12]
        created_on = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_attendee_notification(organization_id, event_id, reminder_id, attendee_id, given_name, family_name, language_code, email, sent_count, message_id, attachment_file_url, status, failure_reason, created_on) "
              "values(%(organization_id)s,%(event_id)s,%(reminder_id)s,%(attendee_id)s,%(given_name)s,%(family_name)s,%(language_code)s,%(email)s,%(sent_count)s,%(message_id)s,%(attachment_file_url)s,%(status)s,%(failure_reason)s,%(created_on)s)")
        data = {  
                'organization_id':organization_id,
                'event_id':event_id,
                'reminder_id':reminder_id,
                'attendee_id':attendee_id,
                'given_name':given_name,
                'family_name':family_name,
                'language_code':language_code,
                'email':email,
                'sent_count':sent_count,
                'message_id':message_id,
                'attachment_file_url':attachment_file_url,
                'status':status,
                'failure_reason':failure_reason,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_attendee_notification =====================
## ================= begin move_vwz_billing_address =====================
def move_vwz_billing_address(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, object_type, object_id, organization_id, address_type, tax_id, company_name, phone, street_address, zip_code, city, province, country_code, created_on, created_by, last_modified, last_modified_by from vwz_billing_address")
    for source_row in source_cursor:
        id = source_row[0]
        object_type = source_row[1]
        object_id = source_row[2]
        organization_id = source_row[3]
        address_type = source_row[4]
        tax_id = source_row[5]
        company_name = source_row[6]
        phone = source_row[7]
        street_address = source_row[8]
        zip_code = source_row[9]
        city = source_row[10]
        province = source_row[11]
        country_code = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_billing_address(id, object_type, object_id, organization_id, address_type, tax_id, company_name, phone, street_address, zip_code, city, province, country_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(object_type)s,%(object_id)s,%(organization_id)s,%(address_type)s,%(tax_id)s,%(company_name)s,%(phone)s,%(street_address)s,%(zip_code)s,%(city)s,%(province)s,%(country_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'object_type':object_type,
                'object_id':object_id,
                'organization_id':organization_id,
                'address_type':address_type,
                'tax_id':tax_id,
                'company_name':company_name,
                'phone':phone,
                'street_address':street_address,
                'zip_code':zip_code,
                'city':city,
                'province':province,
                'country_code':country_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_billing_address =====================
## ================= begin move_vwz_checkin_point =====================
def move_vwz_checkin_point(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, name, description, capacity, color_index, main_point, main_check_in_mandatory, auto_check_in, agenda_type, agenda_id, index, created_on, created_by, last_modified, last_modified_by from vwz_checkin_point")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        name = source_row[3]
        description = source_row[4]
        capacity = source_row[5]
        color_index = source_row[6]
        main_point = source_row[7]
        main_check_in_mandatory = source_row[8]
        auto_check_in = source_row[9]
        agenda_type = source_row[10]
        agenda_id = source_row[11]
        index = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_checkin_point(id, organization_id, event_id, name, description, capacity, color_index, main_point, main_check_in_mandatory, auto_check_in, agenda_type, agenda_id, index, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(name)s,%(description)s,%(capacity)s,%(color_index)s,%(main_point)s,%(main_check_in_mandatory)s,%(auto_check_in)s,%(agenda_type)s,%(agenda_id)s,%(index)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'name':name,
                'description':description,
                'capacity':capacity,
                'color_index':color_index,
                'main_point':main_point,
                'main_check_in_mandatory':main_check_in_mandatory,
                'auto_check_in':auto_check_in,
                'agenda_type':agenda_type,
                'agenda_id':agenda_id,
                'index':index,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_checkin_point =====================
## ================= begin move_vwz_checkin_point_ticket_relation =====================
def move_vwz_checkin_point_ticket_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select check_in_point_id, ticket_id, organization_id, event_id, created_on, created_by, last_modified, last_modified_by from vwz_checkin_point_ticket_relation")
    for source_row in source_cursor:
        check_in_point_id = source_row[0]
        ticket_id = source_row[1]
        organization_id = source_row[2]
        event_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_checkin_point_ticket_relation(check_in_point_id, ticket_id, organization_id, event_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(check_in_point_id)s,%(ticket_id)s,%(organization_id)s,%(event_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'check_in_point_id':check_in_point_id,
                'ticket_id':ticket_id,
                'organization_id':organization_id,
                'event_id':event_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_checkin_point_ticket_relation =====================
## ================= begin move_vwz_client_key =====================
def move_vwz_client_key(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select public_key, private_key, version, is_enabled, created_on, created_by, last_modified, last_modified_by from vwz_client_key")
    for source_row in source_cursor:
        public_key = source_row[0]
        private_key = source_row[1]
        version = source_row[2]
        is_enabled = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_client_key(public_key, private_key, version, is_enabled, created_on, created_by, last_modified, last_modified_by) "
              "values(%(public_key)s,%(private_key)s,%(version)s,%(is_enabled)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'public_key':public_key,
                'private_key':private_key,
                'version':version,
                'is_enabled':is_enabled,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_client_key =====================
## ================= begin move_vwz_community =====================
def move_vwz_community(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, name, sub_header, description, status, member_count, banner_bucket_id, created_on, created_by, last_modified_on, last_modified_by from vwz_community")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        name = source_row[2]
        sub_header = source_row[3]
        description = source_row[4]
        status = source_row[5]
        member_count = source_row[6]
        banner_bucket_id = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified_on = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community(id, organization_id, name, sub_header, description, status, member_count, banner_bucket_id, created_on, created_by, last_modified_on, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(name)s,%(sub_header)s,%(description)s,%(status)s,%(member_count)s,%(banner_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified_on)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'name':name,
                'sub_header':sub_header,
                'description':description,
                'status':status,
                'member_count':member_count,
                'banner_bucket_id':banner_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified_on':last_modified_on,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community =====================
## ================= begin move_vwz_community_comment =====================
def move_vwz_community_comment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, relation_type, relation_id, user_id, post_id, reaction_count, reply_count, text_content, parent_comment_id, parent_nodes, created_on, created_by, last_modified, last_modified_by from vwz_community_comment")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        relation_type = source_row[3]
        relation_id = source_row[4]
        user_id = source_row[5]
        post_id = source_row[6]
        reaction_count = source_row[7]
        reply_count = source_row[8]
        text_content = source_row[9]
        parent_comment_id = source_row[10]
        parent_nodes = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community_comment(id, organization_id, community_id, relation_type, relation_id, user_id, post_id, reaction_count, reply_count, text_content, parent_comment_id, parent_nodes, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(relation_type)s,%(relation_id)s,%(user_id)s,%(post_id)s,%(reaction_count)s,%(reply_count)s,%(text_content)s,%(parent_comment_id)s,%(parent_nodes)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'relation_type':relation_type,
                'relation_id':relation_id,
                'user_id':user_id,
                'post_id':post_id,
                'reaction_count':reaction_count,
                'reply_count':reply_count,
                'text_content':text_content,
                'parent_comment_id':parent_comment_id,
                'parent_nodes':parent_nodes,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community_comment =====================
## ================= begin move_vwz_community_document =====================
def move_vwz_community_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, relation_type, relation_id, user_id, object_type, object_id, document_type, bucket_id, created_on, created_by from vwz_community_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        relation_type = source_row[3]
        relation_id = source_row[4]
        user_id = source_row[5]
        object_type = source_row[6]
        object_id = source_row[7]
        document_type = source_row[8]
        bucket_id = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community_document(id, organization_id, community_id, relation_type, relation_id, user_id, object_type, object_id, document_type, bucket_id, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(relation_type)s,%(relation_id)s,%(user_id)s,%(object_type)s,%(object_id)s,%(document_type)s,%(bucket_id)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'relation_type':relation_type,
                'relation_id':relation_id,
                'user_id':user_id,
                'object_type':object_type,
                'object_id':object_id,
                'document_type':document_type,
                'bucket_id':bucket_id,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community_document =====================
## ================= begin move_vwz_community_group =====================
def move_vwz_community_group(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, name, description, banner_bucket_id, active, private, hidden, deleted, created_on, created_by, last_modified, last_modified_by from vwz_community_group")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        name = source_row[3]
        description = source_row[4]
        banner_bucket_id = source_row[5]
        active = source_row[6]
        private = source_row[7]
        hidden = source_row[8]
        deleted = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community_group(id, organization_id, community_id, name, description, banner_bucket_id, active, private, hidden, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(name)s,%(description)s,%(banner_bucket_id)s,%(active)s,%(private)s,%(hidden)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'name':name,
                'description':description,
                'banner_bucket_id':banner_bucket_id,
                'active':active,
                'private':private,
                'hidden':hidden,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community_group =====================
## ================= begin move_vwz_community_group_join_request =====================
def move_vwz_community_group_join_request(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, group_id, user_id, contact_id, status, created_on, created_by, last_modified_on, last_modified_by from vwz_community_group_join_request")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        group_id = source_row[3]
        user_id = source_row[4]
        contact_id = source_row[5]
        status = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified_on = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community_group_join_request(id, organization_id, community_id, group_id, user_id, contact_id, status, created_on, created_by, last_modified_on, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(group_id)s,%(user_id)s,%(contact_id)s,%(status)s,%(created_on)s,%(created_by)s,%(last_modified_on)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'group_id':group_id,
                'user_id':user_id,
                'contact_id':contact_id,
                'status':status,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified_on':last_modified_on,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community_group_join_request =====================
## ================= begin move_vwz_community_group_member_relation =====================
def move_vwz_community_group_member_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, group_id, contact_id, deleted, created_on, created_by, last_modified_on, last_modified_by from vwz_community_group_member_relation")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        group_id = source_row[3]
        contact_id = source_row[4]
        deleted = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified_on = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community_group_member_relation(id, organization_id, community_id, group_id, contact_id, deleted, created_on, created_by, last_modified_on, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(group_id)s,%(contact_id)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified_on)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'group_id':group_id,
                'contact_id':contact_id,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified_on':last_modified_on,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community_group_member_relation =====================
## ================= begin move_vwz_community_post =====================
def move_vwz_community_post(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, relation_type, relation_id, user_id, reaction_count, comment_count, share_count, popularity, text_content, file_share, created_on, created_by, last_modified, last_modified_by from vwz_community_post")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        relation_type = source_row[3]
        relation_id = source_row[4]
        user_id = source_row[5]
        reaction_count = source_row[6]
        comment_count = source_row[7]
        share_count = source_row[8]
        popularity = source_row[9]
        text_content = source_row[10]
        file_share = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community_post(id, organization_id, community_id, relation_type, relation_id, user_id, reaction_count, comment_count, share_count, popularity, text_content, file_share, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(relation_type)s,%(relation_id)s,%(user_id)s,%(reaction_count)s,%(comment_count)s,%(share_count)s,%(popularity)s,%(text_content)s,%(file_share)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'relation_type':relation_type,
                'relation_id':relation_id,
                'user_id':user_id,
                'reaction_count':reaction_count,
                'comment_count':comment_count,
                'share_count':share_count,
                'popularity':popularity,
                'text_content':text_content,
                'file_share':file_share,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community_post =====================
## ================= begin move_vwz_community_reaction =====================
def move_vwz_community_reaction(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, relation_type, relation_id, user_id, object_type, object_id, action, created_on, created_by from vwz_community_reaction")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        relation_type = source_row[3]
        relation_id = source_row[4]
        user_id = source_row[5]
        object_type = source_row[6]
        object_id = source_row[7]
        action = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_community_reaction(id, organization_id, community_id, relation_type, relation_id, user_id, object_type, object_id, action, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(relation_type)s,%(relation_id)s,%(user_id)s,%(object_type)s,%(object_id)s,%(action)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'relation_type':relation_type,
                'relation_id':relation_id,
                'user_id':user_id,
                'object_type':object_type,
                'object_id':object_id,
                'action':action,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_community_reaction =====================
## ================= begin move_vwz_contact_bak =====================
def move_vwz_contact_bak(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, language_code, is_lead, lead_since, pic_bucket_id, assignee, given_name, family_name, email, company_name, position_title, phone, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, industry_code, is_member, subscribed, created_on, created_by, last_modified, last_modified_by from vwz_contact_bak")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        language_code = source_row[2]
        is_lead = source_row[3]
        lead_since = source_row[4]
        pic_bucket_id = source_row[5]
        assignee = source_row[6]
        given_name = source_row[7]
        family_name = source_row[8]
        email = source_row[9]
        company_name = source_row[10]
        position_title = source_row[11]
        phone = source_row[12]
        street_address = source_row[13]
        city_code = source_row[14]
        city_name = source_row[15]
        country_code = source_row[16]
        country_name = source_row[17]
        city_name_text = source_row[18]
        province = source_row[19]
        zip_code = source_row[20]
        industry_code = source_row[21]
        is_member = source_row[22]
        subscribed = source_row[23]
        created_on = source_row[24]
        created_by = source_row[25]
        last_modified = source_row[26]
        last_modified_by = source_row[27]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_contact_bak(id, organization_id, language_code, is_lead, lead_since, pic_bucket_id, assignee, given_name, family_name, email, company_name, position_title, phone, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, industry_code, is_member, subscribed, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(language_code)s,%(is_lead)s,%(lead_since)s,%(pic_bucket_id)s,%(assignee)s,%(given_name)s,%(family_name)s,%(email)s,%(company_name)s,%(position_title)s,%(phone)s,%(street_address)s,%(city_code)s,%(city_name)s,%(country_code)s,%(country_name)s,%(city_name_text)s,%(province)s,%(zip_code)s,%(industry_code)s,%(is_member)s,%(subscribed)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'language_code':language_code,
                'is_lead':is_lead,
                'lead_since':lead_since,
                'pic_bucket_id':pic_bucket_id,
                'assignee':assignee,
                'given_name':given_name,
                'family_name':family_name,
                'email':email,
                'company_name':company_name,
                'position_title':position_title,
                'phone':phone,
                'street_address':street_address,
                'city_code':city_code,
                'city_name':city_name,
                'country_code':country_code,
                'country_name':country_name,
                'city_name_text':city_name_text,
                'province':province,
                'zip_code':zip_code,
                'industry_code':industry_code,
                'is_member':is_member,
                'subscribed':subscribed,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_contact_bak =====================
## ================= begin move_vwz_contact_delta =====================
def move_vwz_contact_delta(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select old_id, new_id, organization_id, language_code, is_lead, lead_since, pic_bucket_id, assignee, given_name, family_name, email, company_name, position_title, phone, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, industry_code, is_member, subscribed, created_on, created_by, last_modified, last_modified_by from vwz_contact_delta")
    for source_row in source_cursor:
        old_id = source_row[0]
        new_id = source_row[1]
        organization_id = source_row[2]
        language_code = source_row[3]
        is_lead = source_row[4]
        lead_since = source_row[5]
        pic_bucket_id = source_row[6]
        assignee = source_row[7]
        given_name = source_row[8]
        family_name = source_row[9]
        email = source_row[10]
        company_name = source_row[11]
        position_title = source_row[12]
        phone = source_row[13]
        street_address = source_row[14]
        city_code = source_row[15]
        city_name = source_row[16]
        country_code = source_row[17]
        country_name = source_row[18]
        city_name_text = source_row[19]
        province = source_row[20]
        zip_code = source_row[21]
        industry_code = source_row[22]
        is_member = source_row[23]
        subscribed = source_row[24]
        created_on = source_row[25]
        created_by = source_row[26]
        last_modified = source_row[27]
        last_modified_by = source_row[28]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_contact_delta(old_id, new_id, organization_id, language_code, is_lead, lead_since, pic_bucket_id, assignee, given_name, family_name, email, company_name, position_title, phone, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, industry_code, is_member, subscribed, created_on, created_by, last_modified, last_modified_by) "
              "values(%(old_id)s,%(new_id)s,%(organization_id)s,%(language_code)s,%(is_lead)s,%(lead_since)s,%(pic_bucket_id)s,%(assignee)s,%(given_name)s,%(family_name)s,%(email)s,%(company_name)s,%(position_title)s,%(phone)s,%(street_address)s,%(city_code)s,%(city_name)s,%(country_code)s,%(country_name)s,%(city_name_text)s,%(province)s,%(zip_code)s,%(industry_code)s,%(is_member)s,%(subscribed)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'old_id':old_id,
                'new_id':new_id,
                'organization_id':organization_id,
                'language_code':language_code,
                'is_lead':is_lead,
                'lead_since':lead_since,
                'pic_bucket_id':pic_bucket_id,
                'assignee':assignee,
                'given_name':given_name,
                'family_name':family_name,
                'email':email,
                'company_name':company_name,
                'position_title':position_title,
                'phone':phone,
                'street_address':street_address,
                'city_code':city_code,
                'city_name':city_name,
                'country_code':country_code,
                'country_name':country_name,
                'city_name_text':city_name_text,
                'province':province,
                'zip_code':zip_code,
                'industry_code':industry_code,
                'is_member':is_member,
                'subscribed':subscribed,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_contact_delta =====================
## ================= begin move_vwz_coupon =====================
def move_vwz_coupon(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, language_code, organization_id, event_id, title, code, discount_percentage, limit, quantity_available, start_date_time, expiry, is_active, is_deleted, created_on, created_by, last_modified, last_modified_by from vwz_coupon")
    for source_row in source_cursor:
        id = source_row[0]
        language_code = source_row[1]
        organization_id = source_row[2]
        event_id = source_row[3]
        title = source_row[4]
        code = source_row[5]
        discount_percentage = source_row[6]
        limit = source_row[7]
        quantity_available = source_row[8]
        start_date_time = source_row[9]
        expiry = source_row[10]
        is_active = source_row[11]
        is_deleted = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_coupon(id, language_code, organization_id, event_id, title, code, discount_percentage, limit, quantity_available, start_date_time, expiry, is_active, is_deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(language_code)s,%(organization_id)s,%(event_id)s,%(title)s,%(code)s,%(discount_percentage)s,%(limit)s,%(quantity_available)s,%(start_date_time)s,%(expiry)s,%(is_active)s,%(is_deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'language_code':language_code,
                'organization_id':organization_id,
                'event_id':event_id,
                'title':title,
                'code':code,
                'discount_percentage':discount_percentage,
                'limit':limit,
                'quantity_available':quantity_available,
                'start_date_time':start_date_time,
                'expiry':expiry,
                'is_active':is_active,
                'is_deleted':is_deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_coupon =====================
## ================= begin move_vwz_coupon_ticket_relation =====================
def move_vwz_coupon_ticket_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, coupon_id, ticket_id, organization_id, event_id, is_deleted, created_on, created_by, last_modified, last_modified_by from vwz_coupon_ticket_relation")
    for source_row in source_cursor:
        id = source_row[0]
        coupon_id = source_row[1]
        ticket_id = source_row[2]
        organization_id = source_row[3]
        event_id = source_row[4]
        is_deleted = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_coupon_ticket_relation(id, coupon_id, ticket_id, organization_id, event_id, is_deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(coupon_id)s,%(ticket_id)s,%(organization_id)s,%(event_id)s,%(is_deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'coupon_id':coupon_id,
                'ticket_id':ticket_id,
                'organization_id':organization_id,
                'event_id':event_id,
                'is_deleted':is_deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_coupon_ticket_relation =====================
## ================= begin move_vwz_crm_document =====================
def move_vwz_crm_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, object_id, object_type, document_bucket_id, author_id, created_on, created_by, last_modified, last_modified_by from vwz_crm_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        object_id = source_row[2]
        object_type = source_row[3]
        document_bucket_id = source_row[4]
        author_id = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_crm_document(id, organization_id, object_id, object_type, document_bucket_id, author_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(object_id)s,%(object_type)s,%(document_bucket_id)s,%(author_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'object_id':object_id,
                'object_type':object_type,
                'document_bucket_id':document_bucket_id,
                'author_id':author_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_crm_document =====================
## ================= begin move_vwz_crm_opportunity =====================
def move_vwz_crm_opportunity(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, id, name, description, status, stage_id, currency_code, currency_value, expected_date, assignee, assignee_name, company_id, company_name, contact_id, given_name, family_name, contact_company_id, source, is_deleted, sort_index, created_on, created_by, last_modified, last_modified_by from vwz_crm_opportunity")
    for source_row in source_cursor:
        organization_id = source_row[0]
        id = source_row[1]
        name = source_row[2]
        description = source_row[3]
        status = source_row[4]
        stage_id = source_row[5]
        currency_code = source_row[6]
        currency_value = source_row[7]
        expected_date = source_row[8]
        assignee = source_row[9]
        assignee_name = source_row[10]
        company_id = source_row[11]
        company_name = source_row[12]
        contact_id = source_row[13]
        given_name = source_row[14]
        family_name = source_row[15]
        contact_company_id = source_row[16]
        source = source_row[17]
        is_deleted = source_row[18]
        sort_index = source_row[19]
        created_on = source_row[20]
        created_by = source_row[21]
        last_modified = source_row[22]
        last_modified_by = source_row[23]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_crm_opportunity(organization_id, id, name, description, status, stage_id, currency_code, currency_value, expected_date, assignee, assignee_name, company_id, company_name, contact_id, given_name, family_name, contact_company_id, source, is_deleted, sort_index, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(id)s,%(name)s,%(description)s,%(status)s,%(stage_id)s,%(currency_code)s,%(currency_value)s,%(expected_date)s,%(assignee)s,%(assignee_name)s,%(company_id)s,%(company_name)s,%(contact_id)s,%(given_name)s,%(family_name)s,%(contact_company_id)s,%(source)s,%(is_deleted)s,%(sort_index)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'id':id,
                'name':name,
                'description':description,
                'status':status,
                'stage_id':stage_id,
                'currency_code':currency_code,
                'currency_value':currency_value,
                'expected_date':expected_date,
                'assignee':assignee,
                'assignee_name':assignee_name,
                'company_id':company_id,
                'company_name':company_name,
                'contact_id':contact_id,
                'given_name':given_name,
                'family_name':family_name,
                'contact_company_id':contact_company_id,
                'source':source,
                'is_deleted':is_deleted,
                'sort_index':sort_index,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_crm_opportunity =====================
## ================= begin move_vwz_crm_opportunity_company_contacts =====================
def move_vwz_crm_opportunity_company_contacts(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, opportunity_id, contact_id, given_name, family_name, created_on, created_by, last_modified, last_modified_by from vwz_crm_opportunity_company_contacts")
    for source_row in source_cursor:
        organization_id = source_row[0]
        opportunity_id = source_row[1]
        contact_id = source_row[2]
        given_name = source_row[3]
        family_name = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_crm_opportunity_company_contacts(organization_id, opportunity_id, contact_id, given_name, family_name, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(opportunity_id)s,%(contact_id)s,%(given_name)s,%(family_name)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'opportunity_id':opportunity_id,
                'contact_id':contact_id,
                'given_name':given_name,
                'family_name':family_name,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_crm_opportunity_company_contacts =====================
## ================= begin move_vwz_crm_opportunity_products =====================
def move_vwz_crm_opportunity_products(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, opportunity_id, product_id, created_on, created_by, last_modified, last_modified_by from vwz_crm_opportunity_products")
    for source_row in source_cursor:
        organization_id = source_row[0]
        opportunity_id = source_row[1]
        product_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_crm_opportunity_products(organization_id, opportunity_id, product_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(opportunity_id)s,%(product_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'opportunity_id':opportunity_id,
                'product_id':product_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_crm_opportunity_products =====================
## ================= begin move_vwz_crm_product =====================
def move_vwz_crm_product(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, id, name, is_deleted, created_on, created_by, last_modified, last_modified_by from vwz_crm_product")
    for source_row in source_cursor:
        organization_id = source_row[0]
        id = source_row[1]
        name = source_row[2]
        is_deleted = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_crm_product(organization_id, id, name, is_deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(id)s,%(name)s,%(is_deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'id':id,
                'name':name,
                'is_deleted':is_deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_crm_product =====================
## ================= begin move_vwz_currency_exchange_rate =====================
def move_vwz_currency_exchange_rate(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, from_currency, to_currency, rate, expire_on, created_on from vwz_currency_exchange_rate")
    for source_row in source_cursor:
        id = source_row[0]
        from_currency = source_row[1]
        to_currency = source_row[2]
        rate = source_row[3]
        expire_on = source_row[4]
        created_on = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_currency_exchange_rate(id, from_currency, to_currency, rate, expire_on, created_on) "
              "values(%(id)s,%(from_currency)s,%(to_currency)s,%(rate)s,%(expire_on)s,%(created_on)s)")
        data = {  
                'id':id,
                'from_currency':from_currency,
                'to_currency':to_currency,
                'rate':rate,
                'expire_on':expire_on,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_currency_exchange_rate =====================
## ================= begin move_vwz_custom_email_document =====================
def move_vwz_custom_email_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, template_id, language_code, document_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_custom_email_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        template_id = source_row[2]
        language_code = source_row[3]
        document_bucket_id = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_custom_email_document(id, organization_id, template_id, language_code, document_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(template_id)s,%(language_code)s,%(document_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'template_id':template_id,
                'language_code':language_code,
                'document_bucket_id':document_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_custom_email_document =====================
## ================= begin move_vwz_custom_email_template_abstract =====================
def move_vwz_custom_email_template_abstract(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, type, default_language_code, status, reply_to, cc_to_assistant, enable_sending, delivery_option, is_default_content, created_on, created_by, last_modified, last_modified_by from vwz_custom_email_template_abstract")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        type = source_row[2]
        default_language_code = source_row[3]
        status = source_row[4]
        reply_to = source_row[5]
        cc_to_assistant = source_row[6]
        enable_sending = source_row[7]
        delivery_option = source_row[8]
        is_default_content = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_custom_email_template_abstract(id, organization_id, type, default_language_code, status, reply_to, cc_to_assistant, enable_sending, delivery_option, is_default_content, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(type)s,%(default_language_code)s,%(status)s,%(reply_to)s,%(cc_to_assistant)s,%(enable_sending)s,%(delivery_option)s,%(is_default_content)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'type':type,
                'default_language_code':default_language_code,
                'status':status,
                'reply_to':reply_to,
                'cc_to_assistant':cc_to_assistant,
                'enable_sending':enable_sending,
                'delivery_option':delivery_option,
                'is_default_content':is_default_content,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_custom_email_template_abstract =====================
## ================= begin move_vwz_custom_email_template_ctx =====================
def move_vwz_custom_email_template_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, template_id, language_code, subject, preview, pre_header_line, created_on, created_by, last_modified, last_modified_by from vwz_custom_email_template_ctx")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        template_id = source_row[2]
        language_code = source_row[3]
        subject = source_row[4]
        preview = source_row[5]
        pre_header_line = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_custom_email_template_ctx(id, organization_id, template_id, language_code, subject, preview, pre_header_line, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(template_id)s,%(language_code)s,%(subject)s,%(preview)s,%(pre_header_line)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'template_id':template_id,
                'language_code':language_code,
                'subject':subject,
                'preview':preview,
                'pre_header_line':pre_header_line,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_custom_email_template_ctx =====================
## ================= begin move_vwz_data_update_setting =====================
def move_vwz_data_update_setting(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, contact_auto_approve, company_auto_approve, contact_send_notification, company_send_notification, created_on, created_by, last_modified, last_modified_by from vwz_data_update_setting")
    for source_row in source_cursor:
        organization_id = source_row[0]
        contact_auto_approve = source_row[1]
        company_auto_approve = source_row[2]
        contact_send_notification = source_row[3]
        company_send_notification = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_data_update_setting(organization_id, contact_auto_approve, company_auto_approve, contact_send_notification, company_send_notification, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(contact_auto_approve)s,%(company_auto_approve)s,%(contact_send_notification)s,%(company_send_notification)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'contact_auto_approve':contact_auto_approve,
                'company_auto_approve':company_auto_approve,
                'contact_send_notification':contact_send_notification,
                'company_send_notification':company_send_notification,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_data_update_setting =====================
## ================= begin move_vwz_dm_user_block_list =====================
def move_vwz_dm_user_block_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select user_id, blocked_user_id, deleted, created_on, last_modified from vwz_dm_user_block_list")
    for source_row in source_cursor:
        user_id = source_row[0]
        blocked_user_id = source_row[1]
        deleted = source_row[2]
        created_on = source_row[3]
        last_modified = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_dm_user_block_list(user_id, blocked_user_id, deleted, created_on, last_modified) "
              "values(%(user_id)s,%(blocked_user_id)s,%(deleted)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'user_id':user_id,
                'blocked_user_id':blocked_user_id,
                'deleted':deleted,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_dm_user_block_list =====================
## ================= begin move_vwz_dm_user_contact_list =====================
def move_vwz_dm_user_contact_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select user_id, contact_user_id, deleted, created_on, last_modified from vwz_dm_user_contact_list")
    for source_row in source_cursor:
        user_id = source_row[0]
        contact_user_id = source_row[1]
        deleted = source_row[2]
        created_on = source_row[3]
        last_modified = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_dm_user_contact_list(user_id, contact_user_id, deleted, created_on, last_modified) "
              "values(%(user_id)s,%(contact_user_id)s,%(deleted)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'user_id':user_id,
                'contact_user_id':contact_user_id,
                'deleted':deleted,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_dm_user_contact_list =====================
## ================= begin move_vwz_document_bucket =====================
def move_vwz_document_bucket(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, absolute_path, uri, filename, content_type, size, deleted, created_on, created_by, last_modified, last_modified_by from vwz_document_bucket")
    for source_row in source_cursor:
        id = source_row[0]
        absolute_path = source_row[1]
        uri = source_row[2]
        filename = source_row[3]
        content_type = source_row[4]
        size = source_row[5]
        deleted = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_document_bucket(id, absolute_path, uri, filename, content_type, size, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(absolute_path)s,%(uri)s,%(filename)s,%(content_type)s,%(size)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'absolute_path':absolute_path,
                'uri':uri,
                'filename':filename,
                'content_type':content_type,
                'size':size,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_document_bucket =====================
## ================= begin move_vwz_edition_purchase_transaction =====================
def move_vwz_edition_purchase_transaction(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, currency_code, currency_value, edition_name, events_threshold, contacts_threshold, active_users_threshold, emails_threshold, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, created_on, created_by, last_modified_on, last_modified_by, organization_id from vwz_edition_purchase_transaction")
    for source_row in source_cursor:
        id = source_row[0]
        currency_code = source_row[1]
        currency_value = source_row[2]
        edition_name = source_row[3]
        events_threshold = source_row[4]
        contacts_threshold = source_row[5]
        active_users_threshold = source_row[6]
        emails_threshold = source_row[7]
        purchaser_given_name = source_row[8]
        purchaser_family_name = source_row[9]
        purchaser_email = source_row[10]
        purchaser_phone = source_row[11]
        purchaser_company_name = source_row[12]
        purchaser_position_title = source_row[13]
        purchaser_business_function_code = source_row[14]
        purchaser_business_role_code = source_row[15]
        created_on = source_row[16]
        created_by = source_row[17]
        last_modified_on = source_row[18]
        last_modified_by = source_row[19]
        organization_id = source_row[20]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edition_purchase_transaction(id, currency_code, currency_value, edition_name, events_threshold, contacts_threshold, active_users_threshold, emails_threshold, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, created_on, created_by, last_modified_on, last_modified_by, organization_id) "
              "values(%(id)s,%(currency_code)s,%(currency_value)s,%(edition_name)s,%(events_threshold)s,%(contacts_threshold)s,%(active_users_threshold)s,%(emails_threshold)s,%(purchaser_given_name)s,%(purchaser_family_name)s,%(purchaser_email)s,%(purchaser_phone)s,%(purchaser_company_name)s,%(purchaser_position_title)s,%(purchaser_business_function_code)s,%(purchaser_business_role_code)s,%(created_on)s,%(created_by)s,%(last_modified_on)s,%(last_modified_by)s,%(organization_id)s)")
        data = {  
                'id':id,
                'currency_code':currency_code,
                'currency_value':currency_value,
                'edition_name':edition_name,
                'events_threshold':events_threshold,
                'contacts_threshold':contacts_threshold,
                'active_users_threshold':active_users_threshold,
                'emails_threshold':emails_threshold,
                'purchaser_given_name':purchaser_given_name,
                'purchaser_family_name':purchaser_family_name,
                'purchaser_email':purchaser_email,
                'purchaser_phone':purchaser_phone,
                'purchaser_company_name':purchaser_company_name,
                'purchaser_position_title':purchaser_position_title,
                'purchaser_business_function_code':purchaser_business_function_code,
                'purchaser_business_role_code':purchaser_business_role_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified_on':last_modified_on,
                'last_modified_by':last_modified_by,
                'organization_id':organization_id
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edition_purchase_transaction =====================
## ================= begin move_vwz_edition_purchase_transaction_addon =====================
def move_vwz_edition_purchase_transaction_addon(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, transaction_id, feature, allotment, multiplier, created_on, created_by, last_modified_on, last_modified_by from vwz_edition_purchase_transaction_addon")
    for source_row in source_cursor:
        id = source_row[0]
        transaction_id = source_row[1]
        feature = source_row[2]
        allotment = source_row[3]
        multiplier = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified_on = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edition_purchase_transaction_addon(id, transaction_id, feature, allotment, multiplier, created_on, created_by, last_modified_on, last_modified_by) "
              "values(%(id)s,%(transaction_id)s,%(feature)s,%(allotment)s,%(multiplier)s,%(created_on)s,%(created_by)s,%(last_modified_on)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'transaction_id':transaction_id,
                'feature':feature,
                'allotment':allotment,
                'multiplier':multiplier,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified_on':last_modified_on,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edition_purchase_transaction_addon =====================
## ================= begin move_vwz_edition_purchase_transaction_status =====================
def move_vwz_edition_purchase_transaction_status(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, transaction_id, status, is_active, created_on, created_by, last_modified_on, last_modified_by from vwz_edition_purchase_transaction_status")
    for source_row in source_cursor:
        id = source_row[0]
        transaction_id = source_row[1]
        status = source_row[2]
        is_active = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified_on = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edition_purchase_transaction_status(id, transaction_id, status, is_active, created_on, created_by, last_modified_on, last_modified_by) "
              "values(%(id)s,%(transaction_id)s,%(status)s,%(is_active)s,%(created_on)s,%(created_by)s,%(last_modified_on)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'transaction_id':transaction_id,
                'status':status,
                'is_active':is_active,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified_on':last_modified_on,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edition_purchase_transaction_status =====================
## ================= begin move_vwz_edm_campaign_failure =====================
def move_vwz_edm_campaign_failure(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, edm_campaign_id, reason, status, created_by, created_on, last_modified_by, last_modified from vwz_edm_campaign_failure")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        edm_campaign_id = source_row[2]
        reason = source_row[3]
        status = source_row[4]
        created_by = source_row[5]
        created_on = source_row[6]
        last_modified_by = source_row[7]
        last_modified = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_campaign_failure(id, organization_id, edm_campaign_id, reason, status, created_by, created_on, last_modified_by, last_modified) "
              "values(%(id)s,%(organization_id)s,%(edm_campaign_id)s,%(reason)s,%(status)s,%(created_by)s,%(created_on)s,%(last_modified_by)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'edm_campaign_id':edm_campaign_id,
                'reason':reason,
                'status':status,
                'created_by':created_by,
                'created_on':created_on,
                'last_modified_by':last_modified_by,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_campaign_failure =====================
## ================= begin move_vwz_edm_document =====================
def move_vwz_edm_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, edm_template_id, document_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_edm_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        edm_template_id = source_row[2]
        document_bucket_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_document(id, organization_id, edm_template_id, document_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(edm_template_id)s,%(document_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'document_bucket_id':document_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_document =====================
## ================= begin move_vwz_edm_email_trace =====================
def move_vwz_edm_email_trace(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, message_id, organization_id, contact_id, email, edm_template_id, status, created_on, created_by, last_modified, last_modified_by, failure_reason from vwz_edm_email_trace")
    for source_row in source_cursor:
        id = source_row[0]
        message_id = source_row[1]
        organization_id = source_row[2]
        contact_id = source_row[3]
        email = source_row[4]
        edm_template_id = source_row[5]
        status = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        failure_reason = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_email_trace(id, message_id, organization_id, contact_id, email, edm_template_id, status, created_on, created_by, last_modified, last_modified_by, failure_reason) "
              "values(%(id)s,%(message_id)s,%(organization_id)s,%(contact_id)s,%(email)s,%(edm_template_id)s,%(status)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(failure_reason)s)")
        data = {  
                'id':id,
                'message_id':message_id,
                'organization_id':organization_id,
                'contact_id':contact_id,
                'email':email,
                'edm_template_id':edm_template_id,
                'status':status,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'failure_reason':failure_reason
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_email_trace =====================
## ================= begin move_vwz_edm_email_track =====================
def move_vwz_edm_email_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, edm_template_id, id, uid, message_id, event_id, contact_id, email, status, created_on, created_by, last_modified, last_modified_by, failure_reason, first_opened_on, first_clicked_on, open_count, click_count from vwz_edm_email_track")
    for source_row in source_cursor:
        organization_id = source_row[0]
        edm_template_id = source_row[1]
        id = source_row[2]
        uid = source_row[3]
        message_id = source_row[4]
        event_id = source_row[5]
        contact_id = source_row[6]
        email = source_row[7]
        status = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        failure_reason = source_row[13]
        first_opened_on = source_row[14]
        first_clicked_on = source_row[15]
        open_count = source_row[16]
        click_count = source_row[17]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_email_track(organization_id, edm_template_id, id, uid, message_id, event_id, contact_id, email, status, created_on, created_by, last_modified, last_modified_by, failure_reason, first_opened_on, first_clicked_on, open_count, click_count) "
              "values(%(organization_id)s,%(edm_template_id)s,%(id)s,%(uid)s,%(message_id)s,%(event_id)s,%(contact_id)s,%(email)s,%(status)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(failure_reason)s,%(first_opened_on)s,%(first_clicked_on)s,%(open_count)s,%(click_count)s)")
        data = {  
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'id':id,
                'uid':uid,
                'message_id':message_id,
                'event_id':event_id,
                'contact_id':contact_id,
                'email':email,
                'status':status,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'failure_reason':failure_reason,
                'first_opened_on':first_opened_on,
                'first_clicked_on':first_clicked_on,
                'open_count':open_count,
                'click_count':click_count
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_email_track =====================
## ================= begin move_vwz_edm_event_invitation_track =====================
def move_vwz_edm_event_invitation_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, event_id, email, sent, opened, created_on, created_by, last_modified, last_modified_by from vwz_edm_event_invitation_track")
    for source_row in source_cursor:
        organization_id = source_row[0]
        event_id = source_row[1]
        email = source_row[2]
        sent = source_row[3]
        opened = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_event_invitation_track(organization_id, event_id, email, sent, opened, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(event_id)s,%(email)s,%(sent)s,%(opened)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'event_id':event_id,
                'email':email,
                'sent':sent,
                'opened':opened,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_event_invitation_track =====================
## ================= begin move_vwz_edm_notification_track =====================
def move_vwz_edm_notification_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, edm_template_id, uid, email, contact_id, user_id, device_platform, device_token, event_id, status, failure_reason, created_on, created_by, last_modified, last_modified_by from vwz_edm_notification_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        edm_template_id = source_row[2]
        uid = source_row[3]
        email = source_row[4]
        contact_id = source_row[5]
        user_id = source_row[6]
        device_platform = source_row[7]
        device_token = source_row[8]
        event_id = source_row[9]
        status = source_row[10]
        failure_reason = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_notification_track(id, organization_id, edm_template_id, uid, email, contact_id, user_id, device_platform, device_token, event_id, status, failure_reason, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(edm_template_id)s,%(uid)s,%(email)s,%(contact_id)s,%(user_id)s,%(device_platform)s,%(device_token)s,%(event_id)s,%(status)s,%(failure_reason)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'uid':uid,
                'email':email,
                'contact_id':contact_id,
                'user_id':user_id,
                'device_platform':device_platform,
                'device_token':device_token,
                'event_id':event_id,
                'status':status,
                'failure_reason':failure_reason,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_notification_track =====================
## ================= begin move_vwz_edm_preview_email_trace =====================
def move_vwz_edm_preview_email_trace(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, message_id, organization_id, edm_template_id, email, status, created_on, created_by, last_modified, last_modified_by, failure_reason from vwz_edm_preview_email_trace")
    for source_row in source_cursor:
        id = source_row[0]
        message_id = source_row[1]
        organization_id = source_row[2]
        edm_template_id = source_row[3]
        email = source_row[4]
        status = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        failure_reason = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_preview_email_trace(id, message_id, organization_id, edm_template_id, email, status, created_on, created_by, last_modified, last_modified_by, failure_reason) "
              "values(%(id)s,%(message_id)s,%(organization_id)s,%(edm_template_id)s,%(email)s,%(status)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(failure_reason)s)")
        data = {  
                'id':id,
                'message_id':message_id,
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'email':email,
                'status':status,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'failure_reason':failure_reason
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_preview_email_trace =====================
## ================= begin move_vwz_edm_preview_email_track =====================
def move_vwz_edm_preview_email_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, edm_template_id, id, uid, message_id, event_id, contact_id, email, status, sent_type, created_on, created_by, last_modified, last_modified_by, failure_reason from vwz_edm_preview_email_track")
    for source_row in source_cursor:
        organization_id = source_row[0]
        edm_template_id = source_row[1]
        id = source_row[2]
        uid = source_row[3]
        message_id = source_row[4]
        event_id = source_row[5]
        contact_id = source_row[6]
        email = source_row[7]
        status = source_row[8]
        sent_type = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        failure_reason = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_preview_email_track(organization_id, edm_template_id, id, uid, message_id, event_id, contact_id, email, status, sent_type, created_on, created_by, last_modified, last_modified_by, failure_reason) "
              "values(%(organization_id)s,%(edm_template_id)s,%(id)s,%(uid)s,%(message_id)s,%(event_id)s,%(contact_id)s,%(email)s,%(status)s,%(sent_type)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(failure_reason)s)")
        data = {  
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'id':id,
                'uid':uid,
                'message_id':message_id,
                'event_id':event_id,
                'contact_id':contact_id,
                'email':email,
                'status':status,
                'sent_type':sent_type,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'failure_reason':failure_reason
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_preview_email_track =====================
## ================= begin move_vwz_edm_template =====================
def move_vwz_edm_template(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, campaign_name, subject, contacts_count, contact_count_refresh, from, reply_to, header, footer, language_code, content, content_zh, custom_html, status, organization_id, event_id, type, enable_utm, utm_source, is_archive, encoded, scheduler_time, time_zone_joda_id, send_notification, notification_status, notification_count, notification_reason, props, sent_by, edm_category_id, header_size, created_on, created_by, last_modified, last_modified_by, unsubscribe_contact_count, failure_reason from vwz_edm_template")
    for source_row in source_cursor:
        id = source_row[0]
        campaign_name = source_row[1]
        subject = source_row[2]
        contacts_count = source_row[3]
        contact_count_refresh = source_row[4]
        from_field = source_row[5]
        reply_to = source_row[6]
        header = source_row[7]
        footer = source_row[8]
        language_code = source_row[9]
        content = source_row[10]
        content_zh = source_row[11]
        custom_html = source_row[12]
        status = source_row[13]
        organization_id = source_row[14]
        event_id = source_row[15]
        type = source_row[16]
        enable_utm = source_row[17]
        utm_source = source_row[18]
        is_archive = source_row[19]
        encoded = source_row[20]
        scheduler_time = source_row[21]
        time_zone_joda_id = source_row[22]
        send_notification = source_row[23]
        notification_status = source_row[24]
        notification_count = source_row[25]
        notification_reason = source_row[26]
        props = source_row[27]
        sent_by = source_row[28]
        edm_category_id = source_row[29]
        header_size = source_row[30]
        created_on = source_row[31]
        created_by = source_row[32]
        last_modified = source_row[33]
        last_modified_by = source_row[34]
        unsubscribe_contact_count = source_row[35]
        failure_reason = source_row[36]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_template(id, campaign_name, subject, contacts_count, contact_count_refresh, from, reply_to, header, footer, language_code, content, content_zh, custom_html, status, organization_id, event_id, type, enable_utm, utm_source, is_archive, encoded, scheduler_time, time_zone_joda_id, send_notification, notification_status, notification_count, notification_reason, props, sent_by, edm_category_id, header_size, created_on, created_by, last_modified, last_modified_by, unsubscribe_contact_count, failure_reason) "
              "values(%(id)s,%(campaign_name)s,%(subject)s,%(contacts_count)s,%(contact_count_refresh)s,%(from)s,%(reply_to)s,%(header)s,%(footer)s,%(language_code)s,%(content)s,%(content_zh)s,%(custom_html)s,%(status)s,%(organization_id)s,%(event_id)s,%(type)s,%(enable_utm)s,%(utm_source)s,%(is_archive)s,%(encoded)s,%(scheduler_time)s,%(time_zone_joda_id)s,%(send_notification)s,%(notification_status)s,%(notification_count)s,%(notification_reason)s,%(props)s,%(sent_by)s,%(edm_category_id)s,%(header_size)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(unsubscribe_contact_count)s,%(failure_reason)s)")
        data = {  
                'id':id,
                'campaign_name':campaign_name,
                'subject':subject,
                'contacts_count':contacts_count,
                'contact_count_refresh':contact_count_refresh,
                'from':from_field,
                'reply_to':reply_to,
                'header':header,
                'footer':footer,
                'language_code':language_code,
                'content':content,
                'content_zh':content_zh,
                'custom_html':custom_html,
                'status':status,
                'organization_id':organization_id,
                'event_id':event_id,
                'type':type,
                'enable_utm':enable_utm,
                'utm_source':utm_source,
                'is_archive':is_archive,
                'encoded':encoded,
                'scheduler_time':scheduler_time,
                'time_zone_joda_id':time_zone_joda_id,
                'send_notification':send_notification,
                'notification_status':notification_status,
                'notification_count':notification_count,
                'notification_reason':notification_reason,
                'props':props,
                'sent_by':sent_by,
                'edm_category_id':edm_category_id,
                'header_size':header_size,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'unsubscribe_contact_count':unsubscribe_contact_count,
                'failure_reason':failure_reason
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_template =====================
## ================= begin move_vwz_edm_template_receiver_contact_list =====================
def move_vwz_edm_template_receiver_contact_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, edm_template_id, contact_list_id, created_on, created_by, last_modified, last_modified_by from vwz_edm_template_receiver_contact_list")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        edm_template_id = source_row[2]
        contact_list_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_template_receiver_contact_list(id, organization_id, edm_template_id, contact_list_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(edm_template_id)s,%(contact_list_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'contact_list_id':contact_list_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_template_receiver_contact_list =====================
## ================= begin move_vwz_edm_template_setting =====================
def move_vwz_edm_template_setting(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, header, footer, organization_id, created_on, created_by, last_modified, last_modified_by from vwz_edm_template_setting")
    for source_row in source_cursor:
        id = source_row[0]
        header = source_row[1]
        footer = source_row[2]
        organization_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_template_setting(id, header, footer, organization_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(header)s,%(footer)s,%(organization_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'header':header,
                'footer':footer,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_template_setting =====================
## ================= begin move_vwz_edm_trace_summary =====================
def move_vwz_edm_trace_summary(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, edm_template_id, event_id, sent_count, new_sent_count, opened_count, new_opened_count, clicked_count, new_clicked_count, hard_bounced_count, new_hard_bounced_count, soft_bounced_count, new_soft_bounced_count, other_bounced_count, new_other_bounced_count, user_failure_count, new_user_failure_count, forward_count, new_forward_count, ignored_count, created_on from vwz_edm_trace_summary")
    for source_row in source_cursor:
        organization_id = source_row[0]
        edm_template_id = source_row[1]
        event_id = source_row[2]
        sent_count = source_row[3]
        new_sent_count = source_row[4]
        opened_count = source_row[5]
        new_opened_count = source_row[6]
        clicked_count = source_row[7]
        new_clicked_count = source_row[8]
        hard_bounced_count = source_row[9]
        new_hard_bounced_count = source_row[10]
        soft_bounced_count = source_row[11]
        new_soft_bounced_count = source_row[12]
        other_bounced_count = source_row[13]
        new_other_bounced_count = source_row[14]
        user_failure_count = source_row[15]
        new_user_failure_count = source_row[16]
        forward_count = source_row[17]
        new_forward_count = source_row[18]
        ignored_count = source_row[19]
        created_on = source_row[20]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_edm_trace_summary(organization_id, edm_template_id, event_id, sent_count, new_sent_count, opened_count, new_opened_count, clicked_count, new_clicked_count, hard_bounced_count, new_hard_bounced_count, soft_bounced_count, new_soft_bounced_count, other_bounced_count, new_other_bounced_count, user_failure_count, new_user_failure_count, forward_count, new_forward_count, ignored_count, created_on) "
              "values(%(organization_id)s,%(edm_template_id)s,%(event_id)s,%(sent_count)s,%(new_sent_count)s,%(opened_count)s,%(new_opened_count)s,%(clicked_count)s,%(new_clicked_count)s,%(hard_bounced_count)s,%(new_hard_bounced_count)s,%(soft_bounced_count)s,%(new_soft_bounced_count)s,%(other_bounced_count)s,%(new_other_bounced_count)s,%(user_failure_count)s,%(new_user_failure_count)s,%(forward_count)s,%(new_forward_count)s,%(ignored_count)s,%(created_on)s)")
        data = {  
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'event_id':event_id,
                'sent_count':sent_count,
                'new_sent_count':new_sent_count,
                'opened_count':opened_count,
                'new_opened_count':new_opened_count,
                'clicked_count':clicked_count,
                'new_clicked_count':new_clicked_count,
                'hard_bounced_count':hard_bounced_count,
                'new_hard_bounced_count':new_hard_bounced_count,
                'soft_bounced_count':soft_bounced_count,
                'new_soft_bounced_count':new_soft_bounced_count,
                'other_bounced_count':other_bounced_count,
                'new_other_bounced_count':new_other_bounced_count,
                'user_failure_count':user_failure_count,
                'new_user_failure_count':new_user_failure_count,
                'forward_count':forward_count,
                'new_forward_count':new_forward_count,
                'ignored_count':ignored_count,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_edm_trace_summary =====================
## ================= begin move_vwz_email =====================
def move_vwz_email(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, email, is_verified, verification_code, created_on, created_by, last_modified, last_modified_by from vwz_email")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        email = source_row[2]
        is_verified = source_row[3]
        verification_code = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_email(id, user_id, email, is_verified, verification_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(email)s,%(is_verified)s,%(verification_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'email':email,
                'is_verified':is_verified,
                'verification_code':verification_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_email =====================
## ================= begin move_vwz_email_merged =====================
def move_vwz_email_merged(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select uid_1, uid_2, email, is_verified, verification_code, created_on, last_modified from vwz_email_merged")
    for source_row in source_cursor:
        uid_1 = source_row[0]
        uid_2 = source_row[1]
        email = source_row[2]
        is_verified = source_row[3]
        verification_code = source_row[4]
        created_on = source_row[5]
        last_modified = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_email_merged(uid_1, uid_2, email, is_verified, verification_code, created_on, last_modified) "
              "values(%(uid_1)s,%(uid_2)s,%(email)s,%(is_verified)s,%(verification_code)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'uid_1':uid_1,
                'uid_2':uid_2,
                'email':email,
                'is_verified':is_verified,
                'verification_code':verification_code,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_email_merged =====================
## ================= begin move_vwz_email_template =====================
def move_vwz_email_template(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, name, type, url, last_modified from vwz_email_template")
    for source_row in source_cursor:
        id = source_row[0]
        name = source_row[1]
        type = source_row[2]
        url = source_row[3]
        last_modified = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_email_template(id, name, type, url, last_modified) "
              "values(%(id)s,%(name)s,%(type)s,%(url)s,%(last_modified)s)")
        data = {  
                'id':id,
                'name':name,
                'type':type,
                'url':url,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_email_template =====================
## ================= begin move_vwz_email_template_ctx =====================
def move_vwz_email_template_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, email_template_id, language, subject, content, text_content, last_modified from vwz_email_template_ctx")
    for source_row in source_cursor:
        id = source_row[0]
        email_template_id = source_row[1]
        language = source_row[2]
        subject = source_row[3]
        content = source_row[4]
        text_content = source_row[5]
        last_modified = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_email_template_ctx(id, email_template_id, language, subject, content, text_content, last_modified) "
              "values(%(id)s,%(email_template_id)s,%(language)s,%(subject)s,%(content)s,%(text_content)s,%(last_modified)s)")
        data = {  
                'id':id,
                'email_template_id':email_template_id,
                'language':language,
                'subject':subject,
                'content':content,
                'text_content':text_content,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_email_template_ctx =====================
## ================= begin move_vwz_event_abstract =====================
def move_vwz_event_abstract(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, duplicate_event_id, default_language_code, organization_id, industry_code, ad_image_id, event_type_code, subtype, gun_event, popularity, popularity_time, event_role_code, checkin_status, is_published, is_translated, is_public, is_free, is_blueprint, allow_cheque_payment, allow_bank_transfer, allow_online_payment, allow_member_credit, allow_credit_card_form, is_archive, is_tx_fee_attendee_charged, is_invoice_enabled, is_vat_general_invoice_enabled, is_vat_special_invoice_enabled, is_invoice_attendee_charged, is_invoice_system_provided, invoice_charge_percentage, vat_general_invoice_charge_percentage, vat_special_invoice_charge_percentage, vat_general_invoice_minimum_amount, vat_special_invoice_minimum_amount, is_attendee_approval_required, is_attendee_details_required, purchaser_account_option, is_e_ticket_enabled, ean_barcode_enabled, is_use_finance_settings, is_invoice_auto_generated, is_invoice_auto_send, is_receipt_auto_send, is_use_default_company_policy, send_reminder_email, max_attendees, attendees_capacity, first_published_date_time, published_data_time, start_date_time, end_date_time, abs_start_date_time, abs_end_date_time, registration_start_datetime, registration_end_datetime, external_url, city_code, country_code, zip_code, venue_id, latitude, longitude, banner_bucket_id, thumbnail_bucket_id, is_commission_charge_paid_by_attendee, internal_contact_user_id, internal_contact_given_name, internal_contact_family_name, internal_contact_email, internal_contact_phone, event_password, share_allowance, created_on, created_by, last_modified, last_modified_by, off_the_record, spoken_languages, member_only, non_org_event, registration_disabled, one_registration_per_email, auto_approve_whitelist, attendee_limit, timezone, venue_reusable, zoom, chapter, budget_tracking, department_selection, video_record, media_permitted, status_tag, signature_event, show_in_sidebar, host_committee, type_tag, is_gdpr_activated, max_reminder_count, survey_limit, is_checkout_enabled, zoom_created, cpd_event, session_duration, session_break, start_date_time_millis, end_date_time_millis, abs_start_date_time_millis, abs_end_date_time_millis from vwz_event_abstract")
    for source_row in source_cursor:
        id = source_row[0]
        duplicate_event_id = source_row[1]
        default_language_code = source_row[2]
        organization_id = source_row[3]
        industry_code = source_row[4]
        ad_image_id = source_row[5]
        event_type_code = source_row[6]
        subtype = source_row[7]
        gun_event = source_row[8]
        popularity = source_row[9]
        popularity_time = source_row[10]
        event_role_code = source_row[11]
        checkin_status = source_row[12]
        is_published = source_row[13]
        is_translated = source_row[14]
        is_public = source_row[15]
        is_free = source_row[16]
        is_blueprint = source_row[17]
        allow_cheque_payment = source_row[18]
        allow_bank_transfer = source_row[19]
        allow_online_payment = source_row[20]
        allow_member_credit = source_row[21]
        allow_credit_card_form = source_row[22]
        is_archive = source_row[23]
        is_tx_fee_attendee_charged = source_row[24]
        is_invoice_enabled = source_row[25]
        is_vat_general_invoice_enabled = source_row[26]
        is_vat_special_invoice_enabled = source_row[27]
        is_invoice_attendee_charged = source_row[28]
        is_invoice_system_provided = source_row[29]
        invoice_charge_percentage = source_row[30]
        vat_general_invoice_charge_percentage = source_row[31]
        vat_special_invoice_charge_percentage = source_row[32]
        vat_general_invoice_minimum_amount = source_row[33]
        vat_special_invoice_minimum_amount = source_row[34]
        is_attendee_approval_required = source_row[35]
        is_attendee_details_required = source_row[36]
        purchaser_account_option = source_row[37]
        is_e_ticket_enabled = source_row[38]
        ean_barcode_enabled = source_row[39]
        is_use_finance_settings = source_row[40]
        is_invoice_auto_generated = source_row[41]
        is_invoice_auto_send = source_row[42]
        is_receipt_auto_send = source_row[43]
        is_use_default_company_policy = source_row[44]
        send_reminder_email = source_row[45]
        max_attendees = source_row[46]
        attendees_capacity = source_row[47]
        first_published_date_time = source_row[48]
        published_data_time = source_row[49]
        start_date_time = source_row[50]
        end_date_time = source_row[51]
        abs_start_date_time = source_row[52]
        abs_end_date_time = source_row[53]
        registration_start_datetime = source_row[54]
        registration_end_datetime = source_row[55]
        external_url = source_row[56]
        city_code = source_row[57]
        country_code = source_row[58]
        zip_code = source_row[59]
        venue_id = source_row[60]
        latitude = source_row[61]
        longitude = source_row[62]
        banner_bucket_id = source_row[63]
        thumbnail_bucket_id = source_row[64]
        is_commission_charge_paid_by_attendee = source_row[65]
        internal_contact_user_id = source_row[66]
        internal_contact_given_name = source_row[67]
        internal_contact_family_name = source_row[68]
        internal_contact_email = source_row[69]
        internal_contact_phone = source_row[70]
        event_password = source_row[71]
        share_allowance = source_row[72]
        created_on = source_row[73]
        created_by = source_row[74]
        last_modified = source_row[75]
        last_modified_by = source_row[76]
        off_the_record = source_row[77]
        spoken_languages = source_row[78]
        member_only = source_row[79]
        non_org_event = source_row[80]
        registration_disabled = source_row[81]
        one_registration_per_email = source_row[82]
        auto_approve_whitelist = source_row[83]
        attendee_limit = source_row[84]
        timezone = source_row[85]
        venue_reusable = source_row[86]
        zoom = source_row[87]
        chapter = source_row[88]
        budget_tracking = source_row[89]
        department_selection = source_row[90]
        video_record = source_row[91]
        media_permitted = source_row[92]
        status_tag = source_row[93]
        signature_event = source_row[94]
        show_in_sidebar = source_row[95]
        host_committee = source_row[96]
        type_tag = source_row[97]
        is_gdpr_activated = source_row[98]
        max_reminder_count = source_row[99]
        survey_limit = source_row[100]
        is_checkout_enabled = source_row[101]
        zoom_created = source_row[102]
        cpd_event = source_row[103]
        session_duration = source_row[104]
        session_break = source_row[105]
        start_date_time_millis = source_row[106]
        end_date_time_millis = source_row[107]
        abs_start_date_time_millis = source_row[108]
        abs_end_date_time_millis = source_row[109]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_abstract(id, duplicate_event_id, default_language_code, organization_id, industry_code, ad_image_id, event_type_code, subtype, gun_event, popularity, popularity_time, event_role_code, checkin_status, is_published, is_translated, is_public, is_free, is_blueprint, allow_cheque_payment, allow_bank_transfer, allow_online_payment, allow_member_credit, allow_credit_card_form, is_archive, is_tx_fee_attendee_charged, is_invoice_enabled, is_vat_general_invoice_enabled, is_vat_special_invoice_enabled, is_invoice_attendee_charged, is_invoice_system_provided, invoice_charge_percentage, vat_general_invoice_charge_percentage, vat_special_invoice_charge_percentage, vat_general_invoice_minimum_amount, vat_special_invoice_minimum_amount, is_attendee_approval_required, is_attendee_details_required, purchaser_account_option, is_e_ticket_enabled, ean_barcode_enabled, is_use_finance_settings, is_invoice_auto_generated, is_invoice_auto_send, is_receipt_auto_send, is_use_default_company_policy, send_reminder_email, max_attendees, attendees_capacity, first_published_date_time, published_data_time, start_date_time, end_date_time, abs_start_date_time, abs_end_date_time, registration_start_datetime, registration_end_datetime, external_url, city_code, country_code, zip_code, venue_id, latitude, longitude, banner_bucket_id, thumbnail_bucket_id, is_commission_charge_paid_by_attendee, internal_contact_user_id, internal_contact_given_name, internal_contact_family_name, internal_contact_email, internal_contact_phone, event_password, share_allowance, created_on, created_by, last_modified, last_modified_by, off_the_record, spoken_languages, member_only, non_org_event, registration_disabled, one_registration_per_email, auto_approve_whitelist, attendee_limit, timezone, venue_reusable, zoom, chapter, budget_tracking, department_selection, video_record, media_permitted, status_tag, signature_event, show_in_sidebar, host_committee, type_tag, is_gdpr_activated, max_reminder_count, survey_limit, is_checkout_enabled, zoom_created, cpd_event, session_duration, session_break, start_date_time_millis, end_date_time_millis, abs_start_date_time_millis, abs_end_date_time_millis) "
              "values(%(id)s,%(duplicate_event_id)s,%(default_language_code)s,%(organization_id)s,%(industry_code)s,%(ad_image_id)s,%(event_type_code)s,%(subtype)s,%(gun_event)s,%(popularity)s,%(popularity_time)s,%(event_role_code)s,%(checkin_status)s,%(is_published)s,%(is_translated)s,%(is_public)s,%(is_free)s,%(is_blueprint)s,%(allow_cheque_payment)s,%(allow_bank_transfer)s,%(allow_online_payment)s,%(allow_member_credit)s,%(allow_credit_card_form)s,%(is_archive)s,%(is_tx_fee_attendee_charged)s,%(is_invoice_enabled)s,%(is_vat_general_invoice_enabled)s,%(is_vat_special_invoice_enabled)s,%(is_invoice_attendee_charged)s,%(is_invoice_system_provided)s,%(invoice_charge_percentage)s,%(vat_general_invoice_charge_percentage)s,%(vat_special_invoice_charge_percentage)s,%(vat_general_invoice_minimum_amount)s,%(vat_special_invoice_minimum_amount)s,%(is_attendee_approval_required)s,%(is_attendee_details_required)s,%(purchaser_account_option)s,%(is_e_ticket_enabled)s,%(ean_barcode_enabled)s,%(is_use_finance_settings)s,%(is_invoice_auto_generated)s,%(is_invoice_auto_send)s,%(is_receipt_auto_send)s,%(is_use_default_company_policy)s,%(send_reminder_email)s,%(max_attendees)s,%(attendees_capacity)s,%(first_published_date_time)s,%(published_data_time)s,%(start_date_time)s,%(end_date_time)s,%(abs_start_date_time)s,%(abs_end_date_time)s,%(registration_start_datetime)s,%(registration_end_datetime)s,%(external_url)s,%(city_code)s,%(country_code)s,%(zip_code)s,%(venue_id)s,%(latitude)s,%(longitude)s,%(banner_bucket_id)s,%(thumbnail_bucket_id)s,%(is_commission_charge_paid_by_attendee)s,%(internal_contact_user_id)s,%(internal_contact_given_name)s,%(internal_contact_family_name)s,%(internal_contact_email)s,%(internal_contact_phone)s,%(event_password)s,%(share_allowance)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(off_the_record)s,%(spoken_languages)s,%(member_only)s,%(non_org_event)s,%(registration_disabled)s,%(one_registration_per_email)s,%(auto_approve_whitelist)s,%(attendee_limit)s,%(timezone)s,%(venue_reusable)s,%(zoom)s,%(chapter)s,%(budget_tracking)s,%(department_selection)s,%(video_record)s,%(media_permitted)s,%(status_tag)s,%(signature_event)s,%(show_in_sidebar)s,%(host_committee)s,%(type_tag)s,%(is_gdpr_activated)s,%(max_reminder_count)s,%(survey_limit)s,%(is_checkout_enabled)s,%(zoom_created)s,%(cpd_event)s,%(session_duration)s,%(session_break)s,%(start_date_time_millis)s,%(end_date_time_millis)s,%(abs_start_date_time_millis)s,%(abs_end_date_time_millis)s)")
        data = {  
                'id':id,
                'duplicate_event_id':duplicate_event_id,
                'default_language_code':default_language_code,
                'organization_id':organization_id,
                'industry_code':industry_code,
                'ad_image_id':ad_image_id,
                'event_type_code':event_type_code,
                'subtype':subtype,
                'gun_event':gun_event,
                'popularity':popularity,
                'popularity_time':popularity_time,
                'event_role_code':event_role_code,
                'checkin_status':checkin_status,
                'is_published':is_published,
                'is_translated':is_translated,
                'is_public':is_public,
                'is_free':is_free,
                'is_blueprint':is_blueprint,
                'allow_cheque_payment':allow_cheque_payment,
                'allow_bank_transfer':allow_bank_transfer,
                'allow_online_payment':allow_online_payment,
                'allow_member_credit':allow_member_credit,
                'allow_credit_card_form':allow_credit_card_form,
                'is_archive':is_archive,
                'is_tx_fee_attendee_charged':is_tx_fee_attendee_charged,
                'is_invoice_enabled':is_invoice_enabled,
                'is_vat_general_invoice_enabled':is_vat_general_invoice_enabled,
                'is_vat_special_invoice_enabled':is_vat_special_invoice_enabled,
                'is_invoice_attendee_charged':is_invoice_attendee_charged,
                'is_invoice_system_provided':is_invoice_system_provided,
                'invoice_charge_percentage':invoice_charge_percentage,
                'vat_general_invoice_charge_percentage':vat_general_invoice_charge_percentage,
                'vat_special_invoice_charge_percentage':vat_special_invoice_charge_percentage,
                'vat_general_invoice_minimum_amount':vat_general_invoice_minimum_amount,
                'vat_special_invoice_minimum_amount':vat_special_invoice_minimum_amount,
                'is_attendee_approval_required':is_attendee_approval_required,
                'is_attendee_details_required':is_attendee_details_required,
                'purchaser_account_option':purchaser_account_option,
                'is_e_ticket_enabled':is_e_ticket_enabled,
                'ean_barcode_enabled':ean_barcode_enabled,
                'is_use_finance_settings':is_use_finance_settings,
                'is_invoice_auto_generated':is_invoice_auto_generated,
                'is_invoice_auto_send':is_invoice_auto_send,
                'is_receipt_auto_send':is_receipt_auto_send,
                'is_use_default_company_policy':is_use_default_company_policy,
                'send_reminder_email':send_reminder_email,
                'max_attendees':max_attendees,
                'attendees_capacity':attendees_capacity,
                'first_published_date_time':first_published_date_time,
                'published_data_time':published_data_time,
                'start_date_time':start_date_time,
                'end_date_time':end_date_time,
                'abs_start_date_time':abs_start_date_time,
                'abs_end_date_time':abs_end_date_time,
                'registration_start_datetime':registration_start_datetime,
                'registration_end_datetime':registration_end_datetime,
                'external_url':external_url,
                'city_code':city_code,
                'country_code':country_code,
                'zip_code':zip_code,
                'venue_id':venue_id,
                'latitude':latitude,
                'longitude':longitude,
                'banner_bucket_id':banner_bucket_id,
                'thumbnail_bucket_id':thumbnail_bucket_id,
                'is_commission_charge_paid_by_attendee':is_commission_charge_paid_by_attendee,
                'internal_contact_user_id':internal_contact_user_id,
                'internal_contact_given_name':internal_contact_given_name,
                'internal_contact_family_name':internal_contact_family_name,
                'internal_contact_email':internal_contact_email,
                'internal_contact_phone':internal_contact_phone,
                'event_password':event_password,
                'share_allowance':share_allowance,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'off_the_record':off_the_record,
                'spoken_languages':spoken_languages,
                'member_only':member_only,
                'non_org_event':non_org_event,
                'registration_disabled':registration_disabled,
                'one_registration_per_email':one_registration_per_email,
                'auto_approve_whitelist':auto_approve_whitelist,
                'attendee_limit':attendee_limit,
                'timezone':timezone,
                'venue_reusable':venue_reusable,
                'zoom':zoom,
                'chapter':chapter,
                'budget_tracking':budget_tracking,
                'department_selection':department_selection,
                'video_record':video_record,
                'media_permitted':media_permitted,
                'status_tag':status_tag,
                'signature_event':signature_event,
                'show_in_sidebar':show_in_sidebar,
                'host_committee':host_committee,
                'type_tag':type_tag,
                'is_gdpr_activated':is_gdpr_activated,
                'max_reminder_count':max_reminder_count,
                'survey_limit':survey_limit,
                'is_checkout_enabled':is_checkout_enabled,
                'zoom_created':zoom_created,
                'cpd_event':cpd_event,
                'session_duration':session_duration,
                'session_break':session_break,
                'start_date_time_millis':start_date_time_millis,
                'end_date_time_millis':end_date_time_millis,
                'abs_start_date_time_millis':abs_start_date_time_millis,
                'abs_end_date_time_millis':abs_end_date_time_millis
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_abstract =====================
## ================= begin move_vwz_event_agenda =====================
def move_vwz_event_agenda(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, language_code, event_id, scheduled_start, scheduled_end, room_name, title, details, created_on, created_by, last_modified, last_modified_by from vwz_event_agenda")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        language_code = source_row[2]
        event_id = source_row[3]
        scheduled_start = source_row[4]
        scheduled_end = source_row[5]
        room_name = source_row[6]
        title = source_row[7]
        details = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_agenda(id, organization_id, language_code, event_id, scheduled_start, scheduled_end, room_name, title, details, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(language_code)s,%(event_id)s,%(scheduled_start)s,%(scheduled_end)s,%(room_name)s,%(title)s,%(details)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'language_code':language_code,
                'event_id':event_id,
                'scheduled_start':scheduled_start,
                'scheduled_end':scheduled_end,
                'room_name':room_name,
                'title':title,
                'details':details,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_agenda =====================
## ================= begin move_vwz_event_attendee =====================
def move_vwz_event_attendee(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, version, organization_id, event_id, language_code, preferred_language_code, ticket_sale_id, ticket_id, ticket_title, ticket_currency, ticket_payment_way, ticket_price, approval_status, approved_datetime, paid_status, is_member, is_part_of_company_member, is_checked_in, checked_in_on, checked_in_by, checked_in_by_given_name, checked_in_by_family_name, is_checked_out, checked_out_on, checked_out_by, checked_out_by_given_name, checked_out_by_family_name, category_id, contact_id, given_name, family_name, email, phone, company_name, position_title, business_function_code, business_role_code, industry_code, street_address, city_code, country_code, city_name_text, province, zip_code, referral_source_code, referral_source_text, note, barcode, ean_barcode, ticket_file_uri, ticket_file_valid, ticket_allocated, picture_descriptor_id, webinar_link, canceled, deleted, show_in_directory, created_on, created_by, last_modified, last_modified_by, valid, cpd_point_status from vwz_event_attendee")
    for source_row in source_cursor:
        id = source_row[0]
        version = source_row[1]
        organization_id = source_row[2]
        event_id = source_row[3]
        language_code = source_row[4]
        preferred_language_code = source_row[5]
        ticket_sale_id = source_row[6]
        ticket_id = source_row[7]
        ticket_title = source_row[8]
        ticket_currency = source_row[9]
        ticket_payment_way = source_row[10]
        ticket_price = source_row[11]
        approval_status = source_row[12]
        approved_datetime = source_row[13]
        paid_status = source_row[14]
        is_member = source_row[15]
        is_part_of_company_member = source_row[16]
        is_checked_in = source_row[17]
        checked_in_on = source_row[18]
        checked_in_by = source_row[19]
        checked_in_by_given_name = source_row[20]
        checked_in_by_family_name = source_row[21]
        is_checked_out = source_row[22]
        checked_out_on = source_row[23]
        checked_out_by = source_row[24]
        checked_out_by_given_name = source_row[25]
        checked_out_by_family_name = source_row[26]
        category_id = source_row[27]
        contact_id = source_row[28]
        given_name = source_row[29]
        family_name = source_row[30]
        email = source_row[31]
        phone = source_row[32]
        company_name = source_row[33]
        position_title = source_row[34]
        business_function_code = source_row[35]
        business_role_code = source_row[36]
        industry_code = source_row[37]
        street_address = source_row[38]
        city_code = source_row[39]
        country_code = source_row[40]
        city_name_text = source_row[41]
        province = source_row[42]
        zip_code = source_row[43]
        referral_source_code = source_row[44]
        referral_source_text = source_row[45]
        note = source_row[46]
        barcode = source_row[47]
        ean_barcode = source_row[48]
        ticket_file_uri = source_row[49]
        ticket_file_valid = source_row[50]
        ticket_allocated = source_row[51]
        picture_descriptor_id = source_row[52]
        webinar_link = source_row[53]
        canceled = source_row[54]
        deleted = source_row[55]
        show_in_directory = source_row[56]
        created_on = source_row[57]
        created_by = source_row[58]
        last_modified = source_row[59]
        last_modified_by = source_row[60]
        valid = source_row[61]
        cpd_point_status = source_row[62]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_attendee(id, version, organization_id, event_id, language_code, preferred_language_code, ticket_sale_id, ticket_id, ticket_title, ticket_currency, ticket_payment_way, ticket_price, approval_status, approved_datetime, paid_status, is_member, is_part_of_company_member, is_checked_in, checked_in_on, checked_in_by, checked_in_by_given_name, checked_in_by_family_name, is_checked_out, checked_out_on, checked_out_by, checked_out_by_given_name, checked_out_by_family_name, category_id, contact_id, given_name, family_name, email, phone, company_name, position_title, business_function_code, business_role_code, industry_code, street_address, city_code, country_code, city_name_text, province, zip_code, referral_source_code, referral_source_text, note, barcode, ean_barcode, ticket_file_uri, ticket_file_valid, ticket_allocated, picture_descriptor_id, webinar_link, canceled, deleted, show_in_directory, created_on, created_by, last_modified, last_modified_by, valid, cpd_point_status) "
              "values(%(id)s,%(version)s,%(organization_id)s,%(event_id)s,%(language_code)s,%(preferred_language_code)s,%(ticket_sale_id)s,%(ticket_id)s,%(ticket_title)s,%(ticket_currency)s,%(ticket_payment_way)s,%(ticket_price)s,%(approval_status)s,%(approved_datetime)s,%(paid_status)s,%(is_member)s,%(is_part_of_company_member)s,%(is_checked_in)s,%(checked_in_on)s,%(checked_in_by)s,%(checked_in_by_given_name)s,%(checked_in_by_family_name)s,%(is_checked_out)s,%(checked_out_on)s,%(checked_out_by)s,%(checked_out_by_given_name)s,%(checked_out_by_family_name)s,%(category_id)s,%(contact_id)s,%(given_name)s,%(family_name)s,%(email)s,%(phone)s,%(company_name)s,%(position_title)s,%(business_function_code)s,%(business_role_code)s,%(industry_code)s,%(street_address)s,%(city_code)s,%(country_code)s,%(city_name_text)s,%(province)s,%(zip_code)s,%(referral_source_code)s,%(referral_source_text)s,%(note)s,%(barcode)s,%(ean_barcode)s,%(ticket_file_uri)s,%(ticket_file_valid)s,%(ticket_allocated)s,%(picture_descriptor_id)s,%(webinar_link)s,%(canceled)s,%(deleted)s,%(show_in_directory)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(valid)s,%(cpd_point_status)s)")
        data = {  
                'id':id,
                'version':version,
                'organization_id':organization_id,
                'event_id':event_id,
                'language_code':language_code,
                'preferred_language_code':preferred_language_code,
                'ticket_sale_id':ticket_sale_id,
                'ticket_id':ticket_id,
                'ticket_title':ticket_title,
                'ticket_currency':ticket_currency,
                'ticket_payment_way':ticket_payment_way,
                'ticket_price':ticket_price,
                'approval_status':approval_status,
                'approved_datetime':approved_datetime,
                'paid_status':paid_status,
                'is_member':is_member,
                'is_part_of_company_member':is_part_of_company_member,
                'is_checked_in':is_checked_in,
                'checked_in_on':checked_in_on,
                'checked_in_by':checked_in_by,
                'checked_in_by_given_name':checked_in_by_given_name,
                'checked_in_by_family_name':checked_in_by_family_name,
                'is_checked_out':is_checked_out,
                'checked_out_on':checked_out_on,
                'checked_out_by':checked_out_by,
                'checked_out_by_given_name':checked_out_by_given_name,
                'checked_out_by_family_name':checked_out_by_family_name,
                'category_id':category_id,
                'contact_id':contact_id,
                'given_name':given_name,
                'family_name':family_name,
                'email':email,
                'phone':phone,
                'company_name':company_name,
                'position_title':position_title,
                'business_function_code':business_function_code,
                'business_role_code':business_role_code,
                'industry_code':industry_code,
                'street_address':street_address,
                'city_code':city_code,
                'country_code':country_code,
                'city_name_text':city_name_text,
                'province':province,
                'zip_code':zip_code,
                'referral_source_code':referral_source_code,
                'referral_source_text':referral_source_text,
                'note':note,
                'barcode':barcode,
                'ean_barcode':ean_barcode,
                'ticket_file_uri':ticket_file_uri,
                'ticket_file_valid':ticket_file_valid,
                'ticket_allocated':ticket_allocated,
                'picture_descriptor_id':picture_descriptor_id,
                'webinar_link':webinar_link,
                'canceled':canceled,
                'deleted':deleted,
                'show_in_directory':show_in_directory,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'valid':valid,
                'cpd_point_status':cpd_point_status
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_attendee =====================
## ================= begin move_vwz_event_attendee_category =====================
def move_vwz_event_attendee_category(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, name, event_id, is_default, is_attendee_approval_required, order, created_on, created_by, last_modified, last_modified_by from vwz_event_attendee_category")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        name = source_row[2]
        event_id = source_row[3]
        is_default = source_row[4]
        is_attendee_approval_required = source_row[5]
        order = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_attendee_category(id, organization_id, name, event_id, is_default, is_attendee_approval_required, order, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(name)s,%(event_id)s,%(is_default)s,%(is_attendee_approval_required)s,%(order)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'name':name,
                'event_id':event_id,
                'is_default':is_default,
                'is_attendee_approval_required':is_attendee_approval_required,
                'order':order,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_attendee_category =====================
## ================= begin move_vwz_event_collaborator =====================
def move_vwz_event_collaborator(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, language_code, event_collaborator_category_id, name, website_address, description, logo_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_event_collaborator")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        language_code = source_row[2]
        event_collaborator_category_id = source_row[3]
        name = source_row[4]
        website_address = source_row[5]
        description = source_row[6]
        logo_bucket_id = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_collaborator(id, organization_id, language_code, event_collaborator_category_id, name, website_address, description, logo_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(language_code)s,%(event_collaborator_category_id)s,%(name)s,%(website_address)s,%(description)s,%(logo_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'language_code':language_code,
                'event_collaborator_category_id':event_collaborator_category_id,
                'name':name,
                'website_address':website_address,
                'description':description,
                'logo_bucket_id':logo_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_collaborator =====================
## ================= begin move_vwz_event_collaborator_category =====================
def move_vwz_event_collaborator_category(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, language_code, event_id, title, rank, type, created_on, created_by, last_modified, last_modified_by from vwz_event_collaborator_category")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        language_code = source_row[2]
        event_id = source_row[3]
        title = source_row[4]
        rank = source_row[5]
        type = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_collaborator_category(id, organization_id, language_code, event_id, title, rank, type, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(language_code)s,%(event_id)s,%(title)s,%(rank)s,%(type)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'language_code':language_code,
                'event_id':event_id,
                'title':title,
                'rank':rank,
                'type':type,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_collaborator_category =====================
## ================= begin move_vwz_event_ctx =====================
def move_vwz_event_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, language_code, event_id, title, sub_title, description, keywords, public_contact_user_id, public_contact_given_name, public_contact_family_name, public_contact_email, public_contact_phone, summary, bank_transfer_details, cheque_payment_details, refund_policy, city_name_text, province, venue_name, venue_file_id, venue_street_address, banner_bucket_id, thumbnail_bucket_id, created_on, created_by, last_modified, last_modified_by, venue_info, member_credit_details, credit_card_form_details, dress_code, other_info from vwz_event_ctx")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        language_code = source_row[2]
        event_id = source_row[3]
        title = source_row[4]
        sub_title = source_row[5]
        description = source_row[6]
        keywords = source_row[7]
        public_contact_user_id = source_row[8]
        public_contact_given_name = source_row[9]
        public_contact_family_name = source_row[10]
        public_contact_email = source_row[11]
        public_contact_phone = source_row[12]
        summary = source_row[13]
        bank_transfer_details = source_row[14]
        cheque_payment_details = source_row[15]
        refund_policy = source_row[16]
        city_name_text = source_row[17]
        province = source_row[18]
        venue_name = source_row[19]
        venue_file_id = source_row[20]
        venue_street_address = source_row[21]
        banner_bucket_id = source_row[22]
        thumbnail_bucket_id = source_row[23]
        created_on = source_row[24]
        created_by = source_row[25]
        last_modified = source_row[26]
        last_modified_by = source_row[27]
        venue_info = source_row[28]
        member_credit_details = source_row[29]
        credit_card_form_details = source_row[30]
        dress_code = source_row[31]
        other_info = source_row[32]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_ctx(id, organization_id, language_code, event_id, title, sub_title, description, keywords, public_contact_user_id, public_contact_given_name, public_contact_family_name, public_contact_email, public_contact_phone, summary, bank_transfer_details, cheque_payment_details, refund_policy, city_name_text, province, venue_name, venue_file_id, venue_street_address, banner_bucket_id, thumbnail_bucket_id, created_on, created_by, last_modified, last_modified_by, venue_info, member_credit_details, credit_card_form_details, dress_code, other_info) "
              "values(%(id)s,%(organization_id)s,%(language_code)s,%(event_id)s,%(title)s,%(sub_title)s,%(description)s,%(keywords)s,%(public_contact_user_id)s,%(public_contact_given_name)s,%(public_contact_family_name)s,%(public_contact_email)s,%(public_contact_phone)s,%(summary)s,%(bank_transfer_details)s,%(cheque_payment_details)s,%(refund_policy)s,%(city_name_text)s,%(province)s,%(venue_name)s,%(venue_file_id)s,%(venue_street_address)s,%(banner_bucket_id)s,%(thumbnail_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(venue_info)s,%(member_credit_details)s,%(credit_card_form_details)s,%(dress_code)s,%(other_info)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'language_code':language_code,
                'event_id':event_id,
                'title':title,
                'sub_title':sub_title,
                'description':description,
                'keywords':keywords,
                'public_contact_user_id':public_contact_user_id,
                'public_contact_given_name':public_contact_given_name,
                'public_contact_family_name':public_contact_family_name,
                'public_contact_email':public_contact_email,
                'public_contact_phone':public_contact_phone,
                'summary':summary,
                'bank_transfer_details':bank_transfer_details,
                'cheque_payment_details':cheque_payment_details,
                'refund_policy':refund_policy,
                'city_name_text':city_name_text,
                'province':province,
                'venue_name':venue_name,
                'venue_file_id':venue_file_id,
                'venue_street_address':venue_street_address,
                'banner_bucket_id':banner_bucket_id,
                'thumbnail_bucket_id':thumbnail_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'venue_info':venue_info,
                'member_credit_details':member_credit_details,
                'credit_card_form_details':credit_card_form_details,
                'dress_code':dress_code,
                'other_info':other_info
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_ctx =====================
## ================= begin move_vwz_event_currency =====================
def move_vwz_event_currency(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, event_id, currency_code, created_on, last_modified from vwz_event_currency")
    for source_row in source_cursor:
        id = source_row[0]
        event_id = source_row[1]
        currency_code = source_row[2]
        created_on = source_row[3]
        last_modified = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_currency(id, event_id, currency_code, created_on, last_modified) "
              "values(%(id)s,%(event_id)s,%(currency_code)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'event_id':event_id,
                'currency_code':currency_code,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_currency =====================
## ================= begin move_vwz_event_document =====================
def move_vwz_event_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, language_code, event_id, author, downloads, visibility, is_session_only, document_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_event_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        language_code = source_row[2]
        event_id = source_row[3]
        author = source_row[4]
        downloads = source_row[5]
        visibility = source_row[6]
        is_session_only = source_row[7]
        document_bucket_id = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_document(id, organization_id, language_code, event_id, author, downloads, visibility, is_session_only, document_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(language_code)s,%(event_id)s,%(author)s,%(downloads)s,%(visibility)s,%(is_session_only)s,%(document_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'language_code':language_code,
                'event_id':event_id,
                'author':author,
                'downloads':downloads,
                'visibility':visibility,
                'is_session_only':is_session_only,
                'document_bucket_id':document_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_document =====================
## ================= begin move_vwz_event_document_list =====================
def move_vwz_event_document_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, parent_list_id, name, order, created_on, created_by, last_modified, last_modified_by from vwz_event_document_list")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        parent_list_id = source_row[3]
        name = source_row[4]
        order = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_document_list(id, organization_id, event_id, parent_list_id, name, order, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(parent_list_id)s,%(name)s,%(order)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'parent_list_id':parent_list_id,
                'name':name,
                'order':order,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_document_list =====================
## ================= begin move_vwz_event_document_relation =====================
def move_vwz_event_document_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, event_id, list_id, event_document_id, created_on, created_by, last_modified, last_modified_by from vwz_event_document_relation")
    for source_row in source_cursor:
        organization_id = source_row[0]
        event_id = source_row[1]
        list_id = source_row[2]
        event_document_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_document_relation(organization_id, event_id, list_id, event_document_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(event_id)s,%(list_id)s,%(event_document_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'event_id':event_id,
                'list_id':list_id,
                'event_document_id':event_document_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_document_relation =====================
## ================= begin move_vwz_event_eanbar_code_list =====================
def move_vwz_event_eanbar_code_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, attendee_id, ean_barcode, created_on, created_by, last_modified, last_modified_by from vwz_event_eanbar_code_list")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        attendee_id = source_row[3]
        ean_barcode = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_eanbar_code_list(id, organization_id, event_id, attendee_id, ean_barcode, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(attendee_id)s,%(ean_barcode)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'attendee_id':attendee_id,
                'ean_barcode':ean_barcode,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_eanbar_code_list =====================
## ================= begin move_vwz_event_expense_revenue =====================
def move_vwz_event_expense_revenue(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, category, type, currency_code, value, title, description, object_type, object_id, deleted, created_on, created_by, last_modified, last_modified_by from vwz_event_expense_revenue")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        category = source_row[3]
        type = source_row[4]
        currency_code = source_row[5]
        value = source_row[6]
        title = source_row[7]
        description = source_row[8]
        object_type = source_row[9]
        object_id = source_row[10]
        deleted = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_expense_revenue(id, organization_id, event_id, category, type, currency_code, value, title, description, object_type, object_id, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(category)s,%(type)s,%(currency_code)s,%(value)s,%(title)s,%(description)s,%(object_type)s,%(object_id)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'category':category,
                'type':type,
                'currency_code':currency_code,
                'value':value,
                'title':title,
                'description':description,
                'object_type':object_type,
                'object_id':object_id,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_expense_revenue =====================
## ================= begin move_vwz_event_invitee =====================
def move_vwz_event_invitee(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, event_id, contact_id, attendee_id, transaction_id, deleted, status, mail_status, verification_code, created_on, created_by, sent_on, sent_by, last_modified, last_modified_by, failure_reason from vwz_event_invitee")
    for source_row in source_cursor:
        organization_id = source_row[0]
        event_id = source_row[1]
        contact_id = source_row[2]
        attendee_id = source_row[3]
        transaction_id = source_row[4]
        deleted = source_row[5]
        status = source_row[6]
        mail_status = source_row[7]
        verification_code = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        sent_on = source_row[11]
        sent_by = source_row[12]
        last_modified = source_row[13]
        last_modified_by = source_row[14]
        failure_reason = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_invitee(organization_id, event_id, contact_id, attendee_id, transaction_id, deleted, status, mail_status, verification_code, created_on, created_by, sent_on, sent_by, last_modified, last_modified_by, failure_reason) "
              "values(%(organization_id)s,%(event_id)s,%(contact_id)s,%(attendee_id)s,%(transaction_id)s,%(deleted)s,%(status)s,%(mail_status)s,%(verification_code)s,%(created_on)s,%(created_by)s,%(sent_on)s,%(sent_by)s,%(last_modified)s,%(last_modified_by)s,%(failure_reason)s)")
        data = {  
                'organization_id':organization_id,
                'event_id':event_id,
                'contact_id':contact_id,
                'attendee_id':attendee_id,
                'transaction_id':transaction_id,
                'deleted':deleted,
                'status':status,
                'mail_status':mail_status,
                'verification_code':verification_code,
                'created_on':created_on,
                'created_by':created_by,
                'sent_on':sent_on,
                'sent_by':sent_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'failure_reason':failure_reason
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_invitee =====================
## ================= begin move_vwz_event_member =====================
def move_vwz_event_member(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, user_id, role, created_on, created_by, last_modified, last_modified_by from vwz_event_member")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        user_id = source_row[3]
        role = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_member(id, organization_id, event_id, user_id, role, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(user_id)s,%(role)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'user_id':user_id,
                'role':role,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_member =====================
## ================= begin move_vwz_event_member_team_relation =====================
def move_vwz_event_member_team_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_team_id, event_id, organization_id, created_on, created_by, last_modified, last_modified_by from vwz_event_member_team_relation")
    for source_row in source_cursor:
        organization_team_id = source_row[0]
        event_id = source_row[1]
        organization_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_member_team_relation(organization_team_id, event_id, organization_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_team_id)s,%(event_id)s,%(organization_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_team_id':organization_team_id,
                'event_id':event_id,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_member_team_relation =====================
## ================= begin move_vwz_event_multi_checkin =====================
def move_vwz_event_multi_checkin(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, check_in_point_id, attendee_id, organization_id, event_id, type, created_on, created_by, last_modified, last_modified_by from vwz_event_multi_checkin")
    for source_row in source_cursor:
        id = source_row[0]
        check_in_point_id = source_row[1]
        attendee_id = source_row[2]
        organization_id = source_row[3]
        event_id = source_row[4]
        type = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_multi_checkin(id, check_in_point_id, attendee_id, organization_id, event_id, type, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(check_in_point_id)s,%(attendee_id)s,%(organization_id)s,%(event_id)s,%(type)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'check_in_point_id':check_in_point_id,
                'attendee_id':attendee_id,
                'organization_id':organization_id,
                'event_id':event_id,
                'type':type,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_multi_checkin =====================
## ================= begin move_vwz_event_reminder_setting =====================
def move_vwz_event_reminder_setting(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, math, event_id, status, email_send_time, not_send_reason, created_on, created_by, last_modified, last_modified_by from vwz_event_reminder_setting")
    for source_row in source_cursor:
        id = source_row[0]
        math = source_row[1]
        event_id = source_row[2]
        status = source_row[3]
        email_send_time = source_row[4]
        not_send_reason = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_reminder_setting(id, math, event_id, status, email_send_time, not_send_reason, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(math)s,%(event_id)s,%(status)s,%(email_send_time)s,%(not_send_reason)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'math':math,
                'event_id':event_id,
                'status':status,
                'email_send_time':email_send_time,
                'not_send_reason':not_send_reason,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_reminder_setting =====================
## ================= begin move_vwz_event_tag_list =====================
def move_vwz_event_tag_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, default_language_code, is_public, is_deleted, created_on, created_by, last_modified, last_modified_by from vwz_event_tag_list")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        default_language_code = source_row[2]
        is_public = source_row[3]
        is_deleted = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_tag_list(id, organization_id, default_language_code, is_public, is_deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(default_language_code)s,%(is_public)s,%(is_deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'default_language_code':default_language_code,
                'is_public':is_public,
                'is_deleted':is_deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_tag_list =====================
## ================= begin move_vwz_event_tag_list_ctx =====================
def move_vwz_event_tag_list_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select tag_id, language_code, organization_id, name, created_on, created_by, last_modified, last_modified_by from vwz_event_tag_list_ctx")
    for source_row in source_cursor:
        tag_id = source_row[0]
        language_code = source_row[1]
        organization_id = source_row[2]
        name = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_tag_list_ctx(tag_id, language_code, organization_id, name, created_on, created_by, last_modified, last_modified_by) "
              "values(%(tag_id)s,%(language_code)s,%(organization_id)s,%(name)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'tag_id':tag_id,
                'language_code':language_code,
                'organization_id':organization_id,
                'name':name,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_tag_list_ctx =====================
## ================= begin move_vwz_event_tag_relation =====================
def move_vwz_event_tag_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select tag_id, event_id, organization_id, created_on, created_by, last_modified, last_modified_by from vwz_event_tag_relation")
    for source_row in source_cursor:
        tag_id = source_row[0]
        event_id = source_row[1]
        organization_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_tag_relation(tag_id, event_id, organization_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(tag_id)s,%(event_id)s,%(organization_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'tag_id':tag_id,
                'event_id':event_id,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_tag_relation =====================
## ================= begin move_vwz_event_task =====================
def move_vwz_event_task(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, category_id, name, status, assigned_date, assigner_user_id, completer_user_id, complete_date, priority, description, start_date, end_date, created_on, created_by, last_modified, last_modified_by from vwz_event_task")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        category_id = source_row[3]
        name = source_row[4]
        status = source_row[5]
        assigned_date = source_row[6]
        assigner_user_id = source_row[7]
        completer_user_id = source_row[8]
        complete_date = source_row[9]
        priority = source_row[10]
        description = source_row[11]
        start_date = source_row[12]
        end_date = source_row[13]
        created_on = source_row[14]
        created_by = source_row[15]
        last_modified = source_row[16]
        last_modified_by = source_row[17]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_task(id, organization_id, event_id, category_id, name, status, assigned_date, assigner_user_id, completer_user_id, complete_date, priority, description, start_date, end_date, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(category_id)s,%(name)s,%(status)s,%(assigned_date)s,%(assigner_user_id)s,%(completer_user_id)s,%(complete_date)s,%(priority)s,%(description)s,%(start_date)s,%(end_date)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'category_id':category_id,
                'name':name,
                'status':status,
                'assigned_date':assigned_date,
                'assigner_user_id':assigner_user_id,
                'completer_user_id':completer_user_id,
                'complete_date':complete_date,
                'priority':priority,
                'description':description,
                'start_date':start_date,
                'end_date':end_date,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_task =====================
## ================= begin move_vwz_event_task_assignee =====================
def move_vwz_event_task_assignee(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_task_id, assignee_user_id, created_on, created_by, last_modified, last_modified_by from vwz_event_task_assignee")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_task_id = source_row[2]
        assignee_user_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_task_assignee(id, organization_id, event_task_id, assignee_user_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_task_id)s,%(assignee_user_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_task_id':event_task_id,
                'assignee_user_id':assignee_user_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_task_assignee =====================
## ================= begin move_vwz_event_task_category =====================
def move_vwz_event_task_category(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, name, created_on, created_by, last_modified, last_modified_by from vwz_event_task_category")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        name = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_task_category(id, organization_id, event_id, name, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(name)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'name':name,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_task_category =====================
## ================= begin move_vwz_event_task_document =====================
def move_vwz_event_task_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, task_id, document_bucket_id, author, created_on, created_by, last_modified, last_modified_by from vwz_event_task_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        task_id = source_row[2]
        document_bucket_id = source_row[3]
        author = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_task_document(id, organization_id, task_id, document_bucket_id, author, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(task_id)s,%(document_bucket_id)s,%(author)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'task_id':task_id,
                'document_bucket_id':document_bucket_id,
                'author':author,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_task_document =====================
## ================= begin move_vwz_event_task_note =====================
def move_vwz_event_task_note(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_task_id, user_id, description, document_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_event_task_note")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_task_id = source_row[2]
        user_id = source_row[3]
        description = source_row[4]
        document_bucket_id = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_task_note(id, organization_id, event_task_id, user_id, description, document_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(event_task_id)s,%(user_id)s,%(description)s,%(document_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_task_id':event_task_id,
                'user_id':user_id,
                'description':description,
                'document_bucket_id':document_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_task_note =====================
## ================= begin move_vwz_event_translated_language =====================
def move_vwz_event_translated_language(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, event_id, language_code, created_on, created_by, last_modified, last_modified_by from vwz_event_translated_language")
    for source_row in source_cursor:
        id = source_row[0]
        event_id = source_row[1]
        language_code = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_translated_language(id, event_id, language_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(event_id)s,%(language_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'event_id':event_id,
                'language_code':language_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_translated_language =====================
## ================= begin move_vwz_event_url =====================
def move_vwz_event_url(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, event_id, custom_url, active, created_on, created_by, last_modified, last_modified_by from vwz_event_url")
    for source_row in source_cursor:
        id = source_row[0]
        event_id = source_row[1]
        custom_url = source_row[2]
        active = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_url(id, event_id, custom_url, active, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(event_id)s,%(custom_url)s,%(active)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'event_id':event_id,
                'custom_url':custom_url,
                'active':active,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_url =====================
## ================= begin move_vwz_event_working_group =====================
def move_vwz_event_working_group(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select list_id, event_id, organization_id, created_on, created_by, last_modified, last_modified_by from vwz_event_working_group")
    for source_row in source_cursor:
        list_id = source_row[0]
        event_id = source_row[1]
        organization_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_event_working_group(list_id, event_id, organization_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(list_id)s,%(event_id)s,%(organization_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'list_id':list_id,
                'event_id':event_id,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_event_working_group =====================
## ================= begin move_vwz_export_request =====================
def move_vwz_export_request(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, section, export_type, status, language_code, silent_mode, attach_file, sync_export, format_preferences, params, number_of_retries, result_count, file_type, file_name, sheet_name, bucket_id, fail_reason, hidden, deleted, created_on, created_by, last_modified, last_modified_by from vwz_export_request")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        section = source_row[2]
        export_type = source_row[3]
        status = source_row[4]
        language_code = source_row[5]
        silent_mode = source_row[6]
        attach_file = source_row[7]
        sync_export = source_row[8]
        format_preferences = source_row[9]
        params = source_row[10]
        number_of_retries = source_row[11]
        result_count = source_row[12]
        file_type = source_row[13]
        file_name = source_row[14]
        sheet_name = source_row[15]
        bucket_id = source_row[16]
        fail_reason = source_row[17]
        hidden = source_row[18]
        deleted = source_row[19]
        created_on = source_row[20]
        created_by = source_row[21]
        last_modified = source_row[22]
        last_modified_by = source_row[23]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_export_request(id, organization_id, section, export_type, status, language_code, silent_mode, attach_file, sync_export, format_preferences, params, number_of_retries, result_count, file_type, file_name, sheet_name, bucket_id, fail_reason, hidden, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(section)s,%(export_type)s,%(status)s,%(language_code)s,%(silent_mode)s,%(attach_file)s,%(sync_export)s,%(format_preferences)s,%(params)s,%(number_of_retries)s,%(result_count)s,%(file_type)s,%(file_name)s,%(sheet_name)s,%(bucket_id)s,%(fail_reason)s,%(hidden)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'section':section,
                'export_type':export_type,
                'status':status,
                'language_code':language_code,
                'silent_mode':silent_mode,
                'attach_file':attach_file,
                'sync_export':sync_export,
                'format_preferences':format_preferences,
                'params':params,
                'number_of_retries':number_of_retries,
                'result_count':result_count,
                'file_type':file_type,
                'file_name':file_name,
                'sheet_name':sheet_name,
                'bucket_id':bucket_id,
                'fail_reason':fail_reason,
                'hidden':hidden,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_export_request =====================
## ================= begin move_vwz_fapiao_delivery_method =====================
def move_vwz_fapiao_delivery_method(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select object_type, parent_object_id, object_id, delivery_method, created_on, created_by, last_modified, last_modified_by from vwz_fapiao_delivery_method")
    for source_row in source_cursor:
        object_type = source_row[0]
        parent_object_id = source_row[1]
        object_id = source_row[2]
        delivery_method = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_fapiao_delivery_method(object_type, parent_object_id, object_id, delivery_method, created_on, created_by, last_modified, last_modified_by) "
              "values(%(object_type)s,%(parent_object_id)s,%(object_id)s,%(delivery_method)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'object_type':object_type,
                'parent_object_id':parent_object_id,
                'object_id':object_id,
                'delivery_method':delivery_method,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_fapiao_delivery_method =====================
## ================= begin move_vwz_feedback =====================
def move_vwz_feedback(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, topic, sender_email, sender_name, content, created_on, created_by, last_modified, last_modified_by from vwz_feedback")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        topic = source_row[2]
        sender_email = source_row[3]
        sender_name = source_row[4]
        content = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_feedback(id, user_id, topic, sender_email, sender_name, content, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(topic)s,%(sender_email)s,%(sender_name)s,%(content)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'topic':topic,
                'sender_email':sender_email,
                'sender_name':sender_name,
                'content':content,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_feedback =====================
## ================= begin move_vwz_finance_document =====================
def move_vwz_finance_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, object_type, object_id, document_bucket_id, author_id, created_on, created_by, last_modified, last_modified_by from vwz_finance_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        object_type = source_row[2]
        object_id = source_row[3]
        document_bucket_id = source_row[4]
        author_id = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_finance_document(id, organization_id, object_type, object_id, document_bucket_id, author_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(object_type)s,%(object_id)s,%(document_bucket_id)s,%(author_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'object_type':object_type,
                'object_id':object_id,
                'document_bucket_id':document_bucket_id,
                'author_id':author_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_finance_document =====================
## ================= begin move_vwz_finance_fapiao =====================
def move_vwz_finance_fapiao(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, title, tax_registration_id, tax_registration_address, phone, bank_name, bank_account, created_on, created_by, last_modified, last_modified_by from vwz_finance_fapiao")
    for source_row in source_cursor:
        id = source_row[0]
        title = source_row[1]
        tax_registration_id = source_row[2]
        tax_registration_address = source_row[3]
        phone = source_row[4]
        bank_name = source_row[5]
        bank_account = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_finance_fapiao(id, title, tax_registration_id, tax_registration_address, phone, bank_name, bank_account, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(title)s,%(tax_registration_id)s,%(tax_registration_address)s,%(phone)s,%(bank_name)s,%(bank_account)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'title':title,
                'tax_registration_id':tax_registration_id,
                'tax_registration_address':tax_registration_address,
                'phone':phone,
                'bank_name':bank_name,
                'bank_account':bank_account,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_finance_fapiao =====================
## ================= begin move_vwz_finance_invoice =====================
def move_vwz_finance_invoice(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, number, proforma_number, title, issue_date, due_date, invoice_due_date, currency, origin_total, discount_total, invoice_charge_total, additional_members_total, extra_fee_total, tax_total, face_total, balance_due, status, proforma, payment_way, void_date, voided_by, void_reason, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, billing_address, billing_tax_id, billing_company_name, billing_phone, billing_street_address, billing_zip_code, billing_city, billing_province, billing_country_code, comment, company_policy, internal_note, origin, relation_id, contact_id, company_id, allow_online_payment, allow_cash_payment, allow_bank_transfer, allow_cheque_payment, bank_transfer_details, cheque_payment_details, attach_file_in_email, hidden, voided, deleted, created_on, created_by, last_modified, last_modified_by from vwz_finance_invoice")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        number = source_row[2]
        proforma_number = source_row[3]
        title = source_row[4]
        issue_date = source_row[5]
        due_date = source_row[6]
        invoice_due_date = source_row[7]
        currency = source_row[8]
        origin_total = source_row[9]
        discount_total = source_row[10]
        invoice_charge_total = source_row[11]
        additional_members_total = source_row[12]
        extra_fee_total = source_row[13]
        tax_total = source_row[14]
        face_total = source_row[15]
        balance_due = source_row[16]
        status = source_row[17]
        proforma = source_row[18]
        payment_way = source_row[19]
        void_date = source_row[20]
        voided_by = source_row[21]
        void_reason = source_row[22]
        purchaser_given_name = source_row[23]
        purchaser_family_name = source_row[24]
        purchaser_email = source_row[25]
        purchaser_phone = source_row[26]
        purchaser_company_name = source_row[27]
        purchaser_position_title = source_row[28]
        purchaser_business_function_code = source_row[29]
        purchaser_business_role_code = source_row[30]
        billing_address = source_row[31]
        billing_tax_id = source_row[32]
        billing_company_name = source_row[33]
        billing_phone = source_row[34]
        billing_street_address = source_row[35]
        billing_zip_code = source_row[36]
        billing_city = source_row[37]
        billing_province = source_row[38]
        billing_country_code = source_row[39]
        comment = source_row[40]
        company_policy = source_row[41]
        internal_note = source_row[42]
        origin = source_row[43]
        relation_id = source_row[44]
        contact_id = source_row[45]
        company_id = source_row[46]
        allow_online_payment = source_row[47]
        allow_cash_payment = source_row[48]
        allow_bank_transfer = source_row[49]
        allow_cheque_payment = source_row[50]
        bank_transfer_details = source_row[51]
        cheque_payment_details = source_row[52]
        attach_file_in_email = source_row[53]
        hidden = source_row[54]
        voided = source_row[55]
        deleted = source_row[56]
        created_on = source_row[57]
        created_by = source_row[58]
        last_modified = source_row[59]
        last_modified_by = source_row[60]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_finance_invoice(id, organization_id, number, proforma_number, title, issue_date, due_date, invoice_due_date, currency, origin_total, discount_total, invoice_charge_total, additional_members_total, extra_fee_total, tax_total, face_total, balance_due, status, proforma, payment_way, void_date, voided_by, void_reason, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, billing_address, billing_tax_id, billing_company_name, billing_phone, billing_street_address, billing_zip_code, billing_city, billing_province, billing_country_code, comment, company_policy, internal_note, origin, relation_id, contact_id, company_id, allow_online_payment, allow_cash_payment, allow_bank_transfer, allow_cheque_payment, bank_transfer_details, cheque_payment_details, attach_file_in_email, hidden, voided, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(number)s,%(proforma_number)s,%(title)s,%(issue_date)s,%(due_date)s,%(invoice_due_date)s,%(currency)s,%(origin_total)s,%(discount_total)s,%(invoice_charge_total)s,%(additional_members_total)s,%(extra_fee_total)s,%(tax_total)s,%(face_total)s,%(balance_due)s,%(status)s,%(proforma)s,%(payment_way)s,%(void_date)s,%(voided_by)s,%(void_reason)s,%(purchaser_given_name)s,%(purchaser_family_name)s,%(purchaser_email)s,%(purchaser_phone)s,%(purchaser_company_name)s,%(purchaser_position_title)s,%(purchaser_business_function_code)s,%(purchaser_business_role_code)s,%(billing_address)s,%(billing_tax_id)s,%(billing_company_name)s,%(billing_phone)s,%(billing_street_address)s,%(billing_zip_code)s,%(billing_city)s,%(billing_province)s,%(billing_country_code)s,%(comment)s,%(company_policy)s,%(internal_note)s,%(origin)s,%(relation_id)s,%(contact_id)s,%(company_id)s,%(allow_online_payment)s,%(allow_cash_payment)s,%(allow_bank_transfer)s,%(allow_cheque_payment)s,%(bank_transfer_details)s,%(cheque_payment_details)s,%(attach_file_in_email)s,%(hidden)s,%(voided)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'number':number,
                'proforma_number':proforma_number,
                'title':title,
                'issue_date':issue_date,
                'due_date':due_date,
                'invoice_due_date':invoice_due_date,
                'currency':currency,
                'origin_total':origin_total,
                'discount_total':discount_total,
                'invoice_charge_total':invoice_charge_total,
                'additional_members_total':additional_members_total,
                'extra_fee_total':extra_fee_total,
                'tax_total':tax_total,
                'face_total':face_total,
                'balance_due':balance_due,
                'status':status,
                'proforma':proforma,
                'payment_way':payment_way,
                'void_date':void_date,
                'voided_by':voided_by,
                'void_reason':void_reason,
                'purchaser_given_name':purchaser_given_name,
                'purchaser_family_name':purchaser_family_name,
                'purchaser_email':purchaser_email,
                'purchaser_phone':purchaser_phone,
                'purchaser_company_name':purchaser_company_name,
                'purchaser_position_title':purchaser_position_title,
                'purchaser_business_function_code':purchaser_business_function_code,
                'purchaser_business_role_code':purchaser_business_role_code,
                'billing_address':billing_address,
                'billing_tax_id':billing_tax_id,
                'billing_company_name':billing_company_name,
                'billing_phone':billing_phone,
                'billing_street_address':billing_street_address,
                'billing_zip_code':billing_zip_code,
                'billing_city':billing_city,
                'billing_province':billing_province,
                'billing_country_code':billing_country_code,
                'comment':comment,
                'company_policy':company_policy,
                'internal_note':internal_note,
                'origin':origin,
                'relation_id':relation_id,
                'contact_id':contact_id,
                'company_id':company_id,
                'allow_online_payment':allow_online_payment,
                'allow_cash_payment':allow_cash_payment,
                'allow_bank_transfer':allow_bank_transfer,
                'allow_cheque_payment':allow_cheque_payment,
                'bank_transfer_details':bank_transfer_details,
                'cheque_payment_details':cheque_payment_details,
                'attach_file_in_email':attach_file_in_email,
                'hidden':hidden,
                'voided':voided,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_finance_invoice =====================
## ================= begin move_vwz_finance_invoice_item =====================
def move_vwz_finance_invoice_item(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, invoice_id, name, status, type, category, first_relation_id, second_relation_id, third_relation_id, paid_status, payment_id, origin_value, discount_value, invoice_charge_value, additional_members_value, extra_fee_value, tax_value, face_value, description, quantity, created_on, created_by, last_modified, last_modified_by from vwz_finance_invoice_item")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        invoice_id = source_row[2]
        name = source_row[3]
        status = source_row[4]
        type = source_row[5]
        category = source_row[6]
        first_relation_id = source_row[7]
        second_relation_id = source_row[8]
        third_relation_id = source_row[9]
        paid_status = source_row[10]
        payment_id = source_row[11]
        origin_value = source_row[12]
        discount_value = source_row[13]
        invoice_charge_value = source_row[14]
        additional_members_value = source_row[15]
        extra_fee_value = source_row[16]
        tax_value = source_row[17]
        face_value = source_row[18]
        description = source_row[19]
        quantity = source_row[20]
        created_on = source_row[21]
        created_by = source_row[22]
        last_modified = source_row[23]
        last_modified_by = source_row[24]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_finance_invoice_item(id, organization_id, invoice_id, name, status, type, category, first_relation_id, second_relation_id, third_relation_id, paid_status, payment_id, origin_value, discount_value, invoice_charge_value, additional_members_value, extra_fee_value, tax_value, face_value, description, quantity, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(invoice_id)s,%(name)s,%(status)s,%(type)s,%(category)s,%(first_relation_id)s,%(second_relation_id)s,%(third_relation_id)s,%(paid_status)s,%(payment_id)s,%(origin_value)s,%(discount_value)s,%(invoice_charge_value)s,%(additional_members_value)s,%(extra_fee_value)s,%(tax_value)s,%(face_value)s,%(description)s,%(quantity)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'invoice_id':invoice_id,
                'name':name,
                'status':status,
                'type':type,
                'category':category,
                'first_relation_id':first_relation_id,
                'second_relation_id':second_relation_id,
                'third_relation_id':third_relation_id,
                'paid_status':paid_status,
                'payment_id':payment_id,
                'origin_value':origin_value,
                'discount_value':discount_value,
                'invoice_charge_value':invoice_charge_value,
                'additional_members_value':additional_members_value,
                'extra_fee_value':extra_fee_value,
                'tax_value':tax_value,
                'face_value':face_value,
                'description':description,
                'quantity':quantity,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_finance_invoice_item =====================
## ================= begin move_vwz_finance_invoice_itemextracharge =====================
def move_vwz_finance_invoice_itemextracharge(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, item_id, tax_id, type, title, tax_internal_name, tax_number, percentage, value, valid, created_on, created_by, last_modified, last_modified_by from vwz_finance_invoice_itemextracharge")
    for source_row in source_cursor:
        id = source_row[0]
        item_id = source_row[1]
        tax_id = source_row[2]
        type = source_row[3]
        title = source_row[4]
        tax_internal_name = source_row[5]
        tax_number = source_row[6]
        percentage = source_row[7]
        value = source_row[8]
        valid = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_finance_invoice_itemextracharge(id, item_id, tax_id, type, title, tax_internal_name, tax_number, percentage, value, valid, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(item_id)s,%(tax_id)s,%(type)s,%(title)s,%(tax_internal_name)s,%(tax_number)s,%(percentage)s,%(value)s,%(valid)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'item_id':item_id,
                'tax_id':tax_id,
                'type':type,
                'title':title,
                'tax_internal_name':tax_internal_name,
                'tax_number':tax_number,
                'percentage':percentage,
                'value':value,
                'valid':valid,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_finance_invoice_itemextracharge =====================
## ================= begin move_vwz_finance_item_paymentrelation =====================
def move_vwz_finance_item_paymentrelation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, invoice_id, item_id, payment_id, created_on, created_by, last_modified, last_modified_by from vwz_finance_item_paymentrelation")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        invoice_id = source_row[2]
        item_id = source_row[3]
        payment_id = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_finance_item_paymentrelation(id, organization_id, invoice_id, item_id, payment_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(invoice_id)s,%(item_id)s,%(payment_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'invoice_id':invoice_id,
                'item_id':item_id,
                'payment_id':payment_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_finance_item_paymentrelation =====================
## ================= begin move_vwz_finance_payment =====================
def move_vwz_finance_payment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, payment_date, payment_method, payment_gateway, currency, gateway_fee_amount, amount, net_revenue, status, settle_status, void_date, void_by, void_reason, refunded_date, buyer_given_name, buyer_family_name, internal_note, comment, origin_item_id, payment_track_id, payment_process_type, attach_file_in_email, card_type, last4, online_order_id, external_transaction_id, created_on, created_by, last_modified, last_modified_by from vwz_finance_payment")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        payment_date = source_row[2]
        payment_method = source_row[3]
        payment_gateway = source_row[4]
        currency = source_row[5]
        gateway_fee_amount = source_row[6]
        amount = source_row[7]
        net_revenue = source_row[8]
        status = source_row[9]
        settle_status = source_row[10]
        void_date = source_row[11]
        void_by = source_row[12]
        void_reason = source_row[13]
        refunded_date = source_row[14]
        buyer_given_name = source_row[15]
        buyer_family_name = source_row[16]
        internal_note = source_row[17]
        comment = source_row[18]
        origin_item_id = source_row[19]
        payment_track_id = source_row[20]
        payment_process_type = source_row[21]
        attach_file_in_email = source_row[22]
        card_type = source_row[23]
        last4 = source_row[24]
        online_order_id = source_row[25]
        external_transaction_id = source_row[26]
        created_on = source_row[27]
        created_by = source_row[28]
        last_modified = source_row[29]
        last_modified_by = source_row[30]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_finance_payment(id, organization_id, payment_date, payment_method, payment_gateway, currency, gateway_fee_amount, amount, net_revenue, status, settle_status, void_date, void_by, void_reason, refunded_date, buyer_given_name, buyer_family_name, internal_note, comment, origin_item_id, payment_track_id, payment_process_type, attach_file_in_email, card_type, last4, online_order_id, external_transaction_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(payment_date)s,%(payment_method)s,%(payment_gateway)s,%(currency)s,%(gateway_fee_amount)s,%(amount)s,%(net_revenue)s,%(status)s,%(settle_status)s,%(void_date)s,%(void_by)s,%(void_reason)s,%(refunded_date)s,%(buyer_given_name)s,%(buyer_family_name)s,%(internal_note)s,%(comment)s,%(origin_item_id)s,%(payment_track_id)s,%(payment_process_type)s,%(attach_file_in_email)s,%(card_type)s,%(last4)s,%(online_order_id)s,%(external_transaction_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'payment_date':payment_date,
                'payment_method':payment_method,
                'payment_gateway':payment_gateway,
                'currency':currency,
                'gateway_fee_amount':gateway_fee_amount,
                'amount':amount,
                'net_revenue':net_revenue,
                'status':status,
                'settle_status':settle_status,
                'void_date':void_date,
                'void_by':void_by,
                'void_reason':void_reason,
                'refunded_date':refunded_date,
                'buyer_given_name':buyer_given_name,
                'buyer_family_name':buyer_family_name,
                'internal_note':internal_note,
                'comment':comment,
                'origin_item_id':origin_item_id,
                'payment_track_id':payment_track_id,
                'payment_process_type':payment_process_type,
                'attach_file_in_email':attach_file_in_email,
                'card_type':card_type,
                'last4':last4,
                'online_order_id':online_order_id,
                'external_transaction_id':external_transaction_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_finance_payment =====================
## ================= begin move_vwz_gateway_balance =====================
def move_vwz_gateway_balance(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, gateway, merchant_id, type, amount, currency, created_on, last_modified from vwz_gateway_balance")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        gateway = source_row[2]
        merchant_id = source_row[3]
        type = source_row[4]
        amount = source_row[5]
        currency = source_row[6]
        created_on = source_row[7]
        last_modified = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_balance(id, organization_id, gateway, merchant_id, type, amount, currency, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(gateway)s,%(merchant_id)s,%(type)s,%(amount)s,%(currency)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'gateway':gateway,
                'merchant_id':merchant_id,
                'type':type,
                'amount':amount,
                'currency':currency,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_balance =====================
## ================= begin move_vwz_gateway_customer =====================
def move_vwz_gateway_customer(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, gateway, merchant_id, customer_id, user_id, created_on, created_by from vwz_gateway_customer")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        gateway = source_row[2]
        merchant_id = source_row[3]
        customer_id = source_row[4]
        user_id = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_customer(id, organization_id, gateway, merchant_id, customer_id, user_id, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(gateway)s,%(merchant_id)s,%(customer_id)s,%(user_id)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'gateway':gateway,
                'merchant_id':merchant_id,
                'customer_id':customer_id,
                'user_id':user_id,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_customer =====================
## ================= begin move_vwz_gateway_fee_detail =====================
def move_vwz_gateway_fee_detail(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, gateway, gateway_payout_id, gateway_payment_id, amount, currency, application, description, type, created_on, last_modified from vwz_gateway_fee_detail")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        gateway = source_row[2]
        gateway_payout_id = source_row[3]
        gateway_payment_id = source_row[4]
        amount = source_row[5]
        currency = source_row[6]
        application = source_row[7]
        description = source_row[8]
        type = source_row[9]
        created_on = source_row[10]
        last_modified = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_fee_detail(id, organization_id, gateway, gateway_payout_id, gateway_payment_id, amount, currency, application, description, type, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(gateway)s,%(gateway_payout_id)s,%(gateway_payment_id)s,%(amount)s,%(currency)s,%(application)s,%(description)s,%(type)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'gateway':gateway,
                'gateway_payout_id':gateway_payout_id,
                'gateway_payment_id':gateway_payment_id,
                'amount':amount,
                'currency':currency,
                'application':application,
                'description':description,
                'type':type,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_fee_detail =====================
## ================= begin move_vwz_gateway_key =====================
def move_vwz_gateway_key(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, gateway, environment, public_key, private_key, currency_code, country_code, predefined, created_on, created_by, last_modified, last_modified_by from vwz_gateway_key")
    for source_row in source_cursor:
        id = source_row[0]
        gateway = source_row[1]
        environment = source_row[2]
        public_key = source_row[3]
        private_key = source_row[4]
        currency_code = source_row[5]
        country_code = source_row[6]
        predefined = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_key(id, gateway, environment, public_key, private_key, currency_code, country_code, predefined, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(gateway)s,%(environment)s,%(public_key)s,%(private_key)s,%(currency_code)s,%(country_code)s,%(predefined)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'gateway':gateway,
                'environment':environment,
                'public_key':public_key,
                'private_key':private_key,
                'currency_code':currency_code,
                'country_code':country_code,
                'predefined':predefined,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_key =====================
## ================= begin move_vwz_gateway_merchant =====================
def move_vwz_gateway_merchant(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, tenant_key, gateway_key_id, merchant_id, country_code, currency_code, sync_enabled, predefined, transaction_rate, gateway, created_on, created_by, last_modified, last_modified_by from vwz_gateway_merchant")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        tenant_key = source_row[2]
        gateway_key_id = source_row[3]
        merchant_id = source_row[4]
        country_code = source_row[5]
        currency_code = source_row[6]
        sync_enabled = source_row[7]
        predefined = source_row[8]
        transaction_rate = source_row[9]
        gateway = source_row[10]
        created_on = source_row[11]
        created_by = source_row[12]
        last_modified = source_row[13]
        last_modified_by = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_merchant(id, organization_id, tenant_key, gateway_key_id, merchant_id, country_code, currency_code, sync_enabled, predefined, transaction_rate, gateway, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(tenant_key)s,%(gateway_key_id)s,%(merchant_id)s,%(country_code)s,%(currency_code)s,%(sync_enabled)s,%(predefined)s,%(transaction_rate)s,%(gateway)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'tenant_key':tenant_key,
                'gateway_key_id':gateway_key_id,
                'merchant_id':merchant_id,
                'country_code':country_code,
                'currency_code':currency_code,
                'sync_enabled':sync_enabled,
                'predefined':predefined,
                'transaction_rate':transaction_rate,
                'gateway':gateway,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_merchant =====================
## ================= begin move_vwz_gateway_payment =====================
def move_vwz_gateway_payment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, transaction_type, transaction_object_id, transaction_id, gateway, order_id, paid_amount, paid_amount_currency, commission_amount, commission_amount_currency, rate, rated_amount, rated_amount_currency, fixed_amount, fixed_amount_currency, comments, created_on, created_by, last_modified, last_modified_by from vwz_gateway_payment")
    for source_row in source_cursor:
        organization_id = source_row[0]
        transaction_type = source_row[1]
        transaction_object_id = source_row[2]
        transaction_id = source_row[3]
        gateway = source_row[4]
        order_id = source_row[5]
        paid_amount = source_row[6]
        paid_amount_currency = source_row[7]
        commission_amount = source_row[8]
        commission_amount_currency = source_row[9]
        rate = source_row[10]
        rated_amount = source_row[11]
        rated_amount_currency = source_row[12]
        fixed_amount = source_row[13]
        fixed_amount_currency = source_row[14]
        comments = source_row[15]
        created_on = source_row[16]
        created_by = source_row[17]
        last_modified = source_row[18]
        last_modified_by = source_row[19]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_payment(organization_id, transaction_type, transaction_object_id, transaction_id, gateway, order_id, paid_amount, paid_amount_currency, commission_amount, commission_amount_currency, rate, rated_amount, rated_amount_currency, fixed_amount, fixed_amount_currency, comments, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(transaction_type)s,%(transaction_object_id)s,%(transaction_id)s,%(gateway)s,%(order_id)s,%(paid_amount)s,%(paid_amount_currency)s,%(commission_amount)s,%(commission_amount_currency)s,%(rate)s,%(rated_amount)s,%(rated_amount_currency)s,%(fixed_amount)s,%(fixed_amount_currency)s,%(comments)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'transaction_type':transaction_type,
                'transaction_object_id':transaction_object_id,
                'transaction_id':transaction_id,
                'gateway':gateway,
                'order_id':order_id,
                'paid_amount':paid_amount,
                'paid_amount_currency':paid_amount_currency,
                'commission_amount':commission_amount,
                'commission_amount_currency':commission_amount_currency,
                'rate':rate,
                'rated_amount':rated_amount,
                'rated_amount_currency':rated_amount_currency,
                'fixed_amount':fixed_amount,
                'fixed_amount_currency':fixed_amount_currency,
                'comments':comments,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_payment =====================
## ================= begin move_vwz_gateway_payment_method =====================
def move_vwz_gateway_payment_method(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, customer_id, tenant_key, organization_id, account, account_name, is_default, gateway, type, id_at_gateway, token, country_code, expiration_year, expiration_month, last_used_date_time, created_on, created_by, last_modified, is_delete from vwz_gateway_payment_method")
    for source_row in source_cursor:
        id = source_row[0]
        customer_id = source_row[1]
        tenant_key = source_row[2]
        organization_id = source_row[3]
        account = source_row[4]
        account_name = source_row[5]
        is_default = source_row[6]
        gateway = source_row[7]
        type = source_row[8]
        id_at_gateway = source_row[9]
        token = source_row[10]
        country_code = source_row[11]
        expiration_year = source_row[12]
        expiration_month = source_row[13]
        last_used_date_time = source_row[14]
        created_on = source_row[15]
        created_by = source_row[16]
        last_modified = source_row[17]
        is_delete = source_row[18]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_payment_method(id, customer_id, tenant_key, organization_id, account, account_name, is_default, gateway, type, id_at_gateway, token, country_code, expiration_year, expiration_month, last_used_date_time, created_on, created_by, last_modified, is_delete) "
              "values(%(id)s,%(customer_id)s,%(tenant_key)s,%(organization_id)s,%(account)s,%(account_name)s,%(is_default)s,%(gateway)s,%(type)s,%(id_at_gateway)s,%(token)s,%(country_code)s,%(expiration_year)s,%(expiration_month)s,%(last_used_date_time)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(is_delete)s)")
        data = {  
                'id':id,
                'customer_id':customer_id,
                'tenant_key':tenant_key,
                'organization_id':organization_id,
                'account':account,
                'account_name':account_name,
                'is_default':is_default,
                'gateway':gateway,
                'type':type,
                'id_at_gateway':id_at_gateway,
                'token':token,
                'country_code':country_code,
                'expiration_year':expiration_year,
                'expiration_month':expiration_month,
                'last_used_date_time':last_used_date_time,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'is_delete':is_delete
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_payment_method =====================
## ================= begin move_vwz_gateway_payout =====================
def move_vwz_gateway_payout(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, gateway_balance_transaction_id, gateway_payout_id, gateway, merchant_id, amount, arrival_date, gateway_created_on, currency, status, message, created_on, last_modified, automatic from vwz_gateway_payout")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        gateway_balance_transaction_id = source_row[2]
        gateway_payout_id = source_row[3]
        gateway = source_row[4]
        merchant_id = source_row[5]
        amount = source_row[6]
        arrival_date = source_row[7]
        gateway_created_on = source_row[8]
        currency = source_row[9]
        status = source_row[10]
        message = source_row[11]
        created_on = source_row[12]
        last_modified = source_row[13]
        automatic = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_payout(id, organization_id, gateway_balance_transaction_id, gateway_payout_id, gateway, merchant_id, amount, arrival_date, gateway_created_on, currency, status, message, created_on, last_modified, automatic) "
              "values(%(id)s,%(organization_id)s,%(gateway_balance_transaction_id)s,%(gateway_payout_id)s,%(gateway)s,%(merchant_id)s,%(amount)s,%(arrival_date)s,%(gateway_created_on)s,%(currency)s,%(status)s,%(message)s,%(created_on)s,%(last_modified)s,%(automatic)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'gateway_balance_transaction_id':gateway_balance_transaction_id,
                'gateway_payout_id':gateway_payout_id,
                'gateway':gateway,
                'merchant_id':merchant_id,
                'amount':amount,
                'arrival_date':arrival_date,
                'gateway_created_on':gateway_created_on,
                'currency':currency,
                'status':status,
                'message':message,
                'created_on':created_on,
                'last_modified':last_modified,
                'automatic':automatic
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_payout =====================
## ================= begin move_vwz_gateway_transaction =====================
def move_vwz_gateway_transaction(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, gateway_balance_transaction_id, gateway_payment_id, gateway, merchant_id, available_on, gateway_created_on, currency, amount, fee_amount, refund_amount, net_amount, exchange_rate, status, refunded, message, created_on, last_modified from vwz_gateway_transaction")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        gateway_balance_transaction_id = source_row[2]
        gateway_payment_id = source_row[3]
        gateway = source_row[4]
        merchant_id = source_row[5]
        available_on = source_row[6]
        gateway_created_on = source_row[7]
        currency = source_row[8]
        amount = source_row[9]
        fee_amount = source_row[10]
        refund_amount = source_row[11]
        net_amount = source_row[12]
        exchange_rate = source_row[13]
        status = source_row[14]
        refunded = source_row[15]
        message = source_row[16]
        created_on = source_row[17]
        last_modified = source_row[18]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_transaction(id, organization_id, gateway_balance_transaction_id, gateway_payment_id, gateway, merchant_id, available_on, gateway_created_on, currency, amount, fee_amount, refund_amount, net_amount, exchange_rate, status, refunded, message, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(gateway_balance_transaction_id)s,%(gateway_payment_id)s,%(gateway)s,%(merchant_id)s,%(available_on)s,%(gateway_created_on)s,%(currency)s,%(amount)s,%(fee_amount)s,%(refund_amount)s,%(net_amount)s,%(exchange_rate)s,%(status)s,%(refunded)s,%(message)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'gateway_balance_transaction_id':gateway_balance_transaction_id,
                'gateway_payment_id':gateway_payment_id,
                'gateway':gateway,
                'merchant_id':merchant_id,
                'available_on':available_on,
                'gateway_created_on':gateway_created_on,
                'currency':currency,
                'amount':amount,
                'fee_amount':fee_amount,
                'refund_amount':refund_amount,
                'net_amount':net_amount,
                'exchange_rate':exchange_rate,
                'status':status,
                'refunded':refunded,
                'message':message,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_transaction =====================
## ================= begin move_vwz_gateway_transaction_payout_relation =====================
def move_vwz_gateway_transaction_payout_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select payment_id, gateway, gateway_transfer_id, gateway_balance_transaction_id, gateway_payout_id, gateway_payment_id, gateway_charge_id, external_transaction_id, created_on, last_modified from vwz_gateway_transaction_payout_relation")
    for source_row in source_cursor:
        payment_id = source_row[0]
        gateway = source_row[1]
        gateway_transfer_id = source_row[2]
        gateway_balance_transaction_id = source_row[3]
        gateway_payout_id = source_row[4]
        gateway_payment_id = source_row[5]
        gateway_charge_id = source_row[6]
        external_transaction_id = source_row[7]
        created_on = source_row[8]
        last_modified = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gateway_transaction_payout_relation(payment_id, gateway, gateway_transfer_id, gateway_balance_transaction_id, gateway_payout_id, gateway_payment_id, gateway_charge_id, external_transaction_id, created_on, last_modified) "
              "values(%(payment_id)s,%(gateway)s,%(gateway_transfer_id)s,%(gateway_balance_transaction_id)s,%(gateway_payout_id)s,%(gateway_payment_id)s,%(gateway_charge_id)s,%(external_transaction_id)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'payment_id':payment_id,
                'gateway':gateway,
                'gateway_transfer_id':gateway_transfer_id,
                'gateway_balance_transaction_id':gateway_balance_transaction_id,
                'gateway_payout_id':gateway_payout_id,
                'gateway_payment_id':gateway_payment_id,
                'gateway_charge_id':gateway_charge_id,
                'external_transaction_id':external_transaction_id,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gateway_transaction_payout_relation =====================
## ================= begin move_vwz_gun_badge =====================
def move_vwz_gun_badge(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, object_type, object_id, badge_type, badge_name, received_on, active, created_on, last_modified from vwz_gun_badge")
    for source_row in source_cursor:
        id = source_row[0]
        object_type = source_row[1]
        object_id = source_row[2]
        badge_type = source_row[3]
        badge_name = source_row[4]
        received_on = source_row[5]
        active = source_row[6]
        created_on = source_row[7]
        last_modified = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gun_badge(id, object_type, object_id, badge_type, badge_name, received_on, active, created_on, last_modified) "
              "values(%(id)s,%(object_type)s,%(object_id)s,%(badge_type)s,%(badge_name)s,%(received_on)s,%(active)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'object_type':object_type,
                'object_id':object_id,
                'badge_type':badge_type,
                'badge_name':badge_name,
                'received_on':received_on,
                'active':active,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gun_badge =====================
## ================= begin move_vwz_gun_level =====================
def move_vwz_gun_level(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, object_type, object_id, level_type, level_name, received_on, active, created_on, last_modified from vwz_gun_level")
    for source_row in source_cursor:
        id = source_row[0]
        object_type = source_row[1]
        object_id = source_row[2]
        level_type = source_row[3]
        level_name = source_row[4]
        received_on = source_row[5]
        active = source_row[6]
        created_on = source_row[7]
        last_modified = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gun_level(id, object_type, object_id, level_type, level_name, received_on, active, created_on, last_modified) "
              "values(%(id)s,%(object_type)s,%(object_id)s,%(level_type)s,%(level_name)s,%(received_on)s,%(active)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'object_type':object_type,
                'object_id':object_id,
                'level_type':level_type,
                'level_name':level_name,
                'received_on':received_on,
                'active':active,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gun_level =====================
## ================= begin move_vwz_gun_points =====================
def move_vwz_gun_points(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, object_type, object_id, relation_type, relation_id, quantity, source_id, received_on, valid, invalid_reason, created_on, last_modified from vwz_gun_points")
    for source_row in source_cursor:
        id = source_row[0]
        object_type = source_row[1]
        object_id = source_row[2]
        relation_type = source_row[3]
        relation_id = source_row[4]
        quantity = source_row[5]
        source_id = source_row[6]
        received_on = source_row[7]
        valid = source_row[8]
        invalid_reason = source_row[9]
        created_on = source_row[10]
        last_modified = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gun_points(id, object_type, object_id, relation_type, relation_id, quantity, source_id, received_on, valid, invalid_reason, created_on, last_modified) "
              "values(%(id)s,%(object_type)s,%(object_id)s,%(relation_type)s,%(relation_id)s,%(quantity)s,%(source_id)s,%(received_on)s,%(valid)s,%(invalid_reason)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'object_type':object_type,
                'object_id':object_id,
                'relation_type':relation_type,
                'relation_id':relation_id,
                'quantity':quantity,
                'source_id':source_id,
                'received_on':received_on,
                'valid':valid,
                'invalid_reason':invalid_reason,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gun_points =====================
## ================= begin move_vwz_gun_referral =====================
def move_vwz_gun_referral(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, referred_by, status, account_registered_on, referral_completed_on, action, action_source_id, created_on, last_modified from vwz_gun_referral")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        referred_by = source_row[2]
        status = source_row[3]
        account_registered_on = source_row[4]
        referral_completed_on = source_row[5]
        action = source_row[6]
        action_source_id = source_row[7]
        created_on = source_row[8]
        last_modified = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_gun_referral(id, user_id, referred_by, status, account_registered_on, referral_completed_on, action, action_source_id, created_on, last_modified) "
              "values(%(id)s,%(user_id)s,%(referred_by)s,%(status)s,%(account_registered_on)s,%(referral_completed_on)s,%(action)s,%(action_source_id)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'referred_by':referred_by,
                'status':status,
                'account_registered_on':account_registered_on,
                'referral_completed_on':referral_completed_on,
                'action':action,
                'action_source_id':action_source_id,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_gun_referral =====================
## ================= begin move_vwz_meeting =====================
def move_vwz_meeting(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, title, start_date_time, abs_start_date_time, timezone, duration, status, location, url, summary, sender_user_id, receiver_user_id, created_on, created_by, last_modified, last_modified_by from vwz_meeting")
    for source_row in source_cursor:
        id = source_row[0]
        title = source_row[1]
        start_date_time = source_row[2]
        abs_start_date_time = source_row[3]
        timezone = source_row[4]
        duration = source_row[5]
        status = source_row[6]
        location = source_row[7]
        url = source_row[8]
        summary = source_row[9]
        sender_user_id = source_row[10]
        receiver_user_id = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_meeting(id, title, start_date_time, abs_start_date_time, timezone, duration, status, location, url, summary, sender_user_id, receiver_user_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(title)s,%(start_date_time)s,%(abs_start_date_time)s,%(timezone)s,%(duration)s,%(status)s,%(location)s,%(url)s,%(summary)s,%(sender_user_id)s,%(receiver_user_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'title':title,
                'start_date_time':start_date_time,
                'abs_start_date_time':abs_start_date_time,
                'timezone':timezone,
                'duration':duration,
                'status':status,
                'location':location,
                'url':url,
                'summary':summary,
                'sender_user_id':sender_user_id,
                'receiver_user_id':receiver_user_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_meeting =====================
## ================= begin move_vwz_member =====================
def move_vwz_member(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, type, membership_id, purchased_count, ref_id, name, sortable_company_name, given_name, family_name, email, phone, fax, company_id, company_name, status, position_title, business_function_code, business_role_code, industry_code, street_address, city_name, province, zip_code, company_website_address, company_language_code, country_code, primary, verification_code, user_id, profile_picture_id, profile_activated, activated_date, member_since, deleted, show_in_directory, featured, created_on, created_by, last_modified, last_modified_by from vwz_member")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        type = source_row[2]
        membership_id = source_row[3]
        purchased_count = source_row[4]
        ref_id = source_row[5]
        name = source_row[6]
        sortable_company_name = source_row[7]
        given_name = source_row[8]
        family_name = source_row[9]
        email = source_row[10]
        phone = source_row[11]
        fax = source_row[12]
        company_id = source_row[13]
        company_name = source_row[14]
        status = source_row[15]
        position_title = source_row[16]
        business_function_code = source_row[17]
        business_role_code = source_row[18]
        industry_code = source_row[19]
        street_address = source_row[20]
        city_name = source_row[21]
        province = source_row[22]
        zip_code = source_row[23]
        company_website_address = source_row[24]
        company_language_code = source_row[25]
        country_code = source_row[26]
        primary = source_row[27]
        verification_code = source_row[28]
        user_id = source_row[29]
        profile_picture_id = source_row[30]
        profile_activated = source_row[31]
        activated_date = source_row[32]
        member_since = source_row[33]
        deleted = source_row[34]
        show_in_directory = source_row[35]
        featured = source_row[36]
        created_on = source_row[37]
        created_by = source_row[38]
        last_modified = source_row[39]
        last_modified_by = source_row[40]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_member(id, organization_id, type, membership_id, purchased_count, ref_id, name, sortable_company_name, given_name, family_name, email, phone, fax, company_id, company_name, status, position_title, business_function_code, business_role_code, industry_code, street_address, city_name, province, zip_code, company_website_address, company_language_code, country_code, primary, verification_code, user_id, profile_picture_id, profile_activated, activated_date, member_since, deleted, show_in_directory, featured, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(type)s,%(membership_id)s,%(purchased_count)s,%(ref_id)s,%(name)s,%(sortable_company_name)s,%(given_name)s,%(family_name)s,%(email)s,%(phone)s,%(fax)s,%(company_id)s,%(company_name)s,%(status)s,%(position_title)s,%(business_function_code)s,%(business_role_code)s,%(industry_code)s,%(street_address)s,%(city_name)s,%(province)s,%(zip_code)s,%(company_website_address)s,%(company_language_code)s,%(country_code)s,%(primary)s,%(verification_code)s,%(user_id)s,%(profile_picture_id)s,%(profile_activated)s,%(activated_date)s,%(member_since)s,%(deleted)s,%(show_in_directory)s,%(featured)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'type':type,
                'membership_id':membership_id,
                'purchased_count':purchased_count,
                'ref_id':ref_id,
                'name':name,
                'sortable_company_name':sortable_company_name,
                'given_name':given_name,
                'family_name':family_name,
                'email':email,
                'phone':phone,
                'fax':fax,
                'company_id':company_id,
                'company_name':company_name,
                'status':status,
                'position_title':position_title,
                'business_function_code':business_function_code,
                'business_role_code':business_role_code,
                'industry_code':industry_code,
                'street_address':street_address,
                'city_name':city_name,
                'province':province,
                'zip_code':zip_code,
                'company_website_address':company_website_address,
                'company_language_code':company_language_code,
                'country_code':country_code,
                'primary':primary,
                'verification_code':verification_code,
                'user_id':user_id,
                'profile_picture_id':profile_picture_id,
                'profile_activated':profile_activated,
                'activated_date':activated_date,
                'member_since':member_since,
                'deleted':deleted,
                'show_in_directory':show_in_directory,
                'featured':featured,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_member =====================
## ================= begin move_vwz_membership =====================
def move_vwz_membership(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_type_id, membership_type_version, version, term_count, member_id, total_price, membership_count, unit_price, membership_total, additional_members_count, additional_members_unit_price, additional_members_total, extra_members_count, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, extra_fee_percentage, extra_fee_total, extra_fee_invoice_charge_percentage, extra_fee_invoice_charge_total, extra_fee_taxes_total, application_fee_separate, application_fee_paid, discount_percentage, discount_total, taxes_total, currency_code, recurring_payments_enabled, activated_date, start_date, end_date, duration, pro_rata_factor, pro_rata_calc_date, pro_rata_end_date, pro_rata_cross_term, payment_status, status, application_status, assignee_id, renewal_id, deleted, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, type, approval_required, purchased_count, payment_way, gateway, company_name, primary_member_family_name, primary_member_given_name, primary_member_email, assignee_family_name, assignee_given_name, assignee_email, assignee_user_deleted, show_logo, first_term, multi_term, multi_term_end_on, first_term_end_on, active_member_count, last_membership_id, last_modified, last_modified_by, created_on, created_by, last_status_changed_on, last_status_changed_by from vwz_membership")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_type_id = source_row[2]
        membership_type_version = source_row[3]
        version = source_row[4]
        term_count = source_row[5]
        member_id = source_row[6]
        total_price = source_row[7]
        membership_count = source_row[8]
        unit_price = source_row[9]
        membership_total = source_row[10]
        additional_members_count = source_row[11]
        additional_members_unit_price = source_row[12]
        additional_members_total = source_row[13]
        extra_members_count = source_row[14]
        current_invoice_id = source_row[15]
        invoice_type = source_row[16]
        invoice_charge_percentage = source_row[17]
        invoice_charge_total = source_row[18]
        extra_fee_percentage = source_row[19]
        extra_fee_total = source_row[20]
        extra_fee_invoice_charge_percentage = source_row[21]
        extra_fee_invoice_charge_total = source_row[22]
        extra_fee_taxes_total = source_row[23]
        application_fee_separate = source_row[24]
        application_fee_paid = source_row[25]
        discount_percentage = source_row[26]
        discount_total = source_row[27]
        taxes_total = source_row[28]
        currency_code = source_row[29]
        recurring_payments_enabled = source_row[30]
        activated_date = source_row[31]
        start_date = source_row[32]
        end_date = source_row[33]
        duration = source_row[34]
        pro_rata_factor = source_row[35]
        pro_rata_calc_date = source_row[36]
        pro_rata_end_date = source_row[37]
        pro_rata_cross_term = source_row[38]
        payment_status = source_row[39]
        status = source_row[40]
        application_status = source_row[41]
        assignee_id = source_row[42]
        renewal_id = source_row[43]
        deleted = source_row[44]
        purchaser_given_name = source_row[45]
        purchaser_family_name = source_row[46]
        purchaser_email = source_row[47]
        purchaser_phone = source_row[48]
        purchaser_company_name = source_row[49]
        purchaser_position_title = source_row[50]
        purchaser_business_function_code = source_row[51]
        purchaser_business_role_code = source_row[52]
        purchaser_referral_text = source_row[53]
        purchaser_language_code = source_row[54]
        purchaser_address_country_code = source_row[55]
        purchaser_address_zip_code = source_row[56]
        type = source_row[57]
        approval_required = source_row[58]
        purchased_count = source_row[59]
        payment_way = source_row[60]
        gateway = source_row[61]
        company_name = source_row[62]
        primary_member_family_name = source_row[63]
        primary_member_given_name = source_row[64]
        primary_member_email = source_row[65]
        assignee_family_name = source_row[66]
        assignee_given_name = source_row[67]
        assignee_email = source_row[68]
        assignee_user_deleted = source_row[69]
        show_logo = source_row[70]
        first_term = source_row[71]
        multi_term = source_row[72]
        multi_term_end_on = source_row[73]
        first_term_end_on = source_row[74]
        active_member_count = source_row[75]
        last_membership_id = source_row[76]
        last_modified = source_row[77]
        last_modified_by = source_row[78]
        created_on = source_row[79]
        created_by = source_row[80]
        last_status_changed_on = source_row[81]
        last_status_changed_by = source_row[82]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership(id, organization_id, membership_type_id, membership_type_version, version, term_count, member_id, total_price, membership_count, unit_price, membership_total, additional_members_count, additional_members_unit_price, additional_members_total, extra_members_count, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, extra_fee_percentage, extra_fee_total, extra_fee_invoice_charge_percentage, extra_fee_invoice_charge_total, extra_fee_taxes_total, application_fee_separate, application_fee_paid, discount_percentage, discount_total, taxes_total, currency_code, recurring_payments_enabled, activated_date, start_date, end_date, duration, pro_rata_factor, pro_rata_calc_date, pro_rata_end_date, pro_rata_cross_term, payment_status, status, application_status, assignee_id, renewal_id, deleted, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, type, approval_required, purchased_count, payment_way, gateway, company_name, primary_member_family_name, primary_member_given_name, primary_member_email, assignee_family_name, assignee_given_name, assignee_email, assignee_user_deleted, show_logo, first_term, multi_term, multi_term_end_on, first_term_end_on, active_member_count, last_membership_id, last_modified, last_modified_by, created_on, created_by, last_status_changed_on, last_status_changed_by) "
              "values(%(id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(version)s,%(term_count)s,%(member_id)s,%(total_price)s,%(membership_count)s,%(unit_price)s,%(membership_total)s,%(additional_members_count)s,%(additional_members_unit_price)s,%(additional_members_total)s,%(extra_members_count)s,%(current_invoice_id)s,%(invoice_type)s,%(invoice_charge_percentage)s,%(invoice_charge_total)s,%(extra_fee_percentage)s,%(extra_fee_total)s,%(extra_fee_invoice_charge_percentage)s,%(extra_fee_invoice_charge_total)s,%(extra_fee_taxes_total)s,%(application_fee_separate)s,%(application_fee_paid)s,%(discount_percentage)s,%(discount_total)s,%(taxes_total)s,%(currency_code)s,%(recurring_payments_enabled)s,%(activated_date)s,%(start_date)s,%(end_date)s,%(duration)s,%(pro_rata_factor)s,%(pro_rata_calc_date)s,%(pro_rata_end_date)s,%(pro_rata_cross_term)s,%(payment_status)s,%(status)s,%(application_status)s,%(assignee_id)s,%(renewal_id)s,%(deleted)s,%(purchaser_given_name)s,%(purchaser_family_name)s,%(purchaser_email)s,%(purchaser_phone)s,%(purchaser_company_name)s,%(purchaser_position_title)s,%(purchaser_business_function_code)s,%(purchaser_business_role_code)s,%(purchaser_referral_text)s,%(purchaser_language_code)s,%(purchaser_address_country_code)s,%(purchaser_address_zip_code)s,%(type)s,%(approval_required)s,%(purchased_count)s,%(payment_way)s,%(gateway)s,%(company_name)s,%(primary_member_family_name)s,%(primary_member_given_name)s,%(primary_member_email)s,%(assignee_family_name)s,%(assignee_given_name)s,%(assignee_email)s,%(assignee_user_deleted)s,%(show_logo)s,%(first_term)s,%(multi_term)s,%(multi_term_end_on)s,%(first_term_end_on)s,%(active_member_count)s,%(last_membership_id)s,%(last_modified)s,%(last_modified_by)s,%(created_on)s,%(created_by)s,%(last_status_changed_on)s,%(last_status_changed_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'version':version,
                'term_count':term_count,
                'member_id':member_id,
                'total_price':total_price,
                'membership_count':membership_count,
                'unit_price':unit_price,
                'membership_total':membership_total,
                'additional_members_count':additional_members_count,
                'additional_members_unit_price':additional_members_unit_price,
                'additional_members_total':additional_members_total,
                'extra_members_count':extra_members_count,
                'current_invoice_id':current_invoice_id,
                'invoice_type':invoice_type,
                'invoice_charge_percentage':invoice_charge_percentage,
                'invoice_charge_total':invoice_charge_total,
                'extra_fee_percentage':extra_fee_percentage,
                'extra_fee_total':extra_fee_total,
                'extra_fee_invoice_charge_percentage':extra_fee_invoice_charge_percentage,
                'extra_fee_invoice_charge_total':extra_fee_invoice_charge_total,
                'extra_fee_taxes_total':extra_fee_taxes_total,
                'application_fee_separate':application_fee_separate,
                'application_fee_paid':application_fee_paid,
                'discount_percentage':discount_percentage,
                'discount_total':discount_total,
                'taxes_total':taxes_total,
                'currency_code':currency_code,
                'recurring_payments_enabled':recurring_payments_enabled,
                'activated_date':activated_date,
                'start_date':start_date,
                'end_date':end_date,
                'duration':duration,
                'pro_rata_factor':pro_rata_factor,
                'pro_rata_calc_date':pro_rata_calc_date,
                'pro_rata_end_date':pro_rata_end_date,
                'pro_rata_cross_term':pro_rata_cross_term,
                'payment_status':payment_status,
                'status':status,
                'application_status':application_status,
                'assignee_id':assignee_id,
                'renewal_id':renewal_id,
                'deleted':deleted,
                'purchaser_given_name':purchaser_given_name,
                'purchaser_family_name':purchaser_family_name,
                'purchaser_email':purchaser_email,
                'purchaser_phone':purchaser_phone,
                'purchaser_company_name':purchaser_company_name,
                'purchaser_position_title':purchaser_position_title,
                'purchaser_business_function_code':purchaser_business_function_code,
                'purchaser_business_role_code':purchaser_business_role_code,
                'purchaser_referral_text':purchaser_referral_text,
                'purchaser_language_code':purchaser_language_code,
                'purchaser_address_country_code':purchaser_address_country_code,
                'purchaser_address_zip_code':purchaser_address_zip_code,
                'type':type,
                'approval_required':approval_required,
                'purchased_count':purchased_count,
                'payment_way':payment_way,
                'gateway':gateway,
                'company_name':company_name,
                'primary_member_family_name':primary_member_family_name,
                'primary_member_given_name':primary_member_given_name,
                'primary_member_email':primary_member_email,
                'assignee_family_name':assignee_family_name,
                'assignee_given_name':assignee_given_name,
                'assignee_email':assignee_email,
                'assignee_user_deleted':assignee_user_deleted,
                'show_logo':show_logo,
                'first_term':first_term,
                'multi_term':multi_term,
                'multi_term_end_on':multi_term_end_on,
                'first_term_end_on':first_term_end_on,
                'active_member_count':active_member_count,
                'last_membership_id':last_membership_id,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'created_on':created_on,
                'created_by':created_by,
                'last_status_changed_on':last_status_changed_on,
                'last_status_changed_by':last_status_changed_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership =====================
## ================= begin move_vwz_membership_amendment =====================
def move_vwz_membership_amendment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, membership_id, organization_id, membership_type_id, membership_type_version, type, approval_required, recurring_payments_enabled, skipped_confirmation, skipped_approval, from_version, to_version, activation_date, status, last_status, member_id, start_date, end_date, duration, term_count, purchased_count, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, total_price, membership_count, unit_price, membership_total, additional_members_count, additional_members_unit_price, additional_members_total, extra_members_count, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, extra_fee_percentage, extra_fee_total, discount_percentage, discount_total, taxes_total, currency_code, payment_way, gateway, payment_status, cancel_reason_type, cancel_reason, assignee_id, deleted, company_name, primary_member_family_name, primary_member_given_name, assignee_family_name, assignee_given_name, assignee_email, assignee_user_deleted, last_modified, last_modified_by, created_on, created_by, last_status_changed_on, last_status_changed_by from vwz_membership_amendment")
    for source_row in source_cursor:
        id = source_row[0]
        membership_id = source_row[1]
        organization_id = source_row[2]
        membership_type_id = source_row[3]
        membership_type_version = source_row[4]
        type = source_row[5]
        approval_required = source_row[6]
        recurring_payments_enabled = source_row[7]
        skipped_confirmation = source_row[8]
        skipped_approval = source_row[9]
        from_version = source_row[10]
        to_version = source_row[11]
        activation_date = source_row[12]
        status = source_row[13]
        last_status = source_row[14]
        member_id = source_row[15]
        start_date = source_row[16]
        end_date = source_row[17]
        duration = source_row[18]
        term_count = source_row[19]
        purchased_count = source_row[20]
        purchaser_given_name = source_row[21]
        purchaser_family_name = source_row[22]
        purchaser_email = source_row[23]
        purchaser_phone = source_row[24]
        purchaser_company_name = source_row[25]
        purchaser_position_title = source_row[26]
        purchaser_business_function_code = source_row[27]
        purchaser_business_role_code = source_row[28]
        purchaser_referral_text = source_row[29]
        purchaser_language_code = source_row[30]
        purchaser_address_country_code = source_row[31]
        purchaser_address_zip_code = source_row[32]
        total_price = source_row[33]
        membership_count = source_row[34]
        unit_price = source_row[35]
        membership_total = source_row[36]
        additional_members_count = source_row[37]
        additional_members_unit_price = source_row[38]
        additional_members_total = source_row[39]
        extra_members_count = source_row[40]
        current_invoice_id = source_row[41]
        invoice_type = source_row[42]
        invoice_charge_percentage = source_row[43]
        invoice_charge_total = source_row[44]
        extra_fee_percentage = source_row[45]
        extra_fee_total = source_row[46]
        discount_percentage = source_row[47]
        discount_total = source_row[48]
        taxes_total = source_row[49]
        currency_code = source_row[50]
        payment_way = source_row[51]
        gateway = source_row[52]
        payment_status = source_row[53]
        cancel_reason_type = source_row[54]
        cancel_reason = source_row[55]
        assignee_id = source_row[56]
        deleted = source_row[57]
        company_name = source_row[58]
        primary_member_family_name = source_row[59]
        primary_member_given_name = source_row[60]
        assignee_family_name = source_row[61]
        assignee_given_name = source_row[62]
        assignee_email = source_row[63]
        assignee_user_deleted = source_row[64]
        last_modified = source_row[65]
        last_modified_by = source_row[66]
        created_on = source_row[67]
        created_by = source_row[68]
        last_status_changed_on = source_row[69]
        last_status_changed_by = source_row[70]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_amendment(id, membership_id, organization_id, membership_type_id, membership_type_version, type, approval_required, recurring_payments_enabled, skipped_confirmation, skipped_approval, from_version, to_version, activation_date, status, last_status, member_id, start_date, end_date, duration, term_count, purchased_count, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, total_price, membership_count, unit_price, membership_total, additional_members_count, additional_members_unit_price, additional_members_total, extra_members_count, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, extra_fee_percentage, extra_fee_total, discount_percentage, discount_total, taxes_total, currency_code, payment_way, gateway, payment_status, cancel_reason_type, cancel_reason, assignee_id, deleted, company_name, primary_member_family_name, primary_member_given_name, assignee_family_name, assignee_given_name, assignee_email, assignee_user_deleted, last_modified, last_modified_by, created_on, created_by, last_status_changed_on, last_status_changed_by) "
              "values(%(id)s,%(membership_id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(type)s,%(approval_required)s,%(recurring_payments_enabled)s,%(skipped_confirmation)s,%(skipped_approval)s,%(from_version)s,%(to_version)s,%(activation_date)s,%(status)s,%(last_status)s,%(member_id)s,%(start_date)s,%(end_date)s,%(duration)s,%(term_count)s,%(purchased_count)s,%(purchaser_given_name)s,%(purchaser_family_name)s,%(purchaser_email)s,%(purchaser_phone)s,%(purchaser_company_name)s,%(purchaser_position_title)s,%(purchaser_business_function_code)s,%(purchaser_business_role_code)s,%(purchaser_referral_text)s,%(purchaser_language_code)s,%(purchaser_address_country_code)s,%(purchaser_address_zip_code)s,%(total_price)s,%(membership_count)s,%(unit_price)s,%(membership_total)s,%(additional_members_count)s,%(additional_members_unit_price)s,%(additional_members_total)s,%(extra_members_count)s,%(current_invoice_id)s,%(invoice_type)s,%(invoice_charge_percentage)s,%(invoice_charge_total)s,%(extra_fee_percentage)s,%(extra_fee_total)s,%(discount_percentage)s,%(discount_total)s,%(taxes_total)s,%(currency_code)s,%(payment_way)s,%(gateway)s,%(payment_status)s,%(cancel_reason_type)s,%(cancel_reason)s,%(assignee_id)s,%(deleted)s,%(company_name)s,%(primary_member_family_name)s,%(primary_member_given_name)s,%(assignee_family_name)s,%(assignee_given_name)s,%(assignee_email)s,%(assignee_user_deleted)s,%(last_modified)s,%(last_modified_by)s,%(created_on)s,%(created_by)s,%(last_status_changed_on)s,%(last_status_changed_by)s)")
        data = {  
                'id':id,
                'membership_id':membership_id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'type':type,
                'approval_required':approval_required,
                'recurring_payments_enabled':recurring_payments_enabled,
                'skipped_confirmation':skipped_confirmation,
                'skipped_approval':skipped_approval,
                'from_version':from_version,
                'to_version':to_version,
                'activation_date':activation_date,
                'status':status,
                'last_status':last_status,
                'member_id':member_id,
                'start_date':start_date,
                'end_date':end_date,
                'duration':duration,
                'term_count':term_count,
                'purchased_count':purchased_count,
                'purchaser_given_name':purchaser_given_name,
                'purchaser_family_name':purchaser_family_name,
                'purchaser_email':purchaser_email,
                'purchaser_phone':purchaser_phone,
                'purchaser_company_name':purchaser_company_name,
                'purchaser_position_title':purchaser_position_title,
                'purchaser_business_function_code':purchaser_business_function_code,
                'purchaser_business_role_code':purchaser_business_role_code,
                'purchaser_referral_text':purchaser_referral_text,
                'purchaser_language_code':purchaser_language_code,
                'purchaser_address_country_code':purchaser_address_country_code,
                'purchaser_address_zip_code':purchaser_address_zip_code,
                'total_price':total_price,
                'membership_count':membership_count,
                'unit_price':unit_price,
                'membership_total':membership_total,
                'additional_members_count':additional_members_count,
                'additional_members_unit_price':additional_members_unit_price,
                'additional_members_total':additional_members_total,
                'extra_members_count':extra_members_count,
                'current_invoice_id':current_invoice_id,
                'invoice_type':invoice_type,
                'invoice_charge_percentage':invoice_charge_percentage,
                'invoice_charge_total':invoice_charge_total,
                'extra_fee_percentage':extra_fee_percentage,
                'extra_fee_total':extra_fee_total,
                'discount_percentage':discount_percentage,
                'discount_total':discount_total,
                'taxes_total':taxes_total,
                'currency_code':currency_code,
                'payment_way':payment_way,
                'gateway':gateway,
                'payment_status':payment_status,
                'cancel_reason_type':cancel_reason_type,
                'cancel_reason':cancel_reason,
                'assignee_id':assignee_id,
                'deleted':deleted,
                'company_name':company_name,
                'primary_member_family_name':primary_member_family_name,
                'primary_member_given_name':primary_member_given_name,
                'assignee_family_name':assignee_family_name,
                'assignee_given_name':assignee_given_name,
                'assignee_email':assignee_email,
                'assignee_user_deleted':assignee_user_deleted,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'created_on':created_on,
                'created_by':created_by,
                'last_status_changed_on':last_status_changed_on,
                'last_status_changed_by':last_status_changed_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_amendment =====================
## ================= begin move_vwz_membership_custom_email_track =====================
def move_vwz_membership_custom_email_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, member_id, email_type, email_time, email_sent, email_sent_on, not_send_reason, send_lock, created_on, modified_on from vwz_membership_custom_email_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        member_id = source_row[3]
        email_type = source_row[4]
        email_time = source_row[5]
        email_sent = source_row[6]
        email_sent_on = source_row[7]
        not_send_reason = source_row[8]
        send_lock = source_row[9]
        created_on = source_row[10]
        modified_on = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_custom_email_track(id, organization_id, membership_id, member_id, email_type, email_time, email_sent, email_sent_on, not_send_reason, send_lock, created_on, modified_on) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(member_id)s,%(email_type)s,%(email_time)s,%(email_sent)s,%(email_sent_on)s,%(not_send_reason)s,%(send_lock)s,%(created_on)s,%(modified_on)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'member_id':member_id,
                'email_type':email_type,
                'email_time':email_time,
                'email_sent':email_sent,
                'email_sent_on':email_sent_on,
                'not_send_reason':not_send_reason,
                'send_lock':send_lock,
                'created_on':created_on,
                'modified_on':modified_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_custom_email_track =====================
## ================= begin move_vwz_membership_extra_member_purchase =====================
def move_vwz_membership_extra_member_purchase(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, purchased_count, currency_code, total_price, unit_price, prorated_factor, prorated_price, activated_date, start_date, end_date, status, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, taxes_total, payment_way, gateway, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, assignee_id, assignee_family_name, assignee_given_name, assignee_email, assignee_user_deleted, cancel_reason_type, cancel_reason, deleted, created_on, created_by, last_modified, last_modified_by from vwz_membership_extra_member_purchase")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        purchased_count = source_row[3]
        currency_code = source_row[4]
        total_price = source_row[5]
        unit_price = source_row[6]
        prorated_factor = source_row[7]
        prorated_price = source_row[8]
        activated_date = source_row[9]
        start_date = source_row[10]
        end_date = source_row[11]
        status = source_row[12]
        current_invoice_id = source_row[13]
        invoice_type = source_row[14]
        invoice_charge_percentage = source_row[15]
        invoice_charge_total = source_row[16]
        taxes_total = source_row[17]
        payment_way = source_row[18]
        gateway = source_row[19]
        purchaser_given_name = source_row[20]
        purchaser_family_name = source_row[21]
        purchaser_email = source_row[22]
        purchaser_phone = source_row[23]
        purchaser_company_name = source_row[24]
        purchaser_position_title = source_row[25]
        purchaser_business_function_code = source_row[26]
        purchaser_business_role_code = source_row[27]
        purchaser_referral_text = source_row[28]
        purchaser_language_code = source_row[29]
        purchaser_address_country_code = source_row[30]
        purchaser_address_zip_code = source_row[31]
        assignee_id = source_row[32]
        assignee_family_name = source_row[33]
        assignee_given_name = source_row[34]
        assignee_email = source_row[35]
        assignee_user_deleted = source_row[36]
        cancel_reason_type = source_row[37]
        cancel_reason = source_row[38]
        deleted = source_row[39]
        created_on = source_row[40]
        created_by = source_row[41]
        last_modified = source_row[42]
        last_modified_by = source_row[43]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_extra_member_purchase(id, organization_id, membership_id, purchased_count, currency_code, total_price, unit_price, prorated_factor, prorated_price, activated_date, start_date, end_date, status, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, taxes_total, payment_way, gateway, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, assignee_id, assignee_family_name, assignee_given_name, assignee_email, assignee_user_deleted, cancel_reason_type, cancel_reason, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(purchased_count)s,%(currency_code)s,%(total_price)s,%(unit_price)s,%(prorated_factor)s,%(prorated_price)s,%(activated_date)s,%(start_date)s,%(end_date)s,%(status)s,%(current_invoice_id)s,%(invoice_type)s,%(invoice_charge_percentage)s,%(invoice_charge_total)s,%(taxes_total)s,%(payment_way)s,%(gateway)s,%(purchaser_given_name)s,%(purchaser_family_name)s,%(purchaser_email)s,%(purchaser_phone)s,%(purchaser_company_name)s,%(purchaser_position_title)s,%(purchaser_business_function_code)s,%(purchaser_business_role_code)s,%(purchaser_referral_text)s,%(purchaser_language_code)s,%(purchaser_address_country_code)s,%(purchaser_address_zip_code)s,%(assignee_id)s,%(assignee_family_name)s,%(assignee_given_name)s,%(assignee_email)s,%(assignee_user_deleted)s,%(cancel_reason_type)s,%(cancel_reason)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'purchased_count':purchased_count,
                'currency_code':currency_code,
                'total_price':total_price,
                'unit_price':unit_price,
                'prorated_factor':prorated_factor,
                'prorated_price':prorated_price,
                'activated_date':activated_date,
                'start_date':start_date,
                'end_date':end_date,
                'status':status,
                'current_invoice_id':current_invoice_id,
                'invoice_type':invoice_type,
                'invoice_charge_percentage':invoice_charge_percentage,
                'invoice_charge_total':invoice_charge_total,
                'taxes_total':taxes_total,
                'payment_way':payment_way,
                'gateway':gateway,
                'purchaser_given_name':purchaser_given_name,
                'purchaser_family_name':purchaser_family_name,
                'purchaser_email':purchaser_email,
                'purchaser_phone':purchaser_phone,
                'purchaser_company_name':purchaser_company_name,
                'purchaser_position_title':purchaser_position_title,
                'purchaser_business_function_code':purchaser_business_function_code,
                'purchaser_business_role_code':purchaser_business_role_code,
                'purchaser_referral_text':purchaser_referral_text,
                'purchaser_language_code':purchaser_language_code,
                'purchaser_address_country_code':purchaser_address_country_code,
                'purchaser_address_zip_code':purchaser_address_zip_code,
                'assignee_id':assignee_id,
                'assignee_family_name':assignee_family_name,
                'assignee_given_name':assignee_given_name,
                'assignee_email':assignee_email,
                'assignee_user_deleted':assignee_user_deleted,
                'cancel_reason_type':cancel_reason_type,
                'cancel_reason':cancel_reason,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_extra_member_purchase =====================
## ================= begin move_vwz_membership_extra_member_purchase_status_track =====================
def move_vwz_membership_extra_member_purchase_status_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, purchase_id, from, to, require_email, email_template_name, email_id, email_status, note, created_on, created_by, last_modified, last_modified_by from vwz_membership_extra_member_purchase_status_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        purchase_id = source_row[3]
        from_field = source_row[4]
        to = source_row[5]
        require_email = source_row[6]
        email_template_name = source_row[7]
        email_id = source_row[8]
        email_status = source_row[9]
        note = source_row[10]
        created_on = source_row[11]
        created_by = source_row[12]
        last_modified = source_row[13]
        last_modified_by = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_extra_member_purchase_status_track(id, organization_id, membership_id, purchase_id, from, to, require_email, email_template_name, email_id, email_status, note, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(purchase_id)s,%(from)s,%(to)s,%(require_email)s,%(email_template_name)s,%(email_id)s,%(email_status)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'purchase_id':purchase_id,
                'from':from_field,
                'to':to,
                'require_email':require_email,
                'email_template_name':email_template_name,
                'email_id':email_id,
                'email_status':email_status,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_extra_member_purchase_status_track =====================
## ================= begin move_vwz_membership_extra_member_purchase_tax =====================
def move_vwz_membership_extra_member_purchase_tax(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, purchase_id, tax_id, title, percentage, currency_code, total, created_on, created_by, last_modified, last_modified_by from vwz_membership_extra_member_purchase_tax")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        purchase_id = source_row[3]
        tax_id = source_row[4]
        title = source_row[5]
        percentage = source_row[6]
        currency_code = source_row[7]
        total = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_extra_member_purchase_tax(id, organization_id, membership_id, purchase_id, tax_id, title, percentage, currency_code, total, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(purchase_id)s,%(tax_id)s,%(title)s,%(percentage)s,%(currency_code)s,%(total)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'purchase_id':purchase_id,
                'tax_id':tax_id,
                'title':title,
                'percentage':percentage,
                'currency_code':currency_code,
                'total':total,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_extra_member_purchase_tax =====================
## ================= begin move_vwz_membership_group_join_request =====================
def move_vwz_membership_group_join_request(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, community_id, group_id, user_id, member_id, status, created_on, created_by, last_modified, last_modified_by from vwz_membership_group_join_request")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        community_id = source_row[2]
        group_id = source_row[3]
        user_id = source_row[4]
        member_id = source_row[5]
        status = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_group_join_request(id, organization_id, community_id, group_id, user_id, member_id, status, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(community_id)s,%(group_id)s,%(user_id)s,%(member_id)s,%(status)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'community_id':community_id,
                'group_id':group_id,
                'user_id':user_id,
                'member_id':member_id,
                'status':status,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_group_join_request =====================
## ================= begin move_vwz_membership_group_member =====================
def move_vwz_membership_group_member(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, group_id, member_id, deleted, created_on, created_by, last_modified, last_modified_by from vwz_membership_group_member")
    for source_row in source_cursor:
        organization_id = source_row[0]
        group_id = source_row[1]
        member_id = source_row[2]
        deleted = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_group_member(organization_id, group_id, member_id, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(group_id)s,%(member_id)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'group_id':group_id,
                'member_id':member_id,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_group_member =====================
## ================= begin move_vwz_membership_history =====================
def move_vwz_membership_history(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, version, type, approval_required, membership_id, organization_id, membership_type_id, membership_type_version, member_id, total_price, membership_count, unit_price, membership_total, additional_members_count, additional_members_unit_price, additional_members_total, extra_members_count, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, extra_fee_percentage, extra_fee_total, extra_fee_invoice_charge_percentage, extra_fee_invoice_charge_total, extra_fee_taxes_total, application_fee_separate, application_fee_paid, discount_percentage, discount_total, taxes_total, currency_code, recurring_payments_enabled, start_date, end_date, duration, term_count, multi_term_end_on, payment_status, status, application_status, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, renewal_id, purchased_count, payment_way, created_on, created_by, last_modified_by, last_modified from vwz_membership_history")
    for source_row in source_cursor:
        id = source_row[0]
        version = source_row[1]
        type = source_row[2]
        approval_required = source_row[3]
        membership_id = source_row[4]
        organization_id = source_row[5]
        membership_type_id = source_row[6]
        membership_type_version = source_row[7]
        member_id = source_row[8]
        total_price = source_row[9]
        membership_count = source_row[10]
        unit_price = source_row[11]
        membership_total = source_row[12]
        additional_members_count = source_row[13]
        additional_members_unit_price = source_row[14]
        additional_members_total = source_row[15]
        extra_members_count = source_row[16]
        current_invoice_id = source_row[17]
        invoice_type = source_row[18]
        invoice_charge_percentage = source_row[19]
        invoice_charge_total = source_row[20]
        extra_fee_percentage = source_row[21]
        extra_fee_total = source_row[22]
        extra_fee_invoice_charge_percentage = source_row[23]
        extra_fee_invoice_charge_total = source_row[24]
        extra_fee_taxes_total = source_row[25]
        application_fee_separate = source_row[26]
        application_fee_paid = source_row[27]
        discount_percentage = source_row[28]
        discount_total = source_row[29]
        taxes_total = source_row[30]
        currency_code = source_row[31]
        recurring_payments_enabled = source_row[32]
        start_date = source_row[33]
        end_date = source_row[34]
        duration = source_row[35]
        term_count = source_row[36]
        multi_term_end_on = source_row[37]
        payment_status = source_row[38]
        status = source_row[39]
        application_status = source_row[40]
        purchaser_given_name = source_row[41]
        purchaser_family_name = source_row[42]
        purchaser_email = source_row[43]
        purchaser_phone = source_row[44]
        purchaser_company_name = source_row[45]
        purchaser_position_title = source_row[46]
        purchaser_business_function_code = source_row[47]
        purchaser_business_role_code = source_row[48]
        purchaser_referral_text = source_row[49]
        purchaser_language_code = source_row[50]
        purchaser_address_country_code = source_row[51]
        purchaser_address_zip_code = source_row[52]
        renewal_id = source_row[53]
        purchased_count = source_row[54]
        payment_way = source_row[55]
        created_on = source_row[56]
        created_by = source_row[57]
        last_modified_by = source_row[58]
        last_modified = source_row[59]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_history(id, version, type, approval_required, membership_id, organization_id, membership_type_id, membership_type_version, member_id, total_price, membership_count, unit_price, membership_total, additional_members_count, additional_members_unit_price, additional_members_total, extra_members_count, current_invoice_id, invoice_type, invoice_charge_percentage, invoice_charge_total, extra_fee_percentage, extra_fee_total, extra_fee_invoice_charge_percentage, extra_fee_invoice_charge_total, extra_fee_taxes_total, application_fee_separate, application_fee_paid, discount_percentage, discount_total, taxes_total, currency_code, recurring_payments_enabled, start_date, end_date, duration, term_count, multi_term_end_on, payment_status, status, application_status, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, renewal_id, purchased_count, payment_way, created_on, created_by, last_modified_by, last_modified) "
              "values(%(id)s,%(version)s,%(type)s,%(approval_required)s,%(membership_id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(member_id)s,%(total_price)s,%(membership_count)s,%(unit_price)s,%(membership_total)s,%(additional_members_count)s,%(additional_members_unit_price)s,%(additional_members_total)s,%(extra_members_count)s,%(current_invoice_id)s,%(invoice_type)s,%(invoice_charge_percentage)s,%(invoice_charge_total)s,%(extra_fee_percentage)s,%(extra_fee_total)s,%(extra_fee_invoice_charge_percentage)s,%(extra_fee_invoice_charge_total)s,%(extra_fee_taxes_total)s,%(application_fee_separate)s,%(application_fee_paid)s,%(discount_percentage)s,%(discount_total)s,%(taxes_total)s,%(currency_code)s,%(recurring_payments_enabled)s,%(start_date)s,%(end_date)s,%(duration)s,%(term_count)s,%(multi_term_end_on)s,%(payment_status)s,%(status)s,%(application_status)s,%(purchaser_given_name)s,%(purchaser_family_name)s,%(purchaser_email)s,%(purchaser_phone)s,%(purchaser_company_name)s,%(purchaser_position_title)s,%(purchaser_business_function_code)s,%(purchaser_business_role_code)s,%(purchaser_referral_text)s,%(purchaser_language_code)s,%(purchaser_address_country_code)s,%(purchaser_address_zip_code)s,%(renewal_id)s,%(purchased_count)s,%(payment_way)s,%(created_on)s,%(created_by)s,%(last_modified_by)s,%(last_modified)s)")
        data = {  
                'id':id,
                'version':version,
                'type':type,
                'approval_required':approval_required,
                'membership_id':membership_id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'member_id':member_id,
                'total_price':total_price,
                'membership_count':membership_count,
                'unit_price':unit_price,
                'membership_total':membership_total,
                'additional_members_count':additional_members_count,
                'additional_members_unit_price':additional_members_unit_price,
                'additional_members_total':additional_members_total,
                'extra_members_count':extra_members_count,
                'current_invoice_id':current_invoice_id,
                'invoice_type':invoice_type,
                'invoice_charge_percentage':invoice_charge_percentage,
                'invoice_charge_total':invoice_charge_total,
                'extra_fee_percentage':extra_fee_percentage,
                'extra_fee_total':extra_fee_total,
                'extra_fee_invoice_charge_percentage':extra_fee_invoice_charge_percentage,
                'extra_fee_invoice_charge_total':extra_fee_invoice_charge_total,
                'extra_fee_taxes_total':extra_fee_taxes_total,
                'application_fee_separate':application_fee_separate,
                'application_fee_paid':application_fee_paid,
                'discount_percentage':discount_percentage,
                'discount_total':discount_total,
                'taxes_total':taxes_total,
                'currency_code':currency_code,
                'recurring_payments_enabled':recurring_payments_enabled,
                'start_date':start_date,
                'end_date':end_date,
                'duration':duration,
                'term_count':term_count,
                'multi_term_end_on':multi_term_end_on,
                'payment_status':payment_status,
                'status':status,
                'application_status':application_status,
                'purchaser_given_name':purchaser_given_name,
                'purchaser_family_name':purchaser_family_name,
                'purchaser_email':purchaser_email,
                'purchaser_phone':purchaser_phone,
                'purchaser_company_name':purchaser_company_name,
                'purchaser_position_title':purchaser_position_title,
                'purchaser_business_function_code':purchaser_business_function_code,
                'purchaser_business_role_code':purchaser_business_role_code,
                'purchaser_referral_text':purchaser_referral_text,
                'purchaser_language_code':purchaser_language_code,
                'purchaser_address_country_code':purchaser_address_country_code,
                'purchaser_address_zip_code':purchaser_address_zip_code,
                'renewal_id':renewal_id,
                'purchased_count':purchased_count,
                'payment_way':payment_way,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified_by':last_modified_by,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_history =====================
## ================= begin move_vwz_membership_invoice =====================
def move_vwz_membership_invoice(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, object_type, membership_id, object_id, title, currency_code, amount, status, type, pickup_way, street_address, city_name, province, country_code, zip_code, phone, tax_registration_id, tax_registration_address, bank_name, bank_account, note, created_on, created_by, last_modified, last_modified_by, delivery_phone, delivery_email from vwz_membership_invoice")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        object_type = source_row[2]
        membership_id = source_row[3]
        object_id = source_row[4]
        title = source_row[5]
        currency_code = source_row[6]
        amount = source_row[7]
        status = source_row[8]
        type = source_row[9]
        pickup_way = source_row[10]
        street_address = source_row[11]
        city_name = source_row[12]
        province = source_row[13]
        country_code = source_row[14]
        zip_code = source_row[15]
        phone = source_row[16]
        tax_registration_id = source_row[17]
        tax_registration_address = source_row[18]
        bank_name = source_row[19]
        bank_account = source_row[20]
        note = source_row[21]
        created_on = source_row[22]
        created_by = source_row[23]
        last_modified = source_row[24]
        last_modified_by = source_row[25]
        delivery_phone = source_row[26]
        delivery_email = source_row[27]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_invoice(id, organization_id, object_type, membership_id, object_id, title, currency_code, amount, status, type, pickup_way, street_address, city_name, province, country_code, zip_code, phone, tax_registration_id, tax_registration_address, bank_name, bank_account, note, created_on, created_by, last_modified, last_modified_by, delivery_phone, delivery_email) "
              "values(%(id)s,%(organization_id)s,%(object_type)s,%(membership_id)s,%(object_id)s,%(title)s,%(currency_code)s,%(amount)s,%(status)s,%(type)s,%(pickup_way)s,%(street_address)s,%(city_name)s,%(province)s,%(country_code)s,%(zip_code)s,%(phone)s,%(tax_registration_id)s,%(tax_registration_address)s,%(bank_name)s,%(bank_account)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(delivery_phone)s,%(delivery_email)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'object_type':object_type,
                'membership_id':membership_id,
                'object_id':object_id,
                'title':title,
                'currency_code':currency_code,
                'amount':amount,
                'status':status,
                'type':type,
                'pickup_way':pickup_way,
                'street_address':street_address,
                'city_name':city_name,
                'province':province,
                'country_code':country_code,
                'zip_code':zip_code,
                'phone':phone,
                'tax_registration_id':tax_registration_id,
                'tax_registration_address':tax_registration_address,
                'bank_name':bank_name,
                'bank_account':bank_account,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'delivery_phone':delivery_phone,
                'delivery_email':delivery_email
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_invoice =====================
## ================= begin move_vwz_membership_note =====================
def move_vwz_membership_note(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, type, note, created_on, created_by, last_modified, last_modified_by from vwz_membership_note")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        type = source_row[3]
        note = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_note(id, organization_id, membership_id, type, note, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(type)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'type':type,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_note =====================
## ================= begin move_vwz_membership_payment =====================
def move_vwz_membership_payment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, currency_code, paid_value, organization_id, membership_id, transaction_object_id, transaction_type, buyer_family_name, buyer_given_name, status, void_date_time, void_by, method, gateway, date, last_modified, last_modified_by from vwz_membership_payment")
    for source_row in source_cursor:
        id = source_row[0]
        currency_code = source_row[1]
        paid_value = source_row[2]
        organization_id = source_row[3]
        membership_id = source_row[4]
        transaction_object_id = source_row[5]
        transaction_type = source_row[6]
        buyer_family_name = source_row[7]
        buyer_given_name = source_row[8]
        status = source_row[9]
        void_date_time = source_row[10]
        void_by = source_row[11]
        method = source_row[12]
        gateway = source_row[13]
        date = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_payment(id, currency_code, paid_value, organization_id, membership_id, transaction_object_id, transaction_type, buyer_family_name, buyer_given_name, status, void_date_time, void_by, method, gateway, date, last_modified, last_modified_by) "
              "values(%(id)s,%(currency_code)s,%(paid_value)s,%(organization_id)s,%(membership_id)s,%(transaction_object_id)s,%(transaction_type)s,%(buyer_family_name)s,%(buyer_given_name)s,%(status)s,%(void_date_time)s,%(void_by)s,%(method)s,%(gateway)s,%(date)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'currency_code':currency_code,
                'paid_value':paid_value,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'transaction_object_id':transaction_object_id,
                'transaction_type':transaction_type,
                'buyer_family_name':buyer_family_name,
                'buyer_given_name':buyer_given_name,
                'status':status,
                'void_date_time':void_date_time,
                'void_by':void_by,
                'method':method,
                'gateway':gateway,
                'date':date,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_payment =====================
## ================= begin move_vwz_membership_renewal_email_trace =====================
def move_vwz_membership_renewal_email_trace(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, renewal_id, member_id, type, to, status, created_on, smtp_requested_on, smtp_responded_on, failure_cause, recipientLanguageCode, recipientFamilyName, recipientGivenName from vwz_membership_renewal_email_trace")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        renewal_id = source_row[3]
        member_id = source_row[4]
        type = source_row[5]
        to = source_row[6]
        status = source_row[7]
        created_on = source_row[8]
        smtp_requested_on = source_row[9]
        smtp_responded_on = source_row[10]
        failure_cause = source_row[11]
        recipientLanguageCode = source_row[12]
        recipientFamilyName = source_row[13]
        recipientGivenName = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_renewal_email_trace(id, organization_id, membership_id, renewal_id, member_id, type, to, status, created_on, smtp_requested_on, smtp_responded_on, failure_cause, recipientLanguageCode, recipientFamilyName, recipientGivenName) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(renewal_id)s,%(member_id)s,%(type)s,%(to)s,%(status)s,%(created_on)s,%(smtp_requested_on)s,%(smtp_responded_on)s,%(failure_cause)s,%(recipientLanguageCode)s,%(recipientFamilyName)s,%(recipientGivenName)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'renewal_id':renewal_id,
                'member_id':member_id,
                'type':type,
                'to':to,
                'status':status,
                'created_on':created_on,
                'smtp_requested_on':smtp_requested_on,
                'smtp_responded_on':smtp_responded_on,
                'failure_cause':failure_cause,
                'recipientLanguageCode':recipientLanguageCode,
                'recipientFamilyName':recipientFamilyName,
                'recipientGivenName':recipientGivenName
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_renewal_email_trace =====================
## ================= begin move_vwz_membership_renewal_status_track =====================
def move_vwz_membership_renewal_status_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, renewal_id, from, to, note, created_on, created_by from vwz_membership_renewal_status_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        renewal_id = source_row[3]
        from_field = source_row[4]
        to = source_row[5]
        note = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_renewal_status_track(id, organization_id, membership_id, renewal_id, from, to, note, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(renewal_id)s,%(from)s,%(to)s,%(note)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'renewal_id':renewal_id,
                'from':from_field,
                'to':to,
                'note':note,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_renewal_status_track =====================
## ================= begin move_vwz_membership_renewal_tax =====================
def move_vwz_membership_renewal_tax(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, renewal_id, tax_id, title, percentage, currency_code, total, created_on, created_by, last_modified, last_modified_by from vwz_membership_renewal_tax")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        renewal_id = source_row[3]
        tax_id = source_row[4]
        title = source_row[5]
        percentage = source_row[6]
        currency_code = source_row[7]
        total = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_renewal_tax(id, organization_id, membership_id, renewal_id, tax_id, title, percentage, currency_code, total, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(renewal_id)s,%(tax_id)s,%(title)s,%(percentage)s,%(currency_code)s,%(total)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'renewal_id':renewal_id,
                'tax_id':tax_id,
                'title':title,
                'percentage':percentage,
                'currency_code':currency_code,
                'total':total,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_renewal_tax =====================
## ================= begin move_vwz_membership_status_track =====================
def move_vwz_membership_status_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, from, to, require_email, email_template_name, email_id, email_status, note, created_on, created_by, last_modified, last_modified_by from vwz_membership_status_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        from_field = source_row[3]
        to = source_row[4]
        require_email = source_row[5]
        email_template_name = source_row[6]
        email_id = source_row[7]
        email_status = source_row[8]
        note = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_status_track(id, organization_id, membership_id, from, to, require_email, email_template_name, email_id, email_status, note, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(from)s,%(to)s,%(require_email)s,%(email_template_name)s,%(email_id)s,%(email_status)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'from':from_field,
                'to':to,
                'require_email':require_email,
                'email_template_name':email_template_name,
                'email_id':email_id,
                'email_status':email_status,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_status_track =====================
## ================= begin move_vwz_membership_tax =====================
def move_vwz_membership_tax(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, tax_id, title, percentage, currency_code, total, created_on, created_by, last_modified, last_modified_by from vwz_membership_tax")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        tax_id = source_row[3]
        title = source_row[4]
        percentage = source_row[5]
        currency_code = source_row[6]
        total = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_tax(id, organization_id, membership_id, tax_id, title, percentage, currency_code, total, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(tax_id)s,%(title)s,%(percentage)s,%(currency_code)s,%(total)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'tax_id':tax_id,
                'title':title,
                'percentage':percentage,
                'currency_code':currency_code,
                'total':total,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_tax =====================
## ================= begin move_vwz_membership_type =====================
def move_vwz_membership_type(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, title, internal_title, type, status, description, default_language_code, approval_required, renewal_approval_required, member_approval_required, activation_required, renewal_confirm_required, application_fee_separate, recurring_payments_enabled, recurring_payments_only, renewal_skip_to_payment, renewal_creation_period, grace_period_enabled, grace_period, language_code, is_public, show_in_directory, individual_form_id, company_form_id, index, deleted, created_on, created_by, last_modified, last_modified_by, tiered from vwz_membership_type")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        title = source_row[2]
        internal_title = source_row[3]
        type = source_row[4]
        status = source_row[5]
        description = source_row[6]
        default_language_code = source_row[7]
        approval_required = source_row[8]
        renewal_approval_required = source_row[9]
        member_approval_required = source_row[10]
        activation_required = source_row[11]
        renewal_confirm_required = source_row[12]
        application_fee_separate = source_row[13]
        recurring_payments_enabled = source_row[14]
        recurring_payments_only = source_row[15]
        renewal_skip_to_payment = source_row[16]
        renewal_creation_period = source_row[17]
        grace_period_enabled = source_row[18]
        grace_period = source_row[19]
        language_code = source_row[20]
        is_public = source_row[21]
        show_in_directory = source_row[22]
        individual_form_id = source_row[23]
        company_form_id = source_row[24]
        index = source_row[25]
        deleted = source_row[26]
        created_on = source_row[27]
        created_by = source_row[28]
        last_modified = source_row[29]
        last_modified_by = source_row[30]
        tiered = source_row[31]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type(id, organization_id, title, internal_title, type, status, description, default_language_code, approval_required, renewal_approval_required, member_approval_required, activation_required, renewal_confirm_required, application_fee_separate, recurring_payments_enabled, recurring_payments_only, renewal_skip_to_payment, renewal_creation_period, grace_period_enabled, grace_period, language_code, is_public, show_in_directory, individual_form_id, company_form_id, index, deleted, created_on, created_by, last_modified, last_modified_by, tiered) "
              "values(%(id)s,%(organization_id)s,%(title)s,%(internal_title)s,%(type)s,%(status)s,%(description)s,%(default_language_code)s,%(approval_required)s,%(renewal_approval_required)s,%(member_approval_required)s,%(activation_required)s,%(renewal_confirm_required)s,%(application_fee_separate)s,%(recurring_payments_enabled)s,%(recurring_payments_only)s,%(renewal_skip_to_payment)s,%(renewal_creation_period)s,%(grace_period_enabled)s,%(grace_period)s,%(language_code)s,%(is_public)s,%(show_in_directory)s,%(individual_form_id)s,%(company_form_id)s,%(index)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(tiered)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'title':title,
                'internal_title':internal_title,
                'type':type,
                'status':status,
                'description':description,
                'default_language_code':default_language_code,
                'approval_required':approval_required,
                'renewal_approval_required':renewal_approval_required,
                'member_approval_required':member_approval_required,
                'activation_required':activation_required,
                'renewal_confirm_required':renewal_confirm_required,
                'application_fee_separate':application_fee_separate,
                'recurring_payments_enabled':recurring_payments_enabled,
                'recurring_payments_only':recurring_payments_only,
                'renewal_skip_to_payment':renewal_skip_to_payment,
                'renewal_creation_period':renewal_creation_period,
                'grace_period_enabled':grace_period_enabled,
                'grace_period':grace_period,
                'language_code':language_code,
                'is_public':is_public,
                'show_in_directory':show_in_directory,
                'individual_form_id':individual_form_id,
                'company_form_id':company_form_id,
                'index':index,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'tiered':tiered
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type =====================
## ================= begin move_vwz_membership_type_additional_price =====================
def move_vwz_membership_type_additional_price(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_type_id, membership_type_version, unit_price, currency_code, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_additional_price")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_type_id = source_row[2]
        membership_type_version = source_row[3]
        unit_price = source_row[4]
        currency_code = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_additional_price(id, organization_id, membership_type_id, membership_type_version, unit_price, currency_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(unit_price)s,%(currency_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'unit_price':unit_price,
                'currency_code':currency_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_additional_price =====================
## ================= begin move_vwz_membership_type_discount =====================
def move_vwz_membership_type_discount(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_type_id, membership_type_version, applicable_at, percentage, amount, currency_code, term_count, recommanded, pro_rata_enabled, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_discount")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_type_id = source_row[2]
        membership_type_version = source_row[3]
        applicable_at = source_row[4]
        percentage = source_row[5]
        amount = source_row[6]
        currency_code = source_row[7]
        term_count = source_row[8]
        recommanded = source_row[9]
        pro_rata_enabled = source_row[10]
        created_on = source_row[11]
        created_by = source_row[12]
        last_modified = source_row[13]
        last_modified_by = source_row[14]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_discount(id, organization_id, membership_type_id, membership_type_version, applicable_at, percentage, amount, currency_code, term_count, recommanded, pro_rata_enabled, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(applicable_at)s,%(percentage)s,%(amount)s,%(currency_code)s,%(term_count)s,%(recommanded)s,%(pro_rata_enabled)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'applicable_at':applicable_at,
                'percentage':percentage,
                'amount':amount,
                'currency_code':currency_code,
                'term_count':term_count,
                'recommanded':recommanded,
                'pro_rata_enabled':pro_rata_enabled,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_discount =====================
## ================= begin move_vwz_membership_type_email_config =====================
def move_vwz_membership_type_email_config(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_type_id, email_type, purchaser, primary_member, invited_member, members, is_notification, number_of_days, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_email_config")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_type_id = source_row[2]
        email_type = source_row[3]
        purchaser = source_row[4]
        primary_member = source_row[5]
        invited_member = source_row[6]
        members = source_row[7]
        is_notification = source_row[8]
        number_of_days = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_email_config(id, organization_id, membership_type_id, email_type, purchaser, primary_member, invited_member, members, is_notification, number_of_days, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_type_id)s,%(email_type)s,%(purchaser)s,%(primary_member)s,%(invited_member)s,%(members)s,%(is_notification)s,%(number_of_days)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'email_type':email_type,
                'purchaser':purchaser,
                'primary_member':primary_member,
                'invited_member':invited_member,
                'members':members,
                'is_notification':is_notification,
                'number_of_days':number_of_days,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_email_config =====================
## ================= begin move_vwz_membership_type_email_config_staff_recipients =====================
def move_vwz_membership_type_email_config_staff_recipients(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, configuration_id, staff_recipients_type, user_id, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_email_config_staff_recipients")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        configuration_id = source_row[2]
        staff_recipients_type = source_row[3]
        user_id = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_email_config_staff_recipients(id, organization_id, configuration_id, staff_recipients_type, user_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(configuration_id)s,%(staff_recipients_type)s,%(user_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'configuration_id':configuration_id,
                'staff_recipients_type':staff_recipients_type,
                'user_id':user_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_email_config_staff_recipients =====================
## ================= begin move_vwz_membership_type_extra_fee =====================
def move_vwz_membership_type_extra_fee(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_type_id, membership_type_version, applicable_at, percentage, amount, currency_code, pro_rata_enabled, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_extra_fee")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_type_id = source_row[2]
        membership_type_version = source_row[3]
        applicable_at = source_row[4]
        percentage = source_row[5]
        amount = source_row[6]
        currency_code = source_row[7]
        pro_rata_enabled = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_extra_fee(id, organization_id, membership_type_id, membership_type_version, applicable_at, percentage, amount, currency_code, pro_rata_enabled, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(applicable_at)s,%(percentage)s,%(amount)s,%(currency_code)s,%(pro_rata_enabled)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'applicable_at':applicable_at,
                'percentage':percentage,
                'amount':amount,
                'currency_code':currency_code,
                'pro_rata_enabled':pro_rata_enabled,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_extra_fee =====================
## ================= begin move_vwz_membership_type_extra_price =====================
def move_vwz_membership_type_extra_price(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_type_id, membership_type_version, unit_price, currency_code, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_extra_price")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_type_id = source_row[2]
        membership_type_version = source_row[3]
        unit_price = source_row[4]
        currency_code = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_extra_price(id, organization_id, membership_type_id, membership_type_version, unit_price, currency_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(unit_price)s,%(currency_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'unit_price':unit_price,
                'currency_code':currency_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_extra_price =====================
## ================= begin move_vwz_membership_type_invoice =====================
def move_vwz_membership_type_invoice(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_type_id, membership_type_version, is_invoice_applicant_charged, is_bt_invoice_enabled, bt_invoice_charge_percentage, is_vat_general_invoice_enabled, vat_general_invoice_charge_percentage, vat_general_invoice_minimum_amount, is_vat_special_invoice_enabled, vat_special_invoice_charge_percentage, vat_special_invoice_minimum_amount, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_invoice")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_type_id = source_row[2]
        membership_type_version = source_row[3]
        is_invoice_applicant_charged = source_row[4]
        is_bt_invoice_enabled = source_row[5]
        bt_invoice_charge_percentage = source_row[6]
        is_vat_general_invoice_enabled = source_row[7]
        vat_general_invoice_charge_percentage = source_row[8]
        vat_general_invoice_minimum_amount = source_row[9]
        is_vat_special_invoice_enabled = source_row[10]
        vat_special_invoice_charge_percentage = source_row[11]
        vat_special_invoice_minimum_amount = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_invoice(id, organization_id, membership_type_id, membership_type_version, is_invoice_applicant_charged, is_bt_invoice_enabled, bt_invoice_charge_percentage, is_vat_general_invoice_enabled, vat_general_invoice_charge_percentage, vat_general_invoice_minimum_amount, is_vat_special_invoice_enabled, vat_special_invoice_charge_percentage, vat_special_invoice_minimum_amount, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_type_id)s,%(membership_type_version)s,%(is_invoice_applicant_charged)s,%(is_bt_invoice_enabled)s,%(bt_invoice_charge_percentage)s,%(is_vat_general_invoice_enabled)s,%(vat_general_invoice_charge_percentage)s,%(vat_general_invoice_minimum_amount)s,%(is_vat_special_invoice_enabled)s,%(vat_special_invoice_charge_percentage)s,%(vat_special_invoice_minimum_amount)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'membership_type_version':membership_type_version,
                'is_invoice_applicant_charged':is_invoice_applicant_charged,
                'is_bt_invoice_enabled':is_bt_invoice_enabled,
                'bt_invoice_charge_percentage':bt_invoice_charge_percentage,
                'is_vat_general_invoice_enabled':is_vat_general_invoice_enabled,
                'vat_general_invoice_charge_percentage':vat_general_invoice_charge_percentage,
                'vat_general_invoice_minimum_amount':vat_general_invoice_minimum_amount,
                'is_vat_special_invoice_enabled':is_vat_special_invoice_enabled,
                'vat_special_invoice_charge_percentage':vat_special_invoice_charge_percentage,
                'vat_special_invoice_minimum_amount':vat_special_invoice_minimum_amount,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_invoice =====================
## ================= begin move_vwz_membership_type_price =====================
def move_vwz_membership_type_price(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, membership_id, membership_type_version, unit_price, currency_code, deleted, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_price")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        membership_id = source_row[2]
        membership_type_version = source_row[3]
        unit_price = source_row[4]
        currency_code = source_row[5]
        deleted = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_price(id, organization_id, membership_id, membership_type_version, unit_price, currency_code, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(membership_id)s,%(membership_type_version)s,%(unit_price)s,%(currency_code)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'membership_id':membership_id,
                'membership_type_version':membership_type_version,
                'unit_price':unit_price,
                'currency_code':currency_code,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_price =====================
## ================= begin move_vwz_membership_type_version =====================
def move_vwz_membership_type_version(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, membership_type_id, version, status, scheduler_time, duration, member_limit, default_currency_code, new_term_fixed_date, new_term_fixed_day, new_term_fixed_month, pro_rata_enabled, pro_rata_start_from, pro_rata_term_section, pro_rata_price_rounding, cross_term_enabled, cross_term_period, cross_term_exclude_current_price, free, recurring_payments_allowed, recurring_payments_only, additional_members_allowed, additional_members_limit, additional_members_free, extra_members_price_enabled, application_discount, renewal_discount, existing_applications, new_applications, existing_renewals, new_renewals, initiated_on, initiated_by, created_on, created_by, last_modified, last_modified_by from vwz_membership_type_version")
    for source_row in source_cursor:
        organization_id = source_row[0]
        membership_type_id = source_row[1]
        version = source_row[2]
        status = source_row[3]
        scheduler_time = source_row[4]
        duration = source_row[5]
        member_limit = source_row[6]
        default_currency_code = source_row[7]
        new_term_fixed_date = source_row[8]
        new_term_fixed_day = source_row[9]
        new_term_fixed_month = source_row[10]
        pro_rata_enabled = source_row[11]
        pro_rata_start_from = source_row[12]
        pro_rata_term_section = source_row[13]
        pro_rata_price_rounding = source_row[14]
        cross_term_enabled = source_row[15]
        cross_term_period = source_row[16]
        cross_term_exclude_current_price = source_row[17]
        free = source_row[18]
        recurring_payments_allowed = source_row[19]
        recurring_payments_only = source_row[20]
        additional_members_allowed = source_row[21]
        additional_members_limit = source_row[22]
        additional_members_free = source_row[23]
        extra_members_price_enabled = source_row[24]
        application_discount = source_row[25]
        renewal_discount = source_row[26]
        existing_applications = source_row[27]
        new_applications = source_row[28]
        existing_renewals = source_row[29]
        new_renewals = source_row[30]
        initiated_on = source_row[31]
        initiated_by = source_row[32]
        created_on = source_row[33]
        created_by = source_row[34]
        last_modified = source_row[35]
        last_modified_by = source_row[36]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_membership_type_version(organization_id, membership_type_id, version, status, scheduler_time, duration, member_limit, default_currency_code, new_term_fixed_date, new_term_fixed_day, new_term_fixed_month, pro_rata_enabled, pro_rata_start_from, pro_rata_term_section, pro_rata_price_rounding, cross_term_enabled, cross_term_period, cross_term_exclude_current_price, free, recurring_payments_allowed, recurring_payments_only, additional_members_allowed, additional_members_limit, additional_members_free, extra_members_price_enabled, application_discount, renewal_discount, existing_applications, new_applications, existing_renewals, new_renewals, initiated_on, initiated_by, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(membership_type_id)s,%(version)s,%(status)s,%(scheduler_time)s,%(duration)s,%(member_limit)s,%(default_currency_code)s,%(new_term_fixed_date)s,%(new_term_fixed_day)s,%(new_term_fixed_month)s,%(pro_rata_enabled)s,%(pro_rata_start_from)s,%(pro_rata_term_section)s,%(pro_rata_price_rounding)s,%(cross_term_enabled)s,%(cross_term_period)s,%(cross_term_exclude_current_price)s,%(free)s,%(recurring_payments_allowed)s,%(recurring_payments_only)s,%(additional_members_allowed)s,%(additional_members_limit)s,%(additional_members_free)s,%(extra_members_price_enabled)s,%(application_discount)s,%(renewal_discount)s,%(existing_applications)s,%(new_applications)s,%(existing_renewals)s,%(new_renewals)s,%(initiated_on)s,%(initiated_by)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'membership_type_id':membership_type_id,
                'version':version,
                'status':status,
                'scheduler_time':scheduler_time,
                'duration':duration,
                'member_limit':member_limit,
                'default_currency_code':default_currency_code,
                'new_term_fixed_date':new_term_fixed_date,
                'new_term_fixed_day':new_term_fixed_day,
                'new_term_fixed_month':new_term_fixed_month,
                'pro_rata_enabled':pro_rata_enabled,
                'pro_rata_start_from':pro_rata_start_from,
                'pro_rata_term_section':pro_rata_term_section,
                'pro_rata_price_rounding':pro_rata_price_rounding,
                'cross_term_enabled':cross_term_enabled,
                'cross_term_period':cross_term_period,
                'cross_term_exclude_current_price':cross_term_exclude_current_price,
                'free':free,
                'recurring_payments_allowed':recurring_payments_allowed,
                'recurring_payments_only':recurring_payments_only,
                'additional_members_allowed':additional_members_allowed,
                'additional_members_limit':additional_members_limit,
                'additional_members_free':additional_members_free,
                'extra_members_price_enabled':extra_members_price_enabled,
                'application_discount':application_discount,
                'renewal_discount':renewal_discount,
                'existing_applications':existing_applications,
                'new_applications':new_applications,
                'existing_renewals':existing_renewals,
                'new_renewals':new_renewals,
                'initiated_on':initiated_on,
                'initiated_by':initiated_by,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_membership_type_version =====================
## ================= begin move_vwz_member_tag =====================
def move_vwz_member_tag(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, parent_id, type, name, system, created_on, created_by, last_modified, last_modified_by from vwz_member_tag")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        parent_id = source_row[2]
        type = source_row[3]
        name = source_row[4]
        system = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_member_tag(id, organization_id, parent_id, type, name, system, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(parent_id)s,%(type)s,%(name)s,%(system)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'parent_id':parent_id,
                'type':type,
                'name':name,
                'system':system,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_member_tag =====================
## ================= begin move_vwz_migration_task =====================
def move_vwz_migration_task(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, task_name, bean_name, args, scheduler_time, task_type, status, fail_reason, created_on, start_at, end_at from vwz_migration_task")
    for source_row in source_cursor:
        id = source_row[0]
        task_name = source_row[1]
        bean_name = source_row[2]
        args = source_row[3]
        scheduler_time = source_row[4]
        task_type = source_row[5]
        status = source_row[6]
        fail_reason = source_row[7]
        created_on = source_row[8]
        start_at = source_row[9]
        end_at = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_migration_task(id, task_name, bean_name, args, scheduler_time, task_type, status, fail_reason, created_on, start_at, end_at) "
              "values(%(id)s,%(task_name)s,%(bean_name)s,%(args)s,%(scheduler_time)s,%(task_type)s,%(status)s,%(fail_reason)s,%(created_on)s,%(start_at)s,%(end_at)s)")
        data = {  
                'id':id,
                'task_name':task_name,
                'bean_name':bean_name,
                'args':args,
                'scheduler_time':scheduler_time,
                'task_type':task_type,
                'status':status,
                'fail_reason':fail_reason,
                'created_on':created_on,
                'start_at':start_at,
                'end_at':end_at
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_migration_task =====================
## ================= begin move_vwz_notification_message =====================
def move_vwz_notification_message(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select notification_type, language_code, message, created_on, created_by, last_modified, last_modified_by from vwz_notification_message")
    for source_row in source_cursor:
        notification_type = source_row[0]
        language_code = source_row[1]
        message = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_notification_message(notification_type, language_code, message, created_on, created_by, last_modified, last_modified_by) "
              "values(%(notification_type)s,%(language_code)s,%(message)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'notification_type':notification_type,
                'language_code':language_code,
                'message':message,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_notification_message =====================
## ================= begin move_vwz_opportunity_note =====================
def move_vwz_opportunity_note(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, id, opportunity_id, type, note, is_deleted, created_on, created_by, last_modified, last_modified_by from vwz_opportunity_note")
    for source_row in source_cursor:
        organization_id = source_row[0]
        id = source_row[1]
        opportunity_id = source_row[2]
        type = source_row[3]
        note = source_row[4]
        is_deleted = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_opportunity_note(organization_id, id, opportunity_id, type, note, is_deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(id)s,%(opportunity_id)s,%(type)s,%(note)s,%(is_deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'id':id,
                'opportunity_id':opportunity_id,
                'type':type,
                'note':note,
                'is_deleted':is_deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_opportunity_note =====================
## ================= begin move_vwz_opportunity_proposal =====================
def move_vwz_opportunity_proposal(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, opportunity_id, name, description, sent_date, deleted, created_on, created_by, last_modified, last_modified_by from vwz_opportunity_proposal")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        opportunity_id = source_row[2]
        name = source_row[3]
        description = source_row[4]
        sent_date = source_row[5]
        deleted = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_opportunity_proposal(id, organization_id, opportunity_id, name, description, sent_date, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(opportunity_id)s,%(name)s,%(description)s,%(sent_date)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'opportunity_id':opportunity_id,
                'name':name,
                'description':description,
                'sent_date':sent_date,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_opportunity_proposal =====================
## ================= begin move_vwz_opportunity_proposal_document =====================
def move_vwz_opportunity_proposal_document(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, proposal_id, document_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_opportunity_proposal_document")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        proposal_id = source_row[2]
        document_bucket_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_opportunity_proposal_document(id, organization_id, proposal_id, document_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(proposal_id)s,%(document_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'proposal_id':proposal_id,
                'document_bucket_id':document_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_opportunity_proposal_document =====================
## ================= begin move_vwz_opportunity_stage =====================
def move_vwz_opportunity_stage(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, stage_type, probability, predefined_name, default_title, default_language_code, default_stage, deleted, order_index, created_on, created_by, last_modified, last_modified_by from vwz_opportunity_stage")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        stage_type = source_row[2]
        probability = source_row[3]
        predefined_name = source_row[4]
        default_title = source_row[5]
        default_language_code = source_row[6]
        default_stage = source_row[7]
        deleted = source_row[8]
        order_index = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_opportunity_stage(id, organization_id, stage_type, probability, predefined_name, default_title, default_language_code, default_stage, deleted, order_index, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(stage_type)s,%(probability)s,%(predefined_name)s,%(default_title)s,%(default_language_code)s,%(default_stage)s,%(deleted)s,%(order_index)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'stage_type':stage_type,
                'probability':probability,
                'predefined_name':predefined_name,
                'default_title':default_title,
                'default_language_code':default_language_code,
                'default_stage':default_stage,
                'deleted':deleted,
                'order_index':order_index,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_opportunity_stage =====================
## ================= begin move_vwz_opportunity_stagectx =====================
def move_vwz_opportunity_stagectx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, stage_id, organization_id, language_code, title, created_on, created_by, last_modified, last_modified_by from vwz_opportunity_stagectx")
    for source_row in source_cursor:
        id = source_row[0]
        stage_id = source_row[1]
        organization_id = source_row[2]
        language_code = source_row[3]
        title = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_opportunity_stagectx(id, stage_id, organization_id, language_code, title, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(stage_id)s,%(organization_id)s,%(language_code)s,%(title)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'stage_id':stage_id,
                'organization_id':organization_id,
                'language_code':language_code,
                'title':title,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_opportunity_stagectx =====================
## ================= begin move_vwz_organization_abstract =====================
def move_vwz_organization_abstract(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, short_name, default_language_code, umbrella_organization_id, parent_organization_id, organization_level, user_organization, organization_type_code, owner_user_id, city_code, country_code, zip_code, industry_code, phone, email, default_event_template, default_event_blue_print, default_event_venue, main_color, website_address, logo_bucket_id, is_gdpr_activated, membership_live_wall_active, membership_live_wall_banner_id, enable_crm_lookup, default_time_zone, calendar_start_day, currencies, created_on, created_by, last_modified, last_modified_by from vwz_organization_abstract")
    for source_row in source_cursor:
        id = source_row[0]
        short_name = source_row[1]
        default_language_code = source_row[2]
        umbrella_organization_id = source_row[3]
        parent_organization_id = source_row[4]
        organization_level = source_row[5]
        user_organization = source_row[6]
        organization_type_code = source_row[7]
        owner_user_id = source_row[8]
        city_code = source_row[9]
        country_code = source_row[10]
        zip_code = source_row[11]
        industry_code = source_row[12]
        phone = source_row[13]
        email = source_row[14]
        default_event_template = source_row[15]
        default_event_blue_print = source_row[16]
        default_event_venue = source_row[17]
        main_color = source_row[18]
        website_address = source_row[19]
        logo_bucket_id = source_row[20]
        is_gdpr_activated = source_row[21]
        membership_live_wall_active = source_row[22]
        membership_live_wall_banner_id = source_row[23]
        enable_crm_lookup = source_row[24]
        default_time_zone = source_row[25]
        calendar_start_day = source_row[26]
        currencies = source_row[27]
        created_on = source_row[28]
        created_by = source_row[29]
        last_modified = source_row[30]
        last_modified_by = source_row[31]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_abstract(id, short_name, default_language_code, umbrella_organization_id, parent_organization_id, organization_level, user_organization, organization_type_code, owner_user_id, city_code, country_code, zip_code, industry_code, phone, email, default_event_template, default_event_blue_print, default_event_venue, main_color, website_address, logo_bucket_id, is_gdpr_activated, membership_live_wall_active, membership_live_wall_banner_id, enable_crm_lookup, default_time_zone, calendar_start_day, currencies, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(short_name)s,%(default_language_code)s,%(umbrella_organization_id)s,%(parent_organization_id)s,%(organization_level)s,%(user_organization)s,%(organization_type_code)s,%(owner_user_id)s,%(city_code)s,%(country_code)s,%(zip_code)s,%(industry_code)s,%(phone)s,%(email)s,%(default_event_template)s,%(default_event_blue_print)s,%(default_event_venue)s,%(main_color)s,%(website_address)s,%(logo_bucket_id)s,%(is_gdpr_activated)s,%(membership_live_wall_active)s,%(membership_live_wall_banner_id)s,%(enable_crm_lookup)s,%(default_time_zone)s,%(calendar_start_day)s,%(currencies)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'short_name':short_name,
                'default_language_code':default_language_code,
                'umbrella_organization_id':umbrella_organization_id,
                'parent_organization_id':parent_organization_id,
                'organization_level':organization_level,
                'user_organization':user_organization,
                'organization_type_code':organization_type_code,
                'owner_user_id':owner_user_id,
                'city_code':city_code,
                'country_code':country_code,
                'zip_code':zip_code,
                'industry_code':industry_code,
                'phone':phone,
                'email':email,
                'default_event_template':default_event_template,
                'default_event_blue_print':default_event_blue_print,
                'default_event_venue':default_event_venue,
                'main_color':main_color,
                'website_address':website_address,
                'logo_bucket_id':logo_bucket_id,
                'is_gdpr_activated':is_gdpr_activated,
                'membership_live_wall_active':membership_live_wall_active,
                'membership_live_wall_banner_id':membership_live_wall_banner_id,
                'enable_crm_lookup':enable_crm_lookup,
                'default_time_zone':default_time_zone,
                'calendar_start_day':calendar_start_day,
                'currencies':currencies,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_abstract =====================
## ================= begin move_vwz_organization_analytics_settings =====================
def move_vwz_organization_analytics_settings(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, contact_track_contact_batch_count, range_track_contact_batch_count, contact_track_date_batch_count, min_contact_id, created_on from vwz_organization_analytics_settings")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        contact_track_contact_batch_count = source_row[2]
        range_track_contact_batch_count = source_row[3]
        contact_track_date_batch_count = source_row[4]
        min_contact_id = source_row[5]
        created_on = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_analytics_settings(id, organization_id, contact_track_contact_batch_count, range_track_contact_batch_count, contact_track_date_batch_count, min_contact_id, created_on) "
              "values(%(id)s,%(organization_id)s,%(contact_track_contact_batch_count)s,%(range_track_contact_batch_count)s,%(contact_track_date_batch_count)s,%(min_contact_id)s,%(created_on)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'contact_track_contact_batch_count':contact_track_contact_batch_count,
                'range_track_contact_batch_count':range_track_contact_batch_count,
                'contact_track_date_batch_count':contact_track_date_batch_count,
                'min_contact_id':min_contact_id,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_analytics_settings =====================
## ================= begin move_vwz_organization_bankinfo =====================
def move_vwz_organization_bankinfo(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, beneficiary_name, swift_address, account_number, bank_code, bank_street_address, bank_city_code, bank_country_code, beneficiary_street_address, beneficiary_city_code, beneficiary_country_code, notes, created_on, created_by, last_modified, last_modified_by from vwz_organization_bankinfo")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        beneficiary_name = source_row[2]
        swift_address = source_row[3]
        account_number = source_row[4]
        bank_code = source_row[5]
        bank_street_address = source_row[6]
        bank_city_code = source_row[7]
        bank_country_code = source_row[8]
        beneficiary_street_address = source_row[9]
        beneficiary_city_code = source_row[10]
        beneficiary_country_code = source_row[11]
        notes = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_bankinfo(id, organization_id, beneficiary_name, swift_address, account_number, bank_code, bank_street_address, bank_city_code, bank_country_code, beneficiary_street_address, beneficiary_city_code, beneficiary_country_code, notes, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(beneficiary_name)s,%(swift_address)s,%(account_number)s,%(bank_code)s,%(bank_street_address)s,%(bank_city_code)s,%(bank_country_code)s,%(beneficiary_street_address)s,%(beneficiary_city_code)s,%(beneficiary_country_code)s,%(notes)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'beneficiary_name':beneficiary_name,
                'swift_address':swift_address,
                'account_number':account_number,
                'bank_code':bank_code,
                'bank_street_address':bank_street_address,
                'bank_city_code':bank_city_code,
                'bank_country_code':bank_country_code,
                'beneficiary_street_address':beneficiary_street_address,
                'beneficiary_city_code':beneficiary_city_code,
                'beneficiary_country_code':beneficiary_country_code,
                'notes':notes,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_bankinfo =====================
## ================= begin move_vwz_organization_company =====================
def move_vwz_organization_company(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, id, tenant_id, name, language_code, pic_bucket_id, assignee, email, website_address, phone, fax, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, industry_code, is_member, is_sponsor, is_exhibitor, updates_pending, last_updates_applied_on, additional_id, deleted, created_on, created_by, last_modified, last_modified_by from vwz_organization_company")
    for source_row in source_cursor:
        organization_id = source_row[0]
        id = source_row[1]
        tenant_id = source_row[2]
        name = source_row[3]
        language_code = source_row[4]
        pic_bucket_id = source_row[5]
        assignee = source_row[6]
        email = source_row[7]
        website_address = source_row[8]
        phone = source_row[9]
        fax = source_row[10]
        street_address = source_row[11]
        city_code = source_row[12]
        city_name = source_row[13]
        country_code = source_row[14]
        country_name = source_row[15]
        city_name_text = source_row[16]
        province = source_row[17]
        zip_code = source_row[18]
        industry_code = source_row[19]
        is_member = source_row[20]
        is_sponsor = source_row[21]
        is_exhibitor = source_row[22]
        updates_pending = source_row[23]
        last_updates_applied_on = source_row[24]
        additional_id = source_row[25]
        deleted = source_row[26]
        created_on = source_row[27]
        created_by = source_row[28]
        last_modified = source_row[29]
        last_modified_by = source_row[30]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_company(organization_id, id, tenant_id, name, language_code, pic_bucket_id, assignee, email, website_address, phone, fax, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, industry_code, is_member, is_sponsor, is_exhibitor, updates_pending, last_updates_applied_on, additional_id, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(id)s,%(tenant_id)s,%(name)s,%(language_code)s,%(pic_bucket_id)s,%(assignee)s,%(email)s,%(website_address)s,%(phone)s,%(fax)s,%(street_address)s,%(city_code)s,%(city_name)s,%(country_code)s,%(country_name)s,%(city_name_text)s,%(province)s,%(zip_code)s,%(industry_code)s,%(is_member)s,%(is_sponsor)s,%(is_exhibitor)s,%(updates_pending)s,%(last_updates_applied_on)s,%(additional_id)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'id':id,
                'tenant_id':tenant_id,
                'name':name,
                'language_code':language_code,
                'pic_bucket_id':pic_bucket_id,
                'assignee':assignee,
                'email':email,
                'website_address':website_address,
                'phone':phone,
                'fax':fax,
                'street_address':street_address,
                'city_code':city_code,
                'city_name':city_name,
                'country_code':country_code,
                'country_name':country_name,
                'city_name_text':city_name_text,
                'province':province,
                'zip_code':zip_code,
                'industry_code':industry_code,
                'is_member':is_member,
                'is_sponsor':is_sponsor,
                'is_exhibitor':is_exhibitor,
                'updates_pending':updates_pending,
                'last_updates_applied_on':last_updates_applied_on,
                'additional_id':additional_id,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_company =====================
## ================= begin move_vwz_organization_company_list =====================
def move_vwz_organization_company_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, parent_list_id, name, order, created_on, created_by, last_modified, last_modified_by from vwz_organization_company_list")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        parent_list_id = source_row[2]
        name = source_row[3]
        order = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_company_list(id, organization_id, parent_list_id, name, order, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(parent_list_id)s,%(name)s,%(order)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'parent_list_id':parent_list_id,
                'name':name,
                'order':order,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_company_list =====================
## ================= begin move_vwz_organization_company_note =====================
def move_vwz_organization_company_note(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, company_id, type, note, created_on, created_by, last_modified, last_modified_by from vwz_organization_company_note")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        company_id = source_row[2]
        type = source_row[3]
        note = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_company_note(id, organization_id, company_id, type, note, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(company_id)s,%(type)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'company_id':company_id,
                'type':type,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_company_note =====================
## ================= begin move_vwz_organization_company_relation =====================
def move_vwz_organization_company_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, list_id, company_id, created_on, created_by, last_modified, last_modified_by from vwz_organization_company_relation")
    for source_row in source_cursor:
        organization_id = source_row[0]
        list_id = source_row[1]
        company_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_company_relation(organization_id, list_id, company_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(list_id)s,%(company_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'list_id':list_id,
                'company_id':company_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_company_relation =====================
## ================= begin move_vwz_organization_contact =====================
def move_vwz_organization_contact(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, id, tenant_id, language_code, is_lead, lead_since, pic_bucket_id, assignee, given_name, family_name, email, company_name, position_title, phone, website, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, business_function_code, business_role_code, industry_code, is_member, is_speaker, subscribed, is_subscriber, is_working_group, unsubscribed_on, unsubscribed_reason_type, unsubscribed_reason_details, unsubscribed_edm_id, deleted, is_registered_user, gdpr_status, gdpr_status_updated_on, updates_pending, last_updates_applied_on, additional_id, created_on, created_by, last_modified, last_modified_by, assistant_email, cpd_participant from vwz_organization_contact")
    for source_row in source_cursor:
        organization_id = source_row[0]
        id = source_row[1]
        tenant_id = source_row[2]
        language_code = source_row[3]
        is_lead = source_row[4]
        lead_since = source_row[5]
        pic_bucket_id = source_row[6]
        assignee = source_row[7]
        given_name = source_row[8]
        family_name = source_row[9]
        email = source_row[10]
        company_name = source_row[11]
        position_title = source_row[12]
        phone = source_row[13]
        website = source_row[14]
        street_address = source_row[15]
        city_code = source_row[16]
        city_name = source_row[17]
        country_code = source_row[18]
        country_name = source_row[19]
        city_name_text = source_row[20]
        province = source_row[21]
        zip_code = source_row[22]
        business_function_code = source_row[23]
        business_role_code = source_row[24]
        industry_code = source_row[25]
        is_member = source_row[26]
        is_speaker = source_row[27]
        subscribed = source_row[28]
        is_subscriber = source_row[29]
        is_working_group = source_row[30]
        unsubscribed_on = source_row[31]
        unsubscribed_reason_type = source_row[32]
        unsubscribed_reason_details = source_row[33]
        unsubscribed_edm_id = source_row[34]
        deleted = source_row[35]
        is_registered_user = source_row[36]
        gdpr_status = source_row[37]
        gdpr_status_updated_on = source_row[38]
        updates_pending = source_row[39]
        last_updates_applied_on = source_row[40]
        additional_id = source_row[41]
        created_on = source_row[42]
        created_by = source_row[43]
        last_modified = source_row[44]
        last_modified_by = source_row[45]
        assistant_email = source_row[46]
        cpd_participant = source_row[47]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_contact(organization_id, id, tenant_id, language_code, is_lead, lead_since, pic_bucket_id, assignee, given_name, family_name, email, company_name, position_title, phone, website, street_address, city_code, city_name, country_code, country_name, city_name_text, province, zip_code, business_function_code, business_role_code, industry_code, is_member, is_speaker, subscribed, is_subscriber, is_working_group, unsubscribed_on, unsubscribed_reason_type, unsubscribed_reason_details, unsubscribed_edm_id, deleted, is_registered_user, gdpr_status, gdpr_status_updated_on, updates_pending, last_updates_applied_on, additional_id, created_on, created_by, last_modified, last_modified_by, assistant_email, cpd_participant) "
              "values(%(organization_id)s,%(id)s,%(tenant_id)s,%(language_code)s,%(is_lead)s,%(lead_since)s,%(pic_bucket_id)s,%(assignee)s,%(given_name)s,%(family_name)s,%(email)s,%(company_name)s,%(position_title)s,%(phone)s,%(website)s,%(street_address)s,%(city_code)s,%(city_name)s,%(country_code)s,%(country_name)s,%(city_name_text)s,%(province)s,%(zip_code)s,%(business_function_code)s,%(business_role_code)s,%(industry_code)s,%(is_member)s,%(is_speaker)s,%(subscribed)s,%(is_subscriber)s,%(is_working_group)s,%(unsubscribed_on)s,%(unsubscribed_reason_type)s,%(unsubscribed_reason_details)s,%(unsubscribed_edm_id)s,%(deleted)s,%(is_registered_user)s,%(gdpr_status)s,%(gdpr_status_updated_on)s,%(updates_pending)s,%(last_updates_applied_on)s,%(additional_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(assistant_email)s,%(cpd_participant)s)")
        data = {  
                'organization_id':organization_id,
                'id':id,
                'tenant_id':tenant_id,
                'language_code':language_code,
                'is_lead':is_lead,
                'lead_since':lead_since,
                'pic_bucket_id':pic_bucket_id,
                'assignee':assignee,
                'given_name':given_name,
                'family_name':family_name,
                'email':email,
                'company_name':company_name,
                'position_title':position_title,
                'phone':phone,
                'website':website,
                'street_address':street_address,
                'city_code':city_code,
                'city_name':city_name,
                'country_code':country_code,
                'country_name':country_name,
                'city_name_text':city_name_text,
                'province':province,
                'zip_code':zip_code,
                'business_function_code':business_function_code,
                'business_role_code':business_role_code,
                'industry_code':industry_code,
                'is_member':is_member,
                'is_speaker':is_speaker,
                'subscribed':subscribed,
                'is_subscriber':is_subscriber,
                'is_working_group':is_working_group,
                'unsubscribed_on':unsubscribed_on,
                'unsubscribed_reason_type':unsubscribed_reason_type,
                'unsubscribed_reason_details':unsubscribed_reason_details,
                'unsubscribed_edm_id':unsubscribed_edm_id,
                'deleted':deleted,
                'is_registered_user':is_registered_user,
                'gdpr_status':gdpr_status,
                'gdpr_status_updated_on':gdpr_status_updated_on,
                'updates_pending':updates_pending,
                'last_updates_applied_on':last_updates_applied_on,
                'additional_id':additional_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'assistant_email':assistant_email,
                'cpd_participant':cpd_participant
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_contact =====================
## ================= begin move_vwz_organization_contact_list =====================
def move_vwz_organization_contact_list(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, name, organization_id, parent_list_id, is_lead, is_subject, member_auto_subscribe, member_only, non_member_auto_subscribe, user_auto_subscribe, working_group, is_private, is_gdpr_activated, order, created_on, created_by, last_modified, last_modified_by from vwz_organization_contact_list")
    for source_row in source_cursor:
        id = source_row[0]
        name = source_row[1]
        organization_id = source_row[2]
        parent_list_id = source_row[3]
        is_lead = source_row[4]
        is_subject = source_row[5]
        member_auto_subscribe = source_row[6]
        member_only = source_row[7]
        non_member_auto_subscribe = source_row[8]
        user_auto_subscribe = source_row[9]
        working_group = source_row[10]
        is_private = source_row[11]
        is_gdpr_activated = source_row[12]
        order = source_row[13]
        created_on = source_row[14]
        created_by = source_row[15]
        last_modified = source_row[16]
        last_modified_by = source_row[17]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_contact_list(id, name, organization_id, parent_list_id, is_lead, is_subject, member_auto_subscribe, member_only, non_member_auto_subscribe, user_auto_subscribe, working_group, is_private, is_gdpr_activated, order, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(name)s,%(organization_id)s,%(parent_list_id)s,%(is_lead)s,%(is_subject)s,%(member_auto_subscribe)s,%(member_only)s,%(non_member_auto_subscribe)s,%(user_auto_subscribe)s,%(working_group)s,%(is_private)s,%(is_gdpr_activated)s,%(order)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'name':name,
                'organization_id':organization_id,
                'parent_list_id':parent_list_id,
                'is_lead':is_lead,
                'is_subject':is_subject,
                'member_auto_subscribe':member_auto_subscribe,
                'member_only':member_only,
                'non_member_auto_subscribe':non_member_auto_subscribe,
                'user_auto_subscribe':user_auto_subscribe,
                'working_group':working_group,
                'is_private':is_private,
                'is_gdpr_activated':is_gdpr_activated,
                'order':order,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_contact_list =====================
## ================= begin move_vwz_organization_contact_note =====================
def move_vwz_organization_contact_note(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, contact_id, type, note, created_on, created_by, last_modified, last_modified_by from vwz_organization_contact_note")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        contact_id = source_row[2]
        type = source_row[3]
        note = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_contact_note(id, organization_id, contact_id, type, note, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(contact_id)s,%(type)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'contact_id':contact_id,
                'type':type,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_contact_note =====================
## ================= begin move_vwz_organization_contact_relation =====================
def move_vwz_organization_contact_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select list_id, contact_id, organization_id, deleted, created_on, created_by, last_modified, last_modified_by from vwz_organization_contact_relation")
    for source_row in source_cursor:
        list_id = source_row[0]
        contact_id = source_row[1]
        organization_id = source_row[2]
        deleted = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_contact_relation(list_id, contact_id, organization_id, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(list_id)s,%(contact_id)s,%(organization_id)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'list_id':list_id,
                'contact_id':contact_id,
                'organization_id':organization_id,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_contact_relation =====================
## ================= begin move_vwz_organization_contract_email_config =====================
def move_vwz_organization_contract_email_config(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, email_type, eb_staff, organizer, cs, number_of_days, created_on, created_by, last_modified, last_modified_by from vwz_organization_contract_email_config")
    for source_row in source_cursor:
        id = source_row[0]
        email_type = source_row[1]
        eb_staff = source_row[2]
        organizer = source_row[3]
        cs = source_row[4]
        number_of_days = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_contract_email_config(id, email_type, eb_staff, organizer, cs, number_of_days, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(email_type)s,%(eb_staff)s,%(organizer)s,%(cs)s,%(number_of_days)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'email_type':email_type,
                'eb_staff':eb_staff,
                'organizer':organizer,
                'cs':cs,
                'number_of_days':number_of_days,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_contract_email_config =====================
## ================= begin move_vwz_organization_contract_email_trace =====================
def move_vwz_organization_contract_email_trace(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, template_name, recipient_email, recipient_given_name, recipient_family_name, recipient_language_code, status, created_on, smtp_requested_on, smtp_responded_on, failure_cause from vwz_organization_contract_email_trace")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        template_name = source_row[2]
        recipient_email = source_row[3]
        recipient_given_name = source_row[4]
        recipient_family_name = source_row[5]
        recipient_language_code = source_row[6]
        status = source_row[7]
        created_on = source_row[8]
        smtp_requested_on = source_row[9]
        smtp_responded_on = source_row[10]
        failure_cause = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_contract_email_trace(id, organization_id, template_name, recipient_email, recipient_given_name, recipient_family_name, recipient_language_code, status, created_on, smtp_requested_on, smtp_responded_on, failure_cause) "
              "values(%(id)s,%(organization_id)s,%(template_name)s,%(recipient_email)s,%(recipient_given_name)s,%(recipient_family_name)s,%(recipient_language_code)s,%(status)s,%(created_on)s,%(smtp_requested_on)s,%(smtp_responded_on)s,%(failure_cause)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'template_name':template_name,
                'recipient_email':recipient_email,
                'recipient_given_name':recipient_given_name,
                'recipient_family_name':recipient_family_name,
                'recipient_language_code':recipient_language_code,
                'status':status,
                'created_on':created_on,
                'smtp_requested_on':smtp_requested_on,
                'smtp_responded_on':smtp_responded_on,
                'failure_cause':failure_cause
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_contract_email_trace =====================
## ================= begin move_vwz_organization_crm_merge =====================
def move_vwz_organization_crm_merge(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, object_ids, to_object_id, object_type, created_on, created_by from vwz_organization_crm_merge")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        object_ids = source_row[2]
        to_object_id = source_row[3]
        object_type = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_crm_merge(id, organization_id, object_ids, to_object_id, object_type, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(object_ids)s,%(to_object_id)s,%(object_type)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'object_ids':object_ids,
                'to_object_id':to_object_id,
                'object_type':object_type,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_crm_merge =====================
## ================= begin move_vwz_organization_ctx =====================
def move_vwz_organization_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, language_code, organization_id, name, street_address, city_name_text, province, about, contact_given_name, contact_family_name, contact_email, contact_phone, logo_bucket_id, squared_logo_id, banner_bucket_id, privacy_policy_link, terms_of_condition_link, general_legal_text, event_legal_text, subscribe_legal_text, member_legal_text, date_format, is_24_hour_format, number_format, created_on, created_by, last_modified, last_modified_by from vwz_organization_ctx")
    for source_row in source_cursor:
        id = source_row[0]
        language_code = source_row[1]
        organization_id = source_row[2]
        name = source_row[3]
        street_address = source_row[4]
        city_name_text = source_row[5]
        province = source_row[6]
        about = source_row[7]
        contact_given_name = source_row[8]
        contact_family_name = source_row[9]
        contact_email = source_row[10]
        contact_phone = source_row[11]
        logo_bucket_id = source_row[12]
        squared_logo_id = source_row[13]
        banner_bucket_id = source_row[14]
        privacy_policy_link = source_row[15]
        terms_of_condition_link = source_row[16]
        general_legal_text = source_row[17]
        event_legal_text = source_row[18]
        subscribe_legal_text = source_row[19]
        member_legal_text = source_row[20]
        date_format = source_row[21]
        is_24_hour_format = source_row[22]
        number_format = source_row[23]
        created_on = source_row[24]
        created_by = source_row[25]
        last_modified = source_row[26]
        last_modified_by = source_row[27]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_ctx(id, language_code, organization_id, name, street_address, city_name_text, province, about, contact_given_name, contact_family_name, contact_email, contact_phone, logo_bucket_id, squared_logo_id, banner_bucket_id, privacy_policy_link, terms_of_condition_link, general_legal_text, event_legal_text, subscribe_legal_text, member_legal_text, date_format, is_24_hour_format, number_format, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(language_code)s,%(organization_id)s,%(name)s,%(street_address)s,%(city_name_text)s,%(province)s,%(about)s,%(contact_given_name)s,%(contact_family_name)s,%(contact_email)s,%(contact_phone)s,%(logo_bucket_id)s,%(squared_logo_id)s,%(banner_bucket_id)s,%(privacy_policy_link)s,%(terms_of_condition_link)s,%(general_legal_text)s,%(event_legal_text)s,%(subscribe_legal_text)s,%(member_legal_text)s,%(date_format)s,%(is_24_hour_format)s,%(number_format)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'language_code':language_code,
                'organization_id':organization_id,
                'name':name,
                'street_address':street_address,
                'city_name_text':city_name_text,
                'province':province,
                'about':about,
                'contact_given_name':contact_given_name,
                'contact_family_name':contact_family_name,
                'contact_email':contact_email,
                'contact_phone':contact_phone,
                'logo_bucket_id':logo_bucket_id,
                'squared_logo_id':squared_logo_id,
                'banner_bucket_id':banner_bucket_id,
                'privacy_policy_link':privacy_policy_link,
                'terms_of_condition_link':terms_of_condition_link,
                'general_legal_text':general_legal_text,
                'event_legal_text':event_legal_text,
                'subscribe_legal_text':subscribe_legal_text,
                'member_legal_text':member_legal_text,
                'date_format':date_format,
                'is_24_hour_format':is_24_hour_format,
                'number_format':number_format,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_ctx =====================
## ================= begin move_vwz_organization_domain =====================
def move_vwz_organization_domain(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, domain_type, auto_verify_time, domain, sub_domain, status, sendgrid_id, failure_msg, sendgrid_account, created_on, created_by, last_modified, last_modified_by from vwz_organization_domain")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        domain_type = source_row[2]
        auto_verify_time = source_row[3]
        domain = source_row[4]
        sub_domain = source_row[5]
        status = source_row[6]
        sendgrid_id = source_row[7]
        failure_msg = source_row[8]
        sendgrid_account = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_domain(id, organization_id, domain_type, auto_verify_time, domain, sub_domain, status, sendgrid_id, failure_msg, sendgrid_account, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(domain_type)s,%(auto_verify_time)s,%(domain)s,%(sub_domain)s,%(status)s,%(sendgrid_id)s,%(failure_msg)s,%(sendgrid_account)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'domain_type':domain_type,
                'auto_verify_time':auto_verify_time,
                'domain':domain,
                'sub_domain':sub_domain,
                'status':status,
                'sendgrid_id':sendgrid_id,
                'failure_msg':failure_msg,
                'sendgrid_account':sendgrid_account,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_domain =====================
## ================= begin move_vwz_organization_domain_record =====================
def move_vwz_organization_domain_record(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, domain_id, dnsmadeeasy_id, organization_id, org_domain, eb_domain, sendgrid_domain, record_type, verified, created_on, created_by, last_modified, last_modified_by from vwz_organization_domain_record")
    for source_row in source_cursor:
        id = source_row[0]
        domain_id = source_row[1]
        dnsmadeeasy_id = source_row[2]
        organization_id = source_row[3]
        org_domain = source_row[4]
        eb_domain = source_row[5]
        sendgrid_domain = source_row[6]
        record_type = source_row[7]
        verified = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_domain_record(id, domain_id, dnsmadeeasy_id, organization_id, org_domain, eb_domain, sendgrid_domain, record_type, verified, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(domain_id)s,%(dnsmadeeasy_id)s,%(organization_id)s,%(org_domain)s,%(eb_domain)s,%(sendgrid_domain)s,%(record_type)s,%(verified)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'domain_id':domain_id,
                'dnsmadeeasy_id':dnsmadeeasy_id,
                'organization_id':organization_id,
                'org_domain':org_domain,
                'eb_domain':eb_domain,
                'sendgrid_domain':sendgrid_domain,
                'record_type':record_type,
                'verified':verified,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_domain_record =====================
## ================= begin move_vwz_organization_edition =====================
def move_vwz_organization_edition(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, edition_purchase_transaction_id, edition_name, solution, status, is_trial, type, package, purchase_date, start_date, expiry_date, abs_expiry_date, term_end_date, abs_term_end_date, check_expiry, max_attendees_count, purchased_events_capacity, events_position, event_total_position, purchased_contacts_capacity, contacts_position, purchased_active_members_capacity, active_members_position, purchased_active_users_capacity, active_users_position, purchased_emails_capacity, emails_position, emails_total_position, purchased_notifications_capacity, notifications_position, is_active, cs_email, sales_email, agreed_to_terminate_contract, date_of_agreed_to_terminate_contract, reason_of_agreed_to_terminate_contract, date_of_cancel, reason_of_cancel, grace_period, is_test, send_limit_reminder, membership_live_wall_allowed, webinar_engagement_enabled, event_room_enabled, event_room_dm_enabled, cpd_enabled, speed_networking_enabled, smart_matching_enabled, webinar_with_glue_up_enabled, max_webinar_attendee_count, purchased_webinar_capacity, webinar_position, webinar_total_position, survey_position, purchased_survey_capacity, survey_total_position, survey_recipient_position, survey_recipient_total_position, survey_enabled, purchased_community_storage_capacity, community_storage_position, chapter_management_enabled, organization_structure_type, state_organization_limit, chapter_organization_limit, created_on, created_by, last_modified, last_modified_by from vwz_organization_edition")
    for source_row in source_cursor:
        organization_id = source_row[0]
        edition_purchase_transaction_id = source_row[1]
        edition_name = source_row[2]
        solution = source_row[3]
        status = source_row[4]
        is_trial = source_row[5]
        type = source_row[6]
        package = source_row[7]
        purchase_date = source_row[8]
        start_date = source_row[9]
        expiry_date = source_row[10]
        abs_expiry_date = source_row[11]
        term_end_date = source_row[12]
        abs_term_end_date = source_row[13]
        check_expiry = source_row[14]
        max_attendees_count = source_row[15]
        purchased_events_capacity = source_row[16]
        events_position = source_row[17]
        event_total_position = source_row[18]
        purchased_contacts_capacity = source_row[19]
        contacts_position = source_row[20]
        purchased_active_members_capacity = source_row[21]
        active_members_position = source_row[22]
        purchased_active_users_capacity = source_row[23]
        active_users_position = source_row[24]
        purchased_emails_capacity = source_row[25]
        emails_position = source_row[26]
        emails_total_position = source_row[27]
        purchased_notifications_capacity = source_row[28]
        notifications_position = source_row[29]
        is_active = source_row[30]
        cs_email = source_row[31]
        sales_email = source_row[32]
        agreed_to_terminate_contract = source_row[33]
        date_of_agreed_to_terminate_contract = source_row[34]
        reason_of_agreed_to_terminate_contract = source_row[35]
        date_of_cancel = source_row[36]
        reason_of_cancel = source_row[37]
        grace_period = source_row[38]
        is_test = source_row[39]
        send_limit_reminder = source_row[40]
        membership_live_wall_allowed = source_row[41]
        webinar_engagement_enabled = source_row[42]
        event_room_enabled = source_row[43]
        event_room_dm_enabled = source_row[44]
        cpd_enabled = source_row[45]
        speed_networking_enabled = source_row[46]
        smart_matching_enabled = source_row[47]
        webinar_with_glue_up_enabled = source_row[48]
        max_webinar_attendee_count = source_row[49]
        purchased_webinar_capacity = source_row[50]
        webinar_position = source_row[51]
        webinar_total_position = source_row[52]
        survey_position = source_row[53]
        purchased_survey_capacity = source_row[54]
        survey_total_position = source_row[55]
        survey_recipient_position = source_row[56]
        survey_recipient_total_position = source_row[57]
        survey_enabled = source_row[58]
        purchased_community_storage_capacity = source_row[59]
        community_storage_position = source_row[60]
        chapter_management_enabled = source_row[61]
        organization_structure_type = source_row[62]
        state_organization_limit = source_row[63]
        chapter_organization_limit = source_row[64]
        created_on = source_row[65]
        created_by = source_row[66]
        last_modified = source_row[67]
        last_modified_by = source_row[68]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_edition(organization_id, edition_purchase_transaction_id, edition_name, solution, status, is_trial, type, package, purchase_date, start_date, expiry_date, abs_expiry_date, term_end_date, abs_term_end_date, check_expiry, max_attendees_count, purchased_events_capacity, events_position, event_total_position, purchased_contacts_capacity, contacts_position, purchased_active_members_capacity, active_members_position, purchased_active_users_capacity, active_users_position, purchased_emails_capacity, emails_position, emails_total_position, purchased_notifications_capacity, notifications_position, is_active, cs_email, sales_email, agreed_to_terminate_contract, date_of_agreed_to_terminate_contract, reason_of_agreed_to_terminate_contract, date_of_cancel, reason_of_cancel, grace_period, is_test, send_limit_reminder, membership_live_wall_allowed, webinar_engagement_enabled, event_room_enabled, event_room_dm_enabled, cpd_enabled, speed_networking_enabled, smart_matching_enabled, webinar_with_glue_up_enabled, max_webinar_attendee_count, purchased_webinar_capacity, webinar_position, webinar_total_position, survey_position, purchased_survey_capacity, survey_total_position, survey_recipient_position, survey_recipient_total_position, survey_enabled, purchased_community_storage_capacity, community_storage_position, chapter_management_enabled, organization_structure_type, state_organization_limit, chapter_organization_limit, created_on, created_by, last_modified, last_modified_by) "
              "values(%(organization_id)s,%(edition_purchase_transaction_id)s,%(edition_name)s,%(solution)s,%(status)s,%(is_trial)s,%(type)s,%(package)s,%(purchase_date)s,%(start_date)s,%(expiry_date)s,%(abs_expiry_date)s,%(term_end_date)s,%(abs_term_end_date)s,%(check_expiry)s,%(max_attendees_count)s,%(purchased_events_capacity)s,%(events_position)s,%(event_total_position)s,%(purchased_contacts_capacity)s,%(contacts_position)s,%(purchased_active_members_capacity)s,%(active_members_position)s,%(purchased_active_users_capacity)s,%(active_users_position)s,%(purchased_emails_capacity)s,%(emails_position)s,%(emails_total_position)s,%(purchased_notifications_capacity)s,%(notifications_position)s,%(is_active)s,%(cs_email)s,%(sales_email)s,%(agreed_to_terminate_contract)s,%(date_of_agreed_to_terminate_contract)s,%(reason_of_agreed_to_terminate_contract)s,%(date_of_cancel)s,%(reason_of_cancel)s,%(grace_period)s,%(is_test)s,%(send_limit_reminder)s,%(membership_live_wall_allowed)s,%(webinar_engagement_enabled)s,%(event_room_enabled)s,%(event_room_dm_enabled)s,%(cpd_enabled)s,%(speed_networking_enabled)s,%(smart_matching_enabled)s,%(webinar_with_glue_up_enabled)s,%(max_webinar_attendee_count)s,%(purchased_webinar_capacity)s,%(webinar_position)s,%(webinar_total_position)s,%(survey_position)s,%(purchased_survey_capacity)s,%(survey_total_position)s,%(survey_recipient_position)s,%(survey_recipient_total_position)s,%(survey_enabled)s,%(purchased_community_storage_capacity)s,%(community_storage_position)s,%(chapter_management_enabled)s,%(organization_structure_type)s,%(state_organization_limit)s,%(chapter_organization_limit)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'organization_id':organization_id,
                'edition_purchase_transaction_id':edition_purchase_transaction_id,
                'edition_name':edition_name,
                'solution':solution,
                'status':status,
                'is_trial':is_trial,
                'type':type,
                'package':package,
                'purchase_date':purchase_date,
                'start_date':start_date,
                'expiry_date':expiry_date,
                'abs_expiry_date':abs_expiry_date,
                'term_end_date':term_end_date,
                'abs_term_end_date':abs_term_end_date,
                'check_expiry':check_expiry,
                'max_attendees_count':max_attendees_count,
                'purchased_events_capacity':purchased_events_capacity,
                'events_position':events_position,
                'event_total_position':event_total_position,
                'purchased_contacts_capacity':purchased_contacts_capacity,
                'contacts_position':contacts_position,
                'purchased_active_members_capacity':purchased_active_members_capacity,
                'active_members_position':active_members_position,
                'purchased_active_users_capacity':purchased_active_users_capacity,
                'active_users_position':active_users_position,
                'purchased_emails_capacity':purchased_emails_capacity,
                'emails_position':emails_position,
                'emails_total_position':emails_total_position,
                'purchased_notifications_capacity':purchased_notifications_capacity,
                'notifications_position':notifications_position,
                'is_active':is_active,
                'cs_email':cs_email,
                'sales_email':sales_email,
                'agreed_to_terminate_contract':agreed_to_terminate_contract,
                'date_of_agreed_to_terminate_contract':date_of_agreed_to_terminate_contract,
                'reason_of_agreed_to_terminate_contract':reason_of_agreed_to_terminate_contract,
                'date_of_cancel':date_of_cancel,
                'reason_of_cancel':reason_of_cancel,
                'grace_period':grace_period,
                'is_test':is_test,
                'send_limit_reminder':send_limit_reminder,
                'membership_live_wall_allowed':membership_live_wall_allowed,
                'webinar_engagement_enabled':webinar_engagement_enabled,
                'event_room_enabled':event_room_enabled,
                'event_room_dm_enabled':event_room_dm_enabled,
                'cpd_enabled':cpd_enabled,
                'speed_networking_enabled':speed_networking_enabled,
                'smart_matching_enabled':smart_matching_enabled,
                'webinar_with_glue_up_enabled':webinar_with_glue_up_enabled,
                'max_webinar_attendee_count':max_webinar_attendee_count,
                'purchased_webinar_capacity':purchased_webinar_capacity,
                'webinar_position':webinar_position,
                'webinar_total_position':webinar_total_position,
                'survey_position':survey_position,
                'purchased_survey_capacity':purchased_survey_capacity,
                'survey_total_position':survey_total_position,
                'survey_recipient_position':survey_recipient_position,
                'survey_recipient_total_position':survey_recipient_total_position,
                'survey_enabled':survey_enabled,
                'purchased_community_storage_capacity':purchased_community_storage_capacity,
                'community_storage_position':community_storage_position,
                'chapter_management_enabled':chapter_management_enabled,
                'organization_structure_type':organization_structure_type,
                'state_organization_limit':state_organization_limit,
                'chapter_organization_limit':chapter_organization_limit,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_edition =====================
## ================= begin move_vwz_organization_feed =====================
def move_vwz_organization_feed(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, feed_type, feed_id, created_on, created_by, deleted from vwz_organization_feed")
    for source_row in source_cursor:
        organization_id = source_row[0]
        feed_type = source_row[1]
        feed_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        deleted = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_feed(organization_id, feed_type, feed_id, created_on, created_by, deleted) "
              "values(%(organization_id)s,%(feed_type)s,%(feed_id)s,%(created_on)s,%(created_by)s,%(deleted)s)")
        data = {  
                'organization_id':organization_id,
                'feed_type':feed_type,
                'feed_id':feed_id,
                'created_on':created_on,
                'created_by':created_by,
                'deleted':deleted
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_feed =====================
## ================= begin move_vwz_organization_gateway_config =====================
def move_vwz_organization_gateway_config(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, adapay_account_id, adapay_realtime, split_scheduled_date, created_on, created_by, last_modified, last_modified_by from vwz_organization_gateway_config")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        adapay_account_id = source_row[2]
        adapay_realtime = source_row[3]
        split_scheduled_date = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_gateway_config(id, organization_id, adapay_account_id, adapay_realtime, split_scheduled_date, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(adapay_account_id)s,%(adapay_realtime)s,%(split_scheduled_date)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'adapay_account_id':adapay_account_id,
                'adapay_realtime':adapay_realtime,
                'split_scheduled_date':split_scheduled_date,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_gateway_config =====================
## ================= begin move_vwz_organization_member =====================
def move_vwz_organization_member(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, is_default, user_id, role, department, position, status, last_login, last_login_source, created_on, created_by, last_modified, last_modified_by from vwz_organization_member")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        is_default = source_row[2]
        user_id = source_row[3]
        role = source_row[4]
        department = source_row[5]
        position = source_row[6]
        status = source_row[7]
        last_login = source_row[8]
        last_login_source = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_member(id, organization_id, is_default, user_id, role, department, position, status, last_login, last_login_source, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(is_default)s,%(user_id)s,%(role)s,%(department)s,%(position)s,%(status)s,%(last_login)s,%(last_login_source)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'is_default':is_default,
                'user_id':user_id,
                'role':role,
                'department':department,
                'position':position,
                'status':status,
                'last_login':last_login,
                'last_login_source':last_login_source,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_member =====================
## ================= begin move_vwz_organization_memberteam =====================
def move_vwz_organization_memberteam(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, name, order, created_on, created_by, last_modified, last_modified_by from vwz_organization_memberteam")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        name = source_row[2]
        order = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_memberteam(id, organization_id, name, order, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(name)s,%(order)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'name':name,
                'order':order,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_memberteam =====================
## ================= begin move_vwz_organization_memberteam_relation =====================
def move_vwz_organization_memberteam_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select team_id, member_id, organization_id, created_on, created_by, last_modified, last_modified_by from vwz_organization_memberteam_relation")
    for source_row in source_cursor:
        team_id = source_row[0]
        member_id = source_row[1]
        organization_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_memberteam_relation(team_id, member_id, organization_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(team_id)s,%(member_id)s,%(organization_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'team_id':team_id,
                'member_id':member_id,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_memberteam_relation =====================
## ================= begin move_vwz_organization_member_invitation =====================
def move_vwz_organization_member_invitation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_member_id, email, family_name, given_name, verification_code, created_on, created_by, last_modified, last_modified_by from vwz_organization_member_invitation")
    for source_row in source_cursor:
        id = source_row[0]
        organization_member_id = source_row[1]
        email = source_row[2]
        family_name = source_row[3]
        given_name = source_row[4]
        verification_code = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_member_invitation(id, organization_member_id, email, family_name, given_name, verification_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_member_id)s,%(email)s,%(family_name)s,%(given_name)s,%(verification_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_member_id':organization_member_id,
                'email':email,
                'family_name':family_name,
                'given_name':given_name,
                'verification_code':verification_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_member_invitation =====================
## ================= begin move_vwz_organization_smartlist =====================
def move_vwz_organization_smartlist(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, name, section, type, filter, created_on, created_by, deleted, count, count_updated_on from vwz_organization_smartlist")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        name = source_row[2]
        section = source_row[3]
        type = source_row[4]
        filter = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        deleted = source_row[8]
        count = source_row[9]
        count_updated_on = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_smartlist(id, organization_id, name, section, type, filter, created_on, created_by, deleted, count, count_updated_on) "
              "values(%(id)s,%(organization_id)s,%(name)s,%(section)s,%(type)s,%(filter)s,%(created_on)s,%(created_by)s,%(deleted)s,%(count)s,%(count_updated_on)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'name':name,
                'section':section,
                'type':type,
                'filter':filter,
                'created_on':created_on,
                'created_by':created_by,
                'deleted':deleted,
                'count':count,
                'count_updated_on':count_updated_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_smartlist =====================
## ================= begin move_vwz_organization_socialmedia_account_abstract =====================
def move_vwz_organization_socialmedia_account_abstract(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, social_media_platform_code, created_on, created_by, last_modified, last_modified_by from vwz_organization_socialmedia_account_abstract")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        social_media_platform_code = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_socialmedia_account_abstract(id, organization_id, social_media_platform_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(social_media_platform_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'social_media_platform_code':social_media_platform_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_socialmedia_account_abstract =====================
## ================= begin move_vwz_organization_socialmedia_account_ctx =====================
def move_vwz_organization_socialmedia_account_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, social_media_account_id, language_code, identifier, created_on, created_by, last_modified, last_modified_by from vwz_organization_socialmedia_account_ctx")
    for source_row in source_cursor:
        id = source_row[0]
        social_media_account_id = source_row[1]
        language_code = source_row[2]
        identifier = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_socialmedia_account_ctx(id, social_media_account_id, language_code, identifier, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(social_media_account_id)s,%(language_code)s,%(identifier)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'social_media_account_id':social_media_account_id,
                'language_code':language_code,
                'identifier':identifier,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_socialmedia_account_ctx =====================
## ================= begin move_vwz_organization_subscription =====================
def move_vwz_organization_subscription(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, user_id, subscribed, type, email, created_on, created_by, last_modified, last_modified_by from vwz_organization_subscription")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        user_id = source_row[2]
        subscribed = source_row[3]
        type = source_row[4]
        email = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_subscription(id, organization_id, user_id, subscribed, type, email, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(user_id)s,%(subscribed)s,%(type)s,%(email)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'user_id':user_id,
                'subscribed':subscribed,
                'type':type,
                'email':email,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_subscription =====================
## ================= begin move_vwz_organization_taxes =====================
def move_vwz_organization_taxes(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, tax_number, public_name, internal_name, percentage, apply_to_all, system_tax, deleted, created_on, created_by, last_modified, last_modified_by from vwz_organization_taxes")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        tax_number = source_row[2]
        public_name = source_row[3]
        internal_name = source_row[4]
        percentage = source_row[5]
        apply_to_all = source_row[6]
        system_tax = source_row[7]
        deleted = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_taxes(id, organization_id, tax_number, public_name, internal_name, percentage, apply_to_all, system_tax, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(tax_number)s,%(public_name)s,%(internal_name)s,%(percentage)s,%(apply_to_all)s,%(system_tax)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'tax_number':tax_number,
                'public_name':public_name,
                'internal_name':internal_name,
                'percentage':percentage,
                'apply_to_all':apply_to_all,
                'system_tax':system_tax,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_taxes =====================
## ================= begin move_vwz_organization_venue =====================
def move_vwz_organization_venue(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, default_language_code, country_code, zip_code, latitude, longitude, timezone, zoom, chapter, created_on, created_by, last_modified, last_modified_by from vwz_organization_venue")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        default_language_code = source_row[2]
        country_code = source_row[3]
        zip_code = source_row[4]
        latitude = source_row[5]
        longitude = source_row[6]
        timezone = source_row[7]
        zoom = source_row[8]
        chapter = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_venue(id, organization_id, default_language_code, country_code, zip_code, latitude, longitude, timezone, zoom, chapter, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(default_language_code)s,%(country_code)s,%(zip_code)s,%(latitude)s,%(longitude)s,%(timezone)s,%(zoom)s,%(chapter)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'default_language_code':default_language_code,
                'country_code':country_code,
                'zip_code':zip_code,
                'latitude':latitude,
                'longitude':longitude,
                'timezone':timezone,
                'zoom':zoom,
                'chapter':chapter,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_venue =====================
## ================= begin move_vwz_organization_venue_ctx =====================
def move_vwz_organization_venue_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select venue_id, organization_id, language_code, name, city_name, province, file_id, street_address, info, created_on, created_by, last_modified, last_modified_by from vwz_organization_venue_ctx")
    for source_row in source_cursor:
        venue_id = source_row[0]
        organization_id = source_row[1]
        language_code = source_row[2]
        name = source_row[3]
        city_name = source_row[4]
        province = source_row[5]
        file_id = source_row[6]
        street_address = source_row[7]
        info = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_organization_venue_ctx(venue_id, organization_id, language_code, name, city_name, province, file_id, street_address, info, created_on, created_by, last_modified, last_modified_by) "
              "values(%(venue_id)s,%(organization_id)s,%(language_code)s,%(name)s,%(city_name)s,%(province)s,%(file_id)s,%(street_address)s,%(info)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'venue_id':venue_id,
                'organization_id':organization_id,
                'language_code':language_code,
                'name':name,
                'city_name':city_name,
                'province':province,
                'file_id':file_id,
                'street_address':street_address,
                'info':info,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_organization_venue_ctx =====================
## ================= begin move_vwz_passcode =====================
def move_vwz_passcode(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, email, passcode, used, deleted, created_on, created_by, last_modified, last_modified_by, source, type, phone from vwz_passcode")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        email = source_row[2]
        passcode = source_row[3]
        used = source_row[4]
        deleted = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        source = source_row[10]
        type = source_row[11]
        phone = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_passcode(id, user_id, email, passcode, used, deleted, created_on, created_by, last_modified, last_modified_by, source, type, phone) "
              "values(%(id)s,%(user_id)s,%(email)s,%(passcode)s,%(used)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(source)s,%(type)s,%(phone)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'email':email,
                'passcode':passcode,
                'used':used,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'source':source,
                'type':type,
                'phone':phone
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_passcode =====================
## ================= begin move_vwz_payment_detail =====================
def move_vwz_payment_detail(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, transaction_id, transaction_type, ticketsale_id, order_id, external_transaction_id, gateway, success, currency, paid_value, cc_commission_charge, message, created_on, created_by, last_modified, last_modified_by from vwz_payment_detail")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        transaction_id = source_row[2]
        transaction_type = source_row[3]
        ticketsale_id = source_row[4]
        order_id = source_row[5]
        external_transaction_id = source_row[6]
        gateway = source_row[7]
        success = source_row[8]
        currency = source_row[9]
        paid_value = source_row[10]
        cc_commission_charge = source_row[11]
        message = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_payment_detail(id, organization_id, transaction_id, transaction_type, ticketsale_id, order_id, external_transaction_id, gateway, success, currency, paid_value, cc_commission_charge, message, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(transaction_id)s,%(transaction_type)s,%(ticketsale_id)s,%(order_id)s,%(external_transaction_id)s,%(gateway)s,%(success)s,%(currency)s,%(paid_value)s,%(cc_commission_charge)s,%(message)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'transaction_id':transaction_id,
                'transaction_type':transaction_type,
                'ticketsale_id':ticketsale_id,
                'order_id':order_id,
                'external_transaction_id':external_transaction_id,
                'gateway':gateway,
                'success':success,
                'currency':currency,
                'paid_value':paid_value,
                'cc_commission_charge':cc_commission_charge,
                'message':message,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_payment_detail =====================
## ================= begin move_vwz_payment_external_log =====================
def move_vwz_payment_external_log(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, gateway, order_id, type, url, request, response, cost_time, success, created_on, last_modified from vwz_payment_external_log")
    for source_row in source_cursor:
        id = source_row[0]
        gateway = source_row[1]
        order_id = source_row[2]
        type = source_row[3]
        url = source_row[4]
        request = source_row[5]
        response = source_row[6]
        cost_time = source_row[7]
        success = source_row[8]
        created_on = source_row[9]
        last_modified = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_payment_external_log(id, gateway, order_id, type, url, request, response, cost_time, success, created_on, last_modified) "
              "values(%(id)s,%(gateway)s,%(order_id)s,%(type)s,%(url)s,%(request)s,%(response)s,%(cost_time)s,%(success)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'gateway':gateway,
                'order_id':order_id,
                'type':type,
                'url':url,
                'request':request,
                'response':response,
                'cost_time':cost_time,
                'success':success,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_payment_external_log =====================
## ================= begin move_vwz_payment_order_relation =====================
def move_vwz_payment_order_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, order_id, amount, currency, gateway, source, relation_json, meta_data, prefix, postfix, external_order_id, state, split_flag, external_state, external_pay_channel, external_pay_amt, created_on, last_modified from vwz_payment_order_relation")
    for source_row in source_cursor:
        id = source_row[0]
        order_id = source_row[1]
        amount = source_row[2]
        currency = source_row[3]
        gateway = source_row[4]
        source = source_row[5]
        relation_json = source_row[6]
        meta_data = source_row[7]
        prefix = source_row[8]
        postfix = source_row[9]
        external_order_id = source_row[10]
        state = source_row[11]
        split_flag = source_row[12]
        external_state = source_row[13]
        external_pay_channel = source_row[14]
        external_pay_amt = source_row[15]
        created_on = source_row[16]
        last_modified = source_row[17]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_payment_order_relation(id, order_id, amount, currency, gateway, source, relation_json, meta_data, prefix, postfix, external_order_id, state, split_flag, external_state, external_pay_channel, external_pay_amt, created_on, last_modified) "
              "values(%(id)s,%(order_id)s,%(amount)s,%(currency)s,%(gateway)s,%(source)s,%(relation_json)s,%(meta_data)s,%(prefix)s,%(postfix)s,%(external_order_id)s,%(state)s,%(split_flag)s,%(external_state)s,%(external_pay_channel)s,%(external_pay_amt)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'order_id':order_id,
                'amount':amount,
                'currency':currency,
                'gateway':gateway,
                'source':source,
                'relation_json':relation_json,
                'meta_data':meta_data,
                'prefix':prefix,
                'postfix':postfix,
                'external_order_id':external_order_id,
                'state':state,
                'split_flag':split_flag,
                'external_state':external_state,
                'external_pay_channel':external_pay_channel,
                'external_pay_amt':external_pay_amt,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_payment_order_relation =====================
## ================= begin move_vwz_person_abstract =====================
def move_vwz_person_abstract(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, default_language_code, city_code, country_code, industry_code, phone, email, profile_picture_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_person_abstract")
    for source_row in source_cursor:
        id = source_row[0]
        default_language_code = source_row[1]
        city_code = source_row[2]
        country_code = source_row[3]
        industry_code = source_row[4]
        phone = source_row[5]
        email = source_row[6]
        profile_picture_bucket_id = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_person_abstract(id, default_language_code, city_code, country_code, industry_code, phone, email, profile_picture_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(default_language_code)s,%(city_code)s,%(country_code)s,%(industry_code)s,%(phone)s,%(email)s,%(profile_picture_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'default_language_code':default_language_code,
                'city_code':city_code,
                'country_code':country_code,
                'industry_code':industry_code,
                'phone':phone,
                'email':email,
                'profile_picture_bucket_id':profile_picture_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_person_abstract =====================
## ================= begin move_vwz_person_ctx =====================
def move_vwz_person_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, person_id, language_code, given_name, family_name, street_address, company_name, position_title, summary, created_on, created_by, last_modified, last_modified_by from vwz_person_ctx")
    for source_row in source_cursor:
        id = source_row[0]
        person_id = source_row[1]
        language_code = source_row[2]
        given_name = source_row[3]
        family_name = source_row[4]
        street_address = source_row[5]
        company_name = source_row[6]
        position_title = source_row[7]
        summary = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_person_ctx(id, person_id, language_code, given_name, family_name, street_address, company_name, position_title, summary, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(person_id)s,%(language_code)s,%(given_name)s,%(family_name)s,%(street_address)s,%(company_name)s,%(position_title)s,%(summary)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'person_id':person_id,
                'language_code':language_code,
                'given_name':given_name,
                'family_name':family_name,
                'street_address':street_address,
                'company_name':company_name,
                'position_title':position_title,
                'summary':summary,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_person_ctx =====================
## ================= begin move_vwz_phone =====================
def move_vwz_phone(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, phone_number, phone_type_code, created_on, created_by, last_modified, last_modified_by from vwz_phone")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        phone_number = source_row[2]
        phone_type_code = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_phone(id, user_id, phone_number, phone_type_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(phone_number)s,%(phone_type_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'phone_number':phone_number,
                'phone_type_code':phone_type_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_phone =====================
## ================= begin move_vwz_quickbooks_attachments =====================
def move_vwz_quickbooks_attachments(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, realm_id, sync_token, object_id, object_type, bucket_id, qb_attachment_id, created_on, last_modified from vwz_quickbooks_attachments")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        realm_id = source_row[2]
        sync_token = source_row[3]
        object_id = source_row[4]
        object_type = source_row[5]
        bucket_id = source_row[6]
        qb_attachment_id = source_row[7]
        created_on = source_row[8]
        last_modified = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_attachments(id, organization_id, realm_id, sync_token, object_id, object_type, bucket_id, qb_attachment_id, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(realm_id)s,%(sync_token)s,%(object_id)s,%(object_type)s,%(bucket_id)s,%(qb_attachment_id)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'realm_id':realm_id,
                'sync_token':sync_token,
                'object_id':object_id,
                'object_type':object_type,
                'bucket_id':bucket_id,
                'qb_attachment_id':qb_attachment_id,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_attachments =====================
## ================= begin move_vwz_quickbooks_auth =====================
def move_vwz_quickbooks_auth(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, company_name, country, ast_status, realm_id, csrf, access_token, refresh_token, refresh_token_expiry, access_token_expiry, modify_since, created_on, created_by, last_modified, last_modified_by from vwz_quickbooks_auth")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        company_name = source_row[2]
        country = source_row[3]
        ast_status = source_row[4]
        realm_id = source_row[5]
        csrf = source_row[6]
        access_token = source_row[7]
        refresh_token = source_row[8]
        refresh_token_expiry = source_row[9]
        access_token_expiry = source_row[10]
        modify_since = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_auth(id, organization_id, company_name, country, ast_status, realm_id, csrf, access_token, refresh_token, refresh_token_expiry, access_token_expiry, modify_since, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(company_name)s,%(country)s,%(ast_status)s,%(realm_id)s,%(csrf)s,%(access_token)s,%(refresh_token)s,%(refresh_token_expiry)s,%(access_token_expiry)s,%(modify_since)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'company_name':company_name,
                'country':country,
                'ast_status':ast_status,
                'realm_id':realm_id,
                'csrf':csrf,
                'access_token':access_token,
                'refresh_token':refresh_token,
                'refresh_token_expiry':refresh_token_expiry,
                'access_token_expiry':access_token_expiry,
                'modify_since':modify_since,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_auth =====================
## ================= begin move_vwz_quickbooks_authtrack =====================
def move_vwz_quickbooks_authtrack(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, company_name, realm_id, created_on, created_by from vwz_quickbooks_authtrack")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        company_name = source_row[2]
        realm_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_authtrack(id, organization_id, company_name, realm_id, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(company_name)s,%(realm_id)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'company_name':company_name,
                'realm_id':realm_id,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_authtrack =====================
## ================= begin move_vwz_quickbooks_company =====================
def move_vwz_quickbooks_company(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, realm_id, currency, sync_token, company_id, qb_company_id, created_on, last_modified from vwz_quickbooks_company")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        realm_id = source_row[2]
        currency = source_row[3]
        sync_token = source_row[4]
        company_id = source_row[5]
        qb_company_id = source_row[6]
        created_on = source_row[7]
        last_modified = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_company(id, organization_id, realm_id, currency, sync_token, company_id, qb_company_id, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(realm_id)s,%(currency)s,%(sync_token)s,%(company_id)s,%(qb_company_id)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'realm_id':realm_id,
                'currency':currency,
                'sync_token':sync_token,
                'company_id':company_id,
                'qb_company_id':qb_company_id,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_company =====================
## ================= begin move_vwz_quickbooks_contact =====================
def move_vwz_quickbooks_contact(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, realm_id, currency, sync_token, contact_id, qb_contact_id, qb_company_id, created_on, last_modified from vwz_quickbooks_contact")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        realm_id = source_row[2]
        currency = source_row[3]
        sync_token = source_row[4]
        contact_id = source_row[5]
        qb_contact_id = source_row[6]
        qb_company_id = source_row[7]
        created_on = source_row[8]
        last_modified = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_contact(id, organization_id, realm_id, currency, sync_token, contact_id, qb_contact_id, qb_company_id, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(realm_id)s,%(currency)s,%(sync_token)s,%(contact_id)s,%(qb_contact_id)s,%(qb_company_id)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'realm_id':realm_id,
                'currency':currency,
                'sync_token':sync_token,
                'contact_id':contact_id,
                'qb_contact_id':qb_contact_id,
                'qb_company_id':qb_company_id,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_contact =====================
## ================= begin move_vwz_quickbooks_invoice =====================
def move_vwz_quickbooks_invoice(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, realm_id, invoice_id, invoice_number, sync_token, qb_invoice_id, voided, created_on, last_modified from vwz_quickbooks_invoice")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        realm_id = source_row[2]
        invoice_id = source_row[3]
        invoice_number = source_row[4]
        sync_token = source_row[5]
        qb_invoice_id = source_row[6]
        voided = source_row[7]
        created_on = source_row[8]
        last_modified = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_invoice(id, organization_id, realm_id, invoice_id, invoice_number, sync_token, qb_invoice_id, voided, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(realm_id)s,%(invoice_id)s,%(invoice_number)s,%(sync_token)s,%(qb_invoice_id)s,%(voided)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'realm_id':realm_id,
                'invoice_id':invoice_id,
                'invoice_number':invoice_number,
                'sync_token':sync_token,
                'qb_invoice_id':qb_invoice_id,
                'voided':voided,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_invoice =====================
## ================= begin move_vwz_quickbooks_limiter =====================
def move_vwz_quickbooks_limiter(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, realm_id, second_window_single, second_count_single, pass_count_single, second_window_batch, second_count_batch, pass_count_batch, created_on, last_modified from vwz_quickbooks_limiter")
    for source_row in source_cursor:
        id = source_row[0]
        realm_id = source_row[1]
        second_window_single = source_row[2]
        second_count_single = source_row[3]
        pass_count_single = source_row[4]
        second_window_batch = source_row[5]
        second_count_batch = source_row[6]
        pass_count_batch = source_row[7]
        created_on = source_row[8]
        last_modified = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_limiter(id, realm_id, second_window_single, second_count_single, pass_count_single, second_window_batch, second_count_batch, pass_count_batch, created_on, last_modified) "
              "values(%(id)s,%(realm_id)s,%(second_window_single)s,%(second_count_single)s,%(pass_count_single)s,%(second_window_batch)s,%(second_count_batch)s,%(pass_count_batch)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'realm_id':realm_id,
                'second_window_single':second_window_single,
                'second_count_single':second_count_single,
                'pass_count_single':pass_count_single,
                'second_window_batch':second_window_batch,
                'second_count_batch':second_count_batch,
                'pass_count_batch':pass_count_batch,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_limiter =====================
## ================= begin move_vwz_quickbooks_mapping =====================
def move_vwz_quickbooks_mapping(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, object_id, object_type, mapping_id, mapping_name, realm_id, created_on, created_by, last_modified, last_modified_by from vwz_quickbooks_mapping")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        object_id = source_row[2]
        object_type = source_row[3]
        mapping_id = source_row[4]
        mapping_name = source_row[5]
        realm_id = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_mapping(id, organization_id, object_id, object_type, mapping_id, mapping_name, realm_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(object_id)s,%(object_type)s,%(mapping_id)s,%(mapping_name)s,%(realm_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'object_id':object_id,
                'object_type':object_type,
                'mapping_id':mapping_id,
                'mapping_name':mapping_name,
                'realm_id':realm_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_mapping =====================
## ================= begin move_vwz_quickbooks_payment =====================
def move_vwz_quickbooks_payment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, realm_id, sync_token, payment_id, contact_id, invoice_id, qb_payment_id, voided, create_from_qb, created_on, last_modified from vwz_quickbooks_payment")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        realm_id = source_row[2]
        sync_token = source_row[3]
        payment_id = source_row[4]
        contact_id = source_row[5]
        invoice_id = source_row[6]
        qb_payment_id = source_row[7]
        voided = source_row[8]
        create_from_qb = source_row[9]
        created_on = source_row[10]
        last_modified = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_payment(id, organization_id, realm_id, sync_token, payment_id, contact_id, invoice_id, qb_payment_id, voided, create_from_qb, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(realm_id)s,%(sync_token)s,%(payment_id)s,%(contact_id)s,%(invoice_id)s,%(qb_payment_id)s,%(voided)s,%(create_from_qb)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'realm_id':realm_id,
                'sync_token':sync_token,
                'payment_id':payment_id,
                'contact_id':contact_id,
                'invoice_id':invoice_id,
                'qb_payment_id':qb_payment_id,
                'voided':voided,
                'create_from_qb':create_from_qb,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_payment =====================
## ================= begin move_vwz_quickbooks_paymentmethod =====================
def move_vwz_quickbooks_paymentmethod(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, realm_id, method_name, sync_token, qb_method_id, created_on, last_modified from vwz_quickbooks_paymentmethod")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        realm_id = source_row[2]
        method_name = source_row[3]
        sync_token = source_row[4]
        qb_method_id = source_row[5]
        created_on = source_row[6]
        last_modified = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_paymentmethod(id, organization_id, realm_id, method_name, sync_token, qb_method_id, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(realm_id)s,%(method_name)s,%(sync_token)s,%(qb_method_id)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'realm_id':realm_id,
                'method_name':method_name,
                'sync_token':sync_token,
                'qb_method_id':qb_method_id,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_paymentmethod =====================
## ================= begin move_vwz_quickbooks_synctrack =====================
def move_vwz_quickbooks_synctrack(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, realm_id, object_id, sync_type, qb_object_ids, track_status, tried_times, failure_reason, created_on, last_modified from vwz_quickbooks_synctrack")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        realm_id = source_row[2]
        object_id = source_row[3]
        sync_type = source_row[4]
        qb_object_ids = source_row[5]
        track_status = source_row[6]
        tried_times = source_row[7]
        failure_reason = source_row[8]
        created_on = source_row[9]
        last_modified = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_quickbooks_synctrack(id, organization_id, realm_id, object_id, sync_type, qb_object_ids, track_status, tried_times, failure_reason, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(realm_id)s,%(object_id)s,%(sync_type)s,%(qb_object_ids)s,%(track_status)s,%(tried_times)s,%(failure_reason)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'realm_id':realm_id,
                'object_id':object_id,
                'sync_type':sync_type,
                'qb_object_ids':qb_object_ids,
                'track_status':track_status,
                'tried_times':tried_times,
                'failure_reason':failure_reason,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_quickbooks_synctrack =====================
## ================= begin move_vwz_resource_descriptor =====================
def move_vwz_resource_descriptor(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, ref_id, absolute_path, uri, filename, content_type, size, deleted, created_on, created_by, last_modified, last_modified_by from vwz_resource_descriptor")
    for source_row in source_cursor:
        id = source_row[0]
        ref_id = source_row[1]
        absolute_path = source_row[2]
        uri = source_row[3]
        filename = source_row[4]
        content_type = source_row[5]
        size = source_row[6]
        deleted = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_resource_descriptor(id, ref_id, absolute_path, uri, filename, content_type, size, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(ref_id)s,%(absolute_path)s,%(uri)s,%(filename)s,%(content_type)s,%(size)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'ref_id':ref_id,
                'absolute_path':absolute_path,
                'uri':uri,
                'filename':filename,
                'content_type':content_type,
                'size':size,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_resource_descriptor =====================
## ================= begin move_vwz_send_grid_edm_response =====================
def move_vwz_send_grid_edm_response(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, edm_template_id, uid, tracking_id, email, event, reason, status, processing_error, created_on, last_modified from vwz_send_grid_edm_response")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        edm_template_id = source_row[2]
        uid = source_row[3]
        tracking_id = source_row[4]
        email = source_row[5]
        event = source_row[6]
        reason = source_row[7]
        status = source_row[8]
        processing_error = source_row[9]
        created_on = source_row[10]
        last_modified = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_send_grid_edm_response(id, organization_id, edm_template_id, uid, tracking_id, email, event, reason, status, processing_error, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(edm_template_id)s,%(uid)s,%(tracking_id)s,%(email)s,%(event)s,%(reason)s,%(status)s,%(processing_error)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'edm_template_id':edm_template_id,
                'uid':uid,
                'tracking_id':tracking_id,
                'email':email,
                'event':event,
                'reason':reason,
                'status':status,
                'processing_error':processing_error,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_send_grid_edm_response =====================
## ================= begin move_vwz_server_node =====================
def move_vwz_server_node(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select name, sync_url, main_url, created_on from vwz_server_node")
    for source_row in source_cursor:
        name = source_row[0]
        sync_url = source_row[1]
        main_url = source_row[2]
        created_on = source_row[3]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_server_node(name, sync_url, main_url, created_on) "
              "values(%(name)s,%(sync_url)s,%(main_url)s,%(created_on)s)")
        data = {  
                'name':name,
                'sync_url':sync_url,
                'main_url':main_url,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_server_node =====================
## ================= begin move_vwz_server_node_user_relation =====================
def move_vwz_server_node_user_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select node_name, local_user_id, global_user_id, is_default, last_login, created_on, last_modified from vwz_server_node_user_relation")
    for source_row in source_cursor:
        node_name = source_row[0]
        local_user_id = source_row[1]
        global_user_id = source_row[2]
        is_default = source_row[3]
        last_login = source_row[4]
        created_on = source_row[5]
        last_modified = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_server_node_user_relation(node_name, local_user_id, global_user_id, is_default, last_login, created_on, last_modified) "
              "values(%(node_name)s,%(local_user_id)s,%(global_user_id)s,%(is_default)s,%(last_login)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'node_name':node_name,
                'local_user_id':local_user_id,
                'global_user_id':global_user_id,
                'is_default':is_default,
                'last_login':last_login,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_server_node_user_relation =====================
## ================= begin move_vwz_session =====================
def move_vwz_session(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, organization_id, broker_id, token, expiry, root_id, created_on, created_by, last_modified, last_modified_by from vwz_session")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        organization_id = source_row[2]
        broker_id = source_row[3]
        token = source_row[4]
        expiry = source_row[5]
        root_id = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_session(id, user_id, organization_id, broker_id, token, expiry, root_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(organization_id)s,%(broker_id)s,%(token)s,%(expiry)s,%(root_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'organization_id':organization_id,
                'broker_id':broker_id,
                'token':token,
                'expiry':expiry,
                'root_id':root_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_session =====================
## ================= begin move_vwz_session_tag =====================
def move_vwz_session_tag(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, default_language_code, is_deleted, created_on, created_by, last_modified, last_modified_by from vwz_session_tag")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        default_language_code = source_row[2]
        is_deleted = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_session_tag(id, organization_id, default_language_code, is_deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(default_language_code)s,%(is_deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'default_language_code':default_language_code,
                'is_deleted':is_deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_session_tag =====================
## ================= begin move_vwz_session_tag_ctx =====================
def move_vwz_session_tag_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select tag_id, language_code, name, description, created_on, created_by, last_modified, last_modified_by from vwz_session_tag_ctx")
    for source_row in source_cursor:
        tag_id = source_row[0]
        language_code = source_row[1]
        name = source_row[2]
        description = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_session_tag_ctx(tag_id, language_code, name, description, created_on, created_by, last_modified, last_modified_by) "
              "values(%(tag_id)s,%(language_code)s,%(name)s,%(description)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'tag_id':tag_id,
                'language_code':language_code,
                'name':name,
                'description':description,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_session_tag_ctx =====================
## ================= begin move_vwz_sms_platform_log =====================
def move_vwz_sms_platform_log(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, source, phone, contents, response, success, created_on, created_by, last_modified, last_modified_by from vwz_sms_platform_log")
    for source_row in source_cursor:
        id = source_row[0]
        source = source_row[1]
        phone = source_row[2]
        contents = source_row[3]
        response = source_row[4]
        success = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_sms_platform_log(id, source, phone, contents, response, success, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(source)s,%(phone)s,%(contents)s,%(response)s,%(success)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'source':source,
                'phone':phone,
                'contents':contents,
                'response':response,
                'success':success,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_sms_platform_log =====================
## ================= begin move_vwz_sms_template =====================
def move_vwz_sms_template(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, language_code, type, content, created_on, created_by, last_modified, last_modified_by from vwz_sms_template")
    for source_row in source_cursor:
        id = source_row[0]
        language_code = source_row[1]
        type = source_row[2]
        content = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_sms_template(id, language_code, type, content, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(language_code)s,%(type)s,%(content)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'language_code':language_code,
                'type':type,
                'content':content,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_sms_template =====================
## ================= begin move_vwz_sns_relation =====================
def move_vwz_sns_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select sns, sns_uid, sns_token, eb_user_id, created_by, created_on, last_modified, last_modified_by from vwz_sns_relation")
    for source_row in source_cursor:
        sns = source_row[0]
        sns_uid = source_row[1]
        sns_token = source_row[2]
        eb_user_id = source_row[3]
        created_by = source_row[4]
        created_on = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_sns_relation(sns, sns_uid, sns_token, eb_user_id, created_by, created_on, last_modified, last_modified_by) "
              "values(%(sns)s,%(sns_uid)s,%(sns_token)s,%(eb_user_id)s,%(created_by)s,%(created_on)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'sns':sns,
                'sns_uid':sns_uid,
                'sns_token':sns_token,
                'eb_user_id':eb_user_id,
                'created_by':created_by,
                'created_on':created_on,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_sns_relation =====================
## ================= begin move_vwz_sns_relation_merged =====================
def move_vwz_sns_relation_merged(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select global_user_id, sns, sns_uid, sns_token, created_on, last_modified from vwz_sns_relation_merged")
    for source_row in source_cursor:
        global_user_id = source_row[0]
        sns = source_row[1]
        sns_uid = source_row[2]
        sns_token = source_row[3]
        created_on = source_row[4]
        last_modified = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_sns_relation_merged(global_user_id, sns, sns_uid, sns_token, created_on, last_modified) "
              "values(%(global_user_id)s,%(sns)s,%(sns_uid)s,%(sns_token)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'global_user_id':global_user_id,
                'sns':sns,
                'sns_uid':sns_uid,
                'sns_token':sns_token,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_sns_relation_merged =====================
## ================= begin move_vwz_stripe_currency =====================
def move_vwz_stripe_currency(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, currency_code, country_code, card_type, domestic_percentage_fee, domestic_fixed_fee, international_percentage_fee, international_fixed_fee, created_on, created_by, last_modified, last_modified_by from vwz_stripe_currency")
    for source_row in source_cursor:
        id = source_row[0]
        currency_code = source_row[1]
        country_code = source_row[2]
        card_type = source_row[3]
        domestic_percentage_fee = source_row[4]
        domestic_fixed_fee = source_row[5]
        international_percentage_fee = source_row[6]
        international_fixed_fee = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_stripe_currency(id, currency_code, country_code, card_type, domestic_percentage_fee, domestic_fixed_fee, international_percentage_fee, international_fixed_fee, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(currency_code)s,%(country_code)s,%(card_type)s,%(domestic_percentage_fee)s,%(domestic_fixed_fee)s,%(international_percentage_fee)s,%(international_fixed_fee)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'currency_code':currency_code,
                'country_code':country_code,
                'card_type':card_type,
                'domestic_percentage_fee':domestic_percentage_fee,
                'domestic_fixed_fee':domestic_fixed_fee,
                'international_percentage_fee':international_percentage_fee,
                'international_fixed_fee':international_fixed_fee,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_stripe_currency =====================
## ================= begin move_vwz_subject_subscription =====================
def move_vwz_subject_subscription(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select user_id, organization_id, subject_id, subscribed, deleted, created_on, created_by, last_modified, last_modified_by from vwz_subject_subscription")
    for source_row in source_cursor:
        user_id = source_row[0]
        organization_id = source_row[1]
        subject_id = source_row[2]
        subscribed = source_row[3]
        deleted = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_subject_subscription(user_id, organization_id, subject_id, subscribed, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(user_id)s,%(organization_id)s,%(subject_id)s,%(subscribed)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'user_id':user_id,
                'organization_id':organization_id,
                'subject_id':subject_id,
                'subscribed':subscribed,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_subject_subscription =====================
## ================= begin move_vwz_subscribed_campaign =====================
def move_vwz_subscribed_campaign(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select user_id, campaign_id, organization_id, created_on, created_by from vwz_subscribed_campaign")
    for source_row in source_cursor:
        user_id = source_row[0]
        campaign_id = source_row[1]
        organization_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_subscribed_campaign(user_id, campaign_id, organization_id, created_on, created_by) "
              "values(%(user_id)s,%(campaign_id)s,%(organization_id)s,%(created_on)s,%(created_by)s)")
        data = {  
                'user_id':user_id,
                'campaign_id':campaign_id,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_subscribed_campaign =====================
## ================= begin move_vwz_subscribed_event =====================
def move_vwz_subscribed_event(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select user_id, event_id, organization_id, created_on, created_by from vwz_subscribed_event")
    for source_row in source_cursor:
        user_id = source_row[0]
        event_id = source_row[1]
        organization_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_subscribed_event(user_id, event_id, organization_id, created_on, created_by) "
              "values(%(user_id)s,%(event_id)s,%(organization_id)s,%(created_on)s,%(created_by)s)")
        data = {  
                'user_id':user_id,
                'event_id':event_id,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_subscribed_event =====================
## ================= begin move_vwz_subscription_enterprise_request =====================
def move_vwz_subscription_enterprise_request(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, user_id, contact_name, contact_email, contact_phone, edm, events, is_branded_platform, users, notes, storage, created_on, created_by, last_modified, last_modified_by from vwz_subscription_enterprise_request")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        user_id = source_row[2]
        contact_name = source_row[3]
        contact_email = source_row[4]
        contact_phone = source_row[5]
        edm = source_row[6]
        events = source_row[7]
        is_branded_platform = source_row[8]
        users = source_row[9]
        notes = source_row[10]
        storage = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_subscription_enterprise_request(id, organization_id, user_id, contact_name, contact_email, contact_phone, edm, events, is_branded_platform, users, notes, storage, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(user_id)s,%(contact_name)s,%(contact_email)s,%(contact_phone)s,%(edm)s,%(events)s,%(is_branded_platform)s,%(users)s,%(notes)s,%(storage)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'user_id':user_id,
                'contact_name':contact_name,
                'contact_email':contact_email,
                'contact_phone':contact_phone,
                'edm':edm,
                'events':events,
                'is_branded_platform':is_branded_platform,
                'users':users,
                'notes':notes,
                'storage':storage,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_subscription_enterprise_request =====================
## ================= begin move_vwz_suppression =====================
def move_vwz_suppression(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select organization_id, email, contact_id, created_from_edm_id, created_on, bounced_reason from vwz_suppression")
    for source_row in source_cursor:
        organization_id = source_row[0]
        email = source_row[1]
        contact_id = source_row[2]
        created_from_edm_id = source_row[3]
        created_on = source_row[4]
        bounced_reason = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_suppression(organization_id, email, contact_id, created_from_edm_id, created_on, bounced_reason) "
              "values(%(organization_id)s,%(email)s,%(contact_id)s,%(created_from_edm_id)s,%(created_on)s,%(bounced_reason)s)")
        data = {  
                'organization_id':organization_id,
                'email':email,
                'contact_id':contact_id,
                'created_from_edm_id':created_from_edm_id,
                'created_on':created_on,
                'bounced_reason':bounced_reason
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_suppression =====================
## ================= begin move_vwz_survey_recipient_relation =====================
def move_vwz_survey_recipient_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, survey_id, contact_id, email, given_name, family_name, language_code, anonymous, answered, answered_on, seconds_used, tracking_id, mail_status, survey_opened_count, survey_opened_on, opened_on, clicked_on, created_on, not_send_reason from vwz_survey_recipient_relation")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        survey_id = source_row[3]
        contact_id = source_row[4]
        email = source_row[5]
        given_name = source_row[6]
        family_name = source_row[7]
        language_code = source_row[8]
        anonymous = source_row[9]
        answered = source_row[10]
        answered_on = source_row[11]
        seconds_used = source_row[12]
        tracking_id = source_row[13]
        mail_status = source_row[14]
        survey_opened_count = source_row[15]
        survey_opened_on = source_row[16]
        opened_on = source_row[17]
        clicked_on = source_row[18]
        created_on = source_row[19]
        not_send_reason = source_row[20]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_survey_recipient_relation(id, organization_id, event_id, survey_id, contact_id, email, given_name, family_name, language_code, anonymous, answered, answered_on, seconds_used, tracking_id, mail_status, survey_opened_count, survey_opened_on, opened_on, clicked_on, created_on, not_send_reason) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(survey_id)s,%(contact_id)s,%(email)s,%(given_name)s,%(family_name)s,%(language_code)s,%(anonymous)s,%(answered)s,%(answered_on)s,%(seconds_used)s,%(tracking_id)s,%(mail_status)s,%(survey_opened_count)s,%(survey_opened_on)s,%(opened_on)s,%(clicked_on)s,%(created_on)s,%(not_send_reason)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'survey_id':survey_id,
                'contact_id':contact_id,
                'email':email,
                'given_name':given_name,
                'family_name':family_name,
                'language_code':language_code,
                'anonymous':anonymous,
                'answered':answered,
                'answered_on':answered_on,
                'seconds_used':seconds_used,
                'tracking_id':tracking_id,
                'mail_status':mail_status,
                'survey_opened_count':survey_opened_count,
                'survey_opened_on':survey_opened_on,
                'opened_on':opened_on,
                'clicked_on':clicked_on,
                'created_on':created_on,
                'not_send_reason':not_send_reason
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_survey_recipient_relation =====================
## ================= begin move_vwz_system_email_track =====================
def move_vwz_system_email_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, uid, email_template_name, language_code, message_id, status, failure_reason, created_on, created_by, last_modified, last_modified_by from vwz_system_email_track")
    for source_row in source_cursor:
        id = source_row[0]
        uid = source_row[1]
        email_template_name = source_row[2]
        language_code = source_row[3]
        message_id = source_row[4]
        status = source_row[5]
        failure_reason = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_system_email_track(id, uid, email_template_name, language_code, message_id, status, failure_reason, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(uid)s,%(email_template_name)s,%(language_code)s,%(message_id)s,%(status)s,%(failure_reason)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'uid':uid,
                'email_template_name':email_template_name,
                'language_code':language_code,
                'message_id':message_id,
                'status':status,
                'failure_reason':failure_reason,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_system_email_track =====================
## ================= begin move_vwz_task =====================
def move_vwz_task(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, relation_type, relation_id, name, status, type, priority, priority_num, description, assigner_user_id, completer_user_id, complete_date, start_date, end_date, created_on, created_by, last_modified, last_modified_by from vwz_task")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        relation_type = source_row[2]
        relation_id = source_row[3]
        name = source_row[4]
        status = source_row[5]
        type = source_row[6]
        priority = source_row[7]
        priority_num = source_row[8]
        description = source_row[9]
        assigner_user_id = source_row[10]
        completer_user_id = source_row[11]
        complete_date = source_row[12]
        start_date = source_row[13]
        end_date = source_row[14]
        created_on = source_row[15]
        created_by = source_row[16]
        last_modified = source_row[17]
        last_modified_by = source_row[18]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_task(id, organization_id, relation_type, relation_id, name, status, type, priority, priority_num, description, assigner_user_id, completer_user_id, complete_date, start_date, end_date, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(relation_type)s,%(relation_id)s,%(name)s,%(status)s,%(type)s,%(priority)s,%(priority_num)s,%(description)s,%(assigner_user_id)s,%(completer_user_id)s,%(complete_date)s,%(start_date)s,%(end_date)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'relation_type':relation_type,
                'relation_id':relation_id,
                'name':name,
                'status':status,
                'type':type,
                'priority':priority,
                'priority_num':priority_num,
                'description':description,
                'assigner_user_id':assigner_user_id,
                'completer_user_id':completer_user_id,
                'complete_date':complete_date,
                'start_date':start_date,
                'end_date':end_date,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_task =====================
## ================= begin move_vwz_task_assignee =====================
def move_vwz_task_assignee(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, task_id, assignee_user_id, created_on, created_by, last_modified, last_modified_by from vwz_task_assignee")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        task_id = source_row[2]
        assignee_user_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_task_assignee(id, organization_id, task_id, assignee_user_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(task_id)s,%(assignee_user_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'task_id':task_id,
                'assignee_user_id':assignee_user_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_task_assignee =====================
## ================= begin move_vwz_task_update =====================
def move_vwz_task_update(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, task_id, description, document_bucket_id, created_on, created_by, last_modified, last_modified_by from vwz_task_update")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        task_id = source_row[2]
        description = source_row[3]
        document_bucket_id = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_task_update(id, organization_id, task_id, description, document_bucket_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(task_id)s,%(description)s,%(document_bucket_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'task_id':task_id,
                'description':description,
                'document_bucket_id':document_bucket_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_task_update =====================
## ================= begin move_vwz_temporary_user =====================
def move_vwz_temporary_user(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, user_id, enabled, context_id, context_name, password, is_kiosk, start_date, end_date, old_user, mobile, created_on, created_by, last_modified, last_modified_by from vwz_temporary_user")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        user_id = source_row[2]
        enabled = source_row[3]
        context_id = source_row[4]
        context_name = source_row[5]
        password = source_row[6]
        is_kiosk = source_row[7]
        start_date = source_row[8]
        end_date = source_row[9]
        old_user = source_row[10]
        mobile = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_temporary_user(id, organization_id, user_id, enabled, context_id, context_name, password, is_kiosk, start_date, end_date, old_user, mobile, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(user_id)s,%(enabled)s,%(context_id)s,%(context_name)s,%(password)s,%(is_kiosk)s,%(start_date)s,%(end_date)s,%(old_user)s,%(mobile)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'user_id':user_id,
                'enabled':enabled,
                'context_id':context_id,
                'context_name':context_name,
                'password':password,
                'is_kiosk':is_kiosk,
                'start_date':start_date,
                'end_date':end_date,
                'old_user':old_user,
                'mobile':mobile,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_temporary_user =====================
## ================= begin move_vwz_tenant_banner =====================
def move_vwz_tenant_banner(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, tenant_id, language_code, image_path, created_on, created_by, last_modified, last_modified_by from vwz_tenant_banner")
    for source_row in source_cursor:
        id = source_row[0]
        tenant_id = source_row[1]
        language_code = source_row[2]
        image_path = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_tenant_banner(id, tenant_id, language_code, image_path, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(tenant_id)s,%(language_code)s,%(image_path)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'tenant_id':tenant_id,
                'language_code':language_code,
                'image_path':image_path,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_tenant_banner =====================
## ================= begin move_vwz_tenant_branding =====================
def move_vwz_tenant_branding(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, tenant_id, enabled, created_on, created_by, last_modified, last_modified_by from vwz_tenant_branding")
    for source_row in source_cursor:
        id = source_row[0]
        tenant_id = source_row[1]
        enabled = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_tenant_branding(id, tenant_id, enabled, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(tenant_id)s,%(enabled)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'tenant_id':tenant_id,
                'enabled':enabled,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_tenant_branding =====================
## ================= begin move_vwz_tenant_controlled_feature =====================
def move_vwz_tenant_controlled_feature(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, feature_name, enabled, created_on, created_by, last_modified, last_modified_by from vwz_tenant_controlled_feature")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        feature_name = source_row[2]
        enabled = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_tenant_controlled_feature(id, organization_id, feature_name, enabled, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(feature_name)s,%(enabled)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'feature_name':feature_name,
                'enabled':enabled,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_tenant_controlled_feature =====================
## ================= begin move_vwz_ticket =====================
def move_vwz_ticket(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, event_id, language_code, title, quantity_total, quantity_sold, is_group_ticket, group_capacity, is_public, is_attendee_approval_required, one_registration_per_email, order, created_on, created_by, last_modified, last_modified_by, description from vwz_ticket")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        event_id = source_row[2]
        language_code = source_row[3]
        title = source_row[4]
        quantity_total = source_row[5]
        quantity_sold = source_row[6]
        is_group_ticket = source_row[7]
        group_capacity = source_row[8]
        is_public = source_row[9]
        is_attendee_approval_required = source_row[10]
        one_registration_per_email = source_row[11]
        order = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        description = source_row[17]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket(id, organization_id, event_id, language_code, title, quantity_total, quantity_sold, is_group_ticket, group_capacity, is_public, is_attendee_approval_required, one_registration_per_email, order, created_on, created_by, last_modified, last_modified_by, description) "
              "values(%(id)s,%(organization_id)s,%(event_id)s,%(language_code)s,%(title)s,%(quantity_total)s,%(quantity_sold)s,%(is_group_ticket)s,%(group_capacity)s,%(is_public)s,%(is_attendee_approval_required)s,%(one_registration_per_email)s,%(order)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(description)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'event_id':event_id,
                'language_code':language_code,
                'title':title,
                'quantity_total':quantity_total,
                'quantity_sold':quantity_sold,
                'is_group_ticket':is_group_ticket,
                'group_capacity':group_capacity,
                'is_public':is_public,
                'is_attendee_approval_required':is_attendee_approval_required,
                'one_registration_per_email':one_registration_per_email,
                'order':order,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'description':description
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket =====================
## ================= begin move_vwz_ticket_payment =====================
def move_vwz_ticket_payment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, transaction_id, buyer_family_name, buyer_given_name, paid_value, method, gateway, status, void_date_time, void_by, created_on, created_by, last_modified, last_modified_by from vwz_ticket_payment")
    for source_row in source_cursor:
        id = source_row[0]
        transaction_id = source_row[1]
        buyer_family_name = source_row[2]
        buyer_given_name = source_row[3]
        paid_value = source_row[4]
        method = source_row[5]
        gateway = source_row[6]
        status = source_row[7]
        void_date_time = source_row[8]
        void_by = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_payment(id, transaction_id, buyer_family_name, buyer_given_name, paid_value, method, gateway, status, void_date_time, void_by, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(transaction_id)s,%(buyer_family_name)s,%(buyer_given_name)s,%(paid_value)s,%(method)s,%(gateway)s,%(status)s,%(void_date_time)s,%(void_by)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'transaction_id':transaction_id,
                'buyer_family_name':buyer_family_name,
                'buyer_given_name':buyer_given_name,
                'paid_value':paid_value,
                'method':method,
                'gateway':gateway,
                'status':status,
                'void_date_time':void_date_time,
                'void_by':void_by,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_payment =====================
## ================= begin move_vwz_ticket_price_currency =====================
def move_vwz_ticket_price_currency(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, ticket_id, payment_way, currency_code, value, member_only, primary_member_only, membership_type_type, expiration_date_time, min_select_ticket, max_select_ticket, created_on, created_by, last_modified, last_modified_by from vwz_ticket_price_currency")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        ticket_id = source_row[2]
        payment_way = source_row[3]
        currency_code = source_row[4]
        value = source_row[5]
        member_only = source_row[6]
        primary_member_only = source_row[7]
        membership_type_type = source_row[8]
        expiration_date_time = source_row[9]
        min_select_ticket = source_row[10]
        max_select_ticket = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_price_currency(id, organization_id, ticket_id, payment_way, currency_code, value, member_only, primary_member_only, membership_type_type, expiration_date_time, min_select_ticket, max_select_ticket, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(ticket_id)s,%(payment_way)s,%(currency_code)s,%(value)s,%(member_only)s,%(primary_member_only)s,%(membership_type_type)s,%(expiration_date_time)s,%(min_select_ticket)s,%(max_select_ticket)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'ticket_id':ticket_id,
                'payment_way':payment_way,
                'currency_code':currency_code,
                'value':value,
                'member_only':member_only,
                'primary_member_only':primary_member_only,
                'membership_type_type':membership_type_type,
                'expiration_date_time':expiration_date_time,
                'min_select_ticket':min_select_ticket,
                'max_select_ticket':max_select_ticket,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_price_currency =====================
## ================= begin move_vwz_ticket_price_membership_type_relation =====================
def move_vwz_ticket_price_membership_type_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select ticket_price_id, membership_type_id, organization_id, created_on, created_by, last_modified, last_modified_by from vwz_ticket_price_membership_type_relation")
    for source_row in source_cursor:
        ticket_price_id = source_row[0]
        membership_type_id = source_row[1]
        organization_id = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_price_membership_type_relation(ticket_price_id, membership_type_id, organization_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(ticket_price_id)s,%(membership_type_id)s,%(organization_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'ticket_price_id':ticket_price_id,
                'membership_type_id':membership_type_id,
                'organization_id':organization_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_price_membership_type_relation =====================
## ================= begin move_vwz_ticket_sale =====================
def move_vwz_ticket_sale(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, transaction_id, group_id, payment_id, status, attendee_id, ticket_id, ticket_title, payment_way, face_value, ticket_value, discount_value, invoice_charge_value, price_id, qrcode_absolute_path, created_on, created_by, last_modified, last_modified_by from vwz_ticket_sale")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        transaction_id = source_row[2]
        group_id = source_row[3]
        payment_id = source_row[4]
        status = source_row[5]
        attendee_id = source_row[6]
        ticket_id = source_row[7]
        ticket_title = source_row[8]
        payment_way = source_row[9]
        face_value = source_row[10]
        ticket_value = source_row[11]
        discount_value = source_row[12]
        invoice_charge_value = source_row[13]
        price_id = source_row[14]
        qrcode_absolute_path = source_row[15]
        created_on = source_row[16]
        created_by = source_row[17]
        last_modified = source_row[18]
        last_modified_by = source_row[19]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_sale(id, organization_id, transaction_id, group_id, payment_id, status, attendee_id, ticket_id, ticket_title, payment_way, face_value, ticket_value, discount_value, invoice_charge_value, price_id, qrcode_absolute_path, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(transaction_id)s,%(group_id)s,%(payment_id)s,%(status)s,%(attendee_id)s,%(ticket_id)s,%(ticket_title)s,%(payment_way)s,%(face_value)s,%(ticket_value)s,%(discount_value)s,%(invoice_charge_value)s,%(price_id)s,%(qrcode_absolute_path)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'transaction_id':transaction_id,
                'group_id':group_id,
                'payment_id':payment_id,
                'status':status,
                'attendee_id':attendee_id,
                'ticket_id':ticket_id,
                'ticket_title':ticket_title,
                'payment_way':payment_way,
                'face_value':face_value,
                'ticket_value':ticket_value,
                'discount_value':discount_value,
                'invoice_charge_value':invoice_charge_value,
                'price_id':price_id,
                'qrcode_absolute_path':qrcode_absolute_path,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_sale =====================
## ================= begin move_vwz_ticket_sales_group =====================
def move_vwz_ticket_sales_group(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, group_name, group_capacity, paid_status, payment_way, face_value, ticket_value, discount_value, invoice_charge_value, created_on, created_by, last_modified, last_modified_by from vwz_ticket_sales_group")
    for source_row in source_cursor:
        id = source_row[0]
        group_name = source_row[1]
        group_capacity = source_row[2]
        paid_status = source_row[3]
        payment_way = source_row[4]
        face_value = source_row[5]
        ticket_value = source_row[6]
        discount_value = source_row[7]
        invoice_charge_value = source_row[8]
        created_on = source_row[9]
        created_by = source_row[10]
        last_modified = source_row[11]
        last_modified_by = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_sales_group(id, group_name, group_capacity, paid_status, payment_way, face_value, ticket_value, discount_value, invoice_charge_value, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(group_name)s,%(group_capacity)s,%(paid_status)s,%(payment_way)s,%(face_value)s,%(ticket_value)s,%(discount_value)s,%(invoice_charge_value)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'group_name':group_name,
                'group_capacity':group_capacity,
                'paid_status':paid_status,
                'payment_way':payment_way,
                'face_value':face_value,
                'ticket_value':ticket_value,
                'discount_value':discount_value,
                'invoice_charge_value':invoice_charge_value,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_sales_group =====================
## ================= begin move_vwz_ticket_sales_transaction =====================
def move_vwz_ticket_sales_transaction(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, currency_code, event_id, attendee_count, payment_way, has_invoice, coupon_id, coupon_title, coupon_discount, face_total, paid_total, discount_total, invoice_charge_total, invoice_charge_percentage, tx_fee_charge_total, tx_fee_charge_percentage, eb_commission_charge, created_on, created_by, last_modified, last_modified_by from vwz_ticket_sales_transaction")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        purchaser_given_name = source_row[2]
        purchaser_family_name = source_row[3]
        purchaser_email = source_row[4]
        purchaser_phone = source_row[5]
        purchaser_company_name = source_row[6]
        purchaser_position_title = source_row[7]
        purchaser_business_function_code = source_row[8]
        purchaser_business_role_code = source_row[9]
        purchaser_referral_text = source_row[10]
        purchaser_language_code = source_row[11]
        purchaser_address_country_code = source_row[12]
        purchaser_address_zip_code = source_row[13]
        currency_code = source_row[14]
        event_id = source_row[15]
        attendee_count = source_row[16]
        payment_way = source_row[17]
        has_invoice = source_row[18]
        coupon_id = source_row[19]
        coupon_title = source_row[20]
        coupon_discount = source_row[21]
        face_total = source_row[22]
        paid_total = source_row[23]
        discount_total = source_row[24]
        invoice_charge_total = source_row[25]
        invoice_charge_percentage = source_row[26]
        tx_fee_charge_total = source_row[27]
        tx_fee_charge_percentage = source_row[28]
        eb_commission_charge = source_row[29]
        created_on = source_row[30]
        created_by = source_row[31]
        last_modified = source_row[32]
        last_modified_by = source_row[33]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_sales_transaction(id, organization_id, purchaser_given_name, purchaser_family_name, purchaser_email, purchaser_phone, purchaser_company_name, purchaser_position_title, purchaser_business_function_code, purchaser_business_role_code, purchaser_referral_text, purchaser_language_code, purchaser_address_country_code, purchaser_address_zip_code, currency_code, event_id, attendee_count, payment_way, has_invoice, coupon_id, coupon_title, coupon_discount, face_total, paid_total, discount_total, invoice_charge_total, invoice_charge_percentage, tx_fee_charge_total, tx_fee_charge_percentage, eb_commission_charge, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(purchaser_given_name)s,%(purchaser_family_name)s,%(purchaser_email)s,%(purchaser_phone)s,%(purchaser_company_name)s,%(purchaser_position_title)s,%(purchaser_business_function_code)s,%(purchaser_business_role_code)s,%(purchaser_referral_text)s,%(purchaser_language_code)s,%(purchaser_address_country_code)s,%(purchaser_address_zip_code)s,%(currency_code)s,%(event_id)s,%(attendee_count)s,%(payment_way)s,%(has_invoice)s,%(coupon_id)s,%(coupon_title)s,%(coupon_discount)s,%(face_total)s,%(paid_total)s,%(discount_total)s,%(invoice_charge_total)s,%(invoice_charge_percentage)s,%(tx_fee_charge_total)s,%(tx_fee_charge_percentage)s,%(eb_commission_charge)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'purchaser_given_name':purchaser_given_name,
                'purchaser_family_name':purchaser_family_name,
                'purchaser_email':purchaser_email,
                'purchaser_phone':purchaser_phone,
                'purchaser_company_name':purchaser_company_name,
                'purchaser_position_title':purchaser_position_title,
                'purchaser_business_function_code':purchaser_business_function_code,
                'purchaser_business_role_code':purchaser_business_role_code,
                'purchaser_referral_text':purchaser_referral_text,
                'purchaser_language_code':purchaser_language_code,
                'purchaser_address_country_code':purchaser_address_country_code,
                'purchaser_address_zip_code':purchaser_address_zip_code,
                'currency_code':currency_code,
                'event_id':event_id,
                'attendee_count':attendee_count,
                'payment_way':payment_way,
                'has_invoice':has_invoice,
                'coupon_id':coupon_id,
                'coupon_title':coupon_title,
                'coupon_discount':coupon_discount,
                'face_total':face_total,
                'paid_total':paid_total,
                'discount_total':discount_total,
                'invoice_charge_total':invoice_charge_total,
                'invoice_charge_percentage':invoice_charge_percentage,
                'tx_fee_charge_total':tx_fee_charge_total,
                'tx_fee_charge_percentage':tx_fee_charge_percentage,
                'eb_commission_charge':eb_commission_charge,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_sales_transaction =====================
## ================= begin move_vwz_ticket_sales_transaction_invoice =====================
def move_vwz_ticket_sales_transaction_invoice(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, tax_registration_id, tax_registration_address, phone, bank_name, bank_account, organization_id, transaction_id, title, amount, currency_code, pickup_way, status, type, city_name, country_code, province, zip_code, street_address, note, created_on, created_by, last_modified, last_modified_by, delivery_phone, delivery_email from vwz_ticket_sales_transaction_invoice")
    for source_row in source_cursor:
        id = source_row[0]
        tax_registration_id = source_row[1]
        tax_registration_address = source_row[2]
        phone = source_row[3]
        bank_name = source_row[4]
        bank_account = source_row[5]
        organization_id = source_row[6]
        transaction_id = source_row[7]
        title = source_row[8]
        amount = source_row[9]
        currency_code = source_row[10]
        pickup_way = source_row[11]
        status = source_row[12]
        type = source_row[13]
        city_name = source_row[14]
        country_code = source_row[15]
        province = source_row[16]
        zip_code = source_row[17]
        street_address = source_row[18]
        note = source_row[19]
        created_on = source_row[20]
        created_by = source_row[21]
        last_modified = source_row[22]
        last_modified_by = source_row[23]
        delivery_phone = source_row[24]
        delivery_email = source_row[25]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_sales_transaction_invoice(id, tax_registration_id, tax_registration_address, phone, bank_name, bank_account, organization_id, transaction_id, title, amount, currency_code, pickup_way, status, type, city_name, country_code, province, zip_code, street_address, note, created_on, created_by, last_modified, last_modified_by, delivery_phone, delivery_email) "
              "values(%(id)s,%(tax_registration_id)s,%(tax_registration_address)s,%(phone)s,%(bank_name)s,%(bank_account)s,%(organization_id)s,%(transaction_id)s,%(title)s,%(amount)s,%(currency_code)s,%(pickup_way)s,%(status)s,%(type)s,%(city_name)s,%(country_code)s,%(province)s,%(zip_code)s,%(street_address)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(delivery_phone)s,%(delivery_email)s)")
        data = {  
                'id':id,
                'tax_registration_id':tax_registration_id,
                'tax_registration_address':tax_registration_address,
                'phone':phone,
                'bank_name':bank_name,
                'bank_account':bank_account,
                'organization_id':organization_id,
                'transaction_id':transaction_id,
                'title':title,
                'amount':amount,
                'currency_code':currency_code,
                'pickup_way':pickup_way,
                'status':status,
                'type':type,
                'city_name':city_name,
                'country_code':country_code,
                'province':province,
                'zip_code':zip_code,
                'street_address':street_address,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'delivery_phone':delivery_phone,
                'delivery_email':delivery_email
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_sales_transaction_invoice =====================
## ================= begin move_vwz_ticket_sales_transaction_status =====================
def move_vwz_ticket_sales_transaction_status(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, transaction_id, status, is_active, created_on, created_by, last_modified, last_modified_by from vwz_ticket_sales_transaction_status")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        transaction_id = source_row[2]
        status = source_row[3]
        is_active = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_ticket_sales_transaction_status(id, organization_id, transaction_id, status, is_active, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(transaction_id)s,%(status)s,%(is_active)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'transaction_id':transaction_id,
                'status':status,
                'is_active':is_active,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_ticket_sales_transaction_status =====================
## ================= begin move_vwz_translation_language_relation =====================
def move_vwz_translation_language_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, type, object_id, language_code, deleted, created_on, created_by, last_modified, last_modified_by from vwz_translation_language_relation")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        type = source_row[2]
        object_id = source_row[3]
        language_code = source_row[4]
        deleted = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_translation_language_relation(id, organization_id, type, object_id, language_code, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(type)s,%(object_id)s,%(language_code)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'type':type,
                'object_id':object_id,
                'language_code':language_code,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_translation_language_relation =====================
## ================= begin move_vwz_user =====================
def move_vwz_user(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, global_user_id, passphrase, salt, verification_code, is_verified, is_pending_reset, failed_login_attempts, last_login, default_language_code, city_code, country_code, zip_code, business_function_code, business_role_code, industry_code, phone, email, register_phone, profile_picture_bucket_id, activated_after_migration, user_organization_id, is_registered, is_eb_optin, eb_optin_date, community_optout, community_opt_date, dm_enabled, firebase_uid, business_card_enabled, deleted, created_on, created_by, last_modified, last_modified_by from vwz_user")
    for source_row in source_cursor:
        id = source_row[0]
        global_user_id = source_row[1]
        passphrase = source_row[2]
        salt = source_row[3]
        verification_code = source_row[4]
        is_verified = source_row[5]
        is_pending_reset = source_row[6]
        failed_login_attempts = source_row[7]
        last_login = source_row[8]
        default_language_code = source_row[9]
        city_code = source_row[10]
        country_code = source_row[11]
        zip_code = source_row[12]
        business_function_code = source_row[13]
        business_role_code = source_row[14]
        industry_code = source_row[15]
        phone = source_row[16]
        email = source_row[17]
        register_phone = source_row[18]
        profile_picture_bucket_id = source_row[19]
        activated_after_migration = source_row[20]
        user_organization_id = source_row[21]
        is_registered = source_row[22]
        is_eb_optin = source_row[23]
        eb_optin_date = source_row[24]
        community_optout = source_row[25]
        community_opt_date = source_row[26]
        dm_enabled = source_row[27]
        firebase_uid = source_row[28]
        business_card_enabled = source_row[29]
        deleted = source_row[30]
        created_on = source_row[31]
        created_by = source_row[32]
        last_modified = source_row[33]
        last_modified_by = source_row[34]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user(id, global_user_id, passphrase, salt, verification_code, is_verified, is_pending_reset, failed_login_attempts, last_login, default_language_code, city_code, country_code, zip_code, business_function_code, business_role_code, industry_code, phone, email, register_phone, profile_picture_bucket_id, activated_after_migration, user_organization_id, is_registered, is_eb_optin, eb_optin_date, community_optout, community_opt_date, dm_enabled, firebase_uid, business_card_enabled, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(global_user_id)s,%(passphrase)s,%(salt)s,%(verification_code)s,%(is_verified)s,%(is_pending_reset)s,%(failed_login_attempts)s,%(last_login)s,%(default_language_code)s,%(city_code)s,%(country_code)s,%(zip_code)s,%(business_function_code)s,%(business_role_code)s,%(industry_code)s,%(phone)s,%(email)s,%(register_phone)s,%(profile_picture_bucket_id)s,%(activated_after_migration)s,%(user_organization_id)s,%(is_registered)s,%(is_eb_optin)s,%(eb_optin_date)s,%(community_optout)s,%(community_opt_date)s,%(dm_enabled)s,%(firebase_uid)s,%(business_card_enabled)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'global_user_id':global_user_id,
                'passphrase':passphrase,
                'salt':salt,
                'verification_code':verification_code,
                'is_verified':is_verified,
                'is_pending_reset':is_pending_reset,
                'failed_login_attempts':failed_login_attempts,
                'last_login':last_login,
                'default_language_code':default_language_code,
                'city_code':city_code,
                'country_code':country_code,
                'zip_code':zip_code,
                'business_function_code':business_function_code,
                'business_role_code':business_role_code,
                'industry_code':industry_code,
                'phone':phone,
                'email':email,
                'register_phone':register_phone,
                'profile_picture_bucket_id':profile_picture_bucket_id,
                'activated_after_migration':activated_after_migration,
                'user_organization_id':user_organization_id,
                'is_registered':is_registered,
                'is_eb_optin':is_eb_optin,
                'eb_optin_date':eb_optin_date,
                'community_optout':community_optout,
                'community_opt_date':community_opt_date,
                'dm_enabled':dm_enabled,
                'firebase_uid':firebase_uid,
                'business_card_enabled':business_card_enabled,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user =====================
## ================= begin move_vwz_user_account =====================
def move_vwz_user_account(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, browsing_language_code, is_show_profile, is_subscribe_to_notifications, fapiao_title, fapiao_address, default_timezone_city, created_on, created_by, last_modified, last_modified_by from vwz_user_account")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        browsing_language_code = source_row[2]
        is_show_profile = source_row[3]
        is_subscribe_to_notifications = source_row[4]
        fapiao_title = source_row[5]
        fapiao_address = source_row[6]
        default_timezone_city = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        last_modified = source_row[10]
        last_modified_by = source_row[11]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_account(id, user_id, browsing_language_code, is_show_profile, is_subscribe_to_notifications, fapiao_title, fapiao_address, default_timezone_city, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(browsing_language_code)s,%(is_show_profile)s,%(is_subscribe_to_notifications)s,%(fapiao_title)s,%(fapiao_address)s,%(default_timezone_city)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'browsing_language_code':browsing_language_code,
                'is_show_profile':is_show_profile,
                'is_subscribe_to_notifications':is_subscribe_to_notifications,
                'fapiao_title':fapiao_title,
                'fapiao_address':fapiao_address,
                'default_timezone_city':default_timezone_city,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_account =====================
## ================= begin move_vwz_user_ctx =====================
def move_vwz_user_ctx(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, language_code, given_name, family_name, street_address, city_name_text, province, company_name, department, position_title, summary, created_on, created_by, last_modified, last_modified_by from vwz_user_ctx")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        language_code = source_row[2]
        given_name = source_row[3]
        family_name = source_row[4]
        street_address = source_row[5]
        city_name_text = source_row[6]
        province = source_row[7]
        company_name = source_row[8]
        department = source_row[9]
        position_title = source_row[10]
        summary = source_row[11]
        created_on = source_row[12]
        created_by = source_row[13]
        last_modified = source_row[14]
        last_modified_by = source_row[15]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_ctx(id, user_id, language_code, given_name, family_name, street_address, city_name_text, province, company_name, department, position_title, summary, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(language_code)s,%(given_name)s,%(family_name)s,%(street_address)s,%(city_name_text)s,%(province)s,%(company_name)s,%(department)s,%(position_title)s,%(summary)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'language_code':language_code,
                'given_name':given_name,
                'family_name':family_name,
                'street_address':street_address,
                'city_name_text':city_name_text,
                'province':province,
                'company_name':company_name,
                'department':department,
                'position_title':position_title,
                'summary':summary,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_ctx =====================
## ================= begin move_vwz_user_device =====================
def move_vwz_user_device(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, language_code, platform, token, udid, created_on, created_by, last_modified, last_modified_by, account, tag, app_name from vwz_user_device")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        language_code = source_row[2]
        platform = source_row[3]
        token = source_row[4]
        udid = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        account = source_row[10]
        tag = source_row[11]
        app_name = source_row[12]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_device(id, user_id, language_code, platform, token, udid, created_on, created_by, last_modified, last_modified_by, account, tag, app_name) "
              "values(%(id)s,%(user_id)s,%(language_code)s,%(platform)s,%(token)s,%(udid)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(account)s,%(tag)s,%(app_name)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'language_code':language_code,
                'platform':platform,
                'token':token,
                'udid':udid,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'account':account,
                'tag':tag,
                'app_name':app_name
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_device =====================
## ================= begin move_vwz_user_event_agenda =====================
def move_vwz_user_event_agenda(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, event_id, user_id, event_agenda_item_id, created_on, created_by, last_modified, last_modified_by from vwz_user_event_agenda")
    for source_row in source_cursor:
        id = source_row[0]
        event_id = source_row[1]
        user_id = source_row[2]
        event_agenda_item_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_event_agenda(id, event_id, user_id, event_agenda_item_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(event_id)s,%(user_id)s,%(event_agenda_item_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'event_id':event_id,
                'user_id':user_id,
                'event_agenda_item_id':event_agenda_item_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_event_agenda =====================
## ================= begin move_vwz_user_fapiao =====================
def move_vwz_user_fapiao(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, title, street_address, city_name, province, country_code, zip_code, phone, tax_registration_id, tax_registration_address, bank_name, bank_account, pickupWay, note, created_on, created_by, last_modified, last_modified_by, delivery_phone, delivery_email from vwz_user_fapiao")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        title = source_row[2]
        street_address = source_row[3]
        city_name = source_row[4]
        province = source_row[5]
        country_code = source_row[6]
        zip_code = source_row[7]
        phone = source_row[8]
        tax_registration_id = source_row[9]
        tax_registration_address = source_row[10]
        bank_name = source_row[11]
        bank_account = source_row[12]
        pickupWay = source_row[13]
        note = source_row[14]
        created_on = source_row[15]
        created_by = source_row[16]
        last_modified = source_row[17]
        last_modified_by = source_row[18]
        delivery_phone = source_row[19]
        delivery_email = source_row[20]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_fapiao(id, user_id, title, street_address, city_name, province, country_code, zip_code, phone, tax_registration_id, tax_registration_address, bank_name, bank_account, pickupWay, note, created_on, created_by, last_modified, last_modified_by, delivery_phone, delivery_email) "
              "values(%(id)s,%(user_id)s,%(title)s,%(street_address)s,%(city_name)s,%(province)s,%(country_code)s,%(zip_code)s,%(phone)s,%(tax_registration_id)s,%(tax_registration_address)s,%(bank_name)s,%(bank_account)s,%(pickupWay)s,%(note)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s,%(delivery_phone)s,%(delivery_email)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'title':title,
                'street_address':street_address,
                'city_name':city_name,
                'province':province,
                'country_code':country_code,
                'zip_code':zip_code,
                'phone':phone,
                'tax_registration_id':tax_registration_id,
                'tax_registration_address':tax_registration_address,
                'bank_name':bank_name,
                'bank_account':bank_account,
                'pickupWay':pickupWay,
                'note':note,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by,
                'delivery_phone':delivery_phone,
                'delivery_email':delivery_email
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_fapiao =====================
## ================= begin move_vwz_user_feed =====================
def move_vwz_user_feed(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select user_id, organization_id, feed_type, feed_id, created_on, created_by, deleted from vwz_user_feed")
    for source_row in source_cursor:
        user_id = source_row[0]
        organization_id = source_row[1]
        feed_type = source_row[2]
        feed_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        deleted = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_feed(user_id, organization_id, feed_type, feed_id, created_on, created_by, deleted) "
              "values(%(user_id)s,%(organization_id)s,%(feed_type)s,%(feed_id)s,%(created_on)s,%(created_by)s,%(deleted)s)")
        data = {  
                'user_id':user_id,
                'organization_id':organization_id,
                'feed_type':feed_type,
                'feed_id':feed_id,
                'created_on':created_on,
                'created_by':created_by,
                'deleted':deleted
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_feed =====================
## ================= begin move_vwz_user_highlighted_event =====================
def move_vwz_user_highlighted_event(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, organization_id, event_id, created_on, created_by, last_modified, last_modified_by from vwz_user_highlighted_event")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        organization_id = source_row[2]
        event_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_highlighted_event(id, user_id, organization_id, event_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(organization_id)s,%(event_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'organization_id':organization_id,
                'event_id':event_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_highlighted_event =====================
## ================= begin move_vwz_user_like_devent =====================
def move_vwz_user_like_devent(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select user_id, event_id, organization_id, deleted, created_on, last_modified from vwz_user_like_devent")
    for source_row in source_cursor:
        user_id = source_row[0]
        event_id = source_row[1]
        organization_id = source_row[2]
        deleted = source_row[3]
        created_on = source_row[4]
        last_modified = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_like_devent(user_id, event_id, organization_id, deleted, created_on, last_modified) "
              "values(%(user_id)s,%(event_id)s,%(organization_id)s,%(deleted)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'user_id':user_id,
                'event_id':event_id,
                'organization_id':organization_id,
                'deleted':deleted,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_like_devent =====================
## ================= begin move_vwz_user_login_track =====================
def move_vwz_user_login_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, organization_id, broker_id, source, created_on from vwz_user_login_track")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        organization_id = source_row[2]
        broker_id = source_row[3]
        source = source_row[4]
        created_on = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_login_track(id, user_id, organization_id, broker_id, source, created_on) "
              "values(%(id)s,%(user_id)s,%(organization_id)s,%(broker_id)s,%(source)s,%(created_on)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'organization_id':organization_id,
                'broker_id':broker_id,
                'source':source,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_login_track =====================
## ================= begin move_vwz_user_merged =====================
def move_vwz_user_merged(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select uid_1, uid_2, global_user_id, default_node, default_language_code, language_code, browsing_language_code, given_name, family_name, phone, email, street_address, department, city_code, country_code, city_name_text, province, zip_code, default_timezone_city, industry_code, company_name, position_title, summary, passphrase, salt, verification_code, is_verified, is_registered, is_eb_optin, eb_optin_date, is_deleted, last_login, created_on, last_modified from vwz_user_merged")
    for source_row in source_cursor:
        uid_1 = source_row[0]
        uid_2 = source_row[1]
        global_user_id = source_row[2]
        default_node = source_row[3]
        default_language_code = source_row[4]
        language_code = source_row[5]
        browsing_language_code = source_row[6]
        given_name = source_row[7]
        family_name = source_row[8]
        phone = source_row[9]
        email = source_row[10]
        street_address = source_row[11]
        department = source_row[12]
        city_code = source_row[13]
        country_code = source_row[14]
        city_name_text = source_row[15]
        province = source_row[16]
        zip_code = source_row[17]
        default_timezone_city = source_row[18]
        industry_code = source_row[19]
        company_name = source_row[20]
        position_title = source_row[21]
        summary = source_row[22]
        passphrase = source_row[23]
        salt = source_row[24]
        verification_code = source_row[25]
        is_verified = source_row[26]
        is_registered = source_row[27]
        is_eb_optin = source_row[28]
        eb_optin_date = source_row[29]
        is_deleted = source_row[30]
        last_login = source_row[31]
        created_on = source_row[32]
        last_modified = source_row[33]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_merged(uid_1, uid_2, global_user_id, default_node, default_language_code, language_code, browsing_language_code, given_name, family_name, phone, email, street_address, department, city_code, country_code, city_name_text, province, zip_code, default_timezone_city, industry_code, company_name, position_title, summary, passphrase, salt, verification_code, is_verified, is_registered, is_eb_optin, eb_optin_date, is_deleted, last_login, created_on, last_modified) "
              "values(%(uid_1)s,%(uid_2)s,%(global_user_id)s,%(default_node)s,%(default_language_code)s,%(language_code)s,%(browsing_language_code)s,%(given_name)s,%(family_name)s,%(phone)s,%(email)s,%(street_address)s,%(department)s,%(city_code)s,%(country_code)s,%(city_name_text)s,%(province)s,%(zip_code)s,%(default_timezone_city)s,%(industry_code)s,%(company_name)s,%(position_title)s,%(summary)s,%(passphrase)s,%(salt)s,%(verification_code)s,%(is_verified)s,%(is_registered)s,%(is_eb_optin)s,%(eb_optin_date)s,%(is_deleted)s,%(last_login)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'uid_1':uid_1,
                'uid_2':uid_2,
                'global_user_id':global_user_id,
                'default_node':default_node,
                'default_language_code':default_language_code,
                'language_code':language_code,
                'browsing_language_code':browsing_language_code,
                'given_name':given_name,
                'family_name':family_name,
                'phone':phone,
                'email':email,
                'street_address':street_address,
                'department':department,
                'city_code':city_code,
                'country_code':country_code,
                'city_name_text':city_name_text,
                'province':province,
                'zip_code':zip_code,
                'default_timezone_city':default_timezone_city,
                'industry_code':industry_code,
                'company_name':company_name,
                'position_title':position_title,
                'summary':summary,
                'passphrase':passphrase,
                'salt':salt,
                'verification_code':verification_code,
                'is_verified':is_verified,
                'is_registered':is_registered,
                'is_eb_optin':is_eb_optin,
                'eb_optin_date':eb_optin_date,
                'is_deleted':is_deleted,
                'last_login':last_login,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_merged =====================
## ================= begin move_vwz_user_notification_log =====================
def move_vwz_user_notification_log(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, type, context, userId, created_on from vwz_user_notification_log")
    for source_row in source_cursor:
        id = source_row[0]
        type = source_row[1]
        context = source_row[2]
        userId = source_row[3]
        created_on = source_row[4]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_notification_log(id, type, context, userId, created_on) "
              "values(%(id)s,%(type)s,%(context)s,%(userId)s,%(created_on)s)")
        data = {  
                'id':id,
                'type':type,
                'context':context,
                'userId':userId,
                'created_on':created_on
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_notification_log =====================
## ================= begin move_vwz_user_profile =====================
def move_vwz_user_profile(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, created_on, created_by, last_modified, last_modified_by from vwz_user_profile")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        created_on = source_row[2]
        created_by = source_row[3]
        last_modified = source_row[4]
        last_modified_by = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_profile(id, user_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_profile =====================
## ================= begin move_vwz_user_profile_business_interest =====================
def move_vwz_user_profile_business_interest(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_profile_id, language_code, name, created_on, created_by, last_modified, last_modified_by from vwz_user_profile_business_interest")
    for source_row in source_cursor:
        id = source_row[0]
        user_profile_id = source_row[1]
        language_code = source_row[2]
        name = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_profile_business_interest(id, user_profile_id, language_code, name, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_profile_id)s,%(language_code)s,%(name)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_profile_id':user_profile_id,
                'language_code':language_code,
                'name':name,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_profile_business_interest =====================
## ================= begin move_vwz_user_profile_experience =====================
def move_vwz_user_profile_experience(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_profile_id, language_code, beginning_date, ending_date, company_name, position_title, created_on, created_by, last_modified, last_modified_by from vwz_user_profile_experience")
    for source_row in source_cursor:
        id = source_row[0]
        user_profile_id = source_row[1]
        language_code = source_row[2]
        beginning_date = source_row[3]
        ending_date = source_row[4]
        company_name = source_row[5]
        position_title = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_profile_experience(id, user_profile_id, language_code, beginning_date, ending_date, company_name, position_title, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_profile_id)s,%(language_code)s,%(beginning_date)s,%(ending_date)s,%(company_name)s,%(position_title)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_profile_id':user_profile_id,
                'language_code':language_code,
                'beginning_date':beginning_date,
                'ending_date':ending_date,
                'company_name':company_name,
                'position_title':position_title,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_profile_experience =====================
## ================= begin move_vwz_user_profile_language_proficiency =====================
def move_vwz_user_profile_language_proficiency(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_profile_id, language_code, language_proficiency, created_on, created_by, last_modified, last_modified_by from vwz_user_profile_language_proficiency")
    for source_row in source_cursor:
        id = source_row[0]
        user_profile_id = source_row[1]
        language_code = source_row[2]
        language_proficiency = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_profile_language_proficiency(id, user_profile_id, language_code, language_proficiency, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_profile_id)s,%(language_code)s,%(language_proficiency)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_profile_id':user_profile_id,
                'language_code':language_code,
                'language_proficiency':language_proficiency,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_profile_language_proficiency =====================
## ================= begin move_vwz_user_profile_picture =====================
def move_vwz_user_profile_picture(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_profile_id, doc_bucket_id, is_default, created_on, created_by, last_modified, last_modified_by from vwz_user_profile_picture")
    for source_row in source_cursor:
        id = source_row[0]
        user_profile_id = source_row[1]
        doc_bucket_id = source_row[2]
        is_default = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_profile_picture(id, user_profile_id, doc_bucket_id, is_default, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_profile_id)s,%(doc_bucket_id)s,%(is_default)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_profile_id':user_profile_id,
                'doc_bucket_id':doc_bucket_id,
                'is_default':is_default,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_profile_picture =====================
## ================= begin move_vwz_user_profile_skill =====================
def move_vwz_user_profile_skill(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_profile_id, language_code, skill, created_on, created_by, last_modified, last_modified_by from vwz_user_profile_skill")
    for source_row in source_cursor:
        id = source_row[0]
        user_profile_id = source_row[1]
        language_code = source_row[2]
        skill = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_profile_skill(id, user_profile_id, language_code, skill, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_profile_id)s,%(language_code)s,%(skill)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_profile_id':user_profile_id,
                'language_code':language_code,
                'skill':skill,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_profile_skill =====================
## ================= begin move_vwz_user_profile_summary =====================
def move_vwz_user_profile_summary(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_profile_id, language_code, summary, created_on, created_by, last_modified, last_modified_by from vwz_user_profile_summary")
    for source_row in source_cursor:
        id = source_row[0]
        user_profile_id = source_row[1]
        language_code = source_row[2]
        summary = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_profile_summary(id, user_profile_id, language_code, summary, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_profile_id)s,%(language_code)s,%(summary)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_profile_id':user_profile_id,
                'language_code':language_code,
                'summary':summary,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_profile_summary =====================
## ================= begin move_vwz_user_relation =====================
def move_vwz_user_relation(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, relation_id, user_id, deleted, created_on, created_by, last_modified, last_modified_by from vwz_user_relation")
    for source_row in source_cursor:
        id = source_row[0]
        relation_id = source_row[1]
        user_id = source_row[2]
        deleted = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_relation(id, relation_id, user_id, deleted, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(relation_id)s,%(user_id)s,%(deleted)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'relation_id':relation_id,
                'user_id':user_id,
                'deleted':deleted,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_relation =====================
## ================= begin move_vwz_user_saved_event =====================
def move_vwz_user_saved_event(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, user_id, organization_id, event_id, created_on, created_by, last_modified, last_modified_by from vwz_user_saved_event")
    for source_row in source_cursor:
        id = source_row[0]
        user_id = source_row[1]
        organization_id = source_row[2]
        event_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_saved_event(id, user_id, organization_id, event_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(user_id)s,%(organization_id)s,%(event_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'user_id':user_id,
                'organization_id':organization_id,
                'event_id':event_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_saved_event =====================
## ================= begin move_vwz_user_sync_track =====================
def move_vwz_user_sync_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, to_node, user_id, sync_type, success, failure_reason, created_on, last_modified from vwz_user_sync_track")
    for source_row in source_cursor:
        id = source_row[0]
        to_node = source_row[1]
        user_id = source_row[2]
        sync_type = source_row[3]
        success = source_row[4]
        failure_reason = source_row[5]
        created_on = source_row[6]
        last_modified = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_user_sync_track(id, to_node, user_id, sync_type, success, failure_reason, created_on, last_modified) "
              "values(%(id)s,%(to_node)s,%(user_id)s,%(sync_type)s,%(success)s,%(failure_reason)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'to_node':to_node,
                'user_id':user_id,
                'sync_type':sync_type,
                'success':success,
                'failure_reason':failure_reason,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_user_sync_track =====================
## ================= begin move_vwz_xero_account_mapping =====================
def move_vwz_xero_account_mapping(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, data_type, object_id, account_code, account_id, account_name, created_on, created_by, last_modified, last_modified_by from vwz_xero_account_mapping")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        data_type = source_row[2]
        object_id = source_row[3]
        account_code = source_row[4]
        account_id = source_row[5]
        account_name = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        last_modified = source_row[9]
        last_modified_by = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_account_mapping(id, organization_id, data_type, object_id, account_code, account_id, account_name, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(data_type)s,%(object_id)s,%(account_code)s,%(account_id)s,%(account_name)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'data_type':data_type,
                'object_id':object_id,
                'account_code':account_code,
                'account_id':account_id,
                'account_name':account_name,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_account_mapping =====================
## ================= begin move_vwz_xero_auth =====================
def move_vwz_xero_auth(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, short_code, xero_org_id, xero_org_name, temp_token, temp_token_secret, access_token, access_token_secret, session_handle, access_token_expiry, session_expiry, modify_since, created_on, created_by, last_modified, last_modified_by from vwz_xero_auth")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        short_code = source_row[2]
        xero_org_id = source_row[3]
        xero_org_name = source_row[4]
        temp_token = source_row[5]
        temp_token_secret = source_row[6]
        access_token = source_row[7]
        access_token_secret = source_row[8]
        session_handle = source_row[9]
        access_token_expiry = source_row[10]
        session_expiry = source_row[11]
        modify_since = source_row[12]
        created_on = source_row[13]
        created_by = source_row[14]
        last_modified = source_row[15]
        last_modified_by = source_row[16]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_auth(id, organization_id, short_code, xero_org_id, xero_org_name, temp_token, temp_token_secret, access_token, access_token_secret, session_handle, access_token_expiry, session_expiry, modify_since, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(short_code)s,%(xero_org_id)s,%(xero_org_name)s,%(temp_token)s,%(temp_token_secret)s,%(access_token)s,%(access_token_secret)s,%(session_handle)s,%(access_token_expiry)s,%(session_expiry)s,%(modify_since)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'short_code':short_code,
                'xero_org_id':xero_org_id,
                'xero_org_name':xero_org_name,
                'temp_token':temp_token,
                'temp_token_secret':temp_token_secret,
                'access_token':access_token,
                'access_token_secret':access_token_secret,
                'session_handle':session_handle,
                'access_token_expiry':access_token_expiry,
                'session_expiry':session_expiry,
                'modify_since':modify_since,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_auth =====================
## ================= begin move_vwz_xero_auth2 =====================
def move_vwz_xero_auth2(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, xero_tenant_id, xero_tenant_name, csrf, connection_id, access_token, refresh_token, access_token_expiry, modify_since, created_on, created_by, last_modified, last_modified_by from vwz_xero_auth2")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        xero_tenant_id = source_row[2]
        xero_tenant_name = source_row[3]
        csrf = source_row[4]
        connection_id = source_row[5]
        access_token = source_row[6]
        refresh_token = source_row[7]
        access_token_expiry = source_row[8]
        modify_since = source_row[9]
        created_on = source_row[10]
        created_by = source_row[11]
        last_modified = source_row[12]
        last_modified_by = source_row[13]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_auth2(id, organization_id, xero_tenant_id, xero_tenant_name, csrf, connection_id, access_token, refresh_token, access_token_expiry, modify_since, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(xero_tenant_id)s,%(xero_tenant_name)s,%(csrf)s,%(connection_id)s,%(access_token)s,%(refresh_token)s,%(access_token_expiry)s,%(modify_since)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'xero_tenant_id':xero_tenant_id,
                'xero_tenant_name':xero_tenant_name,
                'csrf':csrf,
                'connection_id':connection_id,
                'access_token':access_token,
                'refresh_token':refresh_token,
                'access_token_expiry':access_token_expiry,
                'modify_since':modify_since,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_auth2 =====================
## ================= begin move_vwz_xero_auth_track =====================
def move_vwz_xero_auth_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, op_type, message, callback_org_id, short_code, xero_tenant_id, xero_tenant_name, created_on, created_by from vwz_xero_auth_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        op_type = source_row[2]
        message = source_row[3]
        callback_org_id = source_row[4]
        short_code = source_row[5]
        xero_tenant_id = source_row[6]
        xero_tenant_name = source_row[7]
        created_on = source_row[8]
        created_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_auth_track(id, organization_id, op_type, message, callback_org_id, short_code, xero_tenant_id, xero_tenant_name, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(op_type)s,%(message)s,%(callback_org_id)s,%(short_code)s,%(xero_tenant_id)s,%(xero_tenant_name)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'op_type':op_type,
                'message':message,
                'callback_org_id':callback_org_id,
                'short_code':short_code,
                'xero_tenant_id':xero_tenant_id,
                'xero_tenant_name':xero_tenant_name,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_auth_track =====================
## ================= begin move_vwz_xero_company =====================
def move_vwz_xero_company(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, company_name, xero_contact_id, created_on, created_by, last_modified, last_modified_by from vwz_xero_company")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        company_name = source_row[2]
        xero_contact_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_company(id, organization_id, company_name, xero_contact_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(company_name)s,%(xero_contact_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'company_name':company_name,
                'xero_contact_id':xero_contact_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_company =====================
## ================= begin move_vwz_xero_contact =====================
def move_vwz_xero_contact(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, contact_id, xero_contact_id, created_on, created_by, last_modified, last_modified_by from vwz_xero_contact")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        contact_id = source_row[2]
        xero_contact_id = source_row[3]
        created_on = source_row[4]
        created_by = source_row[5]
        last_modified = source_row[6]
        last_modified_by = source_row[7]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_contact(id, organization_id, contact_id, xero_contact_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(contact_id)s,%(xero_contact_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'contact_id':contact_id,
                'xero_contact_id':xero_contact_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_contact =====================
## ================= begin move_vwz_xero_currency_mapping =====================
def move_vwz_xero_currency_mapping(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, currency_code, created_on, created_by, last_modified, last_modified_by from vwz_xero_currency_mapping")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        currency_code = source_row[2]
        created_on = source_row[3]
        created_by = source_row[4]
        last_modified = source_row[5]
        last_modified_by = source_row[6]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_currency_mapping(id, organization_id, currency_code, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(currency_code)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'currency_code':currency_code,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_currency_mapping =====================
## ================= begin move_vwz_xero_invoice =====================
def move_vwz_xero_invoice(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, invoice_id, invoice_number, xero_invoice_id, xero_invoice_status, created_on, created_by, last_modified, last_modified_by from vwz_xero_invoice")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        invoice_id = source_row[2]
        invoice_number = source_row[3]
        xero_invoice_id = source_row[4]
        xero_invoice_status = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_invoice(id, organization_id, invoice_id, invoice_number, xero_invoice_id, xero_invoice_status, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(invoice_id)s,%(invoice_number)s,%(xero_invoice_id)s,%(xero_invoice_status)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'invoice_id':invoice_id,
                'invoice_number':invoice_number,
                'xero_invoice_id':xero_invoice_id,
                'xero_invoice_status':xero_invoice_status,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_invoice =====================
## ================= begin move_vwz_xero_invoice_attachments =====================
def move_vwz_xero_invoice_attachments(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, invoice_id, bucket_id, xero_attachment_id, created_on, created_by, last_modified, last_modified_by from vwz_xero_invoice_attachments")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        invoice_id = source_row[2]
        bucket_id = source_row[3]
        xero_attachment_id = source_row[4]
        created_on = source_row[5]
        created_by = source_row[6]
        last_modified = source_row[7]
        last_modified_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_invoice_attachments(id, organization_id, invoice_id, bucket_id, xero_attachment_id, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(invoice_id)s,%(bucket_id)s,%(xero_attachment_id)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'invoice_id':invoice_id,
                'bucket_id':bucket_id,
                'xero_attachment_id':xero_attachment_id,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_invoice_attachments =====================
## ================= begin move_vwz_xero_limiter =====================
def move_vwz_xero_limiter(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, xero_tenant_id, hour_window, second_window, hour_count, second_count, pass_count, created_on, last_modified from vwz_xero_limiter")
    for source_row in source_cursor:
        id = source_row[0]
        xero_tenant_id = source_row[1]
        hour_window = source_row[2]
        second_window = source_row[3]
        hour_count = source_row[4]
        second_count = source_row[5]
        pass_count = source_row[6]
        created_on = source_row[7]
        last_modified = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_limiter(id, xero_tenant_id, hour_window, second_window, hour_count, second_count, pass_count, created_on, last_modified) "
              "values(%(id)s,%(xero_tenant_id)s,%(hour_window)s,%(second_window)s,%(hour_count)s,%(second_count)s,%(pass_count)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'xero_tenant_id':xero_tenant_id,
                'hour_window':hour_window,
                'second_window':second_window,
                'hour_count':hour_count,
                'second_count':second_count,
                'pass_count':pass_count,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_limiter =====================
## ================= begin move_vwz_xero_payment =====================
def move_vwz_xero_payment(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, payment_id, invoice_id, xero_payment_id, xero_payment_status, created_on, created_by, last_modified, last_modified_by from vwz_xero_payment")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        payment_id = source_row[2]
        invoice_id = source_row[3]
        xero_payment_id = source_row[4]
        xero_payment_status = source_row[5]
        created_on = source_row[6]
        created_by = source_row[7]
        last_modified = source_row[8]
        last_modified_by = source_row[9]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_payment(id, organization_id, payment_id, invoice_id, xero_payment_id, xero_payment_status, created_on, created_by, last_modified, last_modified_by) "
              "values(%(id)s,%(organization_id)s,%(payment_id)s,%(invoice_id)s,%(xero_payment_id)s,%(xero_payment_status)s,%(created_on)s,%(created_by)s,%(last_modified)s,%(last_modified_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'payment_id':payment_id,
                'invoice_id':invoice_id,
                'xero_payment_id':xero_payment_id,
                'xero_payment_status':xero_payment_status,
                'created_on':created_on,
                'created_by':created_by,
                'last_modified':last_modified,
                'last_modified_by':last_modified_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_payment =====================
## ================= begin move_vwz_xero_pull_track =====================
def move_vwz_xero_pull_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, xero_tenant_id, success, failure_reason, created_on, last_modified from vwz_xero_pull_track")
    for source_row in source_cursor:
        id = source_row[0]
        xero_tenant_id = source_row[1]
        success = source_row[2]
        failure_reason = source_row[3]
        created_on = source_row[4]
        last_modified = source_row[5]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_pull_track(id, xero_tenant_id, success, failure_reason, created_on, last_modified) "
              "values(%(id)s,%(xero_tenant_id)s,%(success)s,%(failure_reason)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'xero_tenant_id':xero_tenant_id,
                'success':success,
                'failure_reason':failure_reason,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_pull_track =====================
## ================= begin move_vwz_xero_sync_track =====================
def move_vwz_xero_sync_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, object_id, object_type, sync_type, xero_object_ids, track_status, tried_times, failure_reason, created_on, last_modified from vwz_xero_sync_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        object_id = source_row[2]
        object_type = source_row[3]
        sync_type = source_row[4]
        xero_object_ids = source_row[5]
        track_status = source_row[6]
        tried_times = source_row[7]
        failure_reason = source_row[8]
        created_on = source_row[9]
        last_modified = source_row[10]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_xero_sync_track(id, organization_id, object_id, object_type, sync_type, xero_object_ids, track_status, tried_times, failure_reason, created_on, last_modified) "
              "values(%(id)s,%(organization_id)s,%(object_id)s,%(object_type)s,%(sync_type)s,%(xero_object_ids)s,%(track_status)s,%(tried_times)s,%(failure_reason)s,%(created_on)s,%(last_modified)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'object_id':object_id,
                'object_type':object_type,
                'sync_type':sync_type,
                'xero_object_ids':xero_object_ids,
                'track_status':track_status,
                'tried_times':tried_times,
                'failure_reason':failure_reason,
                'created_on':created_on,
                'last_modified':last_modified
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_xero_sync_track =====================
## ================= begin move_vwz_zoom_sync_track =====================
def move_vwz_zoom_sync_track(source_conn, target_conn):
    start_datetime = datetime.now()
    source_cursor = source_conn.cursor(buffered=True)
    cntr = 0
    source_row = source_cursor.execute("select id, organization_id, object_type, object_id, attendee_id, sync_type, message, created_on, created_by from vwz_zoom_sync_track")
    for source_row in source_cursor:
        id = source_row[0]
        organization_id = source_row[1]
        object_type = source_row[2]
        object_id = source_row[3]
        attendee_id = source_row[4]
        sync_type = source_row[5]
        message = source_row[6]
        created_on = source_row[7]
        created_by = source_row[8]
        target_cursor = target_conn.cursor(buffered=True)
        query=("insert into stagearea.stg_tbl_zoom_sync_track(id, organization_id, object_type, object_id, attendee_id, sync_type, message, created_on, created_by) "
              "values(%(id)s,%(organization_id)s,%(object_type)s,%(object_id)s,%(attendee_id)s,%(sync_type)s,%(message)s,%(created_on)s,%(created_by)s)")
        data = {  
                'id':id,
                'organization_id':organization_id,
                'object_type':object_type,
                'object_id':object_id,
                'attendee_id':attendee_id,
                'sync_type':sync_type,
                'message':message,
                'created_on':created_on,
                'created_by':created_by
                }
        target_row = target_cursor.execute(query, data)
        if cntr >=10000 : 
              target_conn.commit()
              cntr = 0
              print("Commit")
        target_cursor.close()
    target_conn.commit()
    print (datetime.now() - start_datetime)
    source_cursor.close()
## ================= end move_vwz_zoom_sync_track =====================
