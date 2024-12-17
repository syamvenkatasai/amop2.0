"""
main.py

Module for handling API requests and routing them to the appropriate function.

Functions:
- lambda_handler(event,context=None)

Author: Nikhil N, Phaneendra Y
Date: July 22, 2024
"""
import boto3
import json
import time
import os
from datetime import datetime
import pytz
from common_utils.db_utils import DB
from common_utils.email_trigger import send_sns_email, get_memory_usage, memory_sns, insert_email_audit
from notification_services import (
    total_emails_count,
    failed_emails_count,
    successful_emails_count,
    email_templates_count,
    email_status_pie_chart,
    email_triggers_by_day,
    emails_per_trigger_type_weekly,
    no_of_error_emails_weekly,
    email_list,
    get_email_details,
    email_template_list_view,
    submit_update_copy_status_email_template,
    send_report_emails,
    killbill_mail_trigger
)


from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
from common_utils.authentication_check import validate_token
logging = Logging(name="main")

# db_config = {
#     'host': "amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#     'port': "5432",
#     'user':"root",
#     'password':"AmopTeam123"
# }
db_config = {
    'host': os.environ['HOST'],
    'port': os.environ['PORT'],
    'user': os.environ['USER'],
    'password': os.environ['PASSWORD']
}

# Initialize the SNS client
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')


def lambda_handler(event, context):
    """
    Handles incoming API requests and routes them to the appropriate function.

    Args:
        event (dict): The incoming API request event.

    Returns:
        dict: A dictionary containing the response status code and body.

    Example:
        >>> event = {'data': {'path': '/get_modules'}}
        >>> lambda_handler(event)
        {'statusCode': 200, 'body': '{"flag": True, "modules": [...]}'}
    """
    
    # Set the timezone to India Standard Time
    india_timezone = pytz.timezone('Asia/Kolkata')
    
    function_name = context.function_name if context else 'notification_services'
        
    
    performance_matrix={}
    # Record the start time of the function
    start_time = time.time()
    utc_time_start_time = datetime.utcfromtimestamp(start_time)
    start_time_ = utc_time_start_time.replace(tzinfo=pytz.utc).astimezone(india_timezone)
    performance_matrix['start_time']=f"{start_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((start_time % 1) * 1000):03d}"
    logging.info(f"Request received at {start_time}")
    

    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

    # Extract the HTTP method, path, and query string parameters from the event
    data = event.get('data')
    if not data:
        data = {}

    data = data.get('data', {})
    path = data.get('path', '')
    user = data.get('username') or data.get('user_name') or data.get('user') or 'superadmin'

    access_token=data.get('z_access_token','')
    if access_token and not validate_token(access_token):
        response = {"message": "AD INVALID TOKEN"}
        response.status_code = 401  # HTTP 401 Unauthorized
        return response
    
    # Route based on the path and method
    if path == '/total_emails_count':
        result = total_emails_count(data)
    elif path=='/failed_emails_count':
        result=failed_emails_count(data)
    elif path=='/successful_emails_count':
        result=successful_emails_count(data)
    elif path=='/email_templates_count':
        result=email_templates_count(data)
    elif path=='/email_status_pie_chart':
        result=email_status_pie_chart(data)
    elif path=='/email_triggers_by_day':
        result=email_triggers_by_day(data)
    elif path=='/emails_per_trigger_type_weekly':
        result=emails_per_trigger_type_weekly(data)
    elif path=='/no_of_error_emails_weekly':
        result=no_of_error_emails_weekly(data)
    elif path=='/email_list':
        result=email_list(data)
    elif path=='/get_email_details':
        result=get_email_details(data)
    elif path=='/email_template_list_view':
        result=email_template_list_view(data)
    elif path=='/submit_update_copy_status_email_template':
        result=submit_update_copy_status_email_template(data)
    elif path=='/send_report_emails':
        result=send_report_emails()
    elif path=='/killbill_mail_trigger':
        result=killbill_mail_trigger()
    else:
        result = {'flag': False, 'error': 'Invalid path or method'}
        
    database = DB('altaworx_central', **db_config)

    if result.get('flag') == False:
        status_code = 400  # You can change this to an appropriate error code
        # Sending email
        result_response = send_email('Exception Mail')
        if isinstance(result, dict) and result.get("flag") is False:
            logging.info(result)
        else:
            to_emails, cc_emails, subject, body, from_email, partner_name = result_response
            common_utils_database.update_dict("email_templates", {"last_email_triggered_at": request_received_at}, {"template_name": 'Exception Mail'})
            query = """
                SELECT parents_module_name, sub_module_name, child_module_name, partner_name
                FROM email_templates
                WHERE template_name = 'Exception Mail'
            """

            # Execute the query and fetch the result
            email_template_data = common_utils_database.execute_query(query, True)
            if not email_template_data.empty:
                # Unpack the results
                parents_module_name, sub_module_name, child_module_name, partner_name = email_template_data.iloc[0]
            else:
                # If no data is found, assign default values or log an error
                parents_module_name = ""
                sub_module_name = ""
                child_module_name = ""
                partner_name = ""

            # Email audit logging
            error_message = result.get('error', 'Unknown error occurred')  # Extracting the error message
            email_audit_data = {
                    "template_name": 'Exception Mail',
                    "email_type": 'Application',
                    "partner_name": partner_name,
                    "email_status": 'success',
                    "from_email": from_email,
                    "to_email": to_emails,
                    "cc_email": cc_emails,
                    "comments": f"{path} - Error: {error_message}",  # Adding error message to comments
                    "subject": subject,
                    "action": "Email triggered",
                    "body": body,
                    "parents_module_name":parents_module_name,
                "sub_module_name":sub_module_name,
                "child_module_name":child_module_name
                }
            common_utils_database.update_audit(email_audit_data, 'email_audit')
        
    else:
        status_code = 200
    # database = DB('AmopAlgouatDB', **db_config)
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    performance_feature=common_utils_database.get_data("users",{"username":user},['performance_feature'])['performance_feature'].to_list()[0]

    # Record the end time of the function
    end_time = time.time()
    utc_time_end_time = datetime.utcfromtimestamp(end_time)
    end_time_ = utc_time_end_time.replace(tzinfo=pytz.utc).astimezone(india_timezone)
    performance_matrix['end_time']=f"{end_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((end_time % 1) * 1000):03d}"

    performance_matrix['execution_time']=F"{end_time - start_time:.4f}"

    memory_limit = int(context.memory_limit_in_mb)
    memory_used = int(get_memory_usage())+100
    final_memory_used = get_memory_usage()
    logging.info(f"$$$$$$$$$$$$$$$$$$$$$$$Final Memory Used: {final_memory_used:.2f} MB")
    memory_sns(memory_limit,memory_used,context)

    return {
        'statusCode': status_code,
        'body': json.dumps(result),
        'performance_matrix':  json.dumps(performance_matrix),'performance_matrix_feature':performance_feature
    } 
    