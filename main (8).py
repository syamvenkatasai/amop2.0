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
from datetime import datetime
import pytz
from common_utils.db_utils import DB
import os
import pytz
from common_utils.email_trigger import send_sns_email, get_memory_usage, memory_sns, insert_email_audit
from datetime import datetime  # Ensure datetime is imported
from module_api import (
    get_modules,
    get_module_data,
    get_partner_info,
    get_superadmin_info,
    update_superadmin_data,
    update_people_data,
    export,
    update_partner_info,
    get_user_module_map,
    get_status_history,
    customers_dropdown_data,
    people_revio_customers_list_view,
    add_people_revcustomer_dropdown_data,
    download_people_bulk_upload_template,
    submit_update_info_people_revcustomer,
    reports_data_with_date_filter,
    get_modules_back,
    carrier_rate_plan_list_view,
    user_data,reports_data,
    rate_plan_dropdown_data,rate_plan_dropdown_data_optimization_groups)

from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
from common_utils.authentication_check import validate_token
logging = Logging(name="main")
##database configuration
# db_config = {
#     'host': "amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#     'port': "5432",
#     'user': "root",
#     'password': "AmopTeam123"}

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
    hit_time = time.time()
    hit_time_formatted = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Hit Time: {hit_time_formatted}, Request Received at: {hit_time_formatted}")

    # Set the timezone to India Standard Time
    india_timezone = pytz.timezone('Asia/Kolkata')
    performance_matrix={}
    # Record the start time of the function
    start_time = time.time()
    utc_time_start_time = datetime.utcfromtimestamp(start_time)
    start_time_ = utc_time_start_time.replace(tzinfo=pytz.utc).astimezone(india_timezone)
    performance_matrix['start_time']=f"{start_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((start_time % 1) * 1000):03d}"
    logging.info(f"Request received at {start_time}")



    function_name = context.function_name if context else 'user_authentication'
    logging.info("Lambda function started: %s", function_name)

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
    if path == '/get_modules':
        result = get_modules(data)
    elif path == '/get_module_data':
        result = get_module_data(data)
    elif path == '/get_partner_info':
        result = get_partner_info(data)
    elif path == '/get_superadmin_info':
        result = get_superadmin_info(data)
    elif path == '/update_superadmin_data':
        result = update_superadmin_data(data)
    elif path == '/update_people_data':
        result = update_people_data(data)
    elif path == '/update_partner_info':
        result = update_partner_info(data)
    elif path == '/export':
        result = export(data)
    elif path == '/get_user_module_map':
        result = get_user_module_map(data)
    elif path == '/get_status_history':
        result = get_status_history(data)
    elif path == '/customers_dropdown_data':
        result = customers_dropdown_data(data)
    elif path == '/user_data':
        result = user_data(data)
    elif path == '/people_revio_customers_list_view':
        result = people_revio_customers_list_view(data)
    elif path == '/download_people_bulk_upload_template':
        result = download_people_bulk_upload_template(data)
    elif path == '/add_people_revcustomer_dropdown_data':
        result = add_people_revcustomer_dropdown_data(data)
    elif path == '/people_bulk_import_data':
        result = people_bulk_import_data(data)
    elif path == '/submit_update_info_people_revcustomer':
        result = submit_update_info_people_revcustomer(data)
    elif path == '/reports_data_with_date_filter':
        result = reports_data_with_date_filter(data)
    elif path == '/reports_data':
        result = reports_data(data)
    elif path == '/get_modules_back':
        result = get_modules_back(data)
    elif path == '/carrier_rate_plan_list_view':
        result = carrier_rate_plan_list_view(data)
    elif path == '/rate_plan_dropdown_data':
        result = rate_plan_dropdown_data(data)
    elif path == '/rate_plan_dropdown_data_optimization_groups':
        result = rate_plan_dropdown_data_optimization_groups(data)
    else:
        result = {'flag': False, 'error': 'Invalid path or method'}
        logging.warning("Invalid path or method requested: %s", path)

    if result.get('flag') == False:
        status_code = 400  # You can change this to an appropriate error code
        logging.error("Error in result: %s", result)
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
                    "body": body,
                    "action": "Email triggered",
                    "parents_module_name":parents_module_name,
                "sub_module_name":sub_module_name,
                "child_module_name":child_module_name
                }
            common_utils_database.update_audit(email_audit_data, 'email_audit')
        
    else:
        status_code = 200
    # Capture the request completion time in IST
    request_completed_time = time.time()
    request_completed_time_formatted = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    
    # Calculate the time difference between hit time and request completed time
    time_taken = round(request_completed_time - hit_time, 4)  # Round to 4 decimal places
    logging.info(f"Request Completed: {request_completed_time_formatted}, Time Taken: {time_taken} seconds")

    #database = DB('altaworx_central', **db_config)
    performance_feature=True
    # Record the end time of the function
    end_time = time.time()
    utc_time_end_time = datetime.utcfromtimestamp(end_time)
    end_time_ = utc_time_end_time.replace(tzinfo=pytz.utc).astimezone(india_timezone)
    performance_matrix['end_time']=f"{end_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((end_time % 1) * 1000):03d}"

    performance_matrix['execution_time']=F"{end_time - start_time:.4f}"
    logging.info("Lambda function execution completed in %.4f seconds", end_time - start_time)

    memory_limit = int(context.memory_limit_in_mb)
    memory_used = int(get_memory_usage())+100
    final_memory_used = get_memory_usage()
    logging.info(f"$$$$$$$$$$$$$$$$$$$$$$$Final Memory Used: {final_memory_used:.2f} MB")
    memory_sns(memory_limit,memory_used,context)

    return {
        'statusCode': status_code,
        'body': json.dumps(result),
        'performance_matrix':  json.dumps(performance_matrix),
        'performance_matrix_feature':performance_feature,"started":hit_time_formatted,"time_taken":time_taken,"request_completed_time_formatted":request_completed_time_formatted
    }
