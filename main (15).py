import boto3
import os
import json
import time
import psutil
from datetime import datetime
from user_login import login_using_database,reset_password_email,token_check,password_reset,logout,impersonate_login_using_database,get_modules_back,get_service_account,create_service_account,process_service_account,get_auth_token
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
from common_utils.email_trigger import send_sns_email, get_memory_usage, memory_sns, insert_email_audit
from common_utils.authentication_check import validate_token
# Dictionary to store database configuration settings retrieved from environment variables.
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
logging = Logging(name="main")

# Initialize the SNS client
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

def lambda_handler(event, context):
    
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    performance_matrix={}
    # Record the start time of the function
    start_time = time.time()
    performance_matrix['start_time']=f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}.{int((start_time % 1) * 1000):03d}"
    logging.info(f"Request received at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}.{int((start_time % 1) * 1000):03d}")


    data = event.get('data')
    if data:
        try:
            data = data
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON in body'})
            }
    else:
        data = {}
    # Extract the HTTP method, path, and query string parameters from the event
    data=data.get('data')
    path = data.get('path')
    # Get the current time when the request is received
    request_received_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Route based on the path and method

    access_token=data.get('z_access_token','')
    if access_token and not validate_token(access_token):
        response = {"message": "AD INVALID TOKEN"}
        response.status_code = 401  # HTTP 401 Unauthorized
        return response

    if path == '/login_using_database':
        result = login_using_database(data)
    elif path == '/reset_password_email':
        result = reset_password_email(data)
    elif path == '/token_check':
        result = token_check(data)
        
    elif path == '/get_auth_token':
        result = get_auth_token(data)
    elif path == '/get_service_account':
        result = get_service_account(data)
    elif path == '/create_service_account':
        result = create_service_account(data)
    elif path == '/process_service_account':
        result = process_service_account(data)

    elif path == '/password_reset':
        result = password_reset(data)
    elif path == '/impersonate_login_using_database':
        result = impersonate_login_using_database(data)
    elif path == '/logout':
        result = logout(data)
    elif path == '/get_modules_back':
        result = get_modules_back(data)
    else:
        result = {'flag': False, 'error': 'Invalid path or method'}

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
                    "body": body,
                    "action": "Email triggered",
                    "parents_module_name":parents_module_name,
                "sub_module_name":sub_module_name,
                "child_module_name":child_module_name
                }
            common_utils_database.update_audit(email_audit_data, 'email_audit')
        
    else:
        status_code = 200

    # Record the end time of the function
    end_time = time.time()
    performance_matrix['end_time']=f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
    performance_matrix['execution_time']=F"{end_time - start_time:.4f}"
    logging.info(f"Request processed at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}")
    logging.info(f"Function performance_matrix: {performance_matrix} seconds")
    
    memory_limit = int(context.memory_limit_in_mb)
    memory_used = int(get_memory_usage())+100
    final_memory_used = get_memory_usage()
    logging.info(f"$$$$$$$$$$$$$$$$$$$$$$$Final Memory Used: {final_memory_used:.2f} MB")
    memory_sns(memory_limit,memory_used,context)

    return {
        'statusCode': status_code,
        'body': json.dumps(result),
        'performance_matrix':  json.dumps(performance_matrix)
    }

 