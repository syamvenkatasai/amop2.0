"""
main.py
Module for handling API requests and routing them to the appropriate function.
Functions:
- lambda_handler(event,context=None)
Author: Vyshnavi
Date: Oct 7th, 2024
"""

import boto3
import json
import time
from datetime import datetime
import pytz
from migration_main import (
    MigrationScheduler

    )
##database configuration
# db_config = {
#     'host': "amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#     'port': "5432",
#     'user': "root",
#     'password': "AlgoTeam123"}

# Initialize the SNS client
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

def send_sns_email(subject, message):
    """Send an email via SNS when memory or CPU limits are breached."""
    response = sns.publish(
        TopicArn='arn:aws:sns:us-east-1:008971638399:custom-alert', 
        Message=message,
        Subject=subject)
    print("SNS publish response:", response)
    return response

def get_lambda_metrics(metric_name, function_name):
    """Fetch specific Lambda CloudWatch metrics (invocations or throttles)."""
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/Lambda',
        MetricName=metric_name,
        Dimensions=[{'Name': 'FunctionName', 'Value': function_name}],
        StartTime=time.time() - 300,  # Last 5 minutes
        EndTime=time.time(),
        Period=300,  # 5-minute period
        Statistics=['Sum']
    )
    return response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0
    
def get_api_gateway_metrics(metric_name, api_id):
    """Fetch specific API Gateway CloudWatch metrics."""
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/ApiGateway',
        MetricName=metric_name,
        Dimensions=[{'Name': 'ApiId', 'Value': api_id}],
        StartTime=time.time() - 300,  # Last 5 minutes
        EndTime=time.time(),
        Period=300,  # 5-minute period
        Statistics=['Sum']
    )
    return response['Datapoints'][0]['Sum'] if response['Datapoints'] else 0
    
def get_memory_usage():
    """Get the current memory usage in MB."""
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss / (1024 * 1024)
    
# Check if memory usage exceeds 80% of the limit
def memory_sns(memory_limit,memory_used,context):
    if memory_used > 0.8 * memory_limit:
        subject = 'Lambda Memory Usage Alert'
        message = f"Warning: Memory usage has exceeded allocated limit.\n\nDetails:\nMemory Used: {memory_used} MB\nMemory Limit: {memory_limit} MB\nFunction: {context.function_name}\nRequest ID: {context.aws_request_id}"
        send_sns_email(subject, message)
        request_received_at=datetime.now()
        insert_email_audit( subject,message, request_received_at)
        print("###mail sent")



def insert_email_audit( subject,message, request_received_at):
    """Insert email audit data into the database."""
    try:
        # Connect to the database
        db = DB(database="common_utils", 
                host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", 
                user="root", 
                password="AlgoTeam123", 
                port="5432")

        # Email audit data
        email_audit_data = {
            "template_name": "lambda ram exceeded",
            "email_type": "AWS",
            "partner_name": "Altaworx",
            "email_status": 'success',
            "from_email": "sns.amazon.com",
            "to_email":"AWS sns mails",
            #"cc_email": cc_emails,
            "comments": 'Lambda memory error',
            "subject": subject,
            #"body": body,
            "role": "admin"
            #"action_performed": action_performed
        }
        db.update_audit(email_audit_data, 'email_audit')

        # Auditing data
        email_auditing_data = {
            "service_name": 'Module_management',
            "created_date": datetime.now(),  # Use current timestamp
            "created_by": "AWS",
            "status": str(True),  # Adjust based on your logic
            #"time_consumed_secs": time_consumed,
            #"session_id": session_id,
            "tenant_name": "",  # Add tenant name if applicable
            #"comments": message,
            "module_name": "user_login",
            #"request_received_at": request_received_at
        }
        db.update_audit(email_auditing_data, 'email_auditing')

    except Exception as e:
        print(f"Error inserting email audit data: {e}")
        
def check_throttling_and_alert(function_name, performance_matrix):
    """
    Check for Lambda throttling and send an alert if throttling occurs.
    Additionally, record the throttling information in the performance matrix.
    """
    throttles = get_lambda_metrics('Throttles', function_name)
    print(f"Throttles: {throttles}")
    
    # Log throttling data in performance matrix
    performance_matrix['throttles'] = throttles
    
    # Alert if throttling occurs
    if throttles > 0:
        subject = f"Lambda Throttling Alert for {function_name}"
        message = (f"Warning: Lambda throttling detected.\n\nDetails:\n"
                   f"Throttles: {throttles}\nFunction: {function_name}")
        send_sns_email(subject, message, context)
        request_received_at = datetime.now()
        insert_email_audit(subject, message, request_received_at, context)
        print("###mail sent")
        print(f"Throttling alert sent for {throttles} throttling events.")

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
    
    function_name = context.function_name if context else 'user_authentication'

    # Monitor API Gateway errors (4XX and 5XX)
    api_id = 'v1djztyfcg'  # Replace with your actual API Gateway ID
    client_errors = get_api_gateway_metrics('4XXError', api_id)
    server_errors = get_api_gateway_metrics('5XXError', api_id)

    if client_errors > 0:
        subject = 'API Gateway Client Error Alert'
        message = f"Warning: API Gateway encountered client errors (4XX).\n\nDetails:\nClient Errors: {client_errors}\nAPI: {api_id}"
        send_sns_email(subject, message, context)
        request_received_at = datetime.now()
        insert_email_audit(subject, message, request_received_at, context)
        print("###mail sent")

    if server_errors > 0:
        subject = 'API Gateway Server Error Alert'
        message = f"Warning: API Gateway encountered server errors (5XX).\n\nDetails:\nServer Errors: {server_errors}\nAPI: {api_id}"
        send_sns_email(subject, message, context)
        request_received_at = datetime.now()
        insert_email_audit(subject, message, request_received_at, context)
        print("###mail sent")
    
    # Set the timezone to India Standard Time
    india_timezone = pytz.timezone('Asia/Kolkata')
    performance_matrix={}
    # Record the start time of the function
    start_time = time.time()
    utc_time_start_time = datetime.utcfromtimestamp(start_time)
    start_time_ = utc_time_start_time.replace(tzinfo=pytz.utc).astimezone(india_timezone)
    performance_matrix['start_time']=f"{start_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((start_time % 1) * 1000):03d}"
    print(f"Request received at {start_time}")
    
    check_throttling_and_alert(function_name, performance_matrix)

    # Extract the HTTP method, path, and query string parameters from the event
    data = event.get('data')
    if not data:
        data = {}

    data = data.get('data', {})
    path = data.get('path', '')
    user = data.get('username') or data.get('user_name') or data.get('user') or 'superadmin'
    # Route based on the path and method
    if path == '/migration_main':
        scheduler=MigrationScheduler()
        result = scheduler.main()
    # elif path == '/get_module_data':
    #     result = get_module_data(data)
    # elif path == '/get_partner_info':
    #     result = get_partner_info(data)
    # elif path == '/get_superadmin_info':
    #     result = get_superadmin_info(data)
    # elif path == '/update_superadmin_data':
    #     result = update_superadmin_data(data)
    # elif path == '/update_people_data':
    #     result = update_people_data(data)
    # elif path == '/update_partner_info':
    #     result = update_partner_info(data)
    # elif path == '/export':
    #     result = export(data)
    # elif path == '/get_user_module_map':
    #     result = get_user_module_map(data)
    # elif path == '/inventory_dropdowns_data':
    #     result = inventory_dropdowns_data(data)
    # elif path == '/update_inventory_data':
    #     result = update_inventory_data(data)
    # elif path == '/get_status_history':
    #     result = get_status_history(data)
    # elif path == '/customers_dropdown_data':
    #     result = customers_dropdown_data(data)
    # elif path == '/user_data':
    #     result = user_data(data)
    # elif path == '/people_revio_customers_list_view':
    #     result = people_revio_customers_list_view(data)
    # elif path == '/reports_data':
    #     result = reports_data(data)
    else:
        result = {'flag': False, 'error': 'Invalid path or method'}

    status_code = 400 if result.get('flag') is False else 200
    # database = DB('AmopAlgouatDB', **db_config)
    # performance_feature=database.get_data("users",{"username":user},['performance_feature'])['performance_feature'].to_list()[0]
    # Record the end time of the function
    end_time = time.time()
    utc_time_end_time = datetime.utcfromtimestamp(end_time)
    end_time_ = utc_time_end_time.replace(tzinfo=pytz.utc).astimezone(india_timezone)
    performance_matrix['end_time']=f"{end_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((end_time % 1) * 1000):03d}"

    performance_matrix['execution_time']=F"{end_time - start_time:.4f}"

    return {
        'statusCode': status_code,
        'body': json.dumps(result),
        'performance_matrix':  json.dumps(performance_matrix)
        # 'performance_matrix_feature':performance_feature
    }
