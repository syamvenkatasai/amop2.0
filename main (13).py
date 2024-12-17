"""
main.py

Module for handling API requests and routing them to the appropriate function.

Functions:
- lambda_handler(event,context=None)

Author: Nikhil N, Phaneendra Y
Date: July 22, 2024
"""
import json
import time
import os
import boto3 
import pytz
from datetime import datetime  # Ensure datetime is imported
from common_utils.email_trigger import send_sns_email, get_memory_usage, memory_sns, insert_email_audit
from sim_management import (
    run_db_script,
    get_device_history,
    get_status_history,
    update_inventory_data,
    inventory_dropdowns_data,
    sim_order_form_mail_trigger,
    update_sim_management_modules_data,
    get_bulk_change_history,
    optimization_dropdown_data,
    customer_pool_row_data,
    get_new_bulk_change_data,
    update_bulk_change_data,
    download_bulk_upload_template,
    bulk_import_data,
    get_bulk_change_logs,
    customers_dropdown_inventory,
    get_rev_assurance_data,
    add_service_line_dropdown_data,
    submit_service_line_dropdown_data,
    add_service_product_dropdown_data,
    submit_add_service_product_dropdown_data,
    assign_service_dropdown_data,
    deactivate_service_product,
    get_optimization_data,
    get_customer_charges_data,
    export_optimization_data_zip,
    download_row_data_optimization,
    optimization_row_info_data,
    export_row_data_customer_charges,
    customer_charges_template,
    customers_sessions_customer_charges_export_dropdown_data,
    export_customer_charges,
    manage_deactivated_sims,
    upload_carrier_rate_plan,
    start_optimization,
    get_optimization_pop_up_data,
    upload_customer_charges_data,
    upload_customer_charges_optimization_list_view,
    get_usage_data,
    get_automation_rule_data,
    get_automation_rule_create_pop_up_data,
    insert_automation_data,
    get_device_status_card,getm2m_high_usage_chart_data,
    get_usage_details_card,
    mobility_high_usage_chart,
    mobility_usage_per_customer_pool,
    mobility_usage_per_group_pool,
    bulk_upload_download_template,
    get_rate_plan_data
    ,get_automation_rule_details_data,
    import_bulk_data,
    get_inventory_data,
    update_features_pop_up_data,statuses_inventory)
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
from common_utils.authentication_check import validate_token
# Dictionary to store database configuration settings retrieved from environment variables.
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
    function_name = context.function_name if context else 'sim_management'
    logging.info("Lambda function started: %s", function_name)

    # Record the start time of the function
    performance_matrix={}
    start_time = time.time()
    performance_matrix['start_time'] = (
    f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}."
    f"{int((start_time % 1) * 1000):03d}"
    )
       
    # Extract the HTTP method, path and query string parameters from the event
    data = event.get('data')
    if data:
        try:
            data =data
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON in body'})
            }
    else:
        data = {}
    
    data=data.get('data')
    path = data.get('path')

    access_token=data.get('z_access_token','')
    if access_token and not validate_token(access_token):
        response = {"message": "AD INVALID TOKEN"}
        response.status_code = 401  # HTTP 401 Unauthorized
        return response
    
    # Capture the hit time when the request is received
    # Capture the hit time when the request is received (same as before)
    hit_time = time.time()
    hit_time_formatted = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Hit Time: {hit_time_formatted}, Request Received at: {hit_time_formatted}")
    
        
    request_received_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logging.info("Routing request for path: %s", path)
    # Route based on the path and method
    if path == '/get_bulk_change_logs':
        result = get_bulk_change_logs(data)
    elif path == '/run_db_script':
        result = run_db_script(data)
    elif path == '/get_rev_assurance_data':
        result = get_rev_assurance_data(data)
    elif path == '/update_inventory_info':
        result = get_device_history(data)
    elif path == '/get_status_history':
        result = get_status_history(data)
    elif path == '/update_inventory_data':
        result = update_inventory_data(data)
    elif path == '/inventory_dropdowns_data':
        result = inventory_dropdowns_data(data)
    elif path == '/sim_order_form_mail_trigger':
        result = sim_order_form_mail_trigger(data)
    elif path == '/update_sim_management_modules_data':
        result = update_sim_management_modules_data(data)
    elif path == '/get_bulk_change_history':
        result = get_bulk_change_history(data)
    elif path == '/optimization_dropdown_data':
        result = optimization_dropdown_data(data)
    elif path == '/customer_pool_row_data':
        result = customer_pool_row_data(data)
    elif path == '/get_new_bulk_change_data':
        result = get_new_bulk_change_data(data)
    elif path == '/update_bulk_change_data':
        result = update_bulk_change_data(data)
    elif path == '/download_bulk_upload_template':
        result = download_bulk_upload_template(data)
    elif path == '/bulk_import_data':
        result = bulk_import_data(data)
    elif path == '/customers_dropdown_inventory':
        result = customers_dropdown_inventory(data)
    elif path == '/add_service_line_dropdown_data':
        result = add_service_line_dropdown_data(data)
    elif path == '/submit_service_line_dropdown_data':
        result = submit_service_line_dropdown_data(data)
    elif path == '/add_service_product_dropdown_data':
        result = add_service_product_dropdown_data(data)
    elif path == '/submit_add_service_product_dropdown_data':
        result = submit_add_service_product_dropdown_data(data)
    elif path == '/assign_service_dropdown_data':
        result = assign_service_dropdown_data(data)
    elif path == '/deactivate_service_product':
        result = deactivate_service_product(data)
    elif path == '/get_optimization_data':
        result = get_optimization_data(data)
    elif path == '/get_customer_charges_data':
        result = get_customer_charges_data(data)
    elif path == '/export_optimization_data_zip':
        result = export_optimization_data_zip(data)
    elif path == '/download_row_data_optimization':
        result = download_row_data_optimization(data)
    elif path == '/optimization_row_info_data':
        result = optimization_row_info_data(data)
    elif path == '/export_row_data_customer_charges':
        result = export_row_data_customer_charges(data)
    elif path == '/customer_charges_template':
        result = customer_charges_template(data)
    elif path == '/customers_sessions_customer_charges_export_dropdown_data':
        result = customers_sessions_customer_charges_export_dropdown_data(data)
    elif path == '/export_customer_charges':
        result = export_customer_charges(data)
    elif path == '/upload_carrier_rate_plan':
        result = upload_carrier_rate_plan(data)
    elif path == '/start_optimization':
        result = start_optimization(data)
    elif path == '/get_optimization_pop_up_data':
        result = get_optimization_pop_up_data(data)
    elif path == '/manage_deactivated_sims':
        result = manage_deactivated_sims()
    elif path == '/upload_customer_charges_data':
        result = upload_customer_charges_data(data)
    elif path == '/upload_customer_charges_optimization_list_view':
        result = upload_customer_charges_optimization_list_view(data)
    elif path == '/get_usage_data':
        result = get_usage_data(data)
    elif path == '/get_automation_rule_data':
        result = get_automation_rule_data(data)
    elif path == '/get_automation_rule_create_pop_up_data':
        result = get_automation_rule_create_pop_up_data(data)
    elif path == '/insert_automation_data':
        result = insert_automation_data(data)
    elif path == '/get_device_status_card':
        result = get_device_status_card(data)
    elif path == '/getm2m_high_usage_chart_data':
        result = getm2m_high_usage_chart_data(data)
    elif path == '/get_usage_details_card':
        result = get_usage_details_card(data)
    elif path == '/get_rate_plan_data':
        result = get_rate_plan_data(data)
    elif path == '/mobility_high_usage_chart':
        result = mobility_high_usage_chart(data)
    elif path == '/mobility_usage_per_customer_pool':
        result = mobility_usage_per_customer_pool(data)
    elif path == '/mobility_usage_per_group_pool':
        result = mobility_usage_per_group_pool(data)
    elif path == '/get_automation_rule_details_data':
        result = get_automation_rule_details_data(data)
    elif path == '/import_bulk_data':
        result = import_bulk_data(data)
    elif path == '/get_inventory_data':
        result = get_inventory_data(data)
    elif path == '/bulk_upload_download_template':
        result = bulk_upload_download_template(data)
    elif path == '/update_features_pop_up_data':
        result = update_features_pop_up_data(data)
    elif path == '/statuses_inventory':
        result = statuses_inventory(data)
    else:
        result = {'error': 'Invalid path or method'}
        logging.warning("Invalid path or method requested: %s", path)
        
    # database = DB('AmopAlgouatDB', **db_config)
    tenant_database = data.get('db_name', 'altaworx_central')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
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

    # Record the end time of the function
    end_time = time.time()
    performance_matrix['end_time']=f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
    performance_matrix['execution_time']=F"{end_time - start_time:.4f}"
    logging.info(f"Request processed at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}")
    
    performance_matrix['execution_time'] = f"{end_time - start_time:.4f} seconds"
    
    logging.info(f"Function performance_matrix: {performance_matrix} seconds")
    logging.info("Lambda function execution completed in %.4f seconds", end_time - start_time)

    memory_limit = int(context.memory_limit_in_mb)
    memory_used = int(get_memory_usage())+100
    final_memory_used = get_memory_usage()
    logging.info(f"$$$$$$$$$$$$$$$$$$$$$$$Final Memory Used: {final_memory_used:.2f} MB")
    memory_sns(memory_limit,memory_used,context)

    return {
        'statusCode': status_code,
        'body': json.dumps(result),
        'performance_matrix':  json.dumps(performance_matrix),"started":hit_time_formatted,"time_taken":time_taken,"request_completed_time_formatted":request_completed_time_formatted
    }