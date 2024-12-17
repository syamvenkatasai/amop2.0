"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
# Importing the necessary Libraries
import ast
import boto3
import re
import requests
import os
import pandas as pd
from common_utils.email_trigger import send_email
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
from common_utils.permission_manager import PermissionManager
import datetime
from datetime import time
from datetime import datetime, timedelta
import time
from io import BytesIO
import asyncio
from common_utils.module_utils import get_module_data, get_headers_mapping
from common_utils.data_transfer_main import DataTransfer
import json
from io import BytesIO
import base64
import zipfile
import io
import copy
import openpyxl
import pytds
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content, Attachment, FileContent, FileName, FileType, Disposition
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import base64
import re
from pytz import timezone
import concurrent.futures


db_config = {
    'host': os.environ['HOST'],
    'port': os.environ['PORT'],
    'user': os.environ['USER'],
    'password': os.environ['PASSWORD']
}
logging = Logging(name="charges_history")

def get_headers_mapping(tenant_database,module_list,role,user,tenant_id,sub_parent_module,parent_module,data,common_utils_database):
    '''
    Description: The  function retrieves and organizes field mappings,headers,and module features 
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    '''
    #logging.info("Starting to retrieve headers mapping for modules: %s", module_list)
    ##Database connection
    #database = DB(tenant_database, **db_config)
    #common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    feature_module_name=data.get('feature_module_name','')
    user_name = data.get('username') or data.get('user_name') or data.get('user')
    tenant_name = data.get('tenant_name') or data.get('tenant') 
    try:
        tenant_id=common_utils_database.get_data('tenant',{"tenant_name":tenant_name}['id'])['id'].to_list()[0]
    except Exception as e:
        logging.exception(f"Getting exception at fetching tenant id {e}")
    ret_out={}
    # Iterate over each module name in the provided module list
    for module_name in module_list:
        #logging.debug("Processing module: %s", module_name)
        out=common_utils_database.get_data(
            "field_column_mapping",{"module_name":module_name}
            ).to_dict(orient="records")
        pop_up=[]
        general_fields=[]
        table_fileds={}
        # Categorize the fetched data based on field types
        for data in out:
            if data["pop_col"]:
                pop_up.append(data)
            elif data["table_col"]:
                table_fileds.update({
                data["db_column_name"]:[data["display_name"],data["table_header_order"]]})
            else:
                general_fields.append(data)    
        # Create a dictionary to store categorized fields
        headers={}
        headers['general_fields']=general_fields
        headers['pop_up']=pop_up 
        headers['header_map']=table_fileds 
        try:
            final_features=[]
            
            # Fetch all features for the 'super admin' role
            if role.lower()== 'super admin':
                all_features=common_utils_database.get_data(
                    "module_features",{"module":feature_module_name},['features']
                    )['features'].to_list()
                if all_features:
                    final_features=json.loads(all_features[0])
            else:
                final_features = get_features_by_feature_name(user_name, tenant_id, feature_module_name,common_utils_database)
                logging.info("Fetched features for user '%s': %s", user_name, final_features)

        except Exception as e:
            logging.info(f"there is some error {e}")
            pass
        # Add the final features to the headers dictionary
        headers['module_features']=final_features
        ret_out[module_name]=headers
    #logging.info("Completed headers mapping retrieval.")   
    return ret_out

def get_features_by_feature_name(user_name, tenant_id, feature_name,common_utils_database):
    # Fetch user features from the database
    user_features_raw = common_utils_database.get_data(
        "user_module_tenant_mapping", 
        {"user_name": user_name, "tenant_id": tenant_id},
        ['module_features']
    )['module_features'].to_list()
    logging.debug("Raw user features fetched: %s", user_features_raw)

    # Parse the JSON string to a dictionary
    user_features = json.loads(user_features_raw[0])  # Assuming it's a list with one JSON string

    # Initialize a list to hold features for the specified feature name
    features_list = []

    # Loop through all modules to find the specified feature name
    for module, features in user_features.items():
        if feature_name in features:
            features_list.extend(features[feature_name])
    logging.info("Retrieved features: %s", features_list)

    return features_list




def get_charge_history_data(data):
    '''
    This function retrieves report data by executing a query based 
    on the provided module name and parameters,
    converting the results into a dictionary format. 
    It logs the audit and error details to the database 
    and returns the report data along with a success flag.
    '''
    # Start time and date calculation
    start_time = time.time()
    module_name = data.get('module_name', '')
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', None)
    Partner = data.get('Partner', '')
    mod_pages = data.get('mod_pages', {})
    role_name = data.get('role_name', '')
    limit = mod_pages.get('end', 100)
    offset = mod_pages.get('start', 0)
    end = mod_pages.get('end', 100)
    start = mod_pages.get('start', 0)
    table = data.get('table_name', 'vw_optimization_smi_result_customer_charge_queue')
    # Database connection
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    common_utils_database = DB('common_utils', **db_config)
    return_json_data = {}
    pages = {
    'start': start,
    'end': end
        }

    tenant_timezone = None

    try:
        # Prepare function for parallel execution
        def execute_count_query():
            count_params = [table]
            count_query = f"SELECT COUNT(*) FROM {table}"                
            # count_start_time = time.time()
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            # count_duration = time.time() - count_start_time
            # logging.info(f"Count query execution time: {count_duration:.4f} seconds")
            return int(count_result)

        def execute_main_query():
            module_query_df = common_utils_database.get_data("module_view_queries", {"module_name": module_name})
            if module_query_df.empty:
                raise ValueError(f'No query found for module name: {module_name}')
            query = module_query_df.iloc[0]['module_query']
            if not query:
                raise ValueError(f'Unknown module name: {module_name}')
            params = [offset, limit]
            # main_query_start_time = time.time()
            df = database.execute_query(query, params=params)
            # main_query_duration = time.time() - main_query_start_time
            # logging.info(f"Main query execution time: {main_query_duration:.4f} seconds")
            return df

        def fetch_tenant_timezone():
            tenant_name = data.get('tenant_name', '')
            tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
            # tenant_timezone_start_time = time.time()
            tenant_timezone = common_utils_database.execute_query(tenant_timezone_query, params=[tenant_name])
            # tenant_timezone_duration = time.time() - tenant_timezone_start_time
            # logging.info(f"Tenant timezone query execution time: {tenant_timezone_duration:.4f} seconds")
            if tenant_timezone.empty or tenant_timezone.iloc[0]['time_zone'] is None:
                raise ValueError("No valid timezone found for tenant.")
            tenant_time_zone = tenant_timezone.iloc[0]['time_zone']
            match = re.search(r'\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)', tenant_time_zone)
            if match:
                tenant_time_zone = match.group(1).replace(' ', '')  # Ensure it's formatted correctly
            return tenant_time_zone

        def convert_data_and_map_headers(df, tenant_time_zone):
            # conversion_start_time = time.time()
            df_dict = df.to_dict(orient='records')
            # Convert timestamps
            df_dict = convert_timestamp_data(df_dict, tenant_time_zone)
            # Get headers mapping
            headers_map = get_headers_mapping(
                tenant_database, [module_name], role_name, "username", 
                "main_tenant_id", "sub_parent_module", "parent_module", data, common_utils_database
            )
            # conversion_duration = time.time() - conversion_start_time
            # logging.info(f"Data conversion and headers mapping execution time: {conversion_duration:.4f} seconds")
            return df_dict, headers_map

        # Using ThreadPoolExecutor to run both queries and tenant timezone fetch in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                "count": executor.submit(execute_count_query),
                "main_query": executor.submit(execute_main_query),
                "tenant_timezone": executor.submit(fetch_tenant_timezone)
            }

            # Wait for all futures to complete
            # q_time = time.time()
            count_result = futures["count"].result()
            df = futures["main_query"].result()
            tenant_time_zone = futures["tenant_timezone"].result()

            # Set total pages count
            pages['total'] = count_result

            # Processing data and headers mapping in parallel (this part can also be done in parallel with other parts)
            df_dict, headers_map = executor.submit(convert_data_and_map_headers, df, tenant_time_zone).result()
        # Preparing the response data
        return_json_data.update({
            'message': 'Successfully Fetched the Charges history data',
            'flag': True,
            'headers_map': headers_map,
            'data': df_dict,
            'pages': pages
        })

        # End time and audit logging
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        audit_data_user_actions = {
            "service_name": 'Charges history',
            "created_date": request_received_at,
            "created_by": username,
            "status": str(return_json_data['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": 'Charges history data',
            "module_name": module_name,
            "request_received_at": request_received_at
        }
        common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        return return_json_data

    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        # Get headers mapping
        headers_map = get_headers_mapping(
            tenant_database, [module_name], role_name, "username", 
            "main_tenant_id", "sub_parent_module", "parent_module", data, common_utils_database
        )
        message = f"Unable to fetch the Charges history data{e}"
        response = {"flag": True, "message": message,"headers_map":headers_map}
        error_type = str(type(e).__name__)
        
        # Error logging
        try:
            error_data = {
                "service_name": 'Charges history',
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": username,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "Charges history data",
                "module_name": module_name,
                "request_received_at": request_received_at
            }
            common_utils_database.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception while logging error: {e}")
        return response


def format_timestamp(ts):
    # Check if the timestamp is not None
    if ts is not None:
        # Convert a Timestamp or datetime object to the desired string format
        return ts.strftime("%b %d, %Y, %I:%M %p")
    else:
        # Return a placeholder or empty string if the timestamp is None
        return " "






def export_row_data_customer_charges(data):
    '''
    Description:Exports data into an Excel file. It retrieves data based on the module name from the database,
    processes it, and returns a blob representation of the data if within the allowed row limit.
    '''
    logging.info(f"Request Data Recieved")
    ### Extract parameters from the Request Data
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', None)
    module_name = data.get('module_name', 'Optimization row data')
    user_name = data.get('user_name', '')
    queue_id = data.get('queue_id', '')
    ##database connection for common utilss
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # Start time  and date calculation
    start_time = time.time()
    try:
        ##databse connenction
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        # Fetch the query from the database based on the module name
        module_query_df = db.get_data("export_queries", {"module_name": "Optimization row data"})
        logging.info(module_query_df,'module_query_df')
        ##checking the dataframe is empty or not
        if module_query_df.empty:
            return {
                'flag': False,
                'message': f'No query found for module name: {module_name}'
            }
        # Extract the query string from the DataFrame
        query = module_query_df.iloc[0]['module_query']
        if not query:
            return {
                'flag': False,
                'message': f'Unknown module name: {module_name}'
            }
        ##params for the specific module
        params = [queue_id]
        ##executing the query
        data_frame = database.execute_query(query, params=params)
        # Capitalize each word and add spaces
        data_frame.columns = [
            col.replace('_', ' ').title() for col in data_frame.columns
        ]
        data_frame['S.NO'] = range(1, len(data_frame) + 1)
        # Reorder columns dynamically to put S.NO at the first position
        columns = ['S.NO'] + [col for col in data_frame.columns if col != 'S.NO']
        data_frame = data_frame[columns]
        # Proceed with the export if row count is within the allowed limit
        data_frame = data_frame.astype(str)
        data_frame.replace(to_replace='None', value='', inplace=True)

        blob_data = dataframe_to_blob(data_frame)
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        # Create the filename with the date format YYYYMMDDHHMM
        current_time = datetime.now()
        formatted_date = current_time.strftime('%Y%m%d%H%M')
        filename = f"CustomerChargeDetailOutboardRecycle_{formatted_date}"
        # Return JSON response
        response = {
            'flag': True,
            'blob': blob_data.decode('utf-8'),
            'filename':filename
        }
        audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": request_received_at,
            "created_by": user_name,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "export","request_received_at":request_received_at
        }
        db.update_audit(audit_data_user_actions, 'audit_user_actions')
        return response
    except Exception as e:
        error_type = str(type(e).__name__)
        logging.exception(f"An error occurred: {e}")
        message = f"Error is {e}"
        response = {"flag": False, "message": message}
        try:
            # Log error to database
            error_data = {
                "service_name": 'Module Management',
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "export","request_received_at":request_received_at
            }
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response


    
def dataframe_to_blob(data_frame):
    '''
    Description:The Function is used to convert the dataframe to blob
    '''
    # Create a BytesIO buffer
    bio = BytesIO()
    
    # Use ExcelWriter within a context manager to ensure proper saving
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        data_frame.to_excel(writer, index=False)
    
    # Get the value from the buffer
    bio.seek(0)
    blob_data = base64.b64encode(bio.read())
    return blob_data
 
def customer_charges_template(data):
    '''
    Description:Exports data into an Excel file. It retrieves data based on the module name from the database,
    processes it, and returns a blob representation of the data if within the allowed row limit.
    '''
    logging.info(f"Request Data Recieved")
    ### Extract parameters from the Request Data
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', None)
    user_name = data.get('user_name', '')
    ##database connection for common utilss
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # Start time  and date calculation
    start_time = time.time()
    try:
        #columns to be downloaded for the template
        columns = ["Rev IO ServiceNumber", "Base Charge Amount", "Overage Charge Amount", "Rev IO Product Type Id", "Description",
                   "Billing Start Date", "Billing End Date", "Sms Rev Io Product Type Id", "Sms Charge Amount"]
        # Create an empty DataFrame
        data_frame = pd.DataFrame(columns=columns)
        data_frame.replace(to_replace='None', value='', inplace=True)
        blob_data = dataframe_to_blob(data_frame)
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        filename = f"ExampleCustomerCharges"
        # Return JSON response
        response = {
            'flag': True,
            'blob': blob_data.decode('utf-8'),
            'filename':filename
        }
        audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": request_received_at,
            "created_by": user_name,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "export","request_received_at":request_received_at
        }
        db.update_audit(audit_data_user_actions, 'audit_user_actions')
        return response
    except Exception as e:
        error_type = str(type(e).__name__)
        logging.info(f"An error occurred: {e}")
        message = f"Error is {e}"
        response = {"flag": False, "message": message}
        try:
            # Log error to database
            error_data = {
                "service_name": 'Module Management',
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "export","request_received_at":request_received_at
            }
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response


def customers_sessions_customer_charges_export_dropdown_data(data):
    logging.info("Request Data Received")
    
    # Extract parameters from the request data
    tenant_database = data.get('db_name', '')
    if not tenant_database:
        logging.error("Database name is missing from the request data.")
        return {"flag": False, "customer_sessions": {}, "message": "Database name is required"}
    
    # Database connection setup
    try:
        database = DB(tenant_database, **db_config)  # Assuming DB is a predefined database connection class
    except Exception as e:
        logging.exception(f"Error establishing database connection: {e}")
        return {"flag": False, "customer_sessions": {}, "message": "Error establishing database connection"}

    try:
        # Fetch customer names, session IDs, and billing period end date
        query = """
        SELECT DISTINCT customer_name, CAST(session_id AS TEXT) AS session_id,  TO_CHAR(billing_period_end_date::date, 'YYYY-MM-DD') AS billing_period_end_date
        FROM vw_optimization_smi_result_customer_charge_queue;
        """
        
        # Get data from the database
        customer_session_df = database.execute_query(query, True)  # Assuming execute_query returns a DataFrame
        if customer_session_df.empty:
            logging.info("No data found for customer sessions.")
            return {"flag": True, "customer_sessions": {}, "message": "No customer sessions found"}
        
        # Initialize a dictionary to store the result with differentiation of session details
        customer_sessions = {}
        
        # Iterate over the dataframe to build the dictionary
        for index, row in customer_session_df.iterrows():
            customer_name = row['customer_name']
            session_id = row['session_id']
            billing_period_end_date = row['billing_period_end_date']
            
            # Check if the customer_name already exists in the dictionary
            if customer_name in customer_sessions:
                # Append a dictionary with session details for the existing customer
                customer_sessions[customer_name].append({
                    'session_id': session_id,
                    'billing_period_end_date': billing_period_end_date
                })
            else:
                # Create a new list for the customer with session details
                customer_sessions[customer_name] = [{
                    'session_id': session_id,
                    'billing_period_end_date': billing_period_end_date
                }]
        
        response = {"flag": True, "customer_sessions": customer_sessions}
        return response
    
    except Exception as e:
        logging.exception(f"Error fetching data: {e}")
        return {"flag": False, "customer_sessions": {}, "message": f"Error: {str(e)}"}


def export_customer_charges(data):
    logging.info(f"Request Data Recieved")
    # List to keep track of Excel files to zip later
    excel_buffers = []
    session_ids=data.get('session_ids',[])
    # Database connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    zip_filename = f"CustomerCharges.zip"
    
    # Create an in-memory buffer to hold the zip file
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for session_id in session_ids:
            customer_name = data.get('customer_name','')
            params = [session_id]
            query = '''SELECT 
                            rev_account_number,
                            customer_name,
                            billing_period_end_date - billing_period_start_date AS billing_period_duration,
                            billing_period_start_date,
                            billing_period_end_date,
                            usage_mb,
                            base_charge_amount,
                            rate_charge_amt,
                            overage_charge_amount,
                            is_processed,
                            error_message,
                            iccid,
                            msisdn,
                            sms_usage,
                            sms_charge_amount,
                            total_charge_amount
                        FROM 
                            vw_optimization_smi_result_customer_charge_queue 
            WHERE session_id=%s
            '''
            
            # Execute the query
            dataframe = database.execute_query(query, params=params)
            # Capitalize each word and add spaces
            dataframe.columns = [
                col.replace('_', ' ').title() for col in dataframe.columns
            ]
            dataframe['S.NO'] = range(1, len(dataframe) + 1)
            # Reorder columns dynamically to put S.NO at the first position
            columns = ['S.NO'] + [col for col in dataframe.columns if col != 'S.NO']
            dataframe = dataframe[columns]
            
            
            if not dataframe.empty:
                # Extract the billing period start and end dates from the first row
                billing_period_start_date = dataframe.iloc[0]['Billing Period Start Date'].strftime('%Y%m%d')
                billing_period_end_date = dataframe.iloc[0]['Billing Period End Date'].strftime('%Y%m%d')
                logging.debug(f"billing_period_start_date is :{billing_period_start_date} and billing_period_end_date is:{billing_period_end_date}")
    
                # Create a filename for the Excel file
                excel_filename = f"CustomerChargeDetail_{billing_period_start_date}_{billing_period_end_date}.xlsx"
                
                # Create an in-memory Excel file using BytesIO
                customer_excel_buffer = io.BytesIO()
                with pd.ExcelWriter(customer_excel_buffer, engine='openpyxl') as writer:
                    dataframe.to_excel(writer, index=False, sheet_name='Charges')
                
                # Write the Excel file buffer to the zip archive
                customer_excel_buffer.seek(0)  # Move the pointer back to the start of the buffer
                zipf.writestr(excel_filename, customer_excel_buffer.getvalue())
                
                # Close the Excel buffer (not strictly necessary, but good practice)
                customer_excel_buffer.close()
    
    # Get the content of the zip buffer
    zip_buffer.seek(0)
    zip_blob = zip_buffer.getvalue()
    
    # Convert to base64 (this can be sent to the frontend as a blob)
    encoded_blob = base64.b64encode(zip_blob).decode('utf-8')
    
    # Example response
    response = {
        'flag': True,
        'blob': encoded_blob,
        'filename': zip_filename
    }
    return response





def upload_customer_charges_data(data):
    logging.info(f"Request data Recieved")
    blob_data = data.get('blob')
    # Decode the blob data
    if not blob_data:
        return {"flag": False, "message": "Blob data not provided"}
    try:
        # Process the blob data
        blob_data = blob_data.split(",", 1)[1]
        blob_data += '=' * (-len(blob_data) % 4)  # Padding for base64 decoding
        file_stream = BytesIO(base64.b64decode(blob_data))
        # Read the data into a DataFrame
        uploaded_data = pd.read_excel(file_stream, engine='openpyxl').to_dict(orient='records')
        # Prepare the payload for the request
        data = {
            "uploaded_data": uploaded_data
        }
        # Send the POST request
        response = requests.post(
            "https://sandbox.amop.services/api/OptimizationApi/Upload",
            json=data
        )
        return {"flag": True, "message": "Upload successful", "response": response.json()}
    except Exception as e:
        logging.exception(f"Exception is {e}")
        return {"flag": False, "message": str(e)}





def convert_timestamp_data(df_dict, tenant_time_zone):
    """Convert timestamp columns in the provided dictionary list to the tenant's timezone."""
    # Create a timezone object
    target_timezone = timezone(tenant_time_zone)

    # List of timestamp columns to convert
    timestamp_columns = ['created_date', 'modified_date', 'deleted_date']  # Adjust as needed based on your data

    # Convert specified timestamp columns to the tenant's timezone
    for record in df_dict:
        for col in timestamp_columns:
            if col in record and record[col] is not None:
                # Convert to datetime if it's not already
                timestamp = pd.to_datetime(record[col], errors='coerce')
                if timestamp.tz is None:
                    # If the timestamp is naive, localize it to UTC first
                    timestamp = timestamp.tz_localize('UTC')
                # Now convert to the target timezone
                record[col] = timestamp.tz_convert(target_timezone).strftime('%m-%d-%Y %H:%M:%S')  # Ensure it's a string
    return df_dict

def serialize_data(data):
    """Recursively convert pandas objects in the data structure to serializable types."""
    if isinstance(data, list):
        return [serialize_data(item) for item in data]
    elif isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, pd.Timestamp):
        return data.strftime('%m-%d-%Y %H:%M:%S')  # Convert to string
    else:
        return data  # Return as is if not a pandas object