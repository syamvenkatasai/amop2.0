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
import boto3
import logging
import os
import time
from io import BytesIO
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import threading  # For asynchronous execution


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
logging = Logging(name="sim_management")

def get_headers_mappings(tenant_database,module_list,role,user,tenant_id,sub_parent_module,parent_module,data):
    '''
    Description: The  function retrieves and organizes field mappings,headers,and module features 
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    '''
    ##Database connection
    logging.info(f"Module name is :{module_list} and role is {role}")
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    feature_module_name=data.get('feature_module_name','')
    user_name = data.get('username') or data.get('user_name') or data.get('user')
    tenant_name = data.get('tenant_name') or data.get('tenant') 
    try:
        tenant_id=common_utils_database.get_data('tenant',{"tenant_name":tenant_name}['id'])['id'].to_list()[0]
        logging.debug(f"tenant_id  is :{tenant_id}")
    except Exception as e:
        logging.exception(f"Getting exception at fetching tenant id {e}")
    ret_out={}
    # Iterate over each module name in the provided module list
    for module_name in module_list:
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
        logging.info(f"Got the header map here")
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

        except Exception as e:
            logging.warning(f"there is some error {e}")
            pass
        # Add the final features to the headers dictionary
        headers['module_features']=final_features
        ret_out[module_name]=headers
        
    return ret_out

def get_features_by_feature_name(user_name, tenant_id, feature_name,common_utils_database):
    # Fetch user features from the database
    user_features_raw = common_utils_database.get_data(
        "user_module_tenant_mapping", 
        {"user_name": user_name, "tenant_id": tenant_id},
        ['module_features']
    )['module_features'].to_list()

    # Parse the JSON string to a dictionary
    user_features = json.loads(user_features_raw[0])  # Assuming it's a list with one JSON string
    logging.debug(f"user features are {user_features}")
    # Initialize a list to hold features for the specified feature name
    features_list = []

    # Loop through all modules to find the specified feature name
    for module, features in user_features.items():
        if feature_name in features:
            features_list.extend(features[feature_name])
    logging.debug(f" features_list are {features_list}")
    return features_list


def get_device_history(data):
    '''
    Description: Retrieves device history data from the database based on unique identifiers and columns provided in the input data.
    Validates the acccess token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    logging.info(f"Request Data Recieved")
    Partner = data.get('Partner', '')
    # Start time  and date calculation
    start_time = time.time()
    username = data.get('username', None)
    request_received_at = data.get('request_received_at', '')
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    # database Connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:

        response_data = []
        # fetching the  unique identifiers and column name from the input data
        unique_ids = data.get('unique_ids', [])
        logging.debug(f"unique_ids are {unique_ids}")
        unique_column = data.get('unique_column', '')
        # If both unique IDs and the unique column are provided, fetch data from the database
        if unique_ids and unique_column:
            return_df = database.get_data('sim_management_inventory_action_history', where={
                                          unique_column: unique_ids}).to_dict(orient="records")
            response_data = return_df
        # Preparing a success message for the response
        message = "device histories data sent sucessfully"
        response = {"flag": True, "message": message, "Modules": response_data}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response_data['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "session_id": session_id,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the device history data',
                                       "module_name": "get_device_history",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response

    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting device_history"
        # Preparing error data to log the error details in the database
        # Error Management
        error_data = {"service_name": 'Module_api',
                      "created_date": request_received_at,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id, "tenant_name": tenant_name,
                      "comments": message, "module_name": module_name,
                      "request_received_at": request_received_at}
        common_utils_database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}

def get_status_history(data):
    '''
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_id' for querying the status history.

    Returns:
    - dict: A dictionary containing the status history data, header mapping, 
    and a success message or an error message.
    '''
    logging.info(f"Request data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    ##fetching the request data
    list_view_data_id=data.get('list_view_data_id','')
    logging.debug(f"list_view_data_id is : {list_view_data_id}")
    session_id=data.get('session_id','')
    username=data.get('username','')
    Partner=data.get('Partner','')
    role_name = data.get('role_name', '')
    list_view_data_id=int(list_view_data_id)
    request_received_at = data.get('request_received_at', None)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    tenant_database=data.get('db_name','altaworx_central')
    try:
        ##Database connection
        database = DB(tenant_database, **db_config)
         # Fetch status history data from the database
        sim_management_inventory_action_history_dict=database.get_data(
        "sim_management_inventory_action_history",{'sim_management_inventory_id':list_view_data_id
        },["service_provider","iccid","msisdn","customer_account_number","customer_account_name"
        ,"previous_value","current_value","date_of_change","change_event_type","changed_by"]
        ).to_dict(orient='records')
        # Handle case where no data is returned
        if not sim_management_inventory_action_history_dict:
             ##Fetching the header map for the inventory status history
            headers_map=get_headers_mappings(tenant_database,["inventory status history"
            ],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
            message = "No status history data found for the provided SIM management inventory ID."
            response = {"flag": True, "status_history_data": [], "header_map":headers_map, "message": message}
            return response
        # Helper function to serialize datetime objects to strings
        def serialize_dates(data):
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.strftime('%m-%d-%Y %H:%M:%S')
            return data
        
        # Apply date serialization to each record
        sim_management_inventory_action_history_dict = [
            serialize_dates(record) for record in sim_management_inventory_action_history_dict
        ]
        ##Fetching the header map for the inventory status history
        headers_map=get_headers_mappings(tenant_database,["inventory status history"
        ],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        message = f"Status History data Fetched Successfully"
        # Prepare success response
        response={"flag":True,"status_history_data":sim_management_inventory_action_history_dict,
                  "header_map":headers_map,"message":message}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {"service_name": 'Module Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "session_id": session_id,
                                       "tenant_name": Partner,
                                       "comments": list_view_data_id,
                                       "module_name": "Reports",
                                       "request_received_at": request_received_at}
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.exception(f"exception is {e}")
        return response
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = f"Unable to fetch the status history data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_superadmin_data',
                          "created_date": request_received_at,
                          "error_message": message,
                          "error_type": error_type,
                          "users": username,
                          "session_id": session_id,
                          "tenant_name": Partner,
                          "comments": "",
                          "module_name": "Module Managament",
                          "request_received_at":request_received_at}
            common_utils_database.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"exception is {e}")
        return response



def get_rev_assurance_data(data):
    '''
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_params' for querying the status history.

    Returns:
    - dict: A dictionary containing the List view data, header mapping, and a success message or an error message.
    '''
    logging.info(f"Request Data Recieved")
    Partner = data.get('tenant_name', '')
    role_name = data.get('role_name', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    variance=data.get('variance',False)
    table = data.get('table', 'vw_rev_assurance_list_view_with_count')
    # Start time  and date calculation
    start_time = time.time()
    try:
        # Initialize the database connection
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        tenant_database=data.get('db_name','altaworx_central')
        rev_assurance_data=[]
        pages={}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100   # Default to 100 if no value
            logging.debug(f"starting page is {start} and ending page is {end}")
            limit=data.get('limit',100)
            # Calculate pages 
            pages['start']=start
            pages['end']=end
            count_params = [table]
            if variance==False:
                count_query = "SELECT COUNT(*) FROM %s" % table
            else:
                count_query = "SELECT COUNT(*) FROM %s where device_status <> rev_io_status" % table
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages['total']=int(count_result)

        params=[start,end]
        if variance==False:
            query='''SELECT customer_name,
                    service_number,
                    iccid,
                    customer_rate_plan_name,
                    service_provider,
                    device_status,
                    TO_CHAR(carrier_last_status_date::date, 'YYYY-MM-DD') AS carrier_last_status_date,
                    rev_io_status,
                    TO_CHAR(activated_date::date, 'YYYY-MM-DD') AS activated_date,
                    rev_active_device_count,
                    rev_total_device_count,
                    package_id,
                    rate,
                    description,
                    service_product_id,
                    rev_account_number
                    FROM vw_rev_assurance_list_view_with_count
                    OFFSET %s LIMIT %s;
                    '''
        else:
            query='''SELECT customer_name,
                    service_number,
                    iccid,
                    customer_rate_plan_name,
                    service_provider,
                    device_status,
                    TO_CHAR(carrier_last_status_date::date, 'YYYY-MM-DD') AS carrier_last_status_date,
                    rev_io_status,
                    TO_CHAR(activated_date::date, 'YYYY-MM-DD') AS activated_date,
                    rev_active_device_count,
                    rev_total_device_count,
                    package_id,
                    rate,
                    description,
                    service_product_id,
                    rev_account_number
                    FROM vw_rev_assurance_list_view_with_count where device_status <> rev_io_status
                    OFFSET %s LIMIT %s;
                    '''
        rev_assurance_data=database.execute_query(query,params=params).to_dict(orient='records')
        # Generate the headers mapping
        headers_map = get_headers_mappings(tenant_database,["rev assurance"],role_name,'','','','',data)
        # Prepare the response
        response = {"flag": True, "rev_assurance_data": rev_assurance_data, "header_map": headers_map,"pages":pages}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the rev assurance data',
                                       "module_name": "get_rev_assurance_data",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        # Error Management
        error_data = {"service_name": 'Sim management',
                      "created_date": request_received_at,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "tenant_name": Partner,
                      "comments": message,
                      "module_name": 'get_rev_assurance_data',
                      "request_received_at": request_received_at}
        common_utils_database.log_error_to_db(error_data, 'error_table')
        response = {"flag": False, "error": str(e)}
        return response




def add_service_line_dropdown_data(data):
    '''
    Description: Retrieves add_service_line dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    logging.info(f"Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    # database Connection
    
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        response_data = {}
        service_provider = data.get('service_provider', None)
        rev_customer_name = data.get('customer_name', None)
        authentication_ids=database.get_data('revcustomer',{'customer_name':rev_customer_name,'is_active':True},['integration_authentication_id'])['integration_authentication_id'].to_list()
        logging.debug(f"authentication_ids are {authentication_ids}")
        response_data["customer_rate_plan_dropdown"]=list(set(database.get_data('customerrateplan',{'service_provider_name':service_provider,'is_active':True},['rate_plan_name'])['rate_plan_name'].to_list()))
        response_data["rev_provider"]=list(set(database.get_data('rev_provider',{'integration_authentication_id':authentication_ids,'is_active':True},['description'])['description'].to_list()))
        response_data["service_type"]=list(set(database.get_data('rev_service_type',{'integration_authentication_id':authentication_ids,'is_active':True},['description'])['description'].to_list()))
        response_data["rev_product"]=list(set(database.get_data('rev_product',{'integration_authentication_id':authentication_ids,'is_active':True},['description'])['description'].to_list()))
        response_data["rev_usage_plan_group"]=list(set(database.get_data('rev_usage_plan_group',{'integration_authentication_id':authentication_ids,'is_active':True},['description'])['description'].to_list()))
        #dependent dropdowns
        response_data["rev_provider_id_map"]=database.get_data('rev_provider',{'integration_authentication_id':authentication_ids,'is_active':True},['description','provider_id']).to_dict(orient='records')
        response_data["rev_provider_id_map"]={item['description']: item['provider_id'] for item in response_data["rev_provider_id_map"]}
        response_data["rev_package"]=form_depandent_dropdown_format(database.get_data('rev_package',{'integration_authentication_id':authentication_ids,'is_active':True},['description','provider_id']).to_dict(orient='records'),'provider_id','description')
        response_data["rate"]=form_depandent_dropdown_format(database.get_data('rev_product',{'integration_authentication_id':authentication_ids,'is_active':True},columns=['rate','provider_id']).to_dict(orient='records'),'provider_id','rate')
        logging.info(f"The drop down data is fetched")
        message = "add_service_line data sent sucessfully"
        response = {"flag": True, "message": message, "response_data": response_data}
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response
    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting add service line"
        # Error Management
        error_data = {"service_name": 'SIM management',
                      "created_date": start_time,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id,
                      "tenant_name": tenant_name,
                      "comments": message,
                      "module_name": module_name,
                      "request_received_at": start_time}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False,"response_data": {}, "message": message}
    

    

def submit_service_line_dropdown_data(data):
    '''
    Description: Retrieves add_service_line dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    logging.info(f"Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    tenant_name = data.get('tenant_name', None)
    username = data.get('username', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    service_provider = data.get('service_provider', None)
    # database Connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config) 
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:
        submitted_data = data.get('submit_data', None)
        revio_product = submitted_data.get('revio_product', None)
        service_type = submitted_data.get('service_type', None)
        provider_name = submitted_data.get('provider_name', None)
        rev_usage_plan_group = submitted_data.get('rev_usage_plan_group', None)
        customer_name = submitted_data.get('customer_name', None)
        revio_package = submitted_data.get('revio_package', None)
        iccids = submitted_data.get('iccid', [])
        description = submitted_data.get('description', None)
        rate = submitted_data.get('rate', [])
        quantity = submitted_data.get('quantity',None)
        activation_date = submitted_data.get('activation_date',None)
        add_rate_plan = submitted_data.get('add_rate_plan',None)
        rate_plan = submitted_data.get('rate_plan',None)
        prorate = submitted_data.get('prorate',None)
        tenant_id=common_utils_database.get_data("tenant",{"tenant_name":tenant_name},["id"])["id"].to_list()[0] 
        logging.debug(f"tenant_id is {tenant_id}") 
        Change_type_id=database.get_data("sim_management_bulk_change_type",{"display_name":"Create Rev Service"},["id"])["id"].to_list()[0]    
        try:
            rev_cust_id=database.get_data('revcustomer',{'customer_name':customer_name,'is_active':True},['id'])['id'].to_list()[0]
        except:
            rev_cust_id=None
        try:
            customer_id=database.get_data('customers',{'rev_customer_id':rev_cust_id,'is_active':True},['id'])['id'].to_list()[0]
            customer_id=int(customer_id) if customer_id else None 
        except:
            customer_id=None
        try:
            rev_product_id=database.get_data('rev_product',{'description':revio_product,'is_active':True},['product_id'])['product_id'].to_list()[0]
            rev_product_id=int(rev_product_id) if rev_product_id else None 
        except:
            rev_product_id=None
        try:
            rev_service_type_id=database.get_data('rev_service_type',{'description':service_type,'is_active':True},['service_type_id'])['service_type_id'].to_list()[0]
            rev_service_type_id=int(rev_service_type_id) if rev_service_type_id else None
        except:
            rev_service_type_id=None
        try:
            rev_provider_id=database.get_data('rev_provider',{'description':provider_name,'is_active':True},['provider_id'])['provider_id'].to_list()[0]
            rev_provider_id=int(rev_provider_id) if rev_provider_id else None
        except:
            rev_provider_id=None
        try:
            rev_usage_plan_group_id=database.get_data('rev_usage_plan_group',{'description':rev_usage_plan_group,'is_active':True},['usage_plan_group_id'])['usage_plan_group_id'].to_list()[0]
            rev_usage_plan_group_id=int(rev_usage_plan_group_id) if rev_usage_plan_group_id else None
        except:
            rev_usage_plan_group_id=None  
        try:
            integration_id=database.get_data('revcustomer',{'customer_name':customer_name,'is_active':True},['integration_authentication_id'])['integration_authentication_id'].to_list()[0]
            integration_id=int(integration_id) if integration_id else None
        except:
            integration_id=None
        try:
            service_provider_id=database.get_data("serviceprovider",{"service_provider_name":service_provider},["id"])["id"].to_list()[0]
            service_provider_id=int(service_provider_id) if service_provider_id else None
        except:
            service_provider_id=None
        try:
            rev_customer_id=database.get_data('revcustomer',{'customer_name':customer_name,'is_active':True},['rev_customer_id'])['rev_customer_id'].to_list()[0]
            rev_customer_id=int(rev_customer_id) if rev_customer_id else None
        except:
            rev_customer_id=None
        try:
            rev_package_id=database.get_data('rev_package',{'description':revio_package,'is_active':True},['package_id'])['package_id'].to_list()[0]
            rev_package_id=int(rev_package_id) if rev_package_id else None
        except:
            rev_package_id=None
        return_dict={}
        logging.info(rev_cust_id,'rev_cust_id')
        logging.info(customer_id,'customer_id')
        logging.info(rev_product_id,'rev_product_id')
        logging.info(rev_service_type_id,'rev_service_type_id')
        logging.info(rev_provider_id,'rev_provider_id')
        logging.info(rev_usage_plan_group_id,'rev_usage_plan_group_id')
        logging.info(rev_customer_id,'rev_customer_id')
        logging.info(rev_package_id,'rev_package_id')
        logging.info(integration_id,'integration_id')
        Device_Bulk_Change={}
        Device_Bulk_Change["change_request_type_id"] = Change_type_id
        Device_Bulk_Change["service_provider_id"] = service_provider_id
        Device_Bulk_Change["tenant_id"] = tenant_id
        Device_Bulk_Change["status"] = "NEW"
        Device_Bulk_Change["created_by"] = username
        Device_Bulk_Change["is_active"] = True
        Device_Bulk_Change["is_deleted"] = False
        Device_Bulk_Change["service_provider"]=service_provider
        Device_Bulk_Change["change_request_type"]="Create Rev Service"
        Device_Bulk_Change["modified_by"]=username
        Device_Bulk_Change["uploaded"]=len(iccids)
        Device_Bulk_Change["created_by"]=username
        Device_Bulk_Change["processed_by"]=username
        bulkchangeid=database.insert_data(Device_Bulk_Change,"sim_management_bulk_change")
        return_dict["sim_management_bulk_change"]=[Device_Bulk_Change]
        bulkchange_df=database.get_data("sim_management_bulk_change",Device_Bulk_Change)
        bulkchange_id=bulkchange_df["id"].to_list()[0]
        logging.info(bulkchange_id,'bulkchange_id')
        create_new_bulk_change_request_dict_all=[]
        for iccid in iccids:
            try:
                device_id=int(database.get_data("sim_management_inventory",{"iccid":iccid},["id"])["id"].to_list()[0])
                logging.debug(f"device_id is :{device_id}")
            except:
                device_id=None
            create_new_bulk_change_request_dict={}
            create_new_bulk_change_request_dict['iccid']=iccid
            create_new_bulk_change_request_dict['bulk_change_id']= bulkchange_id
            create_new_bulk_change_request_dict['tenant_id']=tenant_id
            create_new_bulk_change_request_dict["created_by"]=username
            create_new_bulk_change_request_dict["status"]="NEW"
            create_new_bulk_change_request_dict["device_id"]=device_id
            create_new_bulk_change_request_dict["request_created_by"]=username
            create_new_bulk_change_request_dict["is_active"]=True
            create_new_bulk_change_request_dict["is_deleted"]=False

            change_request={
                "Number": iccid,
                "ICCID": iccid,
                "RevCustomerId": rev_customer_id,
                "DeviceId": device_id,
                "CreateRevService": True,
                "ServiceTypeId": rev_service_type_id,
                "RevPackageId": rev_package_id,
                "RevProductIdList": rev_product_id,
                "RateList": rate,
                "Prorate": prorate,
                "Description": description,
                "EffectiveDate": None,
                "AddCustomerRatePlan": add_rate_plan,
                "CustomerRatePlan": rate_plan,
                "CustomerRatePool": None,
                "IntegrationAuthenticationId": integration_id,
                "ProviderId": rev_provider_id,
                "ActivatedDate": activation_date,
                "UsagePlanGroupId": rev_usage_plan_group_id,
                "AddCarrierRatePlan":None,
                "CarrierRatePlan": None,
                "CommPlan": None,
                "JasperDeviceID": device_id,
                "SiteId": customer_id
            }
            create_new_bulk_change_request_dict["change_request"]=json.dumps(change_request)
            create_new_bulk_change_request_dict_all.append(create_new_bulk_change_request_dict)
            change_request_id=database.insert_data(create_new_bulk_change_request_dict,"sim_management_bulk_change_request") 

        return_dict["sim_management_bulk_change_request"]=create_new_bulk_change_request_dict_all
        message = " Add Service Line data submitted sucessfully "
        response = {"flag": True, "message": message}
        response["data"]=return_dict
        response["bulk_chnage_id_20"]=bulkchange_id

        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response

    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting submitting add service line"
        # Error Management
        error_data = {"service_name": 'SIM management',
                      "created_date": start_time,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id,
                      "tenant_name": tenant_name,
                      "comments": message,
                      "module_name": module_name,
                      "request_received_at": start_time}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}
    
def add_service_product_dropdown_data(data):
    '''
    Description: Retrieves add_service_product dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    # checking the access token valididty
    logging.info(f"Request Data recieved")
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    # database Connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    # Check if customer_name is provided
    rev_customer_name = data.get('customer_name', None)
    if rev_customer_name is None:
        message = "customer name is required."
        return {"flag": False, "message": message}
    try:
        response_data = {}
        response_message = ""
        # Query 1: Get customer ID
        query1 = """SELECT id FROM revcustomer WHERE customer_name = %s LIMIT 1"""
        params = [rev_customer_name]
        rev_customer_data = database.execute_query(query1, params=params)
        if rev_customer_data.empty:
            response_message = "Data not provided for customer name"
            response_data["customer_data_error"] = response_message
        else:
            customer_id = rev_customer_data.iloc[0, 0]
            logging.debug(f"customer_id is {customer_id}")
            # Query 2: Get rev_customer_id
            query2 = """SELECT rev_customer_id FROM revcustomer WHERE id = %s"""
            params2 = [customer_id]
            customer_data = database.execute_query(query2, params=params2)
            if customer_data.empty:
                response_message = "Data not provided for customer id"
                response_data["customer_id_error"] = response_message
            else:
                customer_data = customer_data.iloc[0, 0]

                # Query 3: Get service information
                query3 = """SELECT rev_service_id, rev_service_type_id FROM rev_service WHERE rev_customer_id = %s"""
                params3 = [str(customer_id)]
                rev_service_data = database.execute_query(query3, params=params3)
                if rev_service_data.empty:
                    response_message = "Data not provided for service id and service type id"
                    response_data["service_info_error"] = response_message
                else:
                    rev_service_id = rev_service_data.iloc[0, 0]
                    logging.debug(f"rev_service_id is {rev_service_id}")
                    service_type_id = rev_service_data.iloc[0, 1]
                    service_type_id = int(service_type_id)
                    # Query 4: Get service description
                    query4 = """SELECT description FROM rev_service_type WHERE service_type_id = %s"""
                    params4 = [service_type_id]
                    rev_service_type_data = database.execute_query(query4, params=params4)
                    if rev_service_type_data.empty:
                        description = "–"  # Default value when description is not found
                        response_message = f"{customer_id} - Service id: {rev_service_id} - Service Type: {description} (description not provided)"
                    else:
                        description = rev_service_type_data
                        response_message = f"{customer_id} - Service id: {rev_service_id} - Service Type: {description}"
        response_data["customer_name"] = response_message
        # Fetch authentication IDs
        authentication_ids=database.get_data('revcustomer',{'customer_name':rev_customer_name,'is_active':True},['integration_authentication_id'])['integration_authentication_id'].to_list()
        # Fetch products and related information
        response_data["rev_product"]=list(set(database.get_data('rev_product',{'integration_authentication_id':authentication_ids,'is_active':True},['description'])['description'].to_list()))
        #dependent dropdowns
        response_data["rev_product_id_map"]=database.get_data('rev_product',{'integration_authentication_id':authentication_ids,'is_active':True},['description','provider_id']).to_dict(orient='records')
        response_data["rev_product_id_map"]={item['description']: item['provider_id'] for item in response_data["rev_product_id_map"]}
        response_data["rate"]=form_depandent_dropdown_format(database.get_data('rev_product',{'integration_authentication_id':authentication_ids,'is_active':True},columns=['rate','provider_id']).to_dict(orient='records'),'provider_id','rate')
        message = "add_service_product data sent sucessfully"
        response = {"flag": True, "message": message, "response_data": response_data}
        return response
    except Exception as e:
        logging.exception(f"Something went wrong and the error is {e}")
        message = "Something went wrong while getting add service product"
        # Error Management
        error_data = {
            "service_name": 'SIM management',
            "error_message": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
        }
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}


def submit_add_service_product_dropdown_data(data):
    '''
    Description: Submits add_service_product dropdown data to the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    # logging.info(f"Request Data: {data}")

    # Start time  and date calculation
    start_time = time.time()
    tenant_name = data.get('tenant_name', None)
    username = data.get('username', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    # Database Connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        customer_name = data.get('submit_data', {}).get('customer_name', None)
        product_name = data.get('submit_data', {}).get('product_name', None)
        rate = data.get('submit_data', {}).get('rate', None)
        quantity = data.get('submit_data', {}).get('quantity', None)
        prorate = data.get('submit_data', {}).get('prorate', None)
        effective_date = data.get('submit_data', {}).get('effective_date', None)
        description = data.get('submit_data', {}).get('description', None)

        if not customer_name or not product_name or rate is None:
            return {"flag": False, "message": "Customer name, product name, and rate are required."}
        rev_product_id = database.get_data('rev_product', {'description': product_name, 'is_active': True}, ['product_id'])['product_id'].to_list()
        rev_customer_id = database.get_data('revcustomer', {'customer_name': customer_name, 'is_active': True}, ['customer_name'])['customer_name'].to_list()
        logging.debug(f"rev_customer_id is :{rev_customer_id}")
        rev_add_service_product = {
            'rev_customer_id': rev_customer_id,
            'product_id': rev_product_id,
            'rate': rate,
            'quantity': quantity,
            'prorate': prorate,
            'created_by': username,
            'is_active': True,
            'is_deleted': False,
            'effective_date': effective_date,
            'description': description
        }
        # Prepare data for API request
        # url = 'https://api.revioapi.com/v1/ServiceProduct'
        url = os.getenv("SERVICEPRODUCT", " ")
        headers = {
            'Ocp-Apim-Subscription-Key': '04e3d452d3ba44fcabc0b7085cdde431',
            'Authorization': 'Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r'
        }
        params = {
            "customer_id": rev_customer_id,
            "product_id": rev_product_id,
            "rate": rate,
            "quantity": quantity,
            "generate_proration": prorate,
            "effective_date": effective_date,
            "description": description,
        }
        # Call the API and check response
        api_response = requests.get(url, headers=headers, params=params)
        if api_response.status_code == 200:
            # Only insert into database if the API call is successful
            data_rev_service_product = {
                'customer_id': rev_customer_id,
                "product_id": rev_product_id,
                "rate": rate,
                "quantity": quantity,
                "prorate": prorate,
                "created_by": username,
                "is_active": True,
                "is_deleted": False,
                "created_date": effective_date,
                "description": description,
            }
            # Insert data into the database
            product_id=database.insert_data('rev_service_product', data_rev_service_product)
            message = "Add Service product data submitted successfully."
            response_data = {"flag": True, "message": message, "response": api_response.json()}
        else:
            # API call failed, return error message
            raise Exception(f'Failed to retrieve data from client API: {api_response.status_code} - {api_response.text}')
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response_data
    except Exception as e:
        logging.exception(f"Something went wrong and the error is {e}")
        message = "Something went wrong while submitting add service line"
        # Error Management
        error_data = {
            "service_name": 'SIM management',
            "created_date": start_time,
            "error_message": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": start_time
        }
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}
        
        


def assign_service_dropdown_data(data):
    '''
    Description: Retrieves add_service_product dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    logging.info(f"Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    # database Connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        response_data = {}
        rev_customer_name = data.get('customer_name', None)
        try:
            rev_customer_id=database.get_data('revcustomer',{'customer_name':rev_customer_name,'is_active':True},['id'])['id'].to_list()[0]
            rev_customer_id_real=database.get_data('revcustomer',{'customer_name':rev_customer_name,'is_active':True},['rev_customer_id'])['rev_customer_id'].to_list()[0]
            logging.debug(f"rev_customer_id is {rev_customer_id} and rev_customer_id_real is {rev_customer_id_real}")
        except:
            return  {"flag": True, "message": "No data", "response_data": response_data}
        rev_service_query=f'''SELECT CONCAT(rs.number,' - Service_id:',rs.rev_service_id,' - Service_type:',rst.description) AS combined_output,rs.rev_service_id FROM public.rev_service as rs join rev_service_type as rst on rst.service_type_id = rs.rev_service_type_id 
                            where rev_customer_id ='{rev_customer_id}' ORDER BY rs.id'''
        rev_service_df=database.execute_query(rev_service_query,True)
        response_data["rev_service"]=list(set(rev_service_df['combined_output'].to_list()))
        response_data["rev_service_id_map"]=rev_service_df.to_dict(orient='records')
        response_data["rev_service_id_map"]={item['combined_output']: item['rev_service_id'] for item in response_data["rev_service_id_map"]}
        #dependent dropdowns
        package_query=f'''SELECT distinct CONCAT(rp.description,' - package_id:',rp.package_id) AS combined_output,
                            rsp.service_id  from 
                            revcustomer as rc join rev_service as rs on rs.rev_customer_id = rc.id
                            join rev_service_product as rsp on rs.rev_service_id =rsp.service_id 
                            join rev_package AS rp ON rsp.package_id = rp.package_id::integer
                            WHERE rsp.is_active = TRUE
                                AND rsp.is_deleted = FALSE
                                AND rsp.integration_authentication_id = 1
                                AND rsp.status = 'ACTIVE'
                                AND rsp.package_id != 0
                                AND rc.rev_customer_id='{rev_customer_id_real}' '''
        response_data["rev_package"]=database.execute_query(package_query,True).to_dict(orient='records')
        response_data["rev_package"]=form_depandent_dropdown_format(response_data["rev_package"],'service_id','combined_output')
        package_query=f'''SELECT CONCAT(rp.description,' - product_id:',rsp.service_product_id) AS combined_output,
                            rsp.service_id  from 
                            revcustomer as rc join rev_service as rs on rs.rev_customer_id = rc.id
                            join rev_service_product as rsp on rs.rev_service_id =rsp.service_id 
                            join rev_product AS rp ON rsp.product_id = rp.product_id::integer
                            WHERE rsp.is_active = TRUE
                                AND rsp.is_deleted = FALSE
                                AND rsp.integration_authentication_id = 1
                                AND rsp.status = 'ACTIVE'
                                AND rsp.package_id != 0
                                AND rc.rev_customer_id='{rev_customer_id_real}' '''
        response_data["rev_product"]=database.execute_query(package_query,True).to_dict(orient='records')
        response_data["rev_product"]=form_depandent_dropdown_format(response_data["rev_product"],'service_id','combined_output')    
        message = "add_service_product data sent sucessfully"
        response = {"flag": True, "message": message, "response_data": response_data}
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response
    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting assign service line"
        # Error Management
        error_data = {"service_name": 'SIM management',
                      "created_date": start_time,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id,
                      "tenant_name": tenant_name,
                      "comments": message,
                      "module_name": module_name,
                      "request_received_at": start_time}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}
    

def submit_assign_service_data(data):
    '''
    Description: Submit assign_service_data dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    logging.info(f"Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    tenant_name = data.get('tenant_name', None)
    username = data.get('username', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    # database Connection
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    try:
        submit_data=data.get('submit_data',{})
        customer_id=database.get_data('revcustomer',{'customer_name':submit_data.get('customer_name',None),'is_active':True},['rev_customer_id'])['rev_customer_id'].to_list()[0]
        logging.debug(f"customer_id is {customer_id}")
        service_numbers=submit_data.get('service_number',None)
        effective_date=submit_data.get('effective_date',None)
        quantity=submit_data.get('quantity',0)
        package_id=None
        if submit_data['revio_package']:
            match2 = re.search(r'package_id:(\d+)', submit_data['revio_package'])
            if match2:
                package_id = match2.group(1)

        Service_id=None    
        if submit_data['service_type']:
            match2 = re.search(r'Service_id:(\d+)', submit_data['service_type'])
            if match2:
                Service_id = match2.group(1)
                
        product_ids=[]
        if submit_data['revio_product']:
            for product_id in submit_data['revio_product']:
                match2 = re.search(r'product_id:(\d+)', product_id)
                if match2:
                    product_id_ = match2.group(1)
                    product_ids.append(product_id_)
            
        try:
            hit_all_apis(customer_id,Service_id,service_numbers,package_id,product_ids,effective_date,quantity)
        except:
            pass

        message = " Assign Service data submitted sucessfully "
        response = {"flag": True, "message": message}
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response
    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting submitting assign service line"
        # Error Management
        error_data = {"service_name": 'SIM management',
                      "created_date": start_time,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id,
                      "tenant_name": tenant_name,
                      "comments": message,
                      "module_name": module_name,
                      "request_received_at": start_time}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}


def hit_all_apis(customer_id,Service_id,service_numbers,package_id,product_ids,effective_date,quantity):
    logging.info(f"Function recieved here")
    # BASE_URL = "https://api.revioapi.com/v1"
    BASE_URL = os.getenv("REVIOAPI", " ")
    headers = {
        'Content-Type': 'application/json',
        'Ocp-Apim-Subscription-Key':'04e3d452d3ba44fcabc0b7085cdde431',
        'Authorization':'Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r'
    }
    search_inventory_url = f"{BASE_URL}/ServiceProduct?search.service_id={Service_id}&search.status=ACTIVE"
    search_inventory_response = requests.get(search_inventory_url, headers=headers)
    logging.info(f"Search Inventory Response: {search_inventory_response.status_code}")
    service_product_response=search_inventory_response.json()
    service_products=service_product_response['records']
    service_products_by_package_id = [str(item['service_product_id']) for item in service_products if str(item['package_id']) == str(package_id)]
    logging.debug(service_products_by_package_id,'service_products_by_package_id')
    did_service_products = (service_products_by_package_id if product_ids is None else list(product_ids))
    inventory_list=[]
    for identifier in service_numbers:
        get_search_inventory_url = f"{BASE_URL}/InventoryItem?search.identifier={identifier}"
        get_search_inventory_response = requests.get(get_search_inventory_url, headers=headers)
        logging.debug(f"Get Search Inventory Response: {get_search_inventory_response.status_code}")
        logging.debug(get_search_inventory_response.json())
        inventory_items=get_search_inventory_response.json()
        if inventory_items['record_count']<=0:
            create_inventory_url = f"{BASE_URL}/InventoryItem"
            new_inventory_data={"inventory_type_id":20,"identifier":identifier,"customer_id":customer_id,"status":"AVAILABLE"}
            create_inventory_response = requests.post(create_inventory_url, json=new_inventory_data, headers=headers)
            logging.debug(f"Create Inventory Response: {create_inventory_response.status_code}")
            logging.debug(create_inventory_response.json())
            logging.debug("added")
            if create_inventory_response.status_code ==200 or create_inventory_response.status_code ==201:
                inventory_list.append(identifier)
        if inventory_items['records']:
            for record in inventory_items['records']:
                if record['status'] != "ASSIGNED":
                    inventory_list.append(identifier)  
        
    get_dids_url = f"{BASE_URL}/ServiceInventory?search.service_id={Service_id}"
    get_dids_response = requests.get(get_dids_url, headers=headers)
    logging.info(f"Get DIDs Response: {get_dids_response.status_code}")
    dids_response=get_dids_response.json()
    dids_count=dids_response['record_count']

    assign_inventory_url = f"{BASE_URL}/InventoryItem/assignService"
    assign_inventory_data = {"service_id": Service_id, "identifiers": inventory_list}  # Data required for assigning
    assign_inventory_response = requests.patch(assign_inventory_url, json=assign_inventory_data, headers=headers)
    logging.debug(f"Assign Inventory Response: {assign_inventory_response.status_code}")
    logging.info(assign_inventory_response.json())
    if assign_inventory_response.status_code ==200 or assign_inventory_response.status_code ==201:
        for service_product in service_products:
            if str(service_product["service_product_id"]) in did_service_products:
                patch_data = [
                    {"op": "replace", "path": "/quantity", "value": quantity},
                    {"op": "replace", "path": "/effective_date", "value": effective_date}
                ]
                # Serialize to JSON format
                json_patch = json.dumps(patch_data)
                update_request_headers = {
                    'Content-Type': 'application/json-patch+json',  # Equivalent to 'APPLICATION_JSON_PATCH'
                    'Ocp-Apim-Subscription-Key':'04e3d452d3ba44fcabc0b7085cdde431',
                    'Authorization':'Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r'
                }
                service_pro_id=int(service_product["service_product_id"])
                update_product_url = f"{BASE_URL}/ServiceProduct/{service_pro_id}"
                update_product_response = requests.patch(update_product_url, headers=update_request_headers, data=json_patch)

    return True

def get_bulk_change_logs(data):
    '''
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_id' for querying the status history.

    Returns:
    - dict: A dictionary containing the status history data, header mapping, and a success message or an error message.
    '''
    logging.info(f"Request Data Recieved")
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    session_id = data.get('session_id', ' ')
    # Start time  and date calculation
    start_time = time.time()
    try:
        # Initialize the database connection
        tenant_database = data.get('db_name', 'altaworx_central')
        # database Connection
        database = DB(tenant_database, **db_config)
        role_name = data.get('role_name', '')
        common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        tenant_database=data.get('db_name','altaworx_central')
        # Fetch the list_view_data_id from the input data
        list_view_data_id = data.get('list_view_data_id', '')
        if not list_view_data_id:
            raise ValueError("list_view_data_id is required")
        list_view_data_id = int(list_view_data_id)
        logging.debug(f"list_view_data_id is {list_view_data_id}")
        request_id=data.get('requested_id', '')
        request_id= int(request_id)
        bulk_change_data={}
        result = database.get_data('sim_management_bulk_change_request', {'bulk_change_id': list_view_data_id, 'id': request_id}, ['iccid', 'tenant_id']).to_dict(orient='records')


        iccid_data = result[0]
        service = database.get_data('sim_management_bulk_change', {'tenant_id': iccid_data['tenant_id'], 'is_active':True}, ['service_provider']).to_dict(orient='records')
        # Remove duplicates based on the 'service_provider' field
        if service:
            service_df = pd.DataFrame(service)  # Convert to DataFrame
            service_df = service_df.drop_duplicates(subset=['service_provider'])  # Remove duplicates
            service = service_df.to_dict(orient='records')  # Convert back to list of dictionaries
        # Check if ICCID exists in sim_management_inventory with active status
        # Fetch inventory data for ICCID with 'Active' status
        inventory_result = database.get_data(
            'sim_management_inventory',
            {'iccid': iccid_data['iccid'], 'sim_status': 'Active'},
            ['iccid', 'service_provider']
        )
        
        # Debugging: Print the raw result from the database
        print(f"Raw inventory result: {inventory_result}")
        
        if inventory_result is False or not isinstance(inventory_result, pd.DataFrame) or inventory_result.empty:
            # If ICCID is not found, add a note for missing ICCID
            bulk_change_data['note'] = f"The selected ICCID {iccid_data['iccid']} is not found in the inventory with an active status."
            print(bulk_change_data['note'])
        else:
            # Convert inventory data to dictionary
            inventory_dict = inventory_result.to_dict(orient='records')
            
            if inventory_dict:
                # Check for service provider in the inventory data
                service_provider = inventory_dict[0].get('service_provider')
                
                if not service_provider:
                    # If service provider is missing, add a note
                    bulk_change_data['note'] = f"The selected ICCID {iccid_data['iccid']} does not have an associated service provider in the inventory."
                else:
                    # Both ICCID and service provider are valid
                    bulk_change_data['service_provider'] = service_provider
                    
                    
        try:
            bulk_change_data.update(database.get_data('sim_management_bulk_change_request',{'bulk_change_id':list_view_data_id,'id':request_id}).to_dict(orient='records')[0])
        except:
            pass
        try:
            bulk_change_data.update(database.get_data('sim_management_bulk_change_log',{'bulk_change_id':list_view_data_id,'bulk_change_request_id':request_id}).to_dict(orient='records')[0])
        except:
            pass
   
        for key,value in bulk_change_data.items():
            if type(value)!=str:
                bulk_change_data[key] = str(value)
        # Generate the headers mapping
        headers_map = get_headers_mappings(tenant_database,["bulkchange_logs"],role_name, "username", "main_tenant_id", "sub_parent_module", "parent_module",data)
        # Prepare the response
        response = {"flag": True, "get_bulk_change_logs": [bulk_change_data], "header_map": headers_map}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "session_id": session_id,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the bulkchange log history data',
                                       "module_name": "get_bulk_change_logs",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response

    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        # Error Management
        error_data = {"service_name": 'Sim Management',
                      "created_date": request_received_at,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id, "tenant_name": Partner,
                      "comments": message,
                      "module_name": 'get_bulk_change_logs',
                      "request_received_at": request_received_at}
        common_utils_database.log_error_to_db(error_data, 'error_table')
        response = {"flag": False, "error": str(e)}
        return response

def form_depandent_dropdown_format(data,main_col,sub_col):
    logging.info(f"Function reached upto here")
    result = {}
    # Iterate over each item in the list
    for item in data:
        main_col_value = item[main_col]
        sub_col_value = item[sub_col]
        
        # If the service provider is already a key in the dictionary, append the change type to the list
        if main_col_value in result:
            result[main_col_value].append(sub_col_value)
        else:
            # If the service provider is not a key, create a new list with the change type
            result[main_col_value] = [sub_col_value]
    # Sort the change types for each service provider
    for key in result:
        result[key] = sorted(result[key])  # Sort the list of change types

    return result

def form_depandent_dropdown_format_bulk_change(data, main_col, sub_col):
    logging.info(f"Function reached upto here")
    result = {}
    
    # Iterate over each item in the list
    for item in data:
        main_col_value = item[main_col]
        sub_col_value = item[sub_col]
        
        # If the service provider is already a key in the dictionary, add the change type to the set (to avoid duplicates)
        if main_col_value in result:
            result[main_col_value].add(sub_col_value)  # Use set to eliminate duplicates
        else:
            # If the service provider is not a key, create a new set with the change type
            result[main_col_value] = {sub_col_value}  # Initialize as a set to ensure uniqueness

    # Convert the sets back to sorted lists
    for key in result:
        result[key] = sorted(result[key])  # Sort the set and convert it back to a list

    return result


def get_new_bulk_change_data(data):
    '''
    Description: Retrieves device history data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    # logging.info(f"Request Data: {data}")
    # Start time and date calculation
    start_time = time.time()
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    request_received_at = data.get('request_received_at', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    # Database connection
    tenant_database = data.get('db_name', 'altaworx_central')
    database = DB(tenant_database, **db_config)

    try:
        response_data = {}
        service_provider = data.get('service_provider', None)
        Change_type = data.get('change_type', None)
        logging.debug(f"Change_type is {Change_type}")
        Create_Service_Product = data.get('Create Service/Product', None)

        def sorted_unique(data_list):
            """Sort a list and remove None values."""
            return sorted(filter(None, set(data_list)))

        if Create_Service_Product:
            response_data["rev_customer_dropdown"] = sorted_unique(database.get_data('revcustomer', {'is_active': True}, concat_columns=['customer_name', 'rev_customer_id'])['concat_column'].to_list())
            response_data["rev_service_type"] = sorted_unique(database.get_data('rev_service_type', {'is_active': True}, concat_columns=['description', 'service_type_id'])['concat_column'].to_list())
            response_data["rev_product"] = sorted_unique(database.get_data('rev_product', {'is_active': True}, concat_columns=['description', 'product_id'])['concat_column'].to_list())
            message = "New Bulk change popups data sent successfully"
            response = {"flag": True, "message": message, "Modules": response_data}
            return response

        if not service_provider:
            response_data["new_change_SP_CT_map"] = sorted(database.get_data('sim_management_bulk_change_type_service_provider', {'is_active': True}, ['change_type', 'service_provider']).to_dict(orient='records'), key=lambda x: x['service_provider'] or '')
            response_data["new_change_SP_CT_map"] = form_depandent_dropdown_format_bulk_change(response_data["new_change_SP_CT_map"], 'service_provider', 'change_type')
        else:
            response_data['screen_names_seq'] = sorted(database.get_data('bulk_change_popup_screens', {"service_provider": service_provider, "change_type": Change_type}, ['screen_names_seq'])['screen_names_seq'].to_list())
            if response_data['screen_names_seq']:
                response_data['screen_names_seq'] = response_data['screen_names_seq'][0]
            dropdown_data = {}

            if Change_type == 'Assign Customer':
                dropdown_data["rev_customer_dropdown"] = sorted_unique(database.get_data('revcustomer', {'is_active': True}, concat_columns=['customer_name', 'rev_customer_id'])['concat_column'].to_list())
                dropdown_data["customer_rate_plan_dropdown"] = sorted_unique(database.get_data('customerrateplan', {'service_provider_name': service_provider, 'is_active': True}, concat_columns=['rate_plan_name', 'rate_plan_code'])['concat_column'].to_list())
                dropdown_data["customer_rate_pool_dropdown"] = sorted_unique(database.get_data('customer_rate_pool', {'is_active': True, 'service_provider_name': service_provider}, concat_columns=['name', 'id'])['concat_column'].to_list())
                dropdown_data["rev_service_type"] = sorted_unique(database.get_data('rev_service_type', {'is_active': True}, concat_columns=['description', 'service_type_id'])['concat_column'].to_list())
                dropdown_data["rev_product"] = sorted_unique(database.get_data('rev_product', {'is_active': True}, concat_columns=['description', 'product_id'])['concat_column'].to_list())
                dropdown_data["rev_provider"] = sorted(database.get_data('rev_provider', {'is_active': True}, ['description', 'provider_id']).to_dict(orient='records'), key=lambda x: x['description'] or '')
                dropdown_data["rev_package"] = form_depandent_dropdown_format(database.get_data('rev_package', {'is_active': True}, columns=['provider_id'], concat_columns=['description', 'package_id']).to_dict(orient='records'), 'provider_id', 'concat_column')

            elif Change_type == 'Activate New Service':
                dropdown_data["rev_customer_dropdown"] = sorted_unique(database.get_data('revcustomer', {'is_active': True}, concat_columns=['customer_name', 'rev_customer_id'])['concat_column'].to_list())
                dropdown_data["state"] = sorted(["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"])
                dropdown_data["customer_rate_plan_group"] = sorted_unique(database.get_data('mobility_device_usage_aggregate', {'is_active': True, 'service_provider': service_provider}, ['data_group_id'])['data_group_id'].to_list())
                result = database.get_data('mobility_feature', {'service_provider_name': service_provider, 'is_active': True}, concat_columns=['friendly_name', 'soc_code'])
                if result and 'concat_column' in result and isinstance(result, pd.DataFrame):
                    dropdown_data["features_codes"] = sorted_unique(result['concat_column'].to_list())
                else:
                    logging.error(f"Invalid result returned for mobility_feature query: {result}")
                    dropdown_data["features_codes"] = []
                dropdown_data["customer_rate_plan_dropdown"] = sorted_unique(database.get_data('customerrateplan', {'service_provider_name': service_provider, 'is_active': True}, concat_columns=['rate_plan_name', 'rate_plan_code'])['concat_column'].to_list())
                dropdown_data["customer_rate_pool_dropdown"] = sorted_unique(database.get_data('mobility_device_usage_aggregate', {'is_active': True, 'service_provider': service_provider}, ['pool_id'])['pool_id'].to_list())

            elif Change_type == 'Change Carrier Rate Plan':
                dropdown_data["carrier_rate_plan_dropdown"] = sorted_unique(database.get_data('carrier_rate_plan', {'service_provider': service_provider, 'is_active': True}, coalesce_columns=['friendly_name', 'rate_plan_code'])['coalesce_column'].to_list())
                dropdown_data["comm_plan_dropdown"] = sorted_unique(database.get_data('sim_management_communication_plan', {'service_provider_name': service_provider, 'is_active': True}, ['communication_plan_name'])['communication_plan_name'].to_list())
                dropdown_data["optimization_group_dropdown"] = sorted_unique(database.get_data('optimization_group', {'service_provider_name': service_provider, 'is_active': True}, ['optimization_group_name'])['optimization_group_name'].to_list())

            elif Change_type in ['Change Customer Rate Plan', 'Change ICCID/IMEI']:
                dropdown_data["customer_rate_plan_dropdown"] = sorted_unique(database.get_data('customerrateplan', {'service_provider_name': service_provider, 'is_active': True}, concat_columns=['rate_plan_name', 'rate_plan_code'])['concat_column'].to_list())
                dropdown_data["customer_rate_pool_dropdown"] = sorted_unique(database.get_data('customer_rate_pool', {'is_active': True, 'service_provider_name': service_provider}, concat_columns=['name', 'id'])['concat_column'].to_list())

            elif Change_type == 'Update Device Status':
                integration_id = database.get_data("serviceprovider", {"service_provider_name": service_provider}, ["integration_id"])["integration_id"].to_list()[0]
                dropdown_data["Device_Status_dropdown"] = sorted_unique(database.get_data('device_status', {'integration_id': integration_id, "allows_api_update": True, 'is_active': True}, ['display_name'])['display_name'].to_list())
                # Remove "Active" if it exists in the Device_Status_dropdown list
                if "Active" in dropdown_data["Device_Status_dropdown"]:
                    dropdown_data["Device_Status_dropdown"].remove("Active")
                    
                if service_provider in ['Verizon - ThingSpace PN', 'Verizon - ThingSpace IoT']:
                    dropdown_data["carrier_rate_plan_dropdown"] = sorted_unique(database.get_data('carrier_rate_plan', {'service_provider': service_provider, 'is_active': True}, coalesce_columns=['friendly_name', 'rate_plan_code'])['coalesce_column'].to_list())
                    dropdown_data["state"] = sorted(["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"])
                    dropdown_data["public_ip"] = sorted(["Restricted", "Unrestricted"])
                    dropdown_data["reason_code"] = sorted(["General Admin/Maintenance"])

            response_data['dropdown_data'] = dropdown_data
        message = "New Bulk change popups data sent successfully"
        response = {"flag": True, "message": message, "response_data": response_data}

        # End time calculation
        end_time = time.time()
        time_consumed = f"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        return response

    except Exception as e:
        logging.exception(f"Something went wrong and error is: {e}")
        message = "Something went wrong while getting New Bulk change popups"
        # Error Management
        error_data = {
            "service_name": 'SIM management',
            "created_date": request_received_at,
            "error_messag": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": module_name,
            "request_received_at": request_received_at
        }
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}



# Initialize the SNS client for sending alerts
sns_client = boto3.client('sns')

def send_sns_email(subject, message):
    """Send an email via SNS when an alert is triggered."""
    response = sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:YOUR_SNS_TOPIC_ARN', 
        Message=message,
        Subject=subject)
    # logging.info("SNS publish response:", response)
    return response

def bulk_change_lambda_caller(bulk_change_id="2833"):
    logging.info("bulk_change_lambda_caller function is called")
    # Assume the role from Account B
    sts_client = boto3.client('sts')
    assumed_role = sts_client.assume_role(
        RoleArn="arn:aws:iam::130265568833:role/LambdainvocationfromotherAWS",
        RoleSessionName="LambdaInvokeSession"
    )
    # Use the temporary credentials to invoke the Lambda in Account B
    credentials = assumed_role['Credentials']
    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name="us-east-1"
    )
    sqs_client = boto3.client(
        'sqs',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name="us-east-1"
    )
    # queue_url = "https://sqs.us-east-1.amazonaws.com/130265568833/DeviceBulkChange_TEST"
    queue_url = os.getenv("DEVICEBULKCHANGE_TEST", "")
    action_flag="queue"

    message_attributes = {
            "BulkChangeId": {
              "StringValue": bulk_change_id,
              "DataType": "String"
            },
            "AdditionBulkChangeId": {
              "StringValue": "0",
              "DataType": "String"
            }
          }
    message_body = "Not used"
    try:
        if action_flag == "Lambda":
            response = lambda_client.invoke(
                FunctionName="arn:aws:lambda:us-east-1:130265568833:function:lambdascsharp",
                InvocationType="RequestResponse"
            )
            # Check if the response indicates a failure
            if response['StatusCode'] != 200:
                raise Exception(f"Lambda invocation failed with status code: {response['StatusCode']}")

            # logging.info("response---", response)
            return response['Payload'].read().decode('utf-8')

        elif action_flag == 'queue':
            # Send the message to the SQS queue
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                DelaySeconds=0,
                MessageAttributes=message_attributes
            )
            logging.info("Message sent to SQS queue:", response)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Payload sent successfully to SQS queue',
                    'response': response
                })
            }
    except Exception as e:
        # Send alert if invocation fails
        subject = "Alert: Lambda Invocation Failed"
        message = f"An error occurred while invoking the Lambda function: {str(e)}"
        send_sns_email(subject, message)
        logging.info(f"Alert sent: {message}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Invocation failed',
                'message': str(e)
                })
            }


def run_db_script(data_all):
    logging.info("run db script is working here")
    data=data_all['data']
    bulk_chnage_id_20=data_all['bulk_chnage_id_20']
    tenant_database = data.get('db_name', 'altaworx_central')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        data_transfer=DataTransfer()
        
        bulk_change_id_10=data_transfer.save_data_to_10('bulk_change',data)
    except Exception as e:
        logging.exception(f"Error occurred while clling Data Transfer {e}")
    try:
        bulk_change_id=[]
        bulk_change_lambda_caller(str(bulk_change_id_10))
    except Exception as e:
        logging.warning(f"An error occurred: {e}")
        return False
    try:
        logging.info(f"After lamda calling")
        get_data_From_10=data_transfer.save_data_20_from_10(bulk_change_id_10,bulk_chnage_id_20,'bulk_change')
        response={"flag":True,"message":"Bulk change lambda called successfully"}
        return response
    except Exception as e:
        logging.exception(f"Error in Data Tranfer save_data_20_from_10 {e}")
        response={"flag":True,"message":"Bulk change lambda called Failed"}
        return response
    


def update_bulk_change_data(data):

    '''
    updates module data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''

    logging.info(f"Request Data Recieved")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', '')
    changed_data = data.get('changed_data', {})
    # database Connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    dbs = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # Start time  and date calculation
    start_time = time.time()
    try:
        response_data = {}
        tenant_name = data.get('tenant_name', None)
        tenant_id=dbs.get_data("tenant",{"tenant_name":tenant_name},["id"])["id"].to_list()[0]
        Change_type = data.get('change_type', None)
        logging.debug(f"Change_type is: {Change_type}")
        Change_type_id=database.get_data("sim_management_bulk_change_type",{"display_name":Change_type},["id"])["id"].to_list()[0]
        logging.debug(f"Change_type_id is: {Change_type_id}")
        service_provider = data.get('service_provider', None)
        service_provider_id=database.get_data("serviceprovider",{"service_provider_name":service_provider},["id"])["id"].to_list()[0]
        
        modified_by = data.get('modified_by', None)
        created_by = data.get('created_by', "test")
        changed_data = data.get('changed_data', None)
        # Determine ICCIDs based on Change_type
        if Change_type == "Assign Customer":
            iccids = changed_data.get('iccids', [])
        elif Change_type == "Archive":
            iccids = data.get('iccids', [])
        else:
            iccids = data.get('iccids', [])
        uploaded=len(iccids)
        if uploaded == 0:
            iccids=[None]

        return_dict={}
        create_new_bulk_change_dict={}
        create_new_bulk_change_dict["service_provider"]=service_provider
        create_new_bulk_change_dict["change_request_type_id"]=str(Change_type_id)
        create_new_bulk_change_dict["change_request_type"]=Change_type
        create_new_bulk_change_dict["service_provider_id"]=str(service_provider_id)
        create_new_bulk_change_dict["modified_by"]=modified_by
        create_new_bulk_change_dict["status"]="NEW"
        create_new_bulk_change_dict["uploaded"]=str(uploaded)
        create_new_bulk_change_dict["is_active"]="True"
        create_new_bulk_change_dict["is_deleted"]="False"
        create_new_bulk_change_dict["created_by"]=created_by
        create_new_bulk_change_dict["processed_by"]=created_by
        create_new_bulk_change_dict["tenant_id"]=str(tenant_id)
        return_dict["sim_management_bulk_change"]=[create_new_bulk_change_dict]
        change_id=database.insert_data(create_new_bulk_change_dict,"sim_management_bulk_change")
        bulkchange_df=database.get_data("sim_management_bulk_change",create_new_bulk_change_dict)
        bulkchange_id=bulkchange_df["id"].to_list()[0]
        logging.debug(bulkchange_id,'bulkchange_id')
        change_request=changed_data
        if Change_type == "Archive":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        elif Change_type == "Change Customer Rate Plan":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
            
        elif Change_type == "Update Device Status":
            change_request["UpdateStatus"] = change_request.get('UpdateStatus', '')
            change_request["Request"] = {}

        elif Change_type == "Assign Customer":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)

        elif Change_type == "Change Carrier Rate Plan":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)

        elif Change_type == "Edit Username/Cost Center":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)

        elif Change_type == "Change ICCID/IMEI":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)

        elif Change_type == "Activate New Service":
            change_request["ServiceProviderId"] = str(service_provider_id)
            change_request["ChangeType"] = str(Change_type_id)
        create_new_bulk_change_request_dict=[]
        for iccid in iccids:
            temp={}
            temp['iccid']=iccid
            temp['bulk_change_id']=str(bulkchange_id)
            temp['tenant_id']=str(tenant_id)
            temp["created_by"]=created_by
            temp["status"]="NEW"
            try:
                temp["device_id"]=str(database.get_data("sim_management_inventory",{"iccid":iccid},["id"])["id"].to_list()[0])
            except:
                temp["device_id"]=None
            temp["request_created_by"]=created_by
            temp["is_active"]="True"
            temp["is_deleted"]="False"
            temp["change_request"]=json.dumps(change_request)
            create_new_bulk_change_request_dict.append(temp)

        request_id=database.insert_data(create_new_bulk_change_request_dict,"sim_management_bulk_change_request") 
        return_dict["sim_management_bulk_change_request"]=create_new_bulk_change_request_dict
        bulk_change_row=bulkchange_df.to_dict(orient='records')[0]
        bulk_change_row['modified_date']=str(bulk_change_row['modified_date'])
        bulk_change_row['created_date']=str(bulk_change_row['created_date'])
        bulk_change_row['processed_date']=str(bulk_change_row['processed_date'])
        response_data={"flag": True, "message": "Sucessfull New Bulk Change Request Has Been Inserted" , "bulkchange_id":bulk_change_row}
        # End time calculation
        end_time = time.time()
        end_time_str=f"{time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
        time_consumed=F"{end_time - start_time:.4f}"
        audit_data_user_actions = {"service_name": 'Sim Management',
                                    "created_date": start_time,
                                    "created_by": username,
                                    "status": str(response_data['flag']),
                                    "time_consumed_secs": time_consumed,
                                    "session_id": session_id,
                                    "tenant_name": tenant_name,
                                    "comments": json.dumps(changed_data),
                                    "module_name": "update_bulk_change_data",
                                    "request_received_at": request_received_at
        }
        dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
        response_data["data"]=return_dict
        response_data["bulk_chnage_id_20"]=bulkchange_id
        return response_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'Sim Management',"created_date": start_time,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": tenant_name,"comments": "","module_name": "update_bulk_change_data","request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except:
            pass
        return response
    


def get_bulk_change_history(data):
    '''
    Description: Fetches the bulk change history from the database for a given `list_view_data_id`.
    Converts date fields to a serialized string format and returns the data along with header mappings.

    Parameters:
    - data (dict): A dictionary containing the request data, including `list_view_data_id`.

    Returns:
    - response (dict): A dictionary containing a flag for success, the bulk change history data, and header mappings.
    '''
    logging.info(f"Request Data Recieved")
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    session_id = data.get('session_id', ' ')
    # Start time  and date calculation
    start_time = time.time()
    try:
        # Initialize the database connection
        tenant_database=data.get('db_name','altaworx_central')
        role_name = data.get('role_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        # Fetch the list_view_data_id from the input data
        list_view_data_id = data.get('list_view_data_id', '')
        list_view_data_id = int(list_view_data_id)
        logging.debug(f"list_view_data_id is :{list_view_data_id}")
        # Query the database and fetch the required data
        columns=["created_by", "iccid", "status", "processed_date", "processed_by", "created_date","id","change_request","status_details"]
        records = database.get_data("sim_management_bulk_change_request", {'bulk_change_id': list_view_data_id},columns)
        # Convert the DataFrame to a list of dictionaries
        sim_management_bulk_change_history_dict = records.to_dict(orient='records')
        # Define a function to serialize dates
        def serialize_dates(records):
            for record in records:
                if 'processed_date' in record and record['processed_date']:
                    record['processed_date'] = record['processed_date'].strftime('%m-%d-%Y %H:%M:%S')
                if 'created_date' in record and record['created_date']:
                    record['created_date'] = record['created_date'].strftime('%m-%d-%Y %H:%M:%S')
            return records
        # Serialize dates in all records
        sim_management_bulk_change_history_dict = serialize_dates(sim_management_bulk_change_history_dict)
        # Generate the headers mapping
        headers_map = get_headers_mappings(tenant_database,["Bulk Change History"], role_name, "username", "main_tenant_id", "sub_parent_module", "parent_module",data)
        # Prepare the response
        response = {"flag": True, "status_history_data": sim_management_bulk_change_history_dict, "header_map": headers_map}
        try:
            # Preparing audit data to log user actions
            # End time calculation
            end_time = time.time()
            end_time_str=f"{time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
            time_consumed=F"{end_time - start_time:.4f}"

            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "session_id": session_id,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the bulk change  history data',
                                       "module_name": "get_bulk_change__history",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while fetching the bulk change history data"
        # Error Management
        error_data = {"service_name": 'Module_api',
                      "created_date": request_received_at,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id, "tenant_name": Partner,
                      "comments": message,
                      "module_name": 'get_status_history',
                      "request_received_at": request_received_at
                      }
        common_utils_database.log_error_to_db(error_data, 'error_table')
        response = {"flag": False, "error": str(e)}
        return response

def inventory_dropdowns_data(data):
    '''
    Retrieves dropdown data based on the given parameters and returns a response 
    with rate plan and communication plan lists.

    Parameters:
    - data (dict): Dictionary containing the dropdown type and list view data ID.

    Returns:
    - dict: A dictionary with a flag indicating success and lists of rate plans
    and communication plans.
    '''
    # Start time  and date calculation
    #start_time = time.time()
    common_utils_database = DB('common_utils', **db_config)
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config) 
    # Extract parameters from the input data
    dropdown = data.get('dropdown', '')
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', '')
    print("Retrieving service provider ID.")
    # Retrieve service provider ID based on the list view data ID
    list_view_data_id = data.get('list_view_data_id', '')
    service_provider_id = data.get('service_provider_id', '')
    print(f"Service provider ID retrieved: {service_provider_id}")
    try:
        if dropdown == 'Carrier Rate Plan':
            # Retrieve rate plans for the specified service provider
            rate_plan_list_data = database.get_data(
                "carrier_rate_plan", 
                {'service_provider_id': service_provider_id},  # Use the retrieved service_provider_id
                ['rate_plan_code']
            )
            
            # Convert the DataFrame column to a list
            rate_plan_list = rate_plan_list_data['rate_plan_code'].to_list()
            print(rate_plan_list)
            
            # Check if there are any rate plans to process
            if not rate_plan_list:
                print("No rate plans found.")
            else:
                # Prepare the query to get all communication plans for the rate plans in one go
                placeholders = ', '.join(['%s'] * len(rate_plan_list))
                query = f"""
                SELECT carrier_rate_plan_name, communication_plan_name 
                FROM public.smi_communication_plan_carrier_rate_plan 
                WHERE carrier_rate_plan_name IN ({placeholders});
                """
                
                # Execute the query with the rate_plan parameters
                df = database.execute_query(query, params=rate_plan_list)
                
                # Initialize an empty dictionary to store communication plans
                communication_plans_dict = {}
            
                # Check if df is a DataFrame and has results
                if isinstance(df, pd.DataFrame) and not df.empty:
                    for _, row in df.iterrows():
                        plan_name = row['carrier_rate_plan_name']
                        comm_plan_name = row['communication_plan_name']
                        if plan_name not in communication_plans_dict:
                            communication_plans_dict[plan_name] = []
                        communication_plans_dict[plan_name].append(comm_plan_name)
                else:
                    print("No results found or query failed.")
            
                # Now you can work with the communication_plans_dict as needed
                print(communication_plans_dict)
                # Prepare and return the response
                message = "Dropdown data fetched successfully"
                response = {"flag": True, 
                            "communication_rate_plan_list": communication_plans_dict,
                            "message": message}
                return response
        else:
            try:
                # Retrieve rate plans for the specified service provider
                # rate_plan_list_data = database.get_data(
                #     "customerrateplan", 
                #     {'service_provider_id': service_provider_id,"is_active":True},  # Use the retrieved service_provider_id
                #     ['rate_plan_code'],distinct=True
                # )
                rate_plan_query=f"select distinct rate_plan_code from customerrateplan where service_provider_id='{service_provider_id}' and is_active=True"
                rate_plan_list_data=database.execute_query(rate_plan_query,True)
                # Convert the DataFrame column to a list
                rate_plan_list = rate_plan_list_data['rate_plan_code'].to_list()
                rate_pool_list_data = database.get_data(
                    "customer_rate_pool", 
                    {'service_provider_id': service_provider_id},  # Use the retrieved service_provider_id
                    ['name']
                )

                # Convert the DataFrame column to a list
                rate_pool_list = rate_pool_list_data['name'].to_list()
                
                # Prepare and return the response
                message = "Dropdown data fetched successfully"
                response = {"flag": True, 
                            "rate_pool_list": rate_pool_list,
                            "rate_plan_list": rate_plan_list,
                            "message": message}
                return response

            except Exception as e:
                # Handle any exceptions that occur in the else block
                logging.exception("Error occurred while fetching rate plan or rate pool data.")
                message = "Error occurred while fetching rate plan or rate pool data."
                response = {"flag": False, 
                            "rate_pool_list": [],
                            "rate_plan_list": [],
                            "message": message}
                return response
    except Exception as e:
        logging.exception(e)
        # Prepare and return the response
        message = "Dropdown data fetched successfully"
        response = {"flag": True, 
                    "communication_rate_plan_list": {},
                    "message": message}
        return response


def base64_encode(plain_text):
    # Convert the plain text to bytes, then encode it in Base64
    plain_text_bytes = plain_text.encode("utf-8")
    base64_encoded = base64.b64encode(plain_text_bytes).decode("utf-8")
    return base64_encoded

def base64_decode(base64_encoded_data):
    # Decode the Base64 string to bytes
    base64_encoded_bytes = base64.b64decode(base64_encoded_data)
    # Convert the bytes back to a UTF-8 string
    decoded_string = base64_encoded_bytes.decode("utf-8")
    return decoded_string


def update_username_for_iccid(iccid, account_custom9, api_username, api_password):
    # Encode the authorization credentials
    auth_header=base64_encode(f"{api_username}:{base64_decode(api_password)}")
    
    print(auth_header)
    # Define URL and headers
    url = f"https://restapi19.att.com/rws/api/v1/devices/{iccid}"
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Accept": "application/json"
    }
    
    # Define the request data
    data = {
        "accountCustom9": account_custom9
    }
    
    # Send the request
    try:
        response = requests.put(url, headers=headers, json=data)
        
        # Check for a successful response
        if response.status_code in [200, 202]:  # 200 or 204 indicates success
            print(f"Username for ICCID {iccid} updated successfully.")
            return True
        else:
            print(f"Failed to update username for ICCID {iccid}: {response.status_code} - {response.text}")
            return False
    
    except Exception as e:
        print(f"Error while updating username for ICCID {iccid}: {str(e)}")
        return False


def send_revio_request(service_id,username,password,token):

    auth_token=base64_encode(f"{username}:{base64_decode(password)}")
    subscription_key=base64_decode(token)
    print(auth_token,subscription_key)
    
    url = f'https://api.revioapi.com/v1/Services/{service_id}'
    
    headers = {
        "Authorization": f"Basic {auth_token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful
        if response.status_code == 200:
            print("Request successful:", response.json())
            return True
        else:
            print(f"Failed with status code {response.status_code}: {response.text}")
            return False
    
    except Exception as e:
        print(f"Error during request: {str(e)}")
        return False
    

def send_rate_plan_update_request(service_provider, iccid, eid, plan_uuid, rate_plan, communication_plan, username, password, client_id=None, client_secret=None):
    headers = {
        "Accept": "application/json"
    }
    data = {}
    url = ""

    if service_provider in ["1", "TMobile", "8", "Rogers"]:
        # Prepare the URL and headers
        url = f"https://restapi19.att.com/rws/api/v1/devices/{iccid}"
        auth_header = base64_encode(f"{username}:{base64_decode(password)}")
        headers["Authorization"] = f"Basic {auth_header}"
        
        # Prepare the data payload
        data = {
            "ratePlan": rate_plan,
            "communicationPlan": communication_plan,
            "iccid": iccid
        }

    elif service_provider in ["11"]:
        # Prepare the URL with request ID
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")# utc time stamp needed here
        url = f"https://integrationapi.teal.global/api/v1/esims/assign-plan?requestId=AssignRatePlan_{timestamp}"
        
        # Set headers with API Key and API Secret
        headers["ApiKey"] =  base64_decode(base64_encode(username))
        headers["ApiSecret"] = base64_decode(password)

        # Data payload specific to Teal
        data = {
              "entries": [
                {
                  "eid": eid,
                  "planUuid": plan_uuid
                }
              ]
            }

    elif service_provider in ["12","4","5"]:
        # Step 1: Generate OAuth Token
        auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        token_headers = {
            "Authorization": f"Basic {auth_header}",
            "Accept": "application/json"
        }
        token_data = {
            "grant_type": "client_credentials"
        }
        token_url = "https://thingspace.verizon.com/api/ts/v1/oauth2/token"
        
        token_response = requests.post(token_url, headers=token_headers, data=token_data)
        token_response_json = token_response.json()
        access_token = token_response_json.get("access_token")
        
        # Step 2: Generate Session Token
        session_url = "https://thingspace.verizon.com/api/m2m/v1/session/login"
        session_data = {
            "username": username,
            "password": base64_decode(password)
        }
        session_response = requests.post(session_url, headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"}, json=session_data)
        session_response_json = session_response.json()
        session_token = session_response_json.get("sessionToken")
        
        # Step 3: Prepare Final API Request to update rate plan
        url = "https://thingspace.verizon.com/api/m2m/v1/devices/actions/plan"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "VZ-M2M-Token": session_token
        }
        data = {
            "devices": [
                {
                    "deviceIds": [
                        {
                            "kind": "iccid",
                            "id": iccid
                        }
                    ]
                }
            ],
            "servicePlan": rate_plan
        }
    else:
        return True

    # Send the HTTP request
    try:
        print(headers,'headers')
        print(data,'data')
        print(url,'url')
        if service_provider == "11":
            response = requests.post(url, headers=headers, json=data)
        else:
            response = requests.put(url, headers=headers, json=data)

        # Check for successful response
        if response.status_code in [200, 201 ,202]:
            print(f"Successfully updated rate plan for ICCID {iccid}")
            return True
        else:
            print(f"Failed to update rate plan for ICCID {iccid}: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"Error sending request for service provider {service_provider}: {str(e)}")
        return False
    

def hit_lambda(file_id,username):

    try:
        # Assume role to get temporary credentials
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn="arn:aws:iam::130265568833:role/LambdainvocationfromotherAWS",
            RoleSessionName="LambdaInvokeSession"
        )

        # Extract temporary credentials
        credentials = assumed_role['Credentials']

        # Create SQS client with temporary credentials
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name="us-east-1"
        )

        # Prepare the SQS request body
        queue_url = "https://sqs.us-east-1.amazonaws.com/130265568833/JasperDeviceStatusChange_TEST"
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=f"File to work is {file_id}",
            DelaySeconds=0,
            MessageAttributes={
                "FileId": {
                    "DataType": "String",
                    "StringValue": file_id
                },
                "ModifiedBy": {
                    "DataType": "String",
                    "StringValue": username
                }
            }
        )

        # Print the response from SQS
        print("SQS Message Sent:", response)
        
        return True
    except Exception as e:
        print(f' exception is {e}')
        return False


def send_sqs_message(queue_url, entity_id):
    """
    Sends a message to the specified SQS queue.

    :param queue_name: Name of the SQS queue.
    :param entity_id: The entity ID to be sent in the message attributes.
    """
    try:
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn="arn:aws:iam::130265568833:role/LambdainvocationfromotherAWS",
            RoleSessionName="LambdaInvokeSession"
        )

        # Extract temporary credentials
        credentials = assumed_role['Credentials']

        # Create SQS client with temporary credentials
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name="us-east-1"
        )

        # Send the message
        sqs_client.send_message(
            QueueUrl=queue_url,
            DelaySeconds=0,
            MessageAttributes={
                'MobilityLineConfigurationQueueId': {
                    'DataType': 'String',
                    'StringValue': entity_id
                }
            },
            MessageBody='Not used'
        )

        print(f"Message sent successfully to {queue_url} with Entity ID: {entity_id}")
        return True

    except Exception as e:
        print(f"Failed to send message: {str(e)}")
        return False



def update_inventory_data(data):
    '''
    updates module data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''
    logging.info(f"Request Data Recieved")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)

        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)

        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        logging.warning(f"got exception in the restriction")
    
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', '')
    module_name = data.get('module_name', '')
    changed_data = data.get('changed_data', {})
    # logging.info(changed_data, 'changed_data')
    unique_id = changed_data.get('id', None)
    table_name = data.get('table_name', '')
    change_event_type = data.get('change_event_type', '')
    old_data=data.get('old_data', {})
    ##Database connection
    tenant_database = data.get('db_name', '')
    template_name = data.get('template_name', '')
    role = data.get('role_name', '')
    # database Connection
    db = DB(tenant_database, **db_config)
    dbs = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    
    # Start time  and date calculation
    start_time = time.time()
    
    try:
        # Ensure unique_id is available
        if unique_id is not None:
            history = data.get('history', '')

            service_provider=history['service_provider']

            serviceprovider_data=db.get_data("serviceprovider", {'service_provider_name': service_provider}, ['integration_id','id']
                )
            integration_id=serviceprovider_data['integration_id'].to_list()[0]
            service_provider_id=serviceprovider_data['id'].to_list()[0]
            
            authentication_type=db.get_data("integration", {'name': service_provider}, ['authentication_type']
                )['authentication_type'].to_list()[0]
            
            autentication_details=db.get_data("integration_authentication", {'authentication_type': authentication_type,'integration_id':integration_id}
                ).to_dict(orient='records')[0]
            
            username_api=autentication_details['username']
            password=autentication_details['password']
            client_id=autentication_details['oauth2_client_id']
            client_secret=autentication_details['oauth2_client_secret']
            token=autentication_details['token_value']
            
            processd_flag=False
            if change_event_type =='Edit Cost Center':

                iccid=changed_data.get('iccid', None)
                query=f'''select smi.device_id, smi.mobility_device_id,dt.rev_service_id as m_rev_service_id,mdt.rev_service_id as d_rev_service_id from sim_management_inventory smi
                left join device_tenant dt on dt.device_id =smi.device_id
                left join mobility_device_tenant mdt on mdt.mobility_device_id =smi.mobility_device_id
                where iccid="{iccid}"'''
                service_id_data=db.execute_query(query,True)
                m_rev_service_id=service_id_data['m_rev_service_id'].to_list()[0]
                d_rev_service_id=service_id_data['d_rev_service_id'].to_list()[0]

                if m_rev_service_id:
                    res=send_revio_request(m_rev_service_id,username_api,password,token)
                    if res:
                        processd_flag=True
                if d_rev_service_id:
                    res=send_revio_request(d_rev_service_id,username_api,password,token)
                    if res:
                        processd_flag=True

            elif change_event_type =='Update Status':
                
                iccid=old_data.get('iccid', None)
                file_id = db.get_data('device_status_uploaded_file_detail', {'iccid': iccid}, ['uploaded_file_id'])['uploaded_file_id'].to_list()[0]
                username_api = username        # Username initiating the status change

                if hit_lambda(str(file_id),username):
                    processd_flag=True

            elif change_event_type =='Update features':
                
                iccid=old_data.get('iccid', None)
                file_id = db.get_data('device_status_uploaded_file_detail', {'iccid': iccid}, ['uploaded_file_id'])['uploaded_file_id'].to_list()[0]
                queue_name = "https://sqs.us-east-1.amazonaws.com/130265568833/MobilityLineConfigurationQueue_TEST"
                entity_id = "123456"  # Replace with the actual Entity ID
                if send_sqs_message(queue_name, entity_id):
                    processd_flag=True
                            
            elif change_event_type =='Update Carrier Rate Plan':
                
                iccid=changed_data.get('iccid', None)
                carrier_rate_plan=changed_data.get('carrier_rate_plan', None)
                communication_plan=changed_data.get('communication_plan', None)
                eid=old_data.get('eid', None)
                plan_uuid=db.get_data("carrier_rate_plan", {'rate_plan_code': carrier_rate_plan}, ['plan_uuid']
                )['plan_uuid'].to_list()[0]

                if send_rate_plan_update_request(service_provider_id, iccid, eid, plan_uuid, carrier_rate_plan, communication_plan, username, password, client_id, client_secret):
                    processd_flag=True

            elif change_event_type =='Update Username':

                iccid=changed_data.get('iccid', None)
                account_custom9=changed_data.get('username', None)
                if update_username_for_iccid(iccid, account_custom9, username_api, password):
                    processd_flag=True

            # Prepare the update data
            if not processd_flag:
                raise ValueError("Unable to process due to api failure")
            
            update_data = {key: value for key, value in changed_data.items() if key != 'unique_col' and key != 'id'}
            # Perform the update operation
            db.update_dict(table_name, update_data, {'id': unique_id})
            logging.info('edited successfully')
            message = f"Data Edited Successfully"
            tenant_database = data.get('db_name', '')
            # Example usage
            db = DB(database=tenant_database, **db_config)
            history = data.get('history', '')
            try:
                logging.info('history',history)
                # Call the method
                history_id=db.insert_data(history, 'sim_management_inventory_action_history')
            except Exception as e:
                logging.exception(f"Exception is {e}")

            # Check if the change event type is "Update Status" or "Update Username"
            change_event_type = data.get('change_event_type', '')
            logging.info(f"Change Event Type: {change_event_type}")
            
            if change_event_type in ['Update Status', 'Update Username']:
                try:
                    # Send email notification if change_event_type matches
                    to_emails, cc_emails, subject, body, from_email, partner_name = send_email(template_name, id=unique_id)
                    dbs.update_dict("email_templates", {"last_email_triggered_at": request_received_at}, {"template_name": template_name})
                    
                    # Auditing the email notifications
                    email_audit_data = {
                        "template_name": template_name,
                        "email_type": 'Application',
                        "partner_name": partner_name,
                        "email_status": 'success',
                        "from_email": from_email,
                        "to_email": to_emails,
                        "cc_email": cc_emails,
                        "action": "Email triggered",
                        "comments": 'update inventory data',
                        "subject": subject,
                        "body": body,
                        "role": role
                    }
                    dbs.update_audit(email_audit_data, 'email_audit')
                    logging.info("Email notification audited successfully.")
                except Exception as e:
                    logging.exception(f"An error occurred during email notification: {e}")
            
            # Preparing success response
            response_data = {"flag": True, "message": "Data edited successfully and history updated."}
            
            # Audit log
            end_time = time.time()
            time_consumed = int(float(f"{end_time - start_time:.4f}"))
            audit_data_user_actions = {
                "service_name": 'Module Management',
                "created_date": request_received_at,
                "created_by": username,
                "status": str(response_data['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_inventory_data",
                "request_received_at": request_received_at
            }
            dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
            return response_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        # Error response and logging
        response = {"flag": False, "message": "Unable to save the data"}
        error_type = type(e).__name__
        try:
            # Log error to database
            error_data = {"service_name": 'update_superadmin_data',
                          "created_date": request_received_at
                          ,"error_message": message,"error_type": error_type,
                          "users": username,"session_id": session_id,"tenant_name": Partner,
                          "comments": "","module_name": "Module Managament",
                          "request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except:
            pass
        return response





def sim_order_form_mail_trigger(data):
    '''
    Description: Triggers an email with SIM order form details and inserts the order data into the database.

    Parameters:
    - data (dict): A dictionary containing the SIM order form details and other related information.

    Returns:
    - response (dict): A dictionary indicating the success or failure of the operation.
    '''
    logging.info(f"Request Data Receieved")
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    module_name = data.get('module_name', None)
    session_id = data.get('session_id', None)
    template_name = data.get('template_name', 'Sim Order Form')
    role = data.get('role', '')
    start_time = time.time()
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:
        sim_order_data = data.get('sim_order_data', {})
        logging.debug(f"Sim order data is {sim_order_data}")
        sim_order_data['sim_info'] = json.dumps(sim_order_data['sim_info'])
        sim_order_id=database.insert_data(sim_order_data, 'sim_order_form')
        # Sending email
        result = send_email(template_name, username=username, data_dict=sim_order_data)
        logging.debug(f"Mail sent successfully")
        if isinstance(result, dict) and result.get("flag") is False:
            logging.debug(f"result is {result}")
            message = "Failed to send email."
        else:
            to_emails, cc_emails, subject, body, from_email, partner_name = result
            if not to_emails or not subject:
                raise ValueError("Email template retrieval returned invalid results.")

            common_utils_database.update_dict("email_templates", {"last_email_triggered_at": request_received_at}, {"template_name": template_name})
            query = """
                SELECT parents_module_name, sub_module_name, child_module_name, partner_name
                FROM email_templates
                WHERE template_name = %s
            """

            params = [template_name]
            # Execute the query and fetch the result
            email_template_data = common_utils_database.execute_query(query, params=params)
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
            email_audit_data = {
                "template_name": template_name,
                "email_type": 'Application',
                "partner_name": partner_name,
                "email_status": 'success',
                "from_email": from_email,
                "to_email": to_emails,
                "cc_email": cc_emails,
                "comments": 'update sim order data',
                "subject": subject,
                "body": body,
                "role": role,
                "action": "Email triggered",
                "parents_module_name":parents_module_name,
                "sub_module_name":sub_module_name,
                "child_module_name":child_module_name
            }
            common_utils_database.update_audit(email_audit_data, 'email_audit')

        return {"flag": True, "message": "Email sent successfully"}

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        error_data = {
            "service_name": 'Sim Management',
            "created_date": start_time,
            "error_messag": message,
            "error_type": str(e),
            "user": username,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": message,
            "module_name": 'sim_order_form_mail_trigger',
            "request_received_at": start_time
        }
        try:
            common_utils_database.log_error_to_db(error_data, 'error_table')
        except Exception as db_error:
            logging.warning(f"Error inserting data into error_table: {db_error}")
        response = {"flag": False, "error": str(e)}
        return response


def convert_booleans(data):
    for key, value in data.items():
        if isinstance(value, str) and value.lower() == "true":
            data[key] = True
        elif isinstance(value, str) and value.lower() == "false":
            data[key] = False
        elif isinstance(value, dict):  # Recursively process nested dictionaries
            convert_booleans(value)
    return data  # Return the modified dictionary


def update_sim_management_modules_data(data):

    '''
    updates module data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''

    logging.info(f"Request Data Recieved")
    data=convert_booleans(data)
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    # try:
    #     # Create an instance of the PermissionManager class
    #     permission_manager_instance = PermissionManager(db_config)

    #     # Call the permission_manager method with the data dictionary and validation=True
    #     result = permission_manager_instance.permission_manager(data, validation=True)

    #     # Check the result and handle accordingly
    #     if isinstance(result, dict) and result.get("flag") is False:
    #         return result
    #     else:
    #         # Continue with other logic if needed
    #         pass
    # except Exception as e:
    #     logging.exception(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', '')
    module_name = data.get('module', '')
    logging.debug(module_name, 'module_name')
    changed_data = data.get('changed_data', {})
    new_data=data.get('new_data', {})
    new_data = {k: v for k, v in new_data.items() if v != ""}
    # Start time  and date calculation
    start_time = time.time()
    unique_id = changed_data.get('id', None)
    logging.debug(unique_id, 'unique_id')
    table_name = data.get('table_name', '')
    action = data.get('action', '')
    try:
        if module_name == 'Feature Codes':
            if isinstance(new_data['feature_codes'], str):
                try:
                    new_data['feature_codes'] = json.loads(new_data['feature_codes'])
                except json.JSONDecodeError:
                    logging.info("Error: feature_codes could not be converted to a list")
                    new_data['feature_codes'] = []

        if module_name == 'Optimization Group':
            if isinstance(new_data['rate_plans_list'], str):
                try:
                    new_data['rate_plans_list'] = json.loads(new_data['rate_plans_list'])
                except json.JSONDecodeError:
                    logging.info("Error: rate_plans_list could not be converted to a list")
                    new_data['rate_plans_list'] = []

        new_data['rate_plans_list'] = json.dumps(new_data['rate_plans_list'])

    except Exception as e:
        logging.warning(f"An error occurred while processing the data: {e}")

    try:
        if module_name=='Optimization Group':
            try:
                # Convert rate_plans_list from string to list if needed
                if isinstance(changed_data['rate_plans_list'], str):
                    changed_data['rate_plans_list'] = ast.literal_eval(changed_data['rate_plans_list'])
                # Ensure it's a list
                if not isinstance(changed_data['rate_plans_list'], list):
                    changed_data['rate_plans_list'] = []
            except (ValueError, SyntaxError):
                logging.warning("Error: rate_plans_list could not be converted to a list")
                changed_data['rate_plans_list'] = []

        # Convert carrier_rate_plans to JSON string if needed for database storage
        if module_name=='Optimization Group':
            changed_data['rate_plans_list'] = json.dumps(changed_data['rate_plans_list'])
    except:
        pass


    if module_name=='Comm Plan':
        try:
            # Convert carrier_rate_plans from string to list if needed
            if isinstance(changed_data['carrier_rate_plans'], str):
                changed_data['carrier_rate_plans'] = ast.literal_eval(changed_data['carrier_rate_plans'])
            # Ensure it's a list
            if not isinstance(changed_data['carrier_rate_plans'], list):
                changed_data['carrier_rate_plans'] = []
        except Exception as e:
            logging.warning("Error: carrier_rate_plans could not be converted to a list")
            changed_data['carrier_rate_plans'] = []

    # Convert carrier_rate_plans to JSON string if needed for database storage
    if module_name=='Comm Plan':
        changed_data['carrier_rate_plans'] = json.dumps(changed_data['carrier_rate_plans'])
    try:
        if module_name=='Feature Codes':
            try:
                # Convert carrier_rate_plans from string to list if needed
                if isinstance(changed_data['feature_codes'], str):
                    changed_data['feature_codes'] = ast.literal_eval(changed_data['feature_codes'])
                # Ensure it's a list
                if not isinstance(changed_data['feature_codes'], list):
                    changed_data['feature_codes'] = []
            except Exception as e:
                logging.warning("Error: carrier_rate_plans could not be converted to a list")
                changed_data['feature_codes'] = []

        # Convert carrier_rate_plans to JSON string if needed for database storage
        if module_name=='Feature Codes':
            changed_data['feature_codes'] = json.dumps(changed_data['feature_codes'])
    except Exception as e:
        logging.warning(f"Error is {e}")
    tenant_database = data.get('db_name', '')
    # database Connection
    db = DB(database=tenant_database, **db_config)
    dbs = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # Start time  and date calculation
    start_time = time.time()
    try:
        if action == 'create':
            new_data = {k: v for k, v in new_data.items() if v is not None and v != "None"}
            insert_id=db.insert_data(new_data, table_name)
        else:
            # Ensure unique_id is available
            if unique_id is not None:
                # Filter out values that are None or "None"
                changed_data = {k: v for k, v in changed_data.items() if v is not None and v != "None"}
                
                # # Check if 'is_deleted' is True
                # if changed_data.get('is_deleted') == True:
                #     # Query sim_management_inventory to check sim_status
                #     # Retrieve all `sim_status` values for the specified service provider
                #     sim_status_records = db.get_data("sim_management_inventory", {"service_provider": changed_data.get("service_provider_name")}, ['sim_status'])

                #     # Check if any of the `sim_status` values is "Activated"
                #     if "Activated" in sim_status_records['sim_status'].values:
                #         logging.info("Update aborted: sim_status is 'Activated'")
                #         message = "Update aborted: Cannot delete because one or more sim_status is 'Activated'"
                #         response_data = {"flag": True, "message": message}
                #         return response_data


                # Prepare the update data excluding unique columns
                update_data = {key: value for key, value in changed_data.items() if key != 'unique_col' and key != 'id'}
                
                # Perform the update operation
                db.update_dict(table_name, update_data, {'id': unique_id})

        logging.info('Action Done successfully')
        message = f"{action} Successfully"
        response_data = {"flag": True, "message": message}

        
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        audit_data_user_actions = {"service_name": 'Module Management',
                                   "created_date": request_received_at,
                                   "created_by": username,
                                   "status": str(response_data['flag']),
                                   "time_consumed_secs": time_consumed,
                                   "session_id": session_id,
                                   "tenant_name": Partner,
                                   "comments": json.dumps(changed_data),
                                   "module_name": "update_superadmin_data",
                                   "request_received_at": request_received_at
                                   }
        dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
        return response_data
    except Exception as e:
        logging.info(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_superadmin_data',
                          "created_date": request_received_at,"error_message": message,
                          "error_type": error_type,"users": username,"session_id": session_id,
                          "tenant_name": Partner,"comments": "","module_name": "Module Managament",
                          "request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except:
            pass
        return response
    

def format_timestamp(ts):
    # Check if the timestamp is not None
    if ts is not None:
        # Convert a Timestamp or datetime object to the desired string format
        return ts.strftime("%b %d, %Y, %I:%M %p")
    else:
        # Return a placeholder or empty string if the timestamp is None
        return " "

def optimization_dropdown_data(data):
    logging.info(f"Request Data Recieved")
    Partner = data.get('Partner', '')
    username = data.get('username', '')
    module_name = data.get('module_name', '')
    request_received_at = data.get('request_received_at', '')
    # database = DB('altaworx_central', **db_config)
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        # List of service provider names with their ids
        serviceproviders = database.get_data("serviceprovider", {"is_active": True}, ["id", "service_provider_name"])
        service_provider_list = serviceproviders.to_dict(orient='records')  # List of dicts containing both id and service_provider_name
        # Initialize dictionaries to store separate data
        service_provider_customers = {}
        service_provider_billing_periods = {}
        # Iterate over each service provider
        for service_provider in service_provider_list:
            service_provider_id = service_provider['id']
            service_provider_name = service_provider['service_provider_name']
            # Get customer IDs and names
            customers = database.get_data("optimization_customer_processing", {'service_provider': service_provider_name}, ["customer_id", "customer_name"])
            # Combine customer_id and customer_name into a list of dictionaries
            customer_list = [
                {"customer_id": row["customer_id"], "customer_name": row["customer_name"]}
                for row in customers.to_dict(orient='records')
            ]
            # Get billing period data including start date, end date, and ID
            billing_periods = database.get_data(
                "billing_period",
                {'service_provider': service_provider_name, "is_active": True},
                ["id", "billing_cycle_start_date", "billing_cycle_end_date"]
            )
            # Initialize a list to hold the formatted billing periods
            formatted_billing_periods = []
            # Iterate through each billing period and format the start date, end date, and include ID
            for period in billing_periods.to_dict(orient='records'):
                formatted_period = {
                    "id": period["id"],
                    "billing_period_start_date": format_timestamp(period["billing_cycle_start_date"]),
                    "billing_period_end_date": format_timestamp(period["billing_cycle_end_date"])
                }
                formatted_billing_periods.append(formatted_period)
            # Add the service provider's ID, customer list, and formatted billing periods to the dictionary
            service_provider_customers[service_provider_name] = {
                "id": service_provider_id,
                "customers": customer_list
            }
            service_provider_billing_periods[service_provider_name] = formatted_billing_periods

        # Prepare the response
        response = {
            "flag": True,
            "service_provider_customers": service_provider_customers,
            "service_provider_billing_periods": service_provider_billing_periods
        }
        return response
    except Exception as e:
        logging.exception(f"Exception occured while fetching the data {e}")
        # Prepare the response
        response = {
            "flag": True,
            "service_provider_customers": {},
            "service_provider_billing_periods": {}
        }
        return response

def customer_pool_row_data(data):
    try:
        customer_rate_pool_name=data.get('customer_rate_pool_name','')
        table_name=data.get('table_name','')
        pool_row_data = {}
        # logging.info(customer_rate_pool_name)
        # Fetch data from the database
        
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        # Columns to discard
        columns_to_discard = ["carrier_cycle_usage", "customer_cycle_usage"]
        customer_rate_pool_details_df = database.get_data(table_name, {'customer_pool':customer_rate_pool_name}, columns_to_discard)
        # Rename columns dynamically based on the discarded columns
        rename_mapping = {columns_to_discard[0]: 'Total Data Usage MB',columns_to_discard[1]: 'Total Data Allocation MB'}
        customer_rate_pool_details_df.rename(columns=rename_mapping, inplace=True)
        # Convert non-numeric and None values to 0 for summing
        customer_rate_pool_details_df['Total Data Usage MB'] = pd.to_numeric(
            customer_rate_pool_details_df['Total Data Usage MB'], errors='coerce').fillna(0)

        customer_rate_pool_details_df['Total Data Allocation MB'] = pd.to_numeric(
            customer_rate_pool_details_df['Total Data Allocation MB'], errors='coerce').fillna(0)

        # Calculate the sum for each column
        total_data_usage = customer_rate_pool_details_df['Total Data Usage MB'].sum()
        total_data_allocation = customer_rate_pool_details_df['Total Data Allocation MB'].sum()

        # Calculate the percentage usage or set it to 'N/A'
        if total_data_allocation != 0:
            percent_usage = round((total_data_usage / total_data_allocation) * 100, 2)
            percent_usage_str = f"{percent_usage}%"
        else:
            percent_usage_str = 'N/A'

        # Prepare the result dictionary
        pool_row_data['Name'] = customer_rate_pool_name
        pool_row_data['Total Data Allocation MB'] = float(total_data_allocation)
        pool_row_data['Total Data Usage MB'] = float(total_data_usage)
        pool_row_data['Percent Usage'] = percent_usage_str

        response = {"flag": True, "customer_pool_data": pool_row_data}
        return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        response = {"flag": True, "customer_pool_data": {}}
        return response

def dataframe_to_blob_bulk_upload(data_frame, tenant_sheet_df, service_provider_sheet_df):
    '''
    Description: The Function is used to convert the dataframe to blob
    '''
    # Create a BytesIO buffer
    bio = BytesIO()
    
    # Use ExcelWriter within a context manager to ensure proper saving
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        data_frame.to_excel(writer, sheet_name='Template', index=False)
        tenant_sheet_df.to_excel(writer, sheet_name='Tenants', index=False)
        service_provider_sheet_df.to_excel(writer, sheet_name='Service Providers', index=False)
    
    # Get the value from the buffer
    bio.seek(0)
    blob_data = base64.b64encode(bio.read())
    return blob_data

def capitalize_columns(df):
    '''Capitalizes the column names of the DataFrame.'''
    df.columns = df.columns.str.replace('_', ' ').str.capitalize()
    return df

def download_bulk_upload_template(data):
    try:
        logging.info("Request Data Received")
        # Get columns for the specific table
        module_name = data.get('module_name', '')
        table_name = data.get('table_name', '')
        logging.debug(f"table_name is: {table_name} and module name is {module_name}")
        tenant_database = data.get('db_name', '')
        
        # Database Connection
        database = DB(tenant_database, **db_config)
        common_utils_database = DB('common_utils', **db_config)
        
        if module_name in ('Users'):
            columns_df = common_utils_database.get_table_columns(table_name)
        else:
            columns_df = database.get_table_columns(table_name)

        # Specify columns to remove if the module name is 'Customer Rate Pool'
        if module_name == 'Customer Rate Pool':
            columns_to_remove = [
                'created_by',
                'created_date',
                'deleted_by',
                'deleted_date',
                'modified_by',
                'modified_date',
                'is_deleted',
                'is_active'
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[~columns_df['column_name'].str.lower().isin([col.lower() for col in columns_to_remove])]
        elif module_name == 'IMEI Master Table':
            columns_to_remove = [
                'created_date',
                'modified_by',
                'modified_date',
                'deleted_by',
                'deleted_date',
                'is_active','service_provider'
            ]

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[~columns_df['column_name'].str.lower().isin([col.lower() for col in columns_to_remove])]
        elif module_name == 'customer rate plan':
            columns_to_remove = ['created_by','created_date',
                                 'deleted_by','deleted_date',
                                 'modified_by','modified_date',
                                 'is_active','is_deleted','serviceproviderids','service_provider_id','surcharge_3g']

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[~columns_df['column_name'].str.lower().isin([col.lower() for col in columns_to_remove])]
        elif module_name == 'Customer Groups':
            columns_to_remove = ['created_by','created_date',
                                 'deleted_by','deleted_date',
                                 'modified_by','modified_date',
                                 'is_active','is_deleted']

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[~columns_df['column_name'].str.lower().isin([col.lower() for col in columns_to_remove])]
        elif module_name == 'Email Templates':
            columns_to_remove = ['created_by',
                'created_date',
                'modified_by',
                'modified_date',
                'last_email_triggered_at',
                'email_status',
                'attachments']

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[~columns_df['column_name'].str.lower().isin([col.lower() for col in columns_to_remove])]
        elif module_name == 'Users':
            columns_to_remove = ['created_date','created_by','modified_date','modified_by',
                                'deleted_date','deleted_by','is_active','is_deleted','last_modified_by',
                                'module_name','module_id','sub_module_name','sub_module_id',
                                'module_features','migrated','temp_password','mode',
                                'theme','customer_group','customers','service_provider','city',
                                'access_token','user_id']

            # Remove specified columns if they exist in columns_df
            columns_df = columns_df[~columns_df['column_name'].str.lower().isin([col.lower() for col in columns_to_remove])]
        # Remove the 'id' column if it exists
        columns_df = columns_df[columns_df['column_name'] != 'id']
        
        # Capitalize column names
        columns_df['column_name'] = columns_df['column_name'].str.replace('_', ' ').str.capitalize()
        
        # Create an empty DataFrame with the column names as columns
        result_df = pd.DataFrame(columns=columns_df['column_name'].values)

        # Fetch tenant and service provider data
        tenant_sheet_df = common_utils_database.get_data('tenant', {"is_active": True}, ["id", "tenant_name"])
        service_provider_sheet_df = database.get_data('serviceprovider', {"is_active": True}, ["id", "service_provider_name"])
        # Capitalize the column names for tenant and service provider DataFrames
        tenant_sheet_df = capitalize_columns(tenant_sheet_df)
        service_provider_sheet_df = capitalize_columns(service_provider_sheet_df)
        # Convert all DataFrames to a blob
        blob_data = dataframe_to_blob_bulk_upload(result_df, tenant_sheet_df, service_provider_sheet_df)
        
        response = {
            'flag': True,
            'blob': blob_data.decode('utf-8')
        }
        return response
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")   
        response = {
            'flag': False,
            'message': f"Failed to download the Template !! {e}"
        }
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



def bulk_import_data(data):
    logging.info(f"Request data Recieved")
    username = data.get('username', None)
    insert_flag = data.get('insert_flag', 'append')
    table_name = data.get('table_name', '')
    created_by=data.get('username','')
    created_date=data.get('request_received_at','')
    # Initialize the database connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # Check if blob data is provided
    blob_data = data.get('blob')
    if not blob_data:
        message = "Blob data not provided"
        response = {"flag": False, "message": message}
        return response
    try:
        # Extract and decode the blob data
        blob_data = blob_data.split(",", 1)[1]
        blob_data += '=' * (-len(blob_data) % 4)  # Padding for base64 decoding
        file_stream = BytesIO(base64.b64decode(blob_data))
        # Read the data into a DataFrame
        uploaded_dataframe = pd.read_excel(file_stream, engine='openpyxl')
        if uploaded_dataframe.empty:
            response={"flag":False,"message":"Uploaded Excel has no data please add the data"}
            return response
        logging.info("Uploaded DataFrame:\n", uploaded_dataframe)
        uploaded_dataframe.columns = uploaded_dataframe.columns.str.replace(' ', '_').str.lower()
        uploaded_dataframe['created_by'] = created_by
        uploaded_dataframe['created_date'] = created_date
        uploaded_dataframe['action'] = 'Template created'
        # Perform the insertion
        common_utils_database.insert(uploaded_dataframe, table_name, if_exists='append', method='multi')
        message = "Upload operation is done"
        # Get and normalize DataFrame columns
        # columns_ = [col.strip().lower() for col in uploaded_dataframe.columns]
        columns_ = [col.strip().lower().replace(' ', '_') for col in uploaded_dataframe.columns]
        logging.debug("Normalized Columns from DataFrame:", columns_)
        # Get column names from the database table
        columns_df = common_utils_database.get_table_columns(table_name)
        logging.debug("Fetched Columns from Database:\n", columns_df)
        # Remove the 'id' column if it exists
        columns_df = columns_df[columns_df['column_name'] != 'id']
        columns_to_remove = [
            'attachments',
            'modified_date',
            'modified_by',
            'email_status',
            'last_email_triggered_at',
            'reports_name'
        ]
        # Filter out the columns to remove
        columns_df = columns_df[~columns_df['column_name'].isin(columns_to_remove)]
        # Normalize database columns for comparison
        column_names = [col.strip().lower() for col in columns_df['column_name']]
        logging.debug("Normalized Columns from Database Query:", column_names)
        # Compare the column names (ignoring order)
        if sorted(columns_) != sorted(column_names):
            logging.info("Column mismatch detected.")
            logging.info("Columns in DataFrame but not in Database:", set(columns_) - set(column_names))
            logging.info("Columns in Database but not in DataFrame:", set(column_names) - set(columns_))
            message = "Columns didn't match"
            response = {"flag": False, "message": message}
            return response
        # Return success response
        response = {"flag": True, "message": message}
        return response
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        message = f"An error occurred during the import: {str(e)}"
        response = {"flag": False, "message": message}
        return response


def customers_dropdown_inventory(data):
    module_name = data.get('module_name', '')
    service_provider_name = data.get('service_provider', '')
    service_provider_id = data.get('service_provider_id', '6')
    # Initialize DB connection
    try:
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        # Get the service provider name
        if module_name=='SimManagement Inventory':
            service_provider_query = database.get_data(
                "serviceprovider", 
                {"is_active": True, "id": service_provider_id}, 
                ["id"]
            )
            if service_provider_query.empty:
                # Handle case where no service provider was found
                return {"flag": False, "message": "No active service provider found with the provided name."}
            # Extract the service provider name
            service_provider_id = service_provider_query['id'].to_list()[0]
            # Get the tenant_id for the current service provider
            tenant_id_query = database.get_data(
                "service_provider_tenant_configuration", 
                {'service_provider_id': service_provider_id}, 
                ["tenant_id"]
            )
            if tenant_id_query.empty:
                # Handle case where no tenant ID was found
                return {"flag": False, "message": "No tenant ID found for the service provider."}
            tenant_id = tenant_id_query['tenant_id'].to_list()[0]
            # Get the customer names associated with the tenant_id
            customer_names_query = database.get_data(
                "customers", 
                {'tenant_id': str(tenant_id)}, 
                ["customer_name"]
            )
            # If no customers are found, return an empty list
            if customer_names_query.empty:
                customer_names = []
            else:
                customer_names = customer_names_query['customer_name'].to_list()
        else:
            service_provider_query = database.get_data(
                "serviceprovider", 
                {"is_active": True, "service_provider_name": service_provider_name}, 
                ["service_provider_name"]
            )
            if service_provider_query.empty:
                # Handle case where no service provider was found
                return {"flag": False, "message": "No active service provider found with the provided name."}
            # Extract the service provider name
            service_provider_name = service_provider_query['service_provider_name'].to_list()[0]
            # Get the tenant_id for the current service provider
            tenant_id_query = database.get_data(
                "service_provider_tenant_configuration", 
                {'service_provider_name': service_provider_name}, 
                ["tenant_id"]
            )
            if tenant_id_query.empty:
                # Handle case where no tenant ID was found
                return {"flag": False, "message": "No tenant ID found for the service provider."}
            tenant_id = tenant_id_query['tenant_id'].to_list()[0]
            # Get the customer names associated with the tenant_id
            customer_names_query = database.get_data(
                "customers", 
                {'tenant_id': str(tenant_id)}, 
                ["customer_name"]
            )
            # If no customers are found, return an empty list
            if customer_names_query.empty:
                customer_names = []
            else:
                customer_names = customer_names_query['customer_name'].to_list()

        # Return the final response
        return {"flag": True, "customer_name": customer_names}
    except Exception as e:
        return {"flag": False, "customer_name": []}


def deactivate_service_product(data):
    logging.info("Request data recieved")
    username = data.get('username', ' ')
    # Start time  and date calculation
    request_received_at = data.get('request_received_at', ' ')
    session_id = data.get('session_id', ' ')
    Partner = data.get('Partner', ' ')
    start_time = time.time()
    # Database connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    db = DB(tenant_database, **db_config)
    try:
        # url = 'https://api.revioapi.com/v1/serviceProduct'
        url = os.getenv("SERVICEPRODUCT", " ")
        headers = {
            'Ocp-Apim-Subscription-Key': '04e3d452d3ba44fcabc0b7085cdde431',
            'Authorization': 'Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r'
        }
        deactivate_data_list = data.get("deactivate_data", [])
        customer_ids = data.get("customer_id", []) 
        response_data = []
        success_flag = True
        message = ""
        total_selected_product = 0
        # Check that the number of customer IDs matches the number of deactivate data entries
        if len(customer_ids) != len(deactivate_data_list):
            raise ValueError("Mismatch between the number of 'customer_id' and 'deactivate_data' entries.")
        # Validate that all service_product_id values are present
        for i, deactivate_data in enumerate(deactivate_data_list):
            service_product_id = deactivate_data.get("service_product_id")
            effective_date = deactivate_data.get("effective_date")
            generate_proration = deactivate_data.get("generate_proration", True)
            customer_id = str(customer_ids[i])  # Match each service_product_id with the corresponding customer_id

            # Check if service_product_id is null or missing
            if not service_product_id:
                response_data.append({
                    "customer_id": customer_id,
                    "status": "Failed",
                    "error_message": "Service Product ID is missing or null."
                })
                success_flag = False
                continue  # Skip to the next iteration

            # Make API request
            params = {
                'service_product_id': service_product_id,
                'effective_date': effective_date,
                'generate_proration': generate_proration
            }

            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                response_data.append({
                    "service_product_id": service_product_id,
                    "status": "Success"
                })
            else:
                response_data.append({
                    "service_product_id": service_product_id,
                    "status": "Failed",
                    "error_code": response.status_code,
                    "error_message": response.text
                })
                success_flag = False
            
            # Assuming database update logic comes after API response
            update_data = {"status": "DISCONNECTED", "status_date": request_received_at}
            logging.debug(f"Updating the rev_Service_product table using this data{update_data}")
            # Pass customer_id and service_product_id to the query conditions
            database.update_dict('rev_service_product', update_data, {'service_product_id': service_product_id, 'is_active': True, 'customer_id': customer_id})
            # Increment the count of processed service products
            total_selected_product += 1
        # Construct the response
        if success_flag:
            message = "All service products successfully deactivated."
        else:
            message = "Some service products failed to deactivate."
        response = {
            "flag": success_flag,
            "data": response_data,
            "message": message,
            "total_selected_product": total_selected_product,
            "RevCustomerId": customer_ids
        }
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        try:
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": request_received_at,
            "created_by": username,
                "status": str(response['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "get_module_data","request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")

        return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = f"Unable to call the api and update the data"
        response = {"flag": False, "message": message}
        return response
    
    
    
    

# #  function to check for deactivated SIMs and process them
def manage_deactivated_sims():
    """
    This script processes deactivated SIM cards from a database and performs bulk updates and audits.
    Manages SIMs that have been deactivated for over a year by processing and logging bulk changes.
    """
    logging.info("Request Data Recieved")
    # tenant_database = data.get('db_name', '')
    # database Connection
    database = DB('altaworx_central', **db_config)
    dbs = DB(os.environ['COMMON_UTILS_DATABASE'],**db_config)    
    try:
        current_time =  time()
        current_time_dt = datetime.fromtimestamp(current_time)     
        one_year_ago = current_time_dt - timedelta(days=365)
        params = [one_year_ago.strftime('%Y-%m-%d')]
        query = """
                    SELECT * 
                    FROM sim_management_inventory 
                    WHERE sim_status = 'deactive' 
                    AND date_activated < %s 
                """
        deactivated_sims = database.execute_query(query, params=params)  
        if not deactivated_sims.empty: 
            return  
        for sim in deactivated_sims.itertuples():
                    # Process each SIM   
                tenant_id = sim.tenant_id
                tenant_name = dbs.get_data("tenant", {"id": tenant_id}, ["tenant_name"])["tenant_name"].to_list()[0]
                service_provider_id = sim.service_provider_id
                service_provider = database.get_data("serviceprovider", {"id": service_provider_id}, ["service_provider_name"])["service_provider_name"].to_list()[0]
                change_type = "Archive"
                change_type_id = database.get_data("sim_management_bulk_change_type", {"display_name": change_type}, ["id"])["id"].to_list()[0] 
                iccids = sim.iccid
                    # Prepare bulk change request
                bulk_change_data = {
                        "service_provider": service_provider,
                        "change_request_type_id": int(change_type_id),
                        "change_request_type": change_type,
                        "service_provider_id": int(service_provider_id),
                        "modified_by": "system_scheduler",
                        "status": "NEW",
                        "iccid": iccids,
                        "uploaded": len(sim.iccid) if sim.iccid else 0,
                        "is_active": True,
                        "is_deleted": False,
                        "created_by": "system_scheduler",
                        "processed_by": "system_scheduler",
                        "tenant_id": int(tenant_id)
                }   
                    # Insert the bulk change request into the database
                change_id=database.insert_data(bulk_change_data, "sim_management_bulk_change")  
                    # Log the audit trail
                audit_data_user_actions = {
                        "service_name": 'Sim Management',
                        "created_date": current_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                        "created_by": "system_scheduler",
                        "status": "True",
                        "time_consumed_secs": round(time() - current_time, 2),
                        "session_id": "scheduler_session",
                        "tenant_name": tenant_name,
                        "comments": json.dumps(bulk_change_data),
                        "module_name": "update_bulk_change_data",
                        "request_received_at": current_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                }
                dbs.insert_dict(audit_data_user_actions, "audit_logs")
    except Exception as e:
        logging.info(f"An error occurred: {e}")
        message = "Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {
                "service_name": 'Sim Management',
                "created_date": current_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                "error_message": message,
                "error_type": error_type,
                "users": "system_scheduler",  
                "tenant_name": tenant_name,
                "comments": "",
                "module_name": "update_bulk_change_data",
                "request_received_at": current_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            }
            database.insert_dict(error_data, 'error_log_table')
        except Exception as log_error:
            logging.info(f"Failed to log error: {log_error}")      


def get_optimization_data(data):
    '''
    Retrieves the optimization data.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_params' for querying the status history.

    Returns:
    - dict: A dictionary containing the List view data, header mapping, and a success message or an error message.
    '''
    logging.info("Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('tenant_name', '')
    role_name = data.get('role_name', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    table = data.get('table', 'vw_optimization_instance_summary')
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:
        # Initialize the database connection
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        tenant_database=data.get('db_name','altaworx_central')
        optimization_data=[]
        pages={}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100   # Default to 100 if no value
            logging.debug(f"starting page is {start} and ending page is {end}")
            limit=data.get('limit',100)
            # Calculate pages 
            pages['start']=start
            pages['end']=end
            count_params = [table]
            count_query = "SELECT COUNT(*) FROM %s" % table
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages['total']=int(count_result)

        params=[start,end]
        query='''SELECT id,
                service_provider,service_provider_id,
                optimization_type,
                TO_CHAR(run_start_time, 'MM-DD-YYYY HH24:MI:SS') AS run_start_time,
                total_overage_charge_amt,
                TO_CHAR(run_end_time, 'MM-DD-YYYY HH24:MI:SS') AS run_end_time,
                device_count,
                total_cost,
                rev_customer_id,
                customer_name,
                TO_CHAR(billing_period_start_date, 'MM-DD-YYYY HH24:MI:SS') AS billing_period_start_date,
                TO_CHAR(billing_period_end_date, 'MM-DD-YYYY HH24:MI:SS') AS billing_period_end_date,
                run_status,instance_id,
                CAST(session_id AS TEXT) AS session_id,download,upload,info,total_charge_amount,sms_charge_total
                FROM vw_optimization_instance_summary  OFFSET %s LIMIT %s;
                '''
        optimization_data=database.execute_query(query,params=params).to_dict(orient='records')
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,["Optimization"],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        service_providers = database.get_data("serviceprovider",{
            "service_provider_name":"not Null",'is_active':True},["service_provider_name"])['service_provider_name'].to_list()
        # Prepare the response
        response = {"flag": True, "optimization_data": optimization_data, "header_map": headers_map,"pages":pages,
                    "service_provider":service_providers}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the optimization data',
                                       "module_name": "get_optimization_data",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        try:
            # Error Management
            error_data = {"service_name": 'Sim management',
                        "created_date": start_time,
                        "error_messag": message,
                        "error_type": e, "user": username,
                        "tenant_name": Partner,
                        "comments": message,
                        "module_name": 'get_optimization_data',
                        "request_received_at": start_time}
            common_utils_database.log_error_to_db(error_data, 'error_table')
        except Exception as e:
            logging.warning(f"Exception at updating the error table")
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,["Optimization"],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        service_providers = database.get_data("serviceprovider",{
            "service_provider_name":"not Null",'is_active':True},["service_provider_name"])['service_provider_name'].to_list()
        # Prepare the response
        response = {"flag": True, "optimization_data": {}, "header_map": headers_map,"pages":{},
                    "service_provider":service_providers}
        return response

def get_customer_charges_data(data):
    '''
    Retrieves the optimization data.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_params' for querying the status history.

    Returns:
    - dict: A dictionary containing the List view data, header mapping, and a success message or an error message.
    '''
    # Start time  and date calculation
    start_time = time.time()
    logging.info(f"Request Data Recieved")
    Partner = data.get('tenant_name', '')
    role_name = data.get('role_name', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    table = data.get('table', 'vw_optimization_smi_result_customer_charge_queue_summary')
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:
        # Initialize the database connection
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        tenant_database=data.get('db_name','altaworx_central')
        customer_charges_data=[]
        pages={}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100   # Default to 100 if no value
            logging.debug(f"starting page is {start} and ending page is {end}")
            # Calculate pages 
            pages['start']=start
            pages['end']=end
            limit=data.get('limit',100)
            count_params = [table]
            count_query = "SELECT COUNT(*) FROM %s" % table
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages['total']=int(count_result)
        params=[start,end]
        query='''SELECT queue_id,
                rev_account_number,
                customer_name,
                TO_CHAR(billing_period_start_date::date, 'YYYY-MM-DD') AS billing_period_start_date,
                TO_CHAR(billing_period_end_date::date, 'YYYY-MM-DD') AS billing_period_end_date,
                TO_CHAR(billing_period_end_date::date, 'Mon YYYY') AS billing_period_end_mon_yyyy,
                device_count,
                charge_status,
                charge_amount
            FROM vw_optimization_smi_result_customer_charge_queue_summary OFFSET %s LIMIT %s;
                '''
        customer_charges_data=database.execute_query(query,params=params).to_dict(orient='records')
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,["Customer charges"],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        # Prepare the response
        response = {"flag": True, "customer_charges_data": customer_charges_data, "header_map": headers_map,"pages":pages}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the optimization data',
                                       "module_name": "get_optimization_data",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        try:
            # Error Management
            error_data = {"service_name": 'Sim management',
                        "created_date": start_time,
                        "error_messag": message,
                        "error_type": e, "user": username,
                        "tenant_name": Partner,
                        "comments": message,
                        "module_name": 'get_optimization_data',
                        "request_received_at": start_time}
            common_utils_database.log_error_to_db(error_data, 'error_table')
        except Exception as e:
            logging.warning(f"Exception raised at inserting in error table")
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,["Customer charges"],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        # Prepare the response
        response = {"flag": True, "customer_charges_data": {}, "header_map": headers_map,"pages":{}}
        return response

def export_optimization_data_zip(data):
    logging.info(f"Request Data Recieved")
    request_received_at = data.get('request_received_at', '')
    module_name = data.get('module_name', 'Optimization')
    optimization_type = data.get('optimization_type', '')
    service_provider = data.get('service_provider', '')
    billing_period_start_date = data.get('billing_period_start_date', '')
    billing_period_end_date = data.get('billing_period_end_date', '')
    Partner = data.get('Partner', '')
    username = data.get('username', '')
    # Database connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

    # try:
    #     # Fetch the query from the database based on the module name
    #     module_query_df = database.get_data("export_queries", {"module_name": module_name})
    #     query = module_query_df.iloc[0]['module_query']
        
    #     if module_name.lower() == 'optimization':
    #         params = [optimization_type, service_provider, billing_period_start_date, billing_period_end_date]
        
    #     # Executing the query and fetching data
    #     data_frame = database.execute_query(query, params=params)
        
    #     # Check if DataFrame is empty
    #     if data_frame.empty:
    #         return {"flag": False, "message": "No data for the selected range"}
        
    #     # Buffer for the zip file (in memory)
    #     zip_buffer = io.BytesIO()
    #     current_date_str = datetime.now().strftime('%Y%m%d')
    #     zip_filename = f"Optimization_session_{current_date_str}.zip"

    #     with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
    #         # Group rows by 'session_id' and process each group as a batch
    #         grouped = data_frame.groupby('session_id')

    #         for session_id, group in grouped:
    #             # Create a folder inside the ZIP for each session_id
    #             folder_name = f'{session_id}/'
    #             zipf.writestr(folder_name, '')  # Create folder in ZIP

    #             # Buffer for the grouped Excel file (in memory)
    #             excel_buffer = io.BytesIO()

    #             # Write the entire group as a single Excel file in memory
    #             with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
    #                 group.to_excel(writer, index=False)

    #             # Move back to the start of the buffer to read the content
    #             excel_buffer.seek(0)

    #             # Add the Excel file to the ZIP archive inside the session_id folder
    #             zipf.writestr(f'{folder_name}{session_id}.xlsx', excel_buffer.read())

    #     # Get the content of the zip buffer
    #     zip_buffer.seek(0)
    #     zip_blob = zip_buffer.getvalue()

    #     # Convert to base64 (can be sent to frontend as a blob)
    #     encoded_blob = base64.b64encode(zip_blob).decode('utf-8')

    #     return {
    #         'flag': True,
    #         'blob': encoded_blob,
    #         'filename': zip_filename
    #     }
    try:
        # Fetch the query from the database based on the module name
        module_query_df = common_utils_database.get_data("export_queries", {"module_name": module_name})
        query = module_query_df.iloc[0]['module_query']
        
        if module_name.lower() == 'optimization':
            params = [optimization_type, service_provider, billing_period_start_date, billing_period_end_date]
        
        # Executing the query and fetching data
        data_frame = database.execute_query(query, params=params)
        
        # Check if DataFrame is empty
        if data_frame.empty:
            return {"flag": False, "message": "No data for the selected range"}
        
        # Buffer for the Excel file (in memory)
        excel_buffer = io.BytesIO()
        current_date_str = datetime.now().strftime('%Y%m%d')
        excel_filename = f"Optimization_session_{current_date_str}.xlsx"

        # Create a Pandas Excel writer using openpyxl
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            # Write the entire DataFrame to a single sheet
            data_frame.to_excel(writer, index=False, sheet_name='Optimization Data')

        # Move back to the start of the buffer to read the content
        excel_buffer.seek(0)

        # Get the content of the excel buffer
        excel_blob = excel_buffer.getvalue()

        # Convert to base64 (can be sent to frontend as a blob)
        encoded_blob = base64.b64encode(excel_blob).decode('utf-8')

        return {
            'flag': True,
            'blob': encoded_blob,
            'filename': excel_filename
        }

    except Exception as e:
        # Handle exceptions and log error
        logging.info(f"Exception occurred: {e}")
        message = "Something went wrong while exporting the data"
        
        # Error Management
        error_data = {
            "service_name": 'Sim management',
            "created_date": request_received_at,
            "error_message": message,
            "error_type": str(e),  # Convert exception to string
            "user": username,
            "tenant_name": Partner,
            "comments": message,
            "module_name": 'get_optimization_data',
            "request_received_at": request_received_at
        }
        common_utils_database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "error": str(e)}



def get_data_(customer_name,database):
    logging.info("get_data_ function is being called here")
    # This is a placeholder for the actual database call
    # Replace with actual database call logic
    query=f"SELECT iccid,customer_pool,cycle_data_usage_mb,communication_plan,msisdn,uses_proration,date_activated,was_activated_in_this_billing_period,days_activated_in_billing_period FROM public.vw_optimization_export_device_assignments where customer_name=%s"
    params=[customer_name]
    try:
        # Execute the query and fetch the results as a DataFrame
        df = database.execute_query(query, params=params)
        # Check if the DataFrame is empty and return accordingly
        if df.empty:
            logging.warning(f"No data found for customer: {customer_name}")
            return pd.DataFrame()  # Return an empty DataFrame if no data found
        return df  # Return the DataFrame containing the customer's data
    except Exception as e:
        logging.warning(f"Error fetching data for customer {customer_name}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

def download_row_data_optimization(data):
    logging.info(f"Request Data Recieved")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    Partner = data.get('Partner', '')
    username = data.get('username', '')
    # Database connection
    
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    
    # Get new_data
    new_data = data.get("new_data", {})
    
    try:
        # Create a bytes buffer for the zip file
        zip_buffer = io.BytesIO()

        # Define naming formatsd
        current_date_str = datetime.now().strftime('%Y%m%d')
        zip_filename = f"Optimization_session_{current_date_str}.zip"
        new_data_excel_filename = f"Optimization_session_{current_date_str}.xlsx"

        # Create the zip file in memory
        with zipfile.ZipFile(zip_buffer, 'w') as zipf:
            # Create an Excel file for new_data
            new_data_df = pd.DataFrame(new_data)
            # Capitalize each word and add spaces
            new_data_df.columns = [
                col.replace('_', ' ').title() for col in new_data_df.columns
            ]
            # Write the new_data Excel file to the zip
            with io.BytesIO() as new_data_excel_buffer:
                new_data_df.to_excel(new_data_excel_buffer, index=False)
                zipf.writestr(new_data_excel_filename, new_data_excel_buffer.getvalue())
            
            # Save individual customer data to the Rate Plan Assignments folder
            for entry in new_data:
                customer_name = entry.get("customer_name", "")
                
                # Check if customer_name is empty
                if not customer_name:
                    logging.warning(f"No data found for customer: {customer_name}")
                    continue  # Skip to the next entry if customer_name is empty
                
                customer_df = get_data_(customer_name, database)
                # Prepare the path for the customer's Excel file
                customer_excel_filename = f"{customer_name}_device_assignments_{current_date_str}.xlsx"
                customer_excel_path = f"Rate Plan Assignments/{customer_excel_filename}"
                # Save the customer data to Excel
                with io.BytesIO() as customer_excel_buffer:
                    if not customer_df.empty:
                        customer_df.to_excel(customer_excel_buffer, index=False)
                    else:
                        # Create an empty Excel file
                        pd.DataFrame().to_excel(customer_excel_buffer, index=False)

                    # Write to zip
                    zipf.writestr(customer_excel_path, customer_excel_buffer.getvalue())

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
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        # Error Management
        error_data = {
            "service_name": 'Sim management',
            "created_date": request_received_at,
            "error_messag": message,
            "error_type": str(e),
            "user": username,
            "tenant_name": Partner,
            "comments": message,
            "module_name": 'get_optimization_data',
            "request_received_at": request_received_at
        }
        common_utils_database.log_error_to_db(error_data, 'error_table')
        response = {"flag": False, "error": str(e)}
        return response


def optimization_row_info_data(data):
    logging.info(f"Request Data Recieved")
    tenant_database = data.get('db_name', '')
    tenant_database=data.get('db_name','altaworx_central')
    role_name = data.get('role_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        session_id=data.get('session_id','')
        info_query=f"SELECT rev_customer_id,customer_name,device_count,total_charge_amount,sms_charge_total FROM public.vw_optimization_instance_summary where session_id=%s"
        params=[session_id]
        info_dict=database.execute_query(info_query,params=params).to_dict(orient='records')
        # Get the total count of rows (Total Optimization in Sessions)
        total_optimization_sessions = len(info_dict)
        uploaded_optimizations=len(info_dict)
        # Generate the headers mapping
        headers_map = get_headers_mappings(tenant_database,["Optimization Info"],role_name,'','','','',data)
        response={"flag":True,"info_dict":info_dict,
                "Total Optimizations in sessions":total_optimization_sessions,
                "Uploaded Optimizations":uploaded_optimizations,
                'headers_map':headers_map}
        return response
    except Exception as e:
        logging.warning(f"Error Occured is {e}")
        headers_map = get_headers_mappings(tenant_database,["Optimization Info"],role_name,'','','','',data)
        response={"flag":True,"info_dict":{},
                "Total Optimizations in sessions":{},
                "Uploaded Optimizations":{},
                'headers_map':headers_map}
        return response


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
    logging.info(f"Request Data Recieved")
    ### Extract parameters from the Request Data
    # Database connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        # Fetch customer names and session IDs from the database
        query = """
        SELECT customer_name,CAST(session_id AS TEXT) AS session_id
        FROM vw_optimization_smi_result_customer_charge_queue_summary
        """
        # Get data from the database
        customer_session_df = database.execute_query(query,True)
        # Initialize a dictionary to store the result
        customer_sessions = {}
        # Iterate over the dataframe to build the dictionary
        for index, row in customer_session_df.iterrows():
            customer_name = row['customer_name']
            session_id = row['session_id']
            # Check if the customer_name already exists in the dictionary
            if customer_name in customer_sessions:
                customer_sessions[customer_name].append(session_id)
            else:
                customer_sessions[customer_name] = [session_id]
        response={"flag":True,"customer_sessions":customer_sessions}
        return response
    
    except Exception as e:
        logging.exception(f"Error fetching data: {e}")
        response={"flag":True,"customer_sessions":{}}
        return response


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
            params = [session_id, customer_name]
            query = '''SELECT 
                rev_account_number AS "Rev_account_number",
                customer_name AS "Customer_Name",
                billing_period_start_date AS "Billing_period_start_date",
                billing_period_end_date AS "Billing_period_end_date",
                base_charge_amount AS "Base_charge_amount",
                rate_charge_amount AS "Rate_charge_amount",
                total_data_charge_amount AS "Total_data_charge_amount",
                processed_count AS "Is_processed",
                charge_status AS "Error_message",
                sms_charge_amount AS "SMS_charge_amount",
                totalchargeamount AS "Total_charge_amount"
            FROM 
                public.vw_optimization_smi_result_customer_charge_queue_summary 
            WHERE session_id=%s AND customer_name=%s
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


def start_optimization(data):
    logging.info(f"Request Data Recieved")
    try:
        body=data.get('body',{})
        username=data.get('username','')
        tenant_name=data.get('tenant_name','')
        
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        tenant_id=common_utils_database.get_data('tenant',{"tenant_name":tenant_name},['id'])['id'].to_list()[0]
        # Define the URL
        # url = "https://sandbox.amop.services/api/OptimizationApi/start-confirm"
        url = os.getenv("OPTIMIZATIONAPI", " ")
        # Define the headers
        headers = {
            "Authorization": "Basic bnRhbnZpbmhAdG1hLmNvbS52bjpWaW5oQDAxNjg1Njk4MTkz",
            "user-name": username,
            "x-tenant-id": tenant_id,
            "Content-Type": "application/json"  # Specify content type for JSON
        }
        # Send the POST request
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response_data={"flag":True,"status code":response.status_code,"message":response.json()}
        # Return the status code and response JSON
        return response_data
    except Exception as e:
        logging.exception(f"Error fetching data: {e}")


def upload_carrier_rate_plan(data):
    logging.info(f"Request Data Recieved")
    try:
        username = data.get('username', '')
        tenant_name = data.get('tenant_name', '')
        ids = data.get('ids', [])
        # Connect to the database to get tenant_id
        tenant_database = data.get('db_name', '')
        # database Connection
        common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        tenant_id = common_utils_database.get_data('tenant', {"tenant_name": tenant_name}, ['id'])['id'].to_list()[0]
        logging.debug(f"tenant_id is {tenant_id}")
        # Define the URL for rate plan upload
        # url = "https://sandbox.amop.services/api/OptimizationApi/Queue-Rate-Plan-Changes"
        url = os.getenv("OPTIMIZATIONAPIQUEUERATEPLANCHANGES", "")
        # Define the headers
        headers = {
            "Authorization": "Basic bnRhbnZpbmhAdG1hLmNvbS52bjpWaW5oQDAxNjg1Njk4MTkz",
            "user-name": username,
            "x-tenant-id": tenant_id,
            "Content-Type": "application/json"
        }
        # Initialize the response list
        responses = []
        # Loop over the IDs and send POST requests
        for id_ in ids:
            # Create the request body dynamically with InstanceId
            body = {
                "InstanceId": id_
            }
            
            # Send the POST request
            response = requests.post(url, headers=headers, data=json.dumps(body))
            
            # Append the response data for each ID
            response_data = {
                "flag": True,
                "id": id_,
                "status code": response.status_code,
                "message": response.json() if response.content else "No Content"
            }
            responses.append(response_data)
        # Return all responses
        return responses
    except Exception as e:
        logging.exception(f"Error uploading rate plan data: {e}")
        return {"flag": False, "error": str(e)}



def rate_plans_by_customer_count(data,database,common_utils_database):
    logging.info(f"Request Data Recieved")
    ServiceProviderId = data.get('ServiceProviderId', '')
    BillingPeriodId = data.get('BillingPeriodId', '')
    tenant_name = data.get('tenant_name', '')
    TenantId = common_utils_database.get_data('tenant', {"tenant_name": tenant_name}, ['id'])['id'].to_list()[0]
    customer_name = data.get('customer_name', '')
    # Fetch integration_id and portal_id using database queries
    integration_id = database.get_data('serviceprovider', {"id": ServiceProviderId}, ['integration_id'])['integration_id'].to_list()[0]
    portal_id = database.get_data('integration', {"id": integration_id}, ['portal_type_id'])['portal_type_id'].to_list()[0]
    # If portal_id is 0, proceed with M2M connection and stored procedure execution
    if portal_id == 0:
        # Define database connection parameters
        server = 'altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com'
        database_name = 'AltaworxCentral_Test'
        username = 'ALGONOX-Vyshnavi'
        password = 'cs!Vtqe49gM32FDi'
        rev_customer_data = database.get_data('customers', {"customer_name": customer_name}, ['rev_customer_id'])['rev_customer_id'].to_list()  
        # Check if rev_customer_id data is available
        if rev_customer_data:
            RevCustomerIds = ','.join([str(id) for id in rev_customer_data])
            AMOPCustomerIds = ""
            SiteType=1
        else:
            # If rev_customer_id is None or empty, get customer_id data
            AMOPCustomerIds = ','.join([str(id) for id in database.get_data('customers', {"customer_name": customer_name}, ['customer_id'])['customer_id'].to_list()])
            RevCustomerIds = ""
            SiteType=0
        # Try connecting to the database and executing the stored procedure
        try:
            with pytds.connect(server=server, database=database_name, user=username, password=password) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = 'AltaworxCentral_Test.dbo.usp_OptimizationRatePlansByCustomerCount'
                    
                    # Execute the stored procedure
                    cursor.callproc(stored_procedure_name, (RevCustomerIds, ServiceProviderId, TenantId, SiteType, AMOPCustomerIds, BillingPeriodId))
                    
                    # Fetch and return the results
                    results = cursor.fetchall()
                    return results

        except Exception as e:
            logging.exception("Error in connection or execution:", e)
            return None
    elif portal_id==2:
        # Define database connection parameters
        server = 'altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com'
        database_name = 'AltaworxCentral_Test'
        username = 'ALGONOX-Vyshnavi'
        password = 'cs!Vtqe49gM32FDi'
        rev_customer_data = database.get_data('customers', {"customer_name": customer_name}, ['rev_customer_id'])['rev_customer_id'].to_list()
        #working for mobility rate plan count
        # Define your connection parameters
        with pytds.connect(server=server, database=database, user=username, password=password) as conn:
            with conn.cursor() as cursor:
                stored_procedure_name = 'dbo.usp_OptimizationMobilityRatePlansByCustomerCount'
                # Check if rev_customer_id data is available
                if rev_customer_data:
                    RevCustomerIds = ','.join([str(id) for id in rev_customer_data])
                    AMOPCustomerIds = ""
                    SiteType=1
                else:
                    # If rev_customer_id is None or empty, get customer_id data
                    AMOPCustomerIds = ','.join([str(id) for id in database.get_data('customers', {"customer_name": customer_name}, ['customer_id'])['customer_id'].to_list()])
                    RevCustomerIds = ""
                    SiteType=0
                output_param = pytds.output(value=None, param_type=int)
                return_value = cursor.callproc(stored_procedure_name,
                                       (RevCustomerIds,
                                        ServiceProviderId,
                                        TenantId,
                                        SiteType,
                                        AMOPCustomerIds,
                                        BillingPeriodId,
                                        output_param))  # Placeholder for output parameter
                output_value = return_value[-1]
                return output_value
                

def sim_cards_to_optimize_count(data,database,common_utils_database): 
    logging.info(f"Request Data Recieved")
    ServiceProviderIds=data.get('ServiceProviderId','')
    query=f"select sandbox_id from serviceprovider where id={ServiceProviderIds}"
    ServiceProviderId=database.execute_query(query,True)['sandbox_id'].to_list()[0]
    logging.debug(f"ServiceProviderId is {ServiceProviderId}")
    BillingPeriodId=data.get('BillingPeriodId','')
    customer_name=data.get('customer_name','')
    tenant_name = data.get('tenant_name', '')
    TenantId = common_utils_database.get_data('tenant', {"tenant_name": tenant_name}, ['id'])['id'].to_list()[0]
    optimization_type = data.get('optimization_type', '')
    # Define your connection parameters
    server = 'altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com'  # e.g., 'localhost' or 'your_server_name'
    database = 'AltaworxCentral_Test'
    username = 'ALGONOX-Vyshnavi'
    password = 'cs!Vtqe49gM32FDi'
    if optimization_type == 'Customer':
        #customer_name='Easy Shop Supermarket (300007343)'
        # Create a connection to the database
        try:
            with pytds.connect(server=server, database=database, user=username, password=password) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = 'AltaworxCentral_Test.dbo.[usp_OptimizationCustomersGet]'
                    # Optional: Define any parameters to pass to the stored procedure
                    # RevCustomerIds = '6B32A900-5E87-4BBD-BD53-28BD86FD6192,94F8BE71-ACCC-47EB-A0DF-78D3AAF093FA,95599FFC-6F67-40AC-8518-D180B929C430,45519C0F-BFC4-4CB4-8468-F4420E9AE0CC'
                    # ServiceProviderId = 1
                    # TenantId = 1
                    # # SiteType = 1
                    # # AMOPCustomerIds = ''
                    # BillingPeriodId = 412
                    # Execute the stored procedure
                    cursor.callproc(stored_procedure_name, (ServiceProviderId, TenantId, BillingPeriodId))
        
                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
                    for row in results:
                        if row[1] == customer_name:
                            sim_cards_to_optimize=row[-1]
                            break
                    return sim_cards_to_optimize
        
        except Exception as e:
            logging.exception("Error in connection or execution:", e)
    else:
        # Create a connection to the database
        try:
            with pytds.connect(server=server, database=database, user=username, password=password) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = 'AltaworxCentral_Test.dbo.[usp_OptimizationCustomersGet]'
                    # Optional: Define any parameters to pass to the stored procedure
                    # RevCustomerIds = '6B32A900-5E87-4BBD-BD53-28BD86FD6192,94F8BE71-ACCC-47EB-A0DF-78D3AAF093FA,95599FFC-6F67-40AC-8518-D180B929C430,45519C0F-BFC4-4CB4-8468-F4420E9AE0CC'
                    #ServiceProviderId = 1
                    #TenantId = 1
                    # # SiteType = 1
                    # # AMOPCustomerIds = ''
                    BillingPeriodId = 412
                    # Execute the stored procedure
                    cursor.callproc(stored_procedure_name, (ServiceProviderId, TenantId, BillingPeriodId))
                    sim_cards_to_optimize = 0
                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
                    for row in results:
                        last_item = row[-1]
                        sim_cards_to_optimize += last_item
                    
                    return sim_cards_to_optimize
        
        except Exception as e:
            logging.exception("Error in connection or execution:", e)

def get_customer_total_sim_cards_count(rev_customer_id,database):
    logging.info(f"Request Data Recieved")
    try:
        #database = DB('altaworx_central', **db_config)
        rev_customer_ids = database.get_data('revcustomer', {"rev_customer_id": rev_customer_id}, ["id"])["id"].to_list()
        customer_ids_query = f"""
                SELECT id FROM customers 
                WHERE rev_customer_id IN ({', '.join(f"'{id}'" for id in rev_customer_ids)})
                """
                
                # Step 4: Execute the query to get the customer IDs
        customer_ids_dataframe = database.execute_query(customer_ids_query, True)
        # Step 5: Extract customer IDs from the DataFrame
        customer_ids = customer_ids_dataframe['id'].tolist()
        
        # Step 6: Query to count records in sim_management_inventory based on customer IDs
        if customer_ids:  # Check if there are any customer IDs
            customer_ids_tuple = ', '.join(map(str, customer_ids))  # Convert list to tuple string for SQL
            count_query = f"""
            SELECT count(*) FROM public.sim_management_inventory 
            WHERE customer_id IN ({customer_ids_tuple})
            """
            
            # Step 7: Execute the count query
            final_count = database.execute_query(count_query, True)
            count_value = final_count['count'][0]  # Assuming the result is returned as a DataFrame
        return count_value
    except Exception as e:
        logging.exception(f"Exception is {e}")
    
def get_optimization_pop_up_data(data):
    logging.info(f"Request Data in recieved in get_optimization_pop_up_data")
    ##database connection
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    ServiceProviderId = data.get('ServiceProviderId', '')
    try:
        optimization_type = data.get('optimization_type', '')
        
        if optimization_type == 'Customer':
            '''Rate Plan Count'''
            try:
                # Call function to get rate plans count by customer
                results = rate_plans_by_customer_count(data, database, common_utils_database)
                rate_plan_count = int(results[0][0])  # Ensure conversion to standard int
            except Exception as e:
                logging.exception('rate_plan_count', e)
                rate_plan_count = 0
            logging.debug(f"rate_plan_count is {rate_plan_count}")
            
            '''Sim cards to Optimize'''
            try:
                # Call function to get SIM cards to optimize
                sim_cards_to_optimize = sim_cards_to_optimize_count(data, database,common_utils_database)
                if sim_cards_to_optimize is None:
                    sim_cards_to_optimize = 0
                sim_cards_to_optimize = int(sim_cards_to_optimize)  # Ensure conversion to standard int
            except Exception as e:
                logging.warning('sim_cards_to_optimize', e)
                sim_cards_to_optimize = 0
            
            '''Total Sim cards'''
            rev_customer_id = str(data.get('customer_id', ''))
            total_sim_cards_count = int(get_customer_total_sim_cards_count(rev_customer_id,database))  # Ensure conversion to standard int
            
            response = {
                "flag": True,
                "rate_plan_count": rate_plan_count,
                "sim_cards_to_optimize": sim_cards_to_optimize,
                "total_sim_cards_count": total_sim_cards_count
            }
            return response
        
        else:
            '''Rate Plan Count'''
            params = [ServiceProviderId]
            logging.info(ServiceProviderId,'ServiceProviderId')
            rate_plan_count_query = "SELECT count(*) FROM public.carrier_rate_plan where service_provider_id=%s"
            rate_plan_count = int(database.execute_query(rate_plan_count_query, params=params).iloc[0, 0])  # Ensure conversion to standard int
            logging.info(rate_plan_count, 'rate_plan_count')
            
            '''Sim cards to Optimize'''
            try:
                sim_cards_to_optimize = sim_cards_to_optimize_count(data, database,common_utils_database)
                if sim_cards_to_optimize is None:
                    sim_cards_to_optimize = 0
                sim_cards_to_optimize = int(sim_cards_to_optimize)  # Ensure conversion to standard int
            except Exception as e:
                logging.exception('sim_cards_to_optimize', e)
                sim_cards_to_optimize = 0
            
            '''Total Sim cards'''
            params = [ServiceProviderId]
            total_sim_cards_count_query = "SELECT count(*) FROM public.sim_management_inventory where service_provider_id=%s"
            total_sim_cards_count = int(database.execute_query(total_sim_cards_count_query, params=params).iloc[0, 0])  # Ensure conversion to standard int
            
            response = {
                "flag": True,
                "rate_plan_count": rate_plan_count,
                "sim_cards_to_optimize": sim_cards_to_optimize,
                "total_sim_cards_count": total_sim_cards_count
            }
            return response
    
    except Exception as e:
        logging.exception("Error in connection or execution:", e)


def upload_customer_charges_optimization_list_view(data):
    logging.info(f"Request data Recieved")
    try:
        username = data.get('username', '')
        tenant_name = data.get('tenant_name', '')
        session_id = data.get('SessionId', '')
        selected_instances = data.get('selectedInstances', '')  # Placeholder, needs to be discussed
        selected_usage_instances = data.get('selectedUsageInstances', '')  # Placeholder, needs to be discussed
        push_type = data.get('pushType', '')
        # Connect to the database to get tenant_id
        tenant_database = data.get('db_name', '')
        common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        # database Connection
        tenant_id = common_utils_database.get_data('tenant', {"tenant_name": tenant_name}, ['id'])['id'].to_list()[0]
        # Define the URL for customer charges upload
        # url = "https://sandbox.amop.services/api/OptimizationApi/Create-Confirm-Session"
        url = os.getenv("OPTIMIZATIONAPI_CREATECOMFIRM_SESSION", "")
        # Define the headers
        headers = {
            "Authorization": "Basic bnRhbnZpbmhAdG1hLmNvbS52bjpWaW5oQDAxNjg1Njk4MTkz",
            "user-name": username,
            "x-tenant-id": tenant_id,
            "Content-Type": "application/json"
        }

        # Create the request body dynamically
        body = {
            "SessionId": session_id,
            "selectedInstances": selected_instances, 
            "selectedUsageInstances": selected_usage_instances, 
            "pushType": push_type
        }
        
        # Send the POST request
        response = requests.post(url, headers=headers, data=json.dumps(body))
        
        # Prepare the response data
        response_data = {
            "flag": True,
            "status code": response.status_code,
            "message": response.json() if response.content else "No Content"
        }

        # Return the response
        return response_data

    except Exception as e:
        logging.exception(f"Error uploading customer charges data: {e}")
        return {"flag": False, "error": str(e)}




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


def testing(data):
    database_con = DB("altaworx_central", **db_config)
    query=f"select sandbox_id from serviceprovider where id=1"
    integration_id=database_con.execute_query(query,True)['sandbox_id'].to_list()[0]
    #integration_id = database_con.get_data('serviceprovider', {"id": 1}, ['sandbox_id'])['sandbox_id'].to_list()[0]
    response={"flag":True,"integration_id":integration_id}
    return response


def get_usage_data(data):
    logging.info(f"Request data Recieved")
    # ##database connection
    # Define your connection parameters
    server = 'altaworx-test.cd98i7zb3ml3.us-east-1.rds.amazonaws.com'
    database = 'AltaworxCentral_Test'
    username = 'ALGONOX-Vyshnavi'
    password = 'cs!Vtqe49gM32FDi'
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    database_con = DB("altaworx_central", **db_config)
    # Extract values from the input dictionary 'data'
    ServiceProviderId = data.get('ServiceProviderId', 1)
    tenant_name = data.get('tenant_name', 'Altaworx')
    TenantId = db.get_data('tenant', {"tenant_name": tenant_name}, ['id'])['id'].to_list()[0]
    customer_name = data.get('customer_name', '')
    # Fetch integration_id and portal_id using database queries
    integration_id = database_con.get_data('serviceprovider', {"id": ServiceProviderId}, ['integration_id'])['integration_id'].to_list()[0]
    portal_id = database_con.get_data('integration', {"id":integration_id }, ['portal_type_id'])['portal_type_id'].to_list()[0]
    logging.debug(f"portal_id is:{portal_id}")
    if portal_id==0:
        # Create a connection to the database
        try:
            with pytds.connect(server=server, database=database, user=username, password=password) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = 'AltaworxCentral_Test.dbo.[usp_Report_MobilityHistorical_HistoryByLine]'
                    # Define the parameters to pass to the stored procedure
                    ServiceProviderId = 20  # Replace with None if you want to pass NULL
                    SiteIds = None  # Assuming NULL as per your description
                    #TenantId = 1
                    PageIndex = 0
                    PageSize = 50 
                    Filter = None 
                    BillingPeriodIds = 417
    
                    # Execute the stored procedure with parameters
                    cursor.callproc(stored_procedure_name, (
                        ServiceProviderId,
                        SiteIds,
                        TenantId,
                        PageIndex,
                        PageSize,
                        Filter,
                        BillingPeriodIds
                    ))
    
                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
    
                    # Check if there are any results
                    if results:
                        usage_data = []
                        for row in results:
                            # Extract the required values from each tuple
                            customer_name = row[34]  # Updated: 'Netontherun INC (300008190)'
                            ICCID = row[5]           # '89010303300064314942'
                            MSISDN = row[6]          # '2512709661'
                            data_usage = float(row[10])  # Convert Decimal to float for Data Usage
    
                            # Add the extracted data to the response list
                            usage_data.append({
                                'Customer name': customer_name,
                                'ICCID': ICCID,
                                'MSISDN': MSISDN,
                                'Data usage (MB)': data_usage  # In MB
                            })
                        
                        # Return the extracted usage data
                        response={"flag":True,"usage_data":usage_data}
                        return response
                    else:
                        logging.info(f"No results fount")
                        return None
    
        except Exception as e:
            logging.exception("Error in connection or execution:", e)
            return None
    elif portal_id==2:
        # Create a connection to the database
        try:
            with pytds.connect(server=server, database=database, user=username, password=password) as conn:
                with conn.cursor() as cursor:
                    # Define the stored procedure name
                    stored_procedure_name = 'AltaworxCentral_Test.dbo.[usp_Report_MobilityHistorical_HistoryByLine]'
                    # Define the parameters to pass to the stored procedure
                    ServiceProviderId = 20  # Replace with None if you want to pass NULL
                    SiteIds = None  # Assuming NULL as per your description
                    TenantId = 1
                    PageIndex = 0
                    PageSize = 50
                    Filter = None 
                    BillingPeriodIds = 417
    
                    # Execute the stored procedure with parameters
                    cursor.callproc(stored_procedure_name, (
                        ServiceProviderId,
                        SiteIds,
                        TenantId,
                        PageIndex,
                        PageSize,
                        Filter,
                        BillingPeriodIds
                    ))
    
                    # Fetch results if the stored procedure returns any
                    results = cursor.fetchall()
    
                    # Check if there are any results
                    if results:
                        usage_data = []
                        for row in results:
                            # Extract the required values from each tuple
                            customer_name = row[34]  # Updated: 'Netontherun INC (300008190)'
                            ICCID = row[5]           # '89010303300064314942'
                            MSISDN = row[6]          # '2512709661'
                            data_usage = float(row[10])  # Convert Decimal to float for Data Usage
    
                            # Add the extracted data to the response list
                            usage_data.append({
                                'customer_name': customer_name,
                                'iccid': ICCID,
                                'msisdn': MSISDN,
                                'Data usage (MB)': data_usage  # In MB
                            })
                        
                        response={"flag":True,"usage_data":usage_data}
                        return response
                    else:
                        logging.info(f"No results found")
                        return None
    
        except Exception as e:
            logging.exception("Error in connection or execution:", e)
            return None
        
# Function to format dates
def format_date(date):
    if isinstance(date, pd.Timestamp):  # Check if it's a Pandas Timestamp
        return date.strftime('%m-%d-%Y %H:%M:%S')
    return date  # Return as-is if not a Timestamp
    
def get_automation_rule_create_pop_up_data(data): 
    logging.info(f"Request Recieved")
    module_name=data.get('module_name','Automation rule')
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('tenant_name', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    table=data.get('table','automation_rule')
    # Initialize the database connection
    tenant_database = data.get('db_name', 'altaworx_central')
    service_provider_name=data.get('service_provider_name','AT&T - Telegence')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    action=data.get('action','')
    logging.debug(f"action is {action}")
    try:
        if action=='create':
            automation_rule_condition_dict = database.get_data("automation_rule_condition",{"is_active": True},['id','automation_rule_condition_name']).to_dict(orient='records')
            automation_rule_action_dict = database.get_data("automation_rule_action",{"is_active": True},['id','automation_rule_action_name']).to_dict(orient='records')
            automation_rule_followup_effective_date_type_dict = database.get_data("automation_rule_followup_effective_date_type",{"is_active": True},['id','name']).to_dict(orient='records')
            # Get integration_id
            integration_id = database.get_data("serviceprovider", {"service_provider_name": service_provider_name}, ["integration_id"])["integration_id"].to_list()[0]
            # Fetch data from 'device_status' table
            device_status_data = database.get_data('device_status', {'integration_id': integration_id, "allows_api_update": True}, ['id', 'display_name'])
            # Convert the result to a list of dictionaries and remove duplicates based on 'id'
            Has_Current_status_values = list({d['id']: d for d in device_status_data.to_dict(orient='records')}.values())
            service_provider_name_dict= database.get_data("serviceprovider", {"service_provider_name": service_provider_name}, ["id","service_provider_name"]).to_dict(orient='records')
            features_data = database.get_data('mobility_feature', {"service_provider_id": 6},['soc_code','friendly_name']).to_dict(orient='records')
            message=f"Data Fetched Successfully"
            # Prepare the response
            response = {"flag": True,
                        "automation_rule_condition_dict":automation_rule_condition_dict,
                        "automation_rule_action_dict":automation_rule_action_dict,
                        "automation_rule_followup_effective_date_type_dict":automation_rule_followup_effective_date_type_dict,
                        "Has_Current_status_values":Has_Current_status_values,
                        "service_provider_name_dict":service_provider_name_dict,'features_data':features_data,
                        'message':message
                        }
        else:
            automation_rule_condition_dict = database.get_data("automation_rule_condition",{"is_active": True},['id','automation_rule_condition_name']).to_dict(orient='records')
            automation_rule_action_dict = database.get_data("automation_rule_action",{"is_active": True},['id','automation_rule_action_name']).to_dict(orient='records')
            automation_rule_followup_effective_date_type_dict = database.get_data("automation_rule_followup_effective_date_type",{"is_active": True},['id','name']).to_dict(orient='records')
            # Get integration_id
            integration_id = database.get_data("serviceprovider", {"service_provider_name": service_provider_name}, ["integration_id"])["integration_id"].to_list()[0]
            # Fetch data from 'device_status' table
            device_status_data = database.get_data('device_status', {'integration_id': integration_id, "allows_api_update": True}, ['id', 'display_name'])
            # Convert the result to a list of dictionaries and remove duplicates based on 'id'
            Has_Current_status_values = list({d['id']: d for d in device_status_data.to_dict(orient='records')}.values())
            service_provider_name_dict= database.get_data("serviceprovider", {"service_provider_name": service_provider_name}, ["id","service_provider_name"]).to_dict(orient='records')
            features_data = database.get_data('mobility_feature', {"service_provider_id": 6},['soc_code','friendly_name']).to_dict(orient='records')
            automation_rule_id=data.get('automation_rule_id','')
            automation_rule_detail_data = database.get_data('automation_rule_detail', {"automation_rule_id": automation_rule_id,"is_active":True}).to_dict(orient='records')
            # Step 2: Format dates within automation_rule_detail_data
            for record in automation_rule_detail_data:
                record['created_date'] = format_date(record.get('created_date'))
                record['modified_date'] = format_date(record.get('modified_date'))
                record['deleted_date'] = format_date(record.get('deleted_date'))

            # Step 3: Extract rule_followup_id values from automation_rule_detail_data
            rule_followup_ids = [item['rule_followup_id'] for item in automation_rule_detail_data]
            # Step 4: Query automation_rule_followup_detail for each rule_followup_id
            automation_rule_followup_data=[]
            automation_rule_followup_details = []
            for rule_followup_id in rule_followup_ids:
                followup_detail = database.get_data('automation_rule_followup_detail', {"rule_followup_id": rule_followup_id,"is_active":True}).to_dict(orient='records')
                # Step 5: Format dates within each followup_detail
                for record in followup_detail:
                    record['created_date'] = format_date(record.get('created_date'))
                    record['modified_date'] = format_date(record.get('modified_date'))
                    record['deleted_date'] = format_date(record.get('deleted_date'))
                followup_data = database.get_data('automation_rule_followup_effective_date_type', {"id": rule_followup_id,"is_active":True}).to_dict(orient='records')
                automation_rule_followup_data.extend(followup_data)
                automation_rule_followup_details.extend(followup_detail)  # Use extend to flatten the list
            message=f"Data Fetched Successfully"
            # Prepare the response
            response = {"flag": True,"automation_rule_detail_data":automation_rule_detail_data,
                        "automation_rule_followup_details":automation_rule_followup_details,
                        "automation_rule_condition_dict":automation_rule_condition_dict,
                        "automation_rule_action_dict":automation_rule_action_dict,
                        "automation_rule_followup_effective_date_type_dict":automation_rule_followup_effective_date_type_dict,
                        "Has_Current_status_values":Has_Current_status_values,
                        "service_provider_name_dict":service_provider_name_dict,'features_data':features_data,"message":message
                        }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the optimization data',
                                       "module_name": "get_optimization_data",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        # Error Management
        error_data = {"service_name": 'Sim management',
                      "created_date": start_time,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "tenant_name": Partner,
                      "comments": message,
                      "module_name": 'get_optimization_data',
                      "request_received_at": start_time}
        common_utils_database.log_error_to_db(error_data, 'error_table')
        response = {"flag": False, "error": str(e)}
        return response
       
def get_automation_rule_data(data):
    logging.info("Request data Recieved")
    module_name=data.get('module_name','Automation rule')
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('tenant_name', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    table=data.get('table','automation_rule')
    # Initialize the database connection
    tenant_database = data.get('db_name', 'altaworx_central')
    role_name = data.get('role_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # # Get tenant's timezone
    tenant_name = data.get('tenant_name', '')
    tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
    tenant_timezone = common_utils_database.execute_query(tenant_timezone_query, params=[tenant_name])

        # Ensure timezone is valid
    if tenant_timezone.empty or tenant_timezone.iloc[0]['time_zone'] is None:
        raise ValueError("No valid timezone found for tenant.")
        
    tenant_time_zone = tenant_timezone.iloc[0]['time_zone']
    match = re.search(r'\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)', tenant_time_zone)
    if match:
        tenant_time_zone = match.group(1).replace(' ', '')  # Ensure it's formatted correctly
    try: 
        automation_rule_data=[]
        pages={}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100   # Default to 100 if no value
            limit=data.get('limit',100)
            # Calculate pages 
            pages['start']=start
            pages['end']=end
            count_params = [table]
            if module_name=='Automation rule':
                count_query = "SELECT COUNT(*) FROM %s where is_active=True" % table
            else:
                count_query = "SELECT COUNT(*) FROM %s" % table
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages['total']=int(count_result)
        # Fetch the query from the database based on the module name
        module_query_df = common_utils_database.get_data("export_queries", {"module_name":module_name})
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
        params=[start,end]
        automation_rule_data=database.execute_query(query,params=params).to_dict(orient='records')
        automation_rule_data = convert_timestamp_data(automation_rule_data,  tenant_time_zone)
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,[module_name],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        # Prepare the response
        response = {"flag": True, "automation_rule_data": serialize_data(automation_rule_data),
                    "header_map": headers_map,"pages":pages
                    }
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the optimization data',
                                       "module_name": "get_optimization_data",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        try:
            # Error Management
            error_data = {"service_name": 'Sim management',
                        "created_date": start_time,
                        "error_messag": message,
                        "error_type": e, "user": username,
                        "tenant_name": Partner,
                        "comments": message,
                        "module_name": 'get_optimization_data',
                        "request_received_at": start_time}
            common_utils_database.log_error_to_db(error_data, 'error_table')
        except Exception as e:
            logging.warning(f"Exception occured at updating the error table")
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,[module_name],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        # Prepare the response
        response = {"flag": True, "automation_rule_data": {},
                    "header_map": headers_map,"pages":{}
                    }
        return response
    
def delete_automation_rule(data,automation_rule_id):
    #automation_rule_id = data.get('automation_rule_id', '')
    logging.debug(automation_rule_id, 'automation_rule_id')
    database = data.get('db_name', 'altaworx_central')

    # Create the database URL for SQLAlchemy
    db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{database}"
    
    # Create a new engine instance
    engine = create_engine(db_url)
    
    # Create a configured "Session" class
    Session = sessionmaker(bind=engine)
    
    # Create a session
    session = Session()
    try:
        # Get the automation_rule_detail followup IDs
        automation_rule_details_data = session.execute(
            text("SELECT rule_followup_id FROM automation_rule_detail WHERE automation_rule_id = :automation_rule_id"),
            {'automation_rule_id': automation_rule_id}
        ).fetchall()

        # Extract the followup IDs
        rule_followup_ids = [row[0] for row in automation_rule_details_data if row[0] is not None]
        logging.debug(rule_followup_ids, 'rule_followup_ids')

        # Delete from automation_rule_detail
        session.execute(
            text("DELETE FROM automation_rule_detail WHERE automation_rule_id = :automation_rule_id"),
            {'automation_rule_id': automation_rule_id}
        )

        # Delete from automation_rule
        session.execute(
            text("DELETE FROM automation_rule WHERE id = :automation_rule_id"),
            {'automation_rule_id': automation_rule_id}
        )

        # If there are followup IDs, delete from the related tables
        if rule_followup_ids:
            formatted_followup_ids = ', '.join(map(str, rule_followup_ids))
            session.execute(
                text(f"DELETE FROM automation_rule_followup_detail WHERE rule_followup_id IN ({formatted_followup_ids})")
            )
            session.execute(
                text(f"DELETE FROM automation_rule_followup WHERE id IN ({formatted_followup_ids})")
            )
        else:
            logging.info("No valid followup IDs to delete.")

        # Commit the transaction
        session.commit()
        logging.info("Deletion successful in the database")
        response = {"flag": True, "message": "Deleted Successfully"}
        return response

    except Exception as e:
        # Rollback in case of any errors
        session.rollback()
        logging.exception(f"Error: {e}")
        return {"flag": False, "message": str(e)}

    finally:
        # Close the session
        session.close()     

def insert_automation_data(data):
    logging.info("Request data Recieved")
    automation_rule_id = data.get('automation_rule_id', '')
    action=data.get('action','')
    module_name=data.get('module_name','Automation rule')
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('tenant_name', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    data=data.get('data')
    # Initialize the database connection
    tenant_database = data.get('db_name', 'altaworx_central')
    # database Connection
    db = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    
    logging.debug(f"action is {action}")
    if action=='edit':
        delete_automation_rule(data,automation_rule_id)
    try:
        # Insert into automation_rule and retrieve the ID
        automation_rule_data = data['automation_data']['automation_rule']
        automation_rule_id = db.insert_data(automation_rule_data, "automation_rule")
        
        # Iterate over block_details
        for block in data['automation_data']['block_details']:
            automation_followup_id = None
            # Check if automation_followup exists and insert it
            if block.get('automation_rule_followup'):
                followup_data = block['automation_rule_followup']
                #followup_data['automation_rule_id'] = automation_rule_id  # Link to automation_rule_id
                automation_followup_id = db.insert_data(followup_data, "automation_rule_followup")
            
                # Insert into automation_followup_detail
                for followup_detail in block['automation_rule_followup_detail']:
                    followup_detail['rule_followup_id'] = automation_followup_id  # Link to followup ID
                    db.insert_data(followup_detail, "automation_rule_followup_detail")
            
            # Insert into automation_rule_detail or step_details depending on the block
            if 'automation_rule_detail' in block:
                rule_detail_data = block['automation_rule_detail']
            else:
                rule_detail_data = block['step_details']
            
            rule_detail_data['automation_rule_id'] = automation_rule_id  # Link to automation_rule_id
            rule_detail_data['rule_followup_id'] = automation_followup_id  # Link to followup ID
            db.insert_data(rule_detail_data, "automation_rule_detail")  # Insert into rule detail
            # Prepare the response
        response = {"flag": True, "message": "Data Inserted Successfully",}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {"service_name": 'Automation rule',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the optimization data',
                                       "module_name": "get_optimization_data",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        # Error Management
        error_data = {"service_name": 'Automation rule',
                      "created_date": start_time,
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "tenant_name": Partner,
                      "comments": message,
                      "module_name": 'get_optimization_data',
                      "request_received_at": start_time}
        common_utils_database.log_error_to_db(error_data, 'error_table')
        response = {"flag": False, "error": str(e)}
        return response
    
def generate_empty_excel():
    """
    Generates an empty Excel file with a placeholder sheet.
    """
    # Create a buffer to hold the Excel data
    buffer = io.BytesIO()

    # Create an empty DataFrame and write it to Excel
    empty_df = pd.DataFrame()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        empty_df.to_excel(writer, sheet_name='Sheet1', index=False)

    # Retrieve the Excel file data from the buffer
    buffer.seek(0)
    return buffer.getvalue()    

def get_device_status_card(data):
    service_provider = data.get('service_provider', '')
    action = data.get('action', '')
    ##Database connection
    database = DB('altaworx_central', **db_config)
    # Dynamically adjust query based on service_provider presence
    if service_provider:
        query = f'''
        SELECT 
            SUM(activated_count) AS activated_count,
            SUM(activation_ready_count) AS activation_ready_count,
            SUM(deactivated_count) AS deactivated_count,
            SUM(inventory_count) AS inventory_count,
            SUM(test_ready_count) AS test_ready_count,
            (SUM(activated_count) + SUM(activation_ready_count) + SUM(deactivated_count) + 
             SUM(inventory_count) + SUM(test_ready_count)) AS total_count,
            bill_year,
            bill_month
        FROM 
            public.vw_device_status_trend_by_month
        WHERE 
            service_provider = '{service_provider}'
        GROUP BY 
            bill_year, bill_month
        ORDER BY 
            bill_year DESC, bill_month DESC
        '''
    else:
        query = '''
        SELECT 
            SUM(activated_count) AS activated_count,
            SUM(activation_ready_count) AS activation_ready_count,
            SUM(deactivated_count) AS deactivated_count,
            SUM(inventory_count) AS inventory_count,
            SUM(test_ready_count) AS test_ready_count,
            (SUM(activated_count) + SUM(activation_ready_count) + SUM(deactivated_count) + 
             SUM(inventory_count) + SUM(test_ready_count)) AS total_count,
            bill_year,
            bill_month
        FROM 
            public.vw_device_status_trend_by_month
        GROUP BY 
            bill_year, bill_month
        ORDER BY 
            bill_year DESC, bill_month DESC
        '''

    try:
        # Execute the query and fetch the data
        df = database.execute_query(query, True)
        # Handle the case where no data is returned by sending default zeroes
        if df.empty:
            return [{'month': f'{pd.Timestamp(year=year, month=month, day=1).strftime("%b %Y")}',
                     'Active': 0, 'ActivationReady': 0, 'Inventory': 0,
                     'TestReady': 0, 'Deactivated': 0, 'TotalTNs': 0}
                    for year, month in [(2024, m) for m in range(1, 13)]]
        if action=='download':
            if df.empty:
                blob_data = generate_empty_excel()
                # Return JSON response for empty Excel
                response = {
                    'flag': True,
                    'blob': blob_data.decode('utf-8'),  # If you need a decoded version
                    'message': 'No data available. Empty Excel generated.'
                }
                return response
            df.columns = [
                col.replace('_', ' ').title() for col in df.columns
            ]
                # Proceed with the export if row count is within the allowed limit
            df = df.astype(str)
            df.replace(to_replace='None', value='', inplace=True)

            blob_data = dataframe_to_blob(df)
            # Return JSON response
            response = {
                'flag': True,
                'blob': blob_data.decode('utf-8')
            }
            return response
        # Drop rows with None in bill_year or bill_month
        df_cleaned = df.dropna(subset=['bill_year', 'bill_month'])
        # Sort values by bill_year and bill_month if not done in SQL
        monthly_sum = df_cleaned.sort_values(by=['bill_year', 'bill_month'], ascending=[True, True])

        # Get the most recent 12 months of data
        if len(monthly_sum) > 12:
            monthly_sum = monthly_sum.tail(12)

        # Prepare the result list in the required format
        result = [
            {
                'month': f'{pd.Timestamp(year=int(row.bill_year), month=int(row.bill_month), day=1).strftime("%b %Y")}',
                'Active': int(row['activated_count']),
                'ActivationReady': int(row['activation_ready_count']),
                'Inventory': int(row['inventory_count']),
                'TestReady': int(row['test_ready_count']),
                'Deactivated': int(row['deactivated_count']),
                'TotalTNs': int(row['total_count']),
            }
            for _, row in monthly_sum.iterrows()
        ]
        # Return the formatted result
        response={"flag":True,"device_status_card_data":result}
        return response
    except Exception as e:
        logging.exception(f"Exception is {e}")
        return []  
    
def getm2m_high_usage_chart_data(data):
    service_provider = data.get('service_provider', '')
    database = DB('altaworx_central', **db_config)
    action = data.get('action', '')
    # Dynamically adjust query based on service_provider presence
    if service_provider:
        query = f'''
            SELECT ctd_data_usage_mb, ctd_session_count
            FROM public.vw_device_high_usage_scatter_chart
            WHERE service_provider_name = '{service_provider}'
            ORDER BY ctd_data_usage_mb DESC, ctd_session_count ASC;
            '''
    else:
        query=f'''SELECT DISTINCT ctd_data_usage_mb, ctd_session_count
        FROM public.vw_device_high_usage_scatter_chart
        ORDER BY ctd_data_usage_mb DESC, ctd_session_count ASC;
        '''
    data=database.execute_query(query,True)
    if data.empty:
        response={"flag":True,"m2m_high_usage_chart_data":[]}
        return response
    
    data=data.to_dict(orient='records')
    # Sample transformation logic in Python
    unique_data = {}
    
    if action=='download':
            if data.empty:
                blob_data = generate_empty_excel()
                # Return JSON response for empty Excel
                response = {
                    'flag': True,
                    'blob': blob_data.decode('utf-8'),  # If you need a decoded version
                    'message': 'No data available. Empty Excel generated.'
                }
                return response
            data.columns = [
                col.replace('_', ' ').title() for col in data.columns
            ]
                # Proceed with the export if row count is within the allowed limit
            data = data.astype(str)
            data.replace(to_replace='None', value='', inplace=True)

            blob_data = dataframe_to_blob(data)
            # Return JSON response
            response = {
                'flag': True,
                'blob': blob_data.decode('utf-8')
            }
            return response
    
    # Loop through the database result
    for item in data:
        usage = item['ctd_data_usage_mb']
        session = item['ctd_session_count']
        
        # Only store the first occurrence of each data usage, adjusting session if needed
        if usage not in unique_data:
            unique_data[usage] = session
    
    # Convert back to a list of dictionaries
    transformed_data = [{'usage': k, 'session': v} for k, v in unique_data.items()]
    response={"flag":True,"m2m_high_usage_chart_data":transformed_data}
    return response

def get_usage_details_card(data):
    service_provider = data.get('service_provider', '')
    database = DB('altaworx_central', **db_config)
    action = data.get('action', '')
    # SQL query to fetch usage details with optional service provider filter
    query = '''
    SELECT 
        total_usage_mb AS "datausagebymonth", 
        avg_usage_per_card_mb AS "usagepertn", 
        bill_year, 
        bill_month 
    FROM 
        public.vw_device_usage_trend_by_month 
    WHERE 
        bill_year >= EXTRACT(YEAR FROM CURRENT_DATE)-1
    '''
    
    # Add service_provider filter if provided
    if service_provider:
        query += f" AND service_provider = '{service_provider}'"
        logging.info("Applying service provider filter: %s", service_provider)
    
    try:
        # Execute the query and fetch the data
        df = database.execute_query(query, True)
        
        # Handle case where no data is returned
        if df.empty:
            logging.info("No usage details data found for the query.")
            response={"flag":False,"usage_details_card":[]}
            return response
        if action=='download':
            logging.info("Download action requested.")
            if df.empty:
                logging.info("No data available for download; generating empty Excel.")
                blob_data = generate_empty_excel()
                # Return JSON response for empty Excel
                response = {
                    'flag': True,
                    'blob': blob_data.decode('utf-8'),  # If you need a decoded version
                    'message': 'No data available. Empty Excel generated.'
                }
                return response
            df.columns = [
                col.replace('_', ' ').title() for col in df.columns
            ]
            logging.info("Data columns formatted for export.")
                # Proceed with the export if row count is within the allowed limit
            df = df.astype(str)
            df.replace(to_replace='None', value='', inplace=True)

            blob_data = dataframe_to_blob(df)
            logging.info("Blob data prepared for download.")
            # Return JSON response
            response = {
                'flag': True,
                'blob': blob_data.decode('utf-8')
            }
            return response
        
        # Group by bill_year and bill_month, summing the values
        monthly_sum = df.groupby(['bill_year', 'bill_month']).sum().reset_index()

        # Sort values by bill_year and bill_month
        monthly_sum = monthly_sum.sort_values(by=['bill_year', 'bill_month'], ascending=[True, True])
        logging.info("Data grouped by bill year and month successfully.")

        # Get the most recent 12 months of data
        if len(monthly_sum) > 12:
            monthly_sum = monthly_sum.tail(12)
        
        # Prepare the result in the desired format
        result = [
            {
                "name": pd.Timestamp(year=int(row.bill_year), month=int(row.bill_month), day=1).strftime("%b %Y"),
                "usagePerTN": row['usagepertn'],
                "dataUsageByMonth": row['datausagebymonth']
            }
            for _, row in monthly_sum.iterrows()
        ]
        response={"flag":True,"usage_details_card":result}
        logging.info("Response prepared successfully: %s", response)
        return response
    except Exception as e:
        response={"flag":False,"usage_details_card":[]}
        return response


def get_rate_plan_data(data):
    database = DB('altaworx_central', **db_config)
    action = data.get('action', '')
    """
    Fetch rate plan data for carrier_rate_plan or customer_rate_plan based on plan_type.
    
    Params:
        data: dict containing 'service_provider' (optional)
        plan_type: str, either 'carrier' or 'customer' to specify the query source
    """
    service_provider = data.get('service_provider', '')
    plan_type = data.get('plan_type', 'customer')
    # Select the correct table/view based on plan_type
    if plan_type == 'carrier':
        view_name = 'public.vw_smi_sim_cards_by_carrier_rate_plan_limit_report'
    elif plan_type == 'customer':
        view_name = 'public.vw_smi_sim_cards_by_customer_rate_plan_limit_report'
    else:
        logging.error("Invalid plan_type: %s. Must be either 'carrier' or 'customer'.", plan_type)
        raise ValueError("Invalid plan_type. Must be either 'carrier' or 'customer'.")

    # SQL query to fetch SIM card details with optional service provider filter
    query = f'''
    SELECT 
        total_sim_count,
        plan_mb,
        sim_count,
        ctd_session_count,
        (sim_count * 100.0 / total_sim_count) AS sim_percentage
    FROM (
        SELECT 
            COUNT(*) AS total_sim_count,
            plan_mb,
            SUM(sim_count) AS sim_count,
            SUM(ctd_session_count) AS ctd_session_count
        FROM 
            {view_name}
    '''
    
    # Add service_provider filter if provided
    if service_provider:
        query += f" WHERE service_provider_name = '{service_provider}'"
        logging.info("Applying service provider filter: %s", service_provider)

    query += '''
        GROUP BY 
            plan_mb
    ) AS subquery
    LIMIT 12;
    '''
    
    try:
        # Execute the query and fetch the data
        logging.info("Executing query to fetch rate plan data.")
        df = database.execute_query(query, True)
        # Handle case where no data is returned
        if df.empty:
            logging.info("No rate plan data found for the query.")
            response={"flag":False,"rate_plan_data":[]}
            return response
        
        if action=='download':
            logging.info("Download action requested.")
            if df.empty:
                logging.info("No data available for download; generating empty Excel.")
                blob_data = generate_empty_excel()
                # Return JSON response for empty Excel
                response = {
                    'flag': True,
                    'blob': blob_data.decode('utf-8'),  # If you need a decoded version
                    'message': 'No data available. Empty Excel generated.'
                }
                return response
            df.columns = [
                col.replace('_', ' ').title() for col in df.columns
            ]
            logging.info("Data columns formatted for export.")
                # Proceed with the export if row count is within the allowed limit
            df = df.astype(str)
            df.replace(to_replace='None', value='', inplace=True)

            blob_data = dataframe_to_blob(df)
            logging.info("Blob data prepared for download.")
            # Return JSON response
            response = {
                'flag': True,
                'blob': blob_data.decode('utf-8')
            }
            return response
        
        
        # Prepare the result in the desired format
        result = [
            {
                "dataLimit": str(row['plan_mb']),
                "tnCount": row['sim_count']
            }
            for _, row in df.iterrows()
        ]
        logging.info("Rate plan data formatted successfully for response.")

        # Display the results
        response={"flag":True,"rate_plan_data":result}
        logging.info("Response prepared successfully: %s", response)
        return response
    except Exception as e:
        response={"flag":False,"rate_plan_data":[]}
        return response
    
    
    


def mobility_high_usage_chart(data):
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    service_provider = data.get('service_provider', '')
    action = data.get('action', '')
    
    # Determine the query based on service provider
    if service_provider:
        query = f'''
            SELECT 
                *, 
                (CASE 
                    WHEN plan_limit_mb > 0 THEN (ctd_data_usage_mb / plan_limit_mb * 100) 
                    ELSE 0 
                END) AS percentage
            FROM 
                public.vw_mobility_device_high_usage_scatter_charts
            WHERE 
                service_provider_name = '{service_provider}'
            ORDER BY 
                ctd_data_usage_mb ASC, 
                ctd_session_count DESC;
        '''
        logging.info("Query defined with service provider filter: %s", service_provider)
    else:
        query = '''
            SELECT 
                *, 
                (CASE 
                    WHEN plan_limit_mb > 0 THEN (ctd_data_usage_mb / plan_limit_mb * 100) 
                    ELSE 0 
                END) AS percentage
            FROM 
                public.vw_mobility_device_high_usage_scatter_charts
            ORDER BY 
                ctd_data_usage_mb ASC, 
                ctd_session_count DESC;
        '''
        logging.info("Query defined without service provider filter.")
    
    try:
        logging.info("Executing query to fetch high usage data.")
        # Execute the query and fetch the data (assuming DataFrame is returned)
        data = database.execute_query(query, True)
        
        # If data is empty, return empty response
        if data.empty:
            logging.info("No data found for the query.")
            return {"flag": True, "mobility_high_usage_chart": []}
        
        if action=='download':
            logging.info("Download action requested.")
            if data.empty:
                logging.info("No data available for download; generating empty Excel.")
                blob_data = generate_empty_excel()
                # Return JSON response for empty Excel
                response = {
                    'flag': True,
                    'blob': blob_data.decode('utf-8'),  # If you need a decoded version
                    'message': 'No data available. Empty Excel generated.'
                }
                return response
            data.columns = [
                col.replace('_', ' ').title() for col in data.columns
            ]
            logging.info("Data columns formatted for export.")
                # Proceed with the export if row count is within the allowed limit
            data = data.astype(str)
            data.replace(to_replace='None', value='', inplace=True)

            blob_data = dataframe_to_blob(data)
            logging.info("Blob data prepared for download.")
            # Return JSON response
            response = {
                'flag': True,
                'blob': blob_data.decode('utf-8')
            }
            return response
        
        # Convert DataFrame to dictionary records
        data = data.to_dict(orient='records')
        
        # Create a dictionary to store unique usage and session data
        unique_data = {}
        
        # Loop through each item in the data
        for item in data:
            usage = item['ctd_data_usage_mb']
            session = item['ctd_session_count']
            
            # Only store the first occurrence of each data usage, adjusting session if needed
            if usage not in unique_data:
                unique_data[usage] = session
        logging.info("Unique data filtered and stored.")

        # Convert the unique data dictionary back to a list of dictionaries
        transformed_data = [{'usage': k, 'percentUsed': v} for k, v in unique_data.items()]
        logging.info("Data transformed successfully for response.")

        # Create and return the response
        response = {"flag": True, "mobility_high_usage_chart": transformed_data}
        logging.info("Response prepared successfully: %s", response)
        return response

    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        return {"flag": False, "mobility_high_usage_chart": []}
    
    

def mobility_usage_per_customer_pool(data):
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    service_provider = data.get('service_provider', '')
    action = data.get('action', '')
    
    # Determine the query based on service provider
    if service_provider:
        query = f'''
           SELECT 
                customer_rate_pool_id,
                customer_rate_pool_name,
                service_provider_name,
                MAX(customer_rate_pool_data_usage_percenatge) AS customer_rate_pool_data_usage_percentage
            FROM 
                public.vw_mobility_usage_by_customer_pools
            WHERE 
                service_provider_name = '{service_provider}'
            GROUP BY 
                customer_rate_pool_id,
                customer_rate_pool_name,
                service_provider_name
            ORDER BY 
                customer_rate_pool_data_usage_percentage DESC
            LIMIT 12;
        '''
    else:
        query = '''
            SELECT 
                customer_rate_pool_id,
                customer_rate_pool_name,
                service_provider_name,
                MAX(customer_rate_pool_data_usage_percenatge) AS customer_rate_pool_data_usage_percentage
            FROM 
                public.vw_mobility_usage_by_customer_pools
            GROUP BY 
                customer_rate_pool_id,
                customer_rate_pool_name,
                service_provider_name
            ORDER BY 
                customer_rate_pool_data_usage_percentage DESC
            LIMIT 12;
        '''
    
    try:
        # Execute the query and fetch the data (assuming DataFrame is returned)
        data = database.execute_query(query, True)
        
        # If data is empty, return empty response
        if data.empty:
            return {"flag": True, "mobility_usage_per_customer_pool": []}
        
        if action=='download':
            if data.empty:
                blob_data = generate_empty_excel()
                # Return JSON response for empty Excel
                response = {
                    'flag': True,
                    'blob': blob_data.decode('utf-8'),  # If you need a decoded version
                    'message': 'No data available. Empty Excel generated.'
                }
                return response
            data.columns = [
                col.replace('_', ' ').title() for col in data.columns
            ]
                # Proceed with the export if row count is within the allowed limit
            data = data.astype(str)
            data.replace(to_replace='None', value='', inplace=True)

            blob_data = dataframe_to_blob(data)
            # Return JSON response
            response = {
                'flag': True,
                'blob': blob_data.decode('utf-8')
            }
            return response
        
        # Convert DataFrame to dictionary records
        data = data.to_dict(orient='records')
        
        # Create a list to store the formatted data
        formatted_data = []
        
        # Loop through each item in the data and format it as required
        for item in data:
            pool_name = item['customer_rate_pool_name']
            usage_percentage = item['customer_rate_pool_data_usage_percentage']
            
            # Append the formatted data as a dictionary
            formatted_data.append({
                'name': pool_name,
                'value': usage_percentage
            })

        # Create and return the response
        response = {"flag": True, "mobility_usage_per_customer_pool": formatted_data}
        return response

    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        return {"flag": False, "mobility_usage_per_customer_pool": []}
    
    
def mobility_usage_per_group_pool(data):
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    service_provider = data.get('service_provider', '')
    action = data.get('action', '')

    # Determine the query based on service provider
    if service_provider:
        query = f'''
            SELECT 
                foundation_account_number,
                data_group_id,
                pool_id,
                customer_name,
                MAX(data_usage_percentage) AS data_usage_percentage,
                MAX(data_group_device_count) AS data_group_device_count,
                MAX(pool_device_count) AS pool_device_count
            FROM 
                public.vw_mobility_usage_by_group_pools
            WHERE 
                customer_name = '{service_provider}'  -- Filter by customer_name when service_provider is provided
                AND data_usage_percentage IS NOT NULL 
                AND (data_group_device_count > 0 OR pool_device_count > 0)
            GROUP BY 
                foundation_account_number, data_group_id, pool_id, customer_name
            LIMIT 12;
        '''
        logging.info("Query defined with service provider filter: %s", service_provider)
    else:
        query = '''
            SELECT 
                foundation_account_number,
                data_group_id,
                pool_id,
                customer_name,
                MAX(data_usage_percentage) AS data_usage_percentage,
                MAX(data_group_device_count) AS data_group_device_count,
                MAX(pool_device_count) AS pool_device_count
            FROM 
                public.vw_mobility_usage_by_group_pools
            WHERE 
                data_usage_percentage IS NOT NULL 
                AND (data_group_device_count > 0 OR pool_device_count > 0)
            GROUP BY 
                foundation_account_number, data_group_id, pool_id, customer_name
            LIMIT 12;
        '''
        logging.info("Query defined without service provider filter.")
    
    try:
        logging.info("Executing query to fetch mobility usage data.")
        # Execute the query and fetch the data (assuming DataFrame is returned)
        data = database.execute_query(query, True)
        
        # If data is empty, return empty response
        if data.empty:
            logging.info("No data found for the query.")
            return {"flag": True, "mobility_usage_per_group_pool": []}
        
        if action=='download':
            logging.info("Download action requested.")
            if data.empty:
                logging.info("No data available for download; generating empty Excel.")

                blob_data = generate_empty_excel()
                # Return JSON response for empty Excel
                response = {
                    'flag': True,
                    'blob': blob_data.decode('utf-8'),  # If you need a decoded version
                    'message': 'No data available. Empty Excel generated.'
                }
                return response
            data.columns = [
                col.replace('_', ' ').title() for col in data.columns
            ]
            logging.info("Data columns formatted for export.")
                # Proceed with the export if row count is within the allowed limit
            data = data.astype(str)
            data.replace(to_replace='None', value='', inplace=True)

            blob_data = dataframe_to_blob(data)
            logging.info("Blob data prepared for download.")
            # Return JSON response
            response = {
                'flag': True,
                'blob': blob_data.decode('utf-8')
            }
            return response
        
        # Convert DataFrame to dictionary records
        data = data.to_dict(orient='records')
        
        # Create a list to store the formatted data
        formatted_data = []
        
        # Loop through each item in the data and format it as required
        for item in data:
            # Use 'data_group_id' if present, otherwise use 'pool_id'
            pool_name = item['data_group_id'] if item['data_group_id'] else item['pool_id']
            usage_percentage = item['data_usage_percentage']

            # Append the formatted data as a dictionary in the required format
            formatted_data.append({
                'name': pool_name,
                'value': usage_percentage
            })
        logging.info("Data formatted successfully for response.")

        # Create and return the response
        response = {"flag": True, "mobility_usage_per_group_pool": formatted_data}
        logging.info("Response prepared successfully: %s", response)
        return response

    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        return {"flag": False, "mobility_usage_per_group_pool": []}
    





def get_automation_rule_details_data(data):
    database = DB('altaworx_central', **db_config)
    automation_rule_name=data.get('automation_rule_name','')
    service_provider_name=data.get('service_provider_name','AT&T - Telegence')
    logging.info("Fetching automation rule ID for rule name: %s", automation_rule_name)
    automation_rule_id = database.get_data('automation_rule', {"automation_rule_name": automation_rule_name},['id'])['id'].to_list()[0]
    logging.info("Automation rule ID fetched: %s", automation_rule_id)
    automation_rule_detail_data = database.get_data('automation_rule_detail', {"automation_rule_id": automation_rule_id},['rule_followup_id','automation_rule_id','rule_condition_id','rule_condition_value','rule_action_id','rule_action_value','condition_step']).to_dict(orient='records')
    # Get integration_id
    integration_id = database.get_data("serviceprovider", {"service_provider_name": service_provider_name}, ["integration_id"])["integration_id"].to_list()[0]
    # Fetch data from 'device_status' table
    device_status_data = database.get_data('device_status', {'integration_id': integration_id, "allows_api_update": True}, ['id', 'display_name'])
    # Convert the result to a list of dictionaries and remove duplicates based on 'id'
    Has_Current_status_values = list({d['id']: d for d in device_status_data.to_dict(orient='records')}.values())
    features_data = database.get_data('mobility_feature', {"service_provider_id": 6},['soc_code','friendly_name']).to_dict(orient='records')
    automation_rule_condition_dict = database.get_data("automation_rule_condition",{"is_active": True},['id','automation_rule_condition_name']).to_dict(orient='records')
    automation_rule_action_dict = database.get_data("automation_rule_action",{"is_active": True},['id','automation_rule_action_name']).to_dict(orient='records')
    automation_rule_followup_effective_date_type_dict = database.get_data("automation_rule_followup_effective_date_type",{"is_active": True},['id','name']).to_dict(orient='records')
    message=f"Data Fetched Successfully"
    logging.info(message)
    # Prepare the response
    response = {"flag": True,"automation_rule_detail_data":automation_rule_detail_data,"automation_rule_condition_dict":automation_rule_condition_dict,"automation_rule_action_dict":automation_rule_action_dict,"automation_rule_followup_effective_date_type_dict":automation_rule_followup_effective_date_type_dict
                ,"Has_Current_status_values":Has_Current_status_values,'features_data':features_data,
                'message':message
                }
    return response




def bulk_upload_download_template(data):
    # Get columns for the specific table
    module_name = data.get('module_name', '')
    table_name = data.get('table_name', '')
    
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    columns_df = database.get_table_columns(table_name)
    columns_to_remove = [
                'created_by',
                'created_date',
                'modified_by',
                'modified_date',
                'last_email_triggered_at'
                'email_status',
                'attachments'
            ]
    columns_df = columns_df[~columns_df['column_name'].str.lower().isin([col.lower() for col in columns_to_remove])]
    # Remove the 'id' column if it exists
    columns_df = columns_df[columns_df['column_name'] != 'id']
    # Filter out the columns to remove
    columns_df['column_name'] = columns_df['column_name'].str.replace('_', ' ').str.capitalize()
 

    # Create an empty DataFrame with the column names as columns
    result_df = pd.DataFrame(columns=columns_df['column_name'].values)
    #return result_df
    blob_data = dataframe_to_blob(result_df)
    response = {
            'flag': True,
            'blob': blob_data.decode('utf-8')
        }
    return response

def determine_nsdev(value):
    if value in ["yes", "Yes", "YES"]:
        return True
    elif value in ["no", "No", "NO"]:
        return False
    else:
        return True  # Default value   
    
def import_bulk_data(data):
    username = data.get('username', None)
    insert_flag = data.get('insert_flag', 'append')
    table_name = data.get('table_name', '')
    created_by=data.get('username','')
    created_date=data.get('request_received_at','')
    module_name=data.get('module_name','')
    # Initialize the database connection
    
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # Check if blob data is provided
    blob_data = data.get('blob')
    if not blob_data:
        message = "Blob data not provided"
        response = {"flag": False, "message": message}
        return response
    try:
        # Extract and decode the blob data
        blob_data = blob_data.split(",", 1)[1]
        blob_data += '=' * (-len(blob_data) % 4)  # Padding for base64 decoding
        file_stream = BytesIO(base64.b64decode(blob_data))

        # Read the data into a DataFrame
        uploaded_dataframe = pd.read_excel(file_stream, engine='openpyxl')
        if uploaded_dataframe.empty:
            response={"flag":False,"message":"Uploaded Excel has no data please add the data"}
            return response
        uploaded_dataframe.columns = uploaded_dataframe.columns.str.replace(' ', '_').str.lower()
        logging.info("Uploaded DataFrame:\n", uploaded_dataframe)
        if module_name=='Customer Rate Pool':
            # Add necessary columns
            uploaded_dataframe['created_by'] = username
            uploaded_dataframe['created_date'] = created_date
            uploaded_dataframe['modified_by'] = username  # Using username
            uploaded_dataframe['modified_date'] = created_date  # Today's date
            uploaded_dataframe['is_deleted'] = False  # Default value
            uploaded_dataframe['is_active'] = True  # Default value
            uploaded_dataframe['deleted_by'] = ""  # Default value
            uploaded_dataframe['deleted_date'] = created_date  # Default value
        elif module_name=='IMEI Master Table':
            uploaded_dataframe['created_date'] = created_date
            uploaded_dataframe['modified_by'] = username  # Using username
            uploaded_dataframe['modified_date'] = created_date  # Today's date
            uploaded_dataframe['is_active'] = True  # Default value
            uploaded_dataframe['deleted_by'] = ""  # Default value
            uploaded_dataframe['deleted_date'] = created_date  # Default value
            uploaded_dataframe['volte_capable'] = uploaded_dataframe['volte_capable'].apply(determine_nsdev)
            uploaded_dataframe['att_certified'] = uploaded_dataframe['att_certified'].apply(determine_nsdev)
            uploaded_dataframe['nsdev'] = uploaded_dataframe['nsdev'].apply(determine_nsdev)
            
            uploaded_dataframe['service_provider'] = 1  # Default value
        elif module_name=='Customer Groups':
            uploaded_dataframe['created_by'] = username
            uploaded_dataframe['modified_by'] = username  # Using username
            uploaded_dataframe['modified_date'] = created_date  # Today's date
            uploaded_dataframe['is_active'] = True  # Default value
            uploaded_dataframe['is_deleted'] = False  # Default value
            uploaded_dataframe['deleted_by'] = ""  # Default value
            uploaded_dataframe['deleted_date'] = created_date  # Default value
            uploaded_dataframe['created_date'] = created_date  # Default value
        elif module_name=='Users':
            uploaded_dataframe['created_date'] = created_date 
            uploaded_dataframe['created_by'] = username
            uploaded_dataframe['modified_date'] = created_date
            uploaded_dataframe['modified_by'] = username
            uploaded_dataframe['deleted_date'] = created_date
            uploaded_dataframe['deleted_by'] = username
            uploaded_dataframe['is_active'] = True
            uploaded_dataframe['is_deleted'] = False
            uploaded_dataframe['migrated'] = False
            
        elif module_name=='customer rate plan':
            uploaded_dataframe['created_by'] = username
            uploaded_dataframe['created_date'] = created_date
            uploaded_dataframe['modified_by'] = username  # Using username
            uploaded_dataframe['modified_date'] = created_date  # Today's date
            uploaded_dataframe['is_deleted'] = False  # Default value
            uploaded_dataframe['is_active'] = True  # Default value
            uploaded_dataframe['deleted_by'] = ""  # Default value
            uploaded_dataframe['deleted_date'] = created_date  # Default value
            # Add necessary columns
            # Default value
            uploaded_dataframe['service_provider_id'] = None
            uploaded_dataframe['serviceproviderids'] = None    
            uploaded_dataframe['surcharge_3g'] = None   
        if module_name in ("Users"):
            logging.info("Inserting data into common_utils_database table: %s", table_name)
            common_utils_database.insert(uploaded_dataframe, table_name, if_exists='append', method='multi')
        else:
            database.insert(uploaded_dataframe, table_name, if_exists='append', method='multi')
        message = "Upload operation is done"
        if module_name=='Users':
            uploaded_dataframe['last_modified_by'] = username
            uploaded_dataframe['module_name'] = username
            uploaded_dataframe['module_id'] = username
            uploaded_dataframe['sub_module_name'] = username
            uploaded_dataframe['sub_module_id'] = 1
            uploaded_dataframe['module_features'] = ''
            uploaded_dataframe['temp_password'] = ''
            uploaded_dataframe['mode'] = ''
            uploaded_dataframe['theme'] = ''
            uploaded_dataframe['customer_group'] = ''
            uploaded_dataframe['customers'] = ''
            uploaded_dataframe['service_provider'] = ''
            uploaded_dataframe['city'] = ''
            uploaded_dataframe['access_token'] = ''
            uploaded_dataframe['user_id'] = ''  
        # Get and normalize DataFrame columns
        # columns_ = [col.strip().lower() for col in uploaded_dataframe.columns]
        columns_ = [col.strip().lower().replace(' ', '_') for col in uploaded_dataframe.columns]
 
        logging.info("Normalized Columns from DataFrame:", columns_)

        # Get column names from the database table
        columns_df = database.get_table_columns(table_name)
        logging.info("Fetched Columns from Database:\n", columns_df)

        # Remove the 'id' column if it exists
        columns_df = columns_df[columns_df['column_name'] != 'id']

        # Normalize database columns for comparison
        column_names = [col.strip().lower() for col in columns_df['column_name']]
        logging.info("Normalized Columns from Database Query:", column_names)

        # Compare the column names (ignoring order)
        if sorted(columns_) != sorted(column_names):
            logging.info("Column mismatch detected.")
            logging.info("Columns in DataFrame but not in Database:", set(columns_) - set(column_names))
            logging.info("Columns in Database but not in DataFrame:", set(column_names) - set(columns_))
            message = "Columns didn't match"
            response = {"flag": False, "message": message}
            return response

        # Return success response
        response = {"flag": True, "message": message}
        return response
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        message = f"An error occurred during the import: {str(e)}"
        response = {"flag": False, "message": message}
        return response



def get_inventory_data(data):
    '''
    Retrieves the optimization data.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_params' for querying the status history.

    Returns:
    - dict: A dictionary containing the List view data, header mapping, and a success message or an error message.
    '''
    logging.info("Request Data Recieved")
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('tenant_name', '')
    role_name = data.get('role_name', '')
    request_received_at = data.get('request_received_at', '')
    username = data.get('username', ' ')
    table = data.get('table', 'vw_sim_management_inventory_list_view')
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:
        # Initialize the database connection
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        tenant_database=data.get('db_name','altaworx_central')
        inventory_data=[]
        pages={}
        if "mod_pages" in data:
            start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
            end = data["mod_pages"].get("end") or 100   # Default to 100 if no value
            logging.debug(f"starting page is {start} and ending page is {end}")
            limit=data.get('limit',100)
            # Calculate pages 
            pages['start']=start
            pages['end']=end
            count_params = [table]
            count_query = "SELECT COUNT(*) FROM %s where is_active=True" % table
            count_result = database.execute_query(count_query, count_params).iloc[0, 0]
            pages['total']=int(count_result)

        params=[start,end]
        query = '''
            SELECT id,
                service_provider_display_name as service_provider,service_provider_id,
                TO_CHAR(date_added, 'MM-DD-YYYY HH24:MI:SS') AS date_added,
                iccid,
                msisdn,
                eid,
                customer_name,
                username,
                carrier_cycle_usage_mb as carrier_cycle_usage,
                customer_cycle_usage_mb as customer_cycle_usage,
                customer_rate_pool_name,
                customer_rate_plan_name,soc,cost_center,
                carrier_rate_plan_name as carrier_rate_plan,
                sim_status,
                TO_CHAR(date_activated, 'MM-DD-YYYY HH24:MI:SS') AS date_activated,
                ip_address,
                billing_account_number,
                foundation_account_number,
                modified_by,
                TO_CHAR(modified_date, 'MM-DD-YYYY HH24:MI:SS') AS modified_date,
                TO_CHAR(last_usage_date, 'MM-DD-YYYY HH24:MI:SS') AS last_usage_date
                FROM 
                public.sim_management_inventory where is_active=True
            ORDER BY 
                modified_date DESC 
           OFFSET %s LIMIT %s ;
        '''
        inventory_data=database.execute_query(query,params=params).to_dict(orient='records')
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,["SimManagement Inventory"],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        # Prepare the response
        response = {"flag": True, "inventory_data": inventory_data, "header_map": headers_map,"pages":pages}
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))

            audit_data_user_actions = {"service_name": 'Sim Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(response['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "tenant_name": Partner,
                                       "comments": 'fetching the optimization data',
                                       "module_name": "get_inventory_data",
                                       "request_received_at": request_received_at
                                       }
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        # Handle exceptions and provide feedback
        logging.exception(f"Exception occurred: {e}")
        message = "Something went wrong while updating data"
        try:
            # Error Management
            error_data = {"service_name": 'Sim management',
                        "created_date": start_time,
                        "error_messag": message,
                        "error_type": e, "user": username,
                        "tenant_name": Partner,
                        "comments": message,
                        "module_name": 'get_inventory_data',
                        "request_received_at": start_time}
            common_utils_database.log_error_to_db(error_data, 'error_table')
        except Exception as e:
            logging.warning(f"Exception at updating the error table")
        # Generate the headers mapping
        headers_map=get_headers_mappings(tenant_database,["SimManagement Inventory"],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data)
        # Prepare the response
        response = {"flag": True, "inventory_data": {}, "header_map": headers_map,"pages":{}}


def update_features_pop_up_data(data):
    # Initialize the database connection
    tenant_database = data.get('db_name', '')
    service_provider_id = data.get('service_provider_id', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        soc_codes=database.get_data('mobility_feature',{"service_provider_id":service_provider_id,"is_active":True},['soc_code','friendly_name']).to_dict(orient='records')
        response={"flag":True,"soc_codes":soc_codes,"message":"soc codes retrieved successfully"}
        return response
    except Exception as e:
        logging.exception(f"Error occured while getting soc codes")
        response={"flag":True,"soc_codes":{},"message":"soc codes retrieved failed"}
        return response


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


        

# AWS S3 client
s3_client = boto3.client('s3')

def async_upload_to_s3(excel_data, file_name):
    """
    Asynchronously uploads the generated Excel file to S3.
    """
    # You might want to configure the S3 bucket name
    S3_BUCKET_NAME = 'searchexcelssandbox'
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=excel_data,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        logging.info(f"Excel file uploaded successfully to S3 with key: {file_name}")
    except Exception as e:
        logging.error(f"Error during S3 upload: {e}")

def export_to_s3_bucket(data, max_rows=5000000):
    '''
    Exports data into an Excel file, stores it in S3, and returns the URL.
    '''
    # Extract parameters from the request data
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', None)
    module_name = data.get('module_name', '')
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    user_name = data.get('user_name', '')
    session_id = data.get('session_id', '')
    tenant_database = data.get('db_name', '')
    ids = data.get('ids', '')

    # Database connection for common utilities
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    
    print(f"Fetching export query for module: {module_name}")
    
    start_time = time.time()
    try:
        # --- Timer 1: Database query execution ---
        db_start_time = time.time()

        database = DB(tenant_database, **db_config)
        # Fetch the query from the database based on the module name
        module_query_df = db.get_data("export_queries", {"module_name": module_name})

        db_end_time = time.time()
        logging.info(f"Database query took {db_end_time - db_start_time:.4f} seconds.")

        if module_query_df.empty:
            return {
                'flag': False,
                'message': f'No query found for module name: {module_name}'
            }

        query = module_query_df.iloc[0]['module_query']
        if not query:
            logging.warning(f"Unknown module name: {module_name}")
            return {
                'flag': False,
                'message': f'Unknown module name: {module_name}'
            }

        # Params for the specific module
        params = [start_date, end_date] if module_name not in ("inventory status history", "bulkchange status history") else [ids]
        
        # --- Timer 2: Execute query and fetch data ---
        query_start_time = time.time()

        data_frame = database.execute_query(query, params=params)
        
        query_end_time = time.time()
        logging.info(f"Query execution took {query_end_time - query_start_time:.4f} seconds.")

        row_count = data_frame.shape[0]
        if row_count > max_rows:
            return {
                'flag': False,
                'message': f'Cannot export more than {max_rows} rows.'
            }

        # --- Timer 3: DataFrame Processing ---
        processing_start_time = time.time()

        data_frame.columns = [col.replace('_', ' ').capitalize() for col in data_frame.columns]
        data_frame['S.No'] = range(1, len(data_frame) + 1)
        columns = ['S.No'] + [col for col in data_frame.columns if col != 'S.No']
        data_frame = data_frame[columns]

        processing_end_time = time.time()
        logging.info(f"Data processing took {processing_end_time - processing_start_time:.4f} seconds.")

        # --- Timer 4: Excel File Creation ---
        excel_creation_start_time = time.time()

        wb = Workbook()
        ws = wb.active
        ws.title = 'Export Data'
        
        for r_idx, row in enumerate(dataframe_to_rows(data_frame, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)

        excel_creation_end_time = time.time()
        logging.info(f"Excel file creation took {excel_creation_end_time - excel_creation_start_time:.4f} seconds.")

        # Save the Excel file to an in-memory buffer
        excel_buffer = BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)

        # Generate a unique file name for the Excel file
        file_name = f"exports/{module_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        # --- Timer 5: Upload to S3 ---
        upload_start_time = time.time()

        threading.Thread(target=async_upload_to_s3, args=(excel_buffer.getvalue(), file_name)).start()
        
        upload_end_time = time.time()
        logging.info(f"Async upload to S3 started in {upload_end_time - upload_start_time:.4f} seconds.")
        # You might want to configure the S3 bucket name
        S3_BUCKET_NAME = 'searchexcelssandbox'
        # Generate the public URL immediately
        s3_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file_name}"
        
        # Final response construction
        end_time = time.time()
        total_time = f"{end_time - start_time:.4f}"

        response = {
            'flag': True,
            's3_url': s3_url
        }

        # Audit user actions
        audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": request_received_at,
            "created_by": user_name,
            "status": str(response['flag']),
            "time_consumed_secs": total_time,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "export", "request_received_at": request_received_at
        }
        db.update_audit(audit_data_user_actions, 'audit_user_actions')

        logging.info("Auditing user actions.")
        return response

    except Exception as e:
        error_type = str(type(e).__name__)
        logging.exception(f"An error occurred: {e}")
        message = f"Error is {e}"
        response = {"flag": False, "message": message}
        
        try:
            error_data = {
                "service_name": 'Module Management',
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "export", "request_received_at": request_received_at
            }
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception in error logging: {e}")

        return response



def statuses_inventory(data):
    logging.info("Request Data recieved")
    service_provider_id=data.get('service_provider_id','')
    tenant_database = data.get('db_name', 'altaworx_central')
    database = DB(tenant_database, **db_config)
    try:
        integration_id=database.get_data('serviceprovider',{"id":service_provider_id},['integration_id'])['integration_id'].to_list()[0]
        statuses=database.get_data('device_status',{"integration_id":integration_id,"is_active":True,"is_deleted":False},['display_name'])['display_name'].to_list()
        response={"flag":True,"update_status_values":statuses}
        return response
    except Exception as e:
        logging.exception(f"Exception while fetching statuses")
        response={"flag":True,"update_status_values":[]}
        return response
    


