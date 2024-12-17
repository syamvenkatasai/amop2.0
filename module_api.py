"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
# Importing the necessary Libraries
import time
import datetime
from datetime import datetime
from io import BytesIO
import requests
import base64
import json
import boto3
import pandas as pd
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
from common_utils.email_trigger import send_email
from common_utils.permission_manager import PermissionManager
import os
import base64
import re
import threading
from pytz import timezone
import concurrent.futures
import boto3
import logging
import os
import time
from io import BytesIO
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import threading  # For asynchronous execution
import boto3
import os
import time
from io import StringIO

# Dictionary to store database configuration settings retrieved from environment variables.
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
logging = Logging(name="module_api")



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


def form_modules_dict(data,sub_modules,tenant_modules,role_name):
    '''
    Description:The form_modules_dict function constructs a nested dictionary that maps parent modules 
    to their respective submodules and child modules. It filters and organizes modules based on the 
    user's role, tenant permissions, and specified submodules.
    '''
    logging.info("Starting to form modules dictionary.")
    # Initialize an empty dictionary to store the output
    out={}
    # Iterate through the list of modules in the data
    for item in data:
        parent_module = item['parent_module_name']
        logging.debug("Processing parent module: %s", parent_module)
        # Skip modules not assigned to the tenant unless the role is 'super admin'
        if (parent_module not in tenant_modules and parent_module
            ) and role_name.lower() != 'super admin':
            continue
        # If there's no parent module, initialize an empty dictionary for the module
        if not parent_module:
            out[item['module_name']]={}
            continue
        else:
            out[item['parent_module_name']]={}
        # Iterate through the data again to find related modules and submodules
        for module in data:
            temp={}
            # Skip modules not in the specified submodules unless the role is 'super admin'
            if (module['module_name'] not in sub_modules and module['submodule_name'] not in sub_modules
                ) and role_name.lower() != 'super admin':
                logging.debug("Skipping parent module: %s (not in tenant modules)", parent_module)
                continue
            # Handle modules without submodules and create a list for them
            if module['parent_module_name'] == parent_module and module['module_name'
                                            ] and not module['submodule_name']:
                temp={module['module_name']:[]}
            # Handle modules with submodules and map them accordingly
            elif  module['parent_module_name'] == parent_module and module['module_name'] and module['submodule_name']:
                temp={module['submodule_name']:[module['module_name']]}
            # Update the output dictionary with the constructed module mapping
            if temp:
                for key,value in temp.items():
                    if key in out[item['parent_module_name']]:
                        out[item['parent_module_name']][key].append(value[0])
                    elif temp:
                        out[item['parent_module_name']].update(temp)

    # Return the final dictionary containing the module mappings  
    logging.info("Finished forming modules dictionary: %s", out)                  
    return out

def transform_structure(input_data):
    '''
    Description:The transform_structure function transforms a nested dictionary 
    of modules into a list of structured dictionaries,each with queue_order to 
    maintain the order of parent modules, child modules, and sub-children
    '''
    
    logging.info("Starting transformation of input data.")
    
    # Initialize an empty list to store the transformed data
    transformed_data = []
    # Initialize the queue order for parent modules
    queue_order = 1 
    # Iterate over each parent module and its children in the input data
    for parent_module, children in input_data.items():
        transformed_children = []
        child_queue_order = 1
        # Iterate over each child module and its sub-children
        for child_module, sub_children in children.items():
            transformed_sub_children = []
            sub_child_queue_order = 1
            # Iterate over each sub-child module
            for sub_child in sub_children:
                transformed_sub_children.append({
                    "sub_child_module_name": sub_child,
                    "queue_order": sub_child_queue_order,
                    "sub_children": []
                })
                sub_child_queue_order += 1
            # Append the transformed child module with its sub-children
            transformed_children.append({
                "child_module_name": child_module,
                "queue_order": child_queue_order,
                "sub_children": transformed_sub_children
            })
            child_queue_order += 1
        # Append the transformed parent module with its children
        transformed_data.append({
            "parent_module_name": parent_module,
            "queue_order": queue_order,
            "children": transformed_children
        })
        queue_order += 1
    # Return the list of transformed data
    return transformed_data






def get_modules_back(data):
    '''
    Description:Retrieves and combines module data for a specified user and tenant 
    from the database.It executes SQL queries to fetch modules based on user roles 
    and tenant associations, 
    merges the results, removes duplicates, and sorts the data.
    The function then formats the result into JSON, logs audit and error information, 
    and returns the data along with a success or error message.
    '''
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    '''
    if the data is coming from 1.0 then saving and auditing the user  details
    '''
    # Check if "1.0": true exists in the data
    if data.get("1.0"):
        # Add values to the database since "1.0": true is present
        try:
            user_name = data.get("username", '')
            role_name = data.get("role_name", '')
            tenant_name = data.get("tenant_name", '')
            session_id = data.get("session_id", '')
            request_received_at = data.get("request_received_at", '')
            access_token = data.get("access_token", '')
            tenant_database = data.get("db_name", '')

            # Database connection
            database = DB(tenant_database, **db_config)
            db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
            p = {"last_login": request_received_at, "access_token": access_token}
            update_result = db.update_dict("users", p, {"username": user_name})
            
            
            if update_result:
                message = f"User table successfully updated for user: {user_name}."
                logging.info(message)  # Output message for verification
            else:
                message = f"Failed to update user table for user: {user_name}."
                logging.info(message)
            
            if not user_name:
                message = "Username not present in request data."
                logging.info(f"Message: {message}")
                
            # Check and update the live_sessions table
            login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Check if the user already has an active session
            try:
                session_record = db.get_data("live_sessions", {'username':user_name},['username'])['username'].to_list()[0]
            except:
                session_record=None

            # Check if session_record is either a string or a DataFrame
            if isinstance(session_record, pd.DataFrame) and not session_record.empty:
                # If session is active, update the record with new details
                session_data = {
                    "access_token": access_token,
                    "login": login_time,
                    "last_request": login_time
                }
                db.update_dict("live_sessions", session_data, {"username": user_name, "status": "active"})
                logging.info("Session updated.")
            elif isinstance(session_record, str) and session_record == user_name:
                # If session is found but returned as a string (i.e., user already exists)
                session_data = {
                    "access_token": access_token,
                    "login": login_time,
                    "last_request": login_time
                }
                db.update_dict("live_sessions", session_data, {"username": user_name, "status": "active"})
                logging.info("Session updated for user found as string.")
            else:
                # If no active session, insert a new record
                # logging.info(f"No active session found for {user_name}. Inserting new session record.")
                session_data = {
                    "username": user_name,
                    "access_token": access_token,
                    "status": "active",
                    "login": login_time,
                    "last_request": login_time
                }
                sessions_id=db.insert_data(session_data, 'live_sessions')

            # Audit log for user actions
            end_time = time.time()
            time_consumed = int(end_time - start_time)
            audit_data_user_actions = {
                "service_name": 'User_authentication',
                "created_date": request_received_at,
                "created_by": user_name,
                "status": "True",
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": tenant_name,
                "comments": "1.0 User login data added",
                "module_name": "",
                "request_received_at": request_received_at
            }
            db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
            db.update_audit(audit_data_user_actions, 'audit_user_actions')

        except Exception as e:
            logging.exception(f"Something went wrong: {e}")
            message = "Something went wrong while updating user login data."
            error_data = {
                "service_name": 'Module_api',
                "created_date": request_received_at,
                "error_message": message,
                "error_type": str(e),
                "user": user_name,
                "session_id": session_id,
                "tenant_name": tenant_name,
                "comments": message,
                "module_name": "",
                "request_received_at": start_time
            }
            database.log_error_to_db(error_data, 'error_table')
            return {"flag": False, "message": message}
        
        
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
    ##database connection
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    # Start time  and date calculation
    start_time = time.time()
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    role_name = data.get('role_name', None)
    tenant_database = data.get('db_name', '')

    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        # Retrieving the Modules for the User
        final_modules=[]

        tenant_module_query_params = [tenant_name]
        tenant_module_query = '''SELECT t.id,tm.module_name
                                FROM tenant t JOIN tenant_module tm ON t.id = tm.tenant_id 
                                    WHERE t.tenant_name = %s and tm.is_active = true; '''
        tenant_module_dataframe = db.execute_query(
            tenant_module_query, params=tenant_module_query_params)

        tenant_id = tenant_module_dataframe["id"].to_list()[0]
        main_tenant_modules = tenant_module_dataframe["module_name"].to_list()

        role_module_df = database.get_data("role_module",{"role":role_name},["sub_module"])
        role_modules_list = []
        role_main_modules_list=[]
        if not role_module_df.empty:
            role_module = json.loads(role_module_df["sub_module"].to_list()[0])
            for key, value_list in role_module.items():
                role_main_modules_list.append(key)
                role_modules_list.extend(value_list)
            # logging.info(role_modules_list,role_main_modules_list,'role_modules_list')

        user_module_df = database.get_data(
            "user_module_tenant_mapping",{"user_name":username,"tenant_id":tenant_id
                                          },["module_names"])
            
        user_modules_list = []
        user_main_modules_list=[]
        try:
            if not user_module_df.empty:
                user_module = json.loads(user_module_df["module_names"].to_list()[0])
                for key, value_list in user_module.items():
                    user_main_modules_list.append(key)
                    user_modules_list.extend(value_list)
        except:
            pass
        # Determine the final list of modules based on user and role data
        final_user_role__main_module_list=[]
        if user_modules_list:
            final_user_role__main_module_list=user_main_modules_list
            for item in user_modules_list:
                final_modules.append(item)
        else:
            final_user_role__main_module_list=role_main_modules_list
            for item in role_modules_list:
                final_modules.append(item)
                    
        main_tenant_modules = list(set(main_tenant_modules
                                       ) & set(final_user_role__main_module_list))
        # Retrieve module data and transform it into the required structure
        # logging.info(final_modules,main_tenant_modules)
        module_table_df=db.get_data(
            "module",{"is_active":True},["module_name","parent_module_name","submodule_name"
                                         ],{'id':"asc"}).to_dict(orient="records")
        return_dict=form_modules_dict(module_table_df,final_modules,main_tenant_modules,role_name)
        return_dict=transform_structure(return_dict)
        # Retrieve tenant logo
        logo=db.get_data("tenant",{'tenant_name':tenant_name},['logo'])['logo'].to_list()[0]
        message = "Module data sent sucessfully"
        response = {"flag": True, "message": message, "Modules": return_dict,"logo":logo}
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        try:
            audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": request_received_at,
            "created_by": username,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "get_modules","request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')     
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting modules"
        # Error Management
        error_data = {"service_name": 'Module_api', "created_date": request_received_at,
                       "error_messag": message, "error_type": e, "user": username,
                      "session_id": session_id, "tenant_name": tenant_name, "comments": message,
                      "module_name": "", "request_received_at": start_time}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}

def get_module_data(data,flag=False,):
    '''
    Retrieves module data for a specified module by checking user and tenant to get the features
      by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, 
    handles errors, and logs relevant information.
    '''
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    # Extract  fields from Request Data
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    role = data.get('role_name', None)
    module_name=data.get('module_name', None)
    session_id=data.get('session_id', None)
    sub_parent_module=data.get('sub_parent_module', None)
    parent_module=data.get('parent_module', None)
    Partner = data.get('Partner', '')
    tenant_id=('tenant_id',1)
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    
    # # Get tenant's timezone
    tenant_name = data.get('tenant_name', '')
    tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
    tenant_timezone = db.execute_query(tenant_timezone_query, params=[tenant_name])

        # Ensure timezone is valid
    if tenant_timezone.empty or tenant_timezone.iloc[0]['time_zone'] is None:
        raise ValueError("No valid timezone found for tenant.")
        
    tenant_time_zone = tenant_timezone.iloc[0]['time_zone']
    match = re.search(r'\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)', tenant_time_zone)
    if match:
        tenant_time_zone = match.group(1).replace(' ', '')  # Ensure it's formatted correctly
    # ##Restriction Check for the Amop API's
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
    #     logging.warning(f"got exception in the restriction {e}")
    try:  
        tenant_database = data.get('db_name', '')
        # database Connection
        database = DB(tenant_database, **db_config)
        # # Fetch tenant_id based on tenant_name
        # tenant_id=db.get_data(
        #     "tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
        data_list={}
        pages={}
        features=[]
        # if not flag:
        #     # Create an instance of PermissionManager and call permission manager method
        #     pm = PermissionManager(db_config)
        #     # # Retrieving the features for the user
        #     flag_, features, allowed_sites, allowed_servivceproviders = pm.permission_manager(
        #         data)
        #     # logging.info('allowed_sites',allowed_sites)
        #     # logging.info('allowed_servivceproviders',allowed_servivceproviders)
        #     if not flag_:
        #         message = "Access Denied"
        #         return {"flag": False, "message": message}
        
        # query to find the column mapping for the module
        module_mappings_df = db.get_data('module_column_mappings', {
        'module_name': module_name}, ['columns_mapped', 'master_data_flag','tables_mapped',
        'view_name','condition','drop_down_col'
        ,'main_update_table','order_by','tenant_filter','combine'])
        columns_data = module_mappings_df['columns_mapped'].to_list()[0]
        main_update_table=module_mappings_df['main_update_table'].to_list()[0]
        tenant_filter=module_mappings_df['tenant_filter'].to_list()[0]
        try:
            columns_data=json.loads(columns_data)
        except Exception as e:
            logging.warning(f"Exception is {e}")
        master_data_flag = module_mappings_df['master_data_flag'].to_list()[0]
        # logging.info('master_data_flag',master_data_flag)
        tables_list = module_mappings_df['tables_mapped'].to_list()[0]
        try:
            tables_list=json.loads(tables_list)
        except Exception as e:
            logging.warning(f"Exception is {e}")  
        view_name = module_mappings_df['view_name'].to_list()[0]
        # logging.info('view_name',view_name)
        condition = module_mappings_df['condition'].to_list()[0]
        try:
            condition=json.loads(condition)
        except:
            condition={}  
        drop_down_col = module_mappings_df['drop_down_col'].to_list()[0]
        try:
            drop_down_col=json.loads(drop_down_col)
        except Exception as e:
            logging.warning(f"Exception is {e}")
        order_by = module_mappings_df['order_by'].to_list()[0]
        try:
            order_by=json.loads(order_by)
        except Exception as e:
            logging.warning(f"Exception is {e}")
        combine_all = module_mappings_df['combine'].to_list()[0]
        try:
            combine_all=json.loads(combine_all)
        except Exception as e:
            logging.warning(f"Exception is {e}")
        # Check if tables_list is not empty
        if tables_list:
            
            for table in tables_list:
                # Switch connection to os.environ['COMMON_UTILS_DATABASE'] if table is 'tenant' or 'users'
                if table in ['tenant','roles', 'users', 'amop_apis', 'carrier_apis', 'mapping_table', 'master_amop_apis',
                             'master_carrier_apis', 'master_roles', 'module', 'tenant_module','module_features']:
                    current_db = db  # Use common_utils connection
                else:
                    current_db = database  # Use tenant-specific connection
                # Use order_by if current table is main_update_table
                if table == main_update_table:
                    order=order_by
                else:
                    order=None

                if tenant_filter:
                    temp={}
                    temp[tenant_filter]={}
                    temp[tenant_filter]["tenant_id"]=str(tenant_id)
                    temp[tenant_filter].update(condition[tenant_filter])
                    condition[tenant_filter]=temp[tenant_filter]

                if combine_all and table in combine_all:
                    combine=combine_all[table]
                else:
                    combine=[]
                if drop_down_col and columns_data and table in drop_down_col and table in columns_data:
                    mod_pages=None
                else:
                    # Get pagination details from data
                    mod_pages = data.get('mod_pages', {})
                    logging.info(mod_pages,'mod_pages')
                # Fetch data based on table, condition, and columns
                if columns_data and table in columns_data and condition:
                    if table in condition:
                        ##fetching the data for the table using conditions
                        data_dataframe=current_db.get_data(
                            table,condition[table],columns_data[table],order,combine,None,mod_pages)
                    else:
                        ##fetching the data for the table using conditions
                        data_dataframe=current_db.get_data(
                            table,{},columns_data[table],order,combine,None,mod_pages)
                elif columns_data and table in columns_data:
                    ##fetching the data for the table using conditions
                    data_dataframe=current_db.get_data(
                        table,{},columns_data[table],order,combine,None,mod_pages)
                elif condition and table in condition and columns_data:
                    if table in columns_data:
                        ##fetching the data for the table using conditions
                        data_dataframe=current_db.get_data(table,condition[table
                                                    ],columns_data[table],order,combine,None,mod_pages)
                    else:
                        ##fetching the data for the table using conditions
                        data_dataframe=current_db.get_data(table,condition[table
                                            ],None,order,combine,None,mod_pages)
                else:
                    ##fetching the data for the table using conditions
                    data_dataframe=current_db.get_data(table,{},None,order,combine,None,mod_pages)
                # Handle dropdown columns differently
                if drop_down_col and columns_data and table in drop_down_col and table in columns_data:
                    for col in columns_data[table]:
                        #data_list[col]=data_dataframe[col].to_list()
                        data_list[col]=list(set(data_dataframe[col].tolist()))
                else:
                    ##converting the dataframe to dict
                    df=data_dataframe.to_dict(orient='records')
                    if parent_module.lower() not in ('people') :
                        if "mod_pages" in data and table == main_update_table:
                            # Calculate pages 
                            pages['start']=data["mod_pages"]["start"]
                            pages['end']=data["mod_pages"]["end"]
                            count_params = [table]
                            if module_name in ("Sim Order Form","Feature Codes"):
                                count_query = "SELECT COUNT(*) FROM %s" % table
                            else:
                                count_query = "SELECT COUNT(*) FROM %s where is_active=True" % table
                            count_result = current_db.execute_query(count_query, count_params).iloc[0, 0]
                            pages['total']=int(count_result)
                    # Add fetched data to data_list
                    data_list[table]=df
        message = "Data fetched successfully"
        if parent_module.lower() == 'people':
            data_list,total=get_people_data(data_list,module_name,tenant_id,database)
            pages['start']=data["mod_pages"]["start"]
            pages['end']=data["mod_pages"]["end"]
            pages['total']=total
        #convert all time stamps into str
        new_data = {
                table: (
                    values if values and isinstance(values[0], str) else [
                        {
                            key: str(value).split('.')[0] if key == 'modified_date' else str(value)
                            for key, value in item.items()
                        }
                        for item in values
                    ]
                )
                for table, values in data_list.items()
            }
            
        # if module_name== 'Optimization Group':
        #     rate_plans_list=rate_plan_dropdown_data_optimization_groups(data,database)
        #     new_data['rate_plans_list'] = rate_plans_list
        
        # if module_name== 'Comm Plans':
        #     rate_plans_list=rate_plan_dropdown_data(data,database)
        #     new_data['carrier_rate_plans'] = rate_plans_list
        # Response including pagination metadata
        if module_name == 'Customer Rate Plan':


            # If columns are correct, select them as follows:
            try:
                df = database.get_data("customerrateplan",{'is_active': True},['rate_plan_code', 'service_provider_name']).drop_duplicates(subset=['rate_plan_code', 'service_provider_name'])
                new_data['soc_list'] = df[['rate_plan_code', 'service_provider_name']].to_records(index=False).tolist()
            except Exception as e:
                logging.warning(f"Failed to fetch automation_rule data: {e}")
            try:
                automation_rule_list = database.get_data("automation_rule", {'is_active': True}, ['automation_rule_name'])['automation_rule_name'].to_list()
                new_data['automation_rule'] = automation_rule_list
            except Exception as e:
                logging.warning(f"Failed to fetch automation_rule data: {e}")
            # List of keys that need rounding
            keys_to_round = {"surcharge_3g", "plan_mb", "base_rate", "rate_charge_amt", "sms_rate"}
            keys_with_dollar_symbol = {"plan_mb", "base_rate", "rate_charge_amt", "sms_rate"}

            # Process the dictionary
            # Use a single loop for rounding and formatting
            for records in new_data.values():
                if isinstance(records, list):
                    for record in records:
                        if isinstance(record, dict):
                            for key in (keys_to_round & record.keys()):
                                value = record[key]
                                if isinstance(value, (int, float)):
                                    rounded_value = round(value, 2)
                                    record[key] = f"${rounded_value}" if key in keys_with_dollar_symbol else rounded_value
                                elif isinstance(value, str):
                                    try:
                                        rounded_value = round(float(value), 2)
                                        record[key] = f"${rounded_value}" if key in keys_with_dollar_symbol else rounded_value
                                    except ValueError:
                                        logging.warning(f"Cannot round non-numeric string: {value}")
        if module_name == 'Bulk Change':
            new_data = {
                table: [
                    {
                        **{
                            key: int(float(value)) if key in ['errors', 'uploaded', 'success'] and 
                            value.replace('.', '', 1).isdigit() and not value.endswith('.') else value
                            for key, value in item.items()
                        }
                    }
                    for item in values
                ]
                for table, values in new_data.items()
            }  
        
        if not flag:
            
            #calling get header to get headers mapping   
            headers_map=get_headers_mapping(tenant_database,[module_name
                ],role,username,tenant_id,sub_parent_module,parent_module,data,db) 
            # Convert timestamps to string format before returning
            new_data = convert_timestamp_data(new_data,tenant_time_zone)

            
            response = {"flag": True,"message":message, "data": serialize_data(new_data), 
                        "pages":pages,"features": features ,"headers_map":headers_map}
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            try:
                audit_data_user_actions = {"service_name": 'Module Management',
                                           "created_date": request_received_at,
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
        else:
            return new_data,pages
    except Exception as e:
        logging.warning(F"Something went wrong and error is {e}")
        message = "Something went wrong fetching module data {e}"
        #response={"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'get_module_data',
                          "created_date": request_received_at,
                          "error_message": message,
                          "error_type": error_type,"users": username,
                          "session_id": session_id,
                          "tenant_name": Partner,"comments": "",
                          "module_name": "Module Managament",
                          "request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        #calling get header to get headers mapping   
        headers_map=get_headers_mapping(tenant_database,[module_name
            ],role,username,"","","",data,db) 
        response = {"flag": True,"message":message, "data": {}, 
                    "pages":{},"features": {} ,"headers_map":headers_map}
        return response
        
def user_data(data):
    logging.info("Starting to retrieve user data.")
    try:
        user_name = data.get('user_name', None)
        tenant_database = data.get('db_name', None)
        database = DB(tenant_database, **db_config)
        common_utils_database = DB('common_utils', **db_config)
        role_name = data.get('role_name', 'Agent')
        logging.debug("Fetching role modules for role: %s", role_name)

        # Fetch role modules
        role_module = database.get_data(
            "role_module", {"role": role_name}
        ).to_dict(orient='records')
        
        logging.info("Retrieved role modules: %s", role_module)


        # Fetch user-specific data
        logging.debug("Fetching user-specific data for user: %s", user_name)
        user_specific_data = common_utils_database.get_data(
            "user_module_tenant_mapping", 
            {"user_name": user_name}, 
            ['sub_module', 'module_names', 'module_features']
        ).to_dict(orient='records')
        logging.info("Retrieved user-specific data: %s", user_specific_data)

        # Check if user_specific_data is empty
        if not user_specific_data:
            logging.warning("No user-specific data found for user: %s", user_name)
            response = {
                "flag": True,
                "user_specific_data": [],
                "role_module": role_module  # Return role_module when user_specific_data is empty
            }
            return response

        # Process the JSON fields
        for item in user_specific_data:
            item['sub_module'] = json.loads(item.get('sub_module', '{}'))
            item['module_names'] = json.loads(item.get('module_names', '[]'))
            item['module_features'] = json.loads(item.get('module_features', '[]'))  # Change to list
            # Check for empty lists in sub_module
            # if isinstance(item['sub_module'], dict):  # Ensure sub_module is a dictionary
            #     for key, value in item['sub_module'].items():
            #         if not value:  # Check if the list is empty
            #             logging.debug("Submodule %s is empty. Replacing with its key.", key)
            #             item['sub_module'][key] = [key]  # Replace with a list containing the key name

        # # Fetch module features
        # module_features = common_utils_database.get_data(
        #     'module_features', {}, ['module', 'features']
        # ).to_dict(orient='records')
        # logging.info("Retrieved module features: %s", module_features)

        # # Add module features to user_specific_data
        # for item in user_specific_data:
        #     item['module_features'] = module_features  # Assigning features to each item

        response = {
            "flag": True,
            "user_specific_data": user_specific_data,
            "role_module": role_module
        }
        logging.info("Final response data prepared successfully.")
        return response

    except Exception as e:
        print(f"Exception is: {e}") 
        response = {
            "flag": True,  # Set to False on error
            "user_specific_data": [],
            "role_module": []
        }
        return response 

def customers_dropdown_data(data):
    tenant_database = data.get('db_name', 'altaworx_central')
    database = DB(tenant_database, **db_config)
    logging.info(f"Request Recieved")
    try:
        # Fetch all active service providers with their IDs
        service_providers_df = database.get_data(
            "serviceprovider", {"is_active": True, "service_provider_name": "AT&T - Telegence"}, ["id", "service_provider_name"]
        )
        service_provider_ids = service_providers_df['id'].to_list()
        service_provider_names = service_providers_df['service_provider_name'].to_list()

        # Fetch all tenant configurations for the active service providers
        tenant_ids_df = database.get_data(
            "service_provider_tenant_configuration",
            {'service_provider_id': service_provider_ids},  # Batch query
            ["service_provider_id", "tenant_id"]
        )

        # Fetch all customers for the retrieved tenant IDs
        tenant_ids = tenant_ids_df['tenant_id'].to_list()
        customers_df = database.get_data(
            "customers",
            {'tenant_id': tenant_ids,"is_active": True},
            ["tenant_id", "customer_name"]
        )

        # Prepare the response
        service_provider_customers = {}
        for index, row in customers_df.iterrows():
            tenant_id = row['tenant_id']
            customer_name = row['customer_name']
            service_provider_id = tenant_ids_df[tenant_ids_df['tenant_id'] == tenant_id]['service_provider_id'].to_list()

            if service_provider_id:
                service_provider_id = service_provider_id[0]  # Assuming a one-to-one mapping
                service_provider_name = service_provider_names[service_provider_ids.index(service_provider_id)]
                
                # Append the customer to the respective service provider list
                if service_provider_name not in service_provider_customers:
                    service_provider_customers[service_provider_name] = []
                service_provider_customers[service_provider_name].append(customer_name)

        # Fetch feature codes in one query (if needed)
        feature_codes = database.get_data(
            "mobility_feature", {}, ["soc_code"]
        )['soc_code'].to_list()

        # Prepare the final response
        response = {
            "flag": True,
            "service_provider_customers": service_provider_customers,
            "feature_codes": list(set(feature_codes))  # Ensure uniqueness
        }
        return response

    except Exception as e:
        logging.exception(f"Data fetch issue: {e}")
        return {
            "flag": False,
            "service_provider_customers": {},
            "feature_codes": []
        }
def rate_plan_dropdown_data(data):
    #logging.info("Starting to retrieve rate plan dropdown data.")
    # logging.info(f"Request Data is {data}")
    tenant_database = data.get('db_name', None)
    database = DB(tenant_database, **db_config)
    # Get unique service provider names
    serviceproviders = database.get_data(
        "serviceprovider",{"is_active":True}, ["service_provider_name"]
        )['service_provider_name'].to_list()
    service_provider_names = list(set(serviceproviders))
    #logging.info("Retrieved unique service provider names: %s", service_provider_names)
    # Initialize an empty dictionary to store the results
    rate_plans_list = {}
    # Iterate over each service provider name
    for service_provider_name in service_provider_names:
        #logging.debug("Processing service provider: %s", service_provider_name)
        # Get the rate plan codes for the current service provider name
        rate_plan_items = database.get_data(
            "carrier_rate_plan", 
            {'service_provider': service_provider_name,"is_active":True}, 
            ["rate_plan_code"]
        )['rate_plan_code'].to_list()
        #logging.debug("Retrieved rate plan codes for %s: %s", service_provider_name, rate_plan_items)
        
        # Add the result to the dictionary
        rate_plans_list[service_provider_name] = rate_plan_items
        #logging.info("Final rate plans list: %s", rate_plans_list)
    
    # Return the resulting dictionary
    return {"flag":True,'carrier_rate_plans':rate_plans_list}



def form_Partner_module_access(tenant_id,module_data):
    '''
    Description:Forms access information for a specific tenant by filtering and organizing modules.
    The function processes tenant-specific module data and structures it for use in the system.
    '''
    # Extract the 'tenant_module' list from module_data
    tenant_module=module_data['tenant_module']
    # Initialize an empty list to hold the module names for the specified tenant
    modules_names=[]
    # Iterate over each item in tenant_module to filter modules for the specified tenant_id
    for data_item in tenant_module:
        if str(data_item['tenant_id']) == str(tenant_id):
            # logging.info(data_item['tenant_id'],tenant_id)
            modules_names.append(data_item['module_name'])
            logging.debug("Matched tenant_id: %s with module_name: %s", tenant_id, data_item['module_name'])
    # Extract the 'module' list from module_data  
    modules_data=module_data['module']
     # Initialize an empty list to hold unique parent module names and a dictionary to structure modules by parent
    return_modules=[]
    modules={}
    logging.info("Filtered modules names for tenant_id %s: %s", tenant_id, modules_names)

    # logging.info(modules_names,"module ids")
    # Iterate over each item in modules_data to organize modules by their parent module name
    for data_item in modules_data:
        # Add parent module name to return_modules if it's not already present
        if not data_item['parent_module_name'] or data_item['parent_module_name'] == "None":
            continue
        # Initialize an empty list for the parent module in the modules dictionary if it's not already there
        if data_item['parent_module_name'] not in return_modules:
            return_modules.append(data_item['parent_module_name'])
        if data_item['parent_module_name'] not in modules:
            modules[data_item['parent_module_name']]=[]
        modules[data_item['parent_module_name']].append(data_item['module_name'])
    modules_to_remove = {"Super admin"}

    # Remove "Super admin" from return_modules
    return_modules = [module for module in return_modules if module not in modules_to_remove]

    # Remove "Super admin" from the modules dictionary if it exists
    if "Super admin" in modules:
        del modules["Super admin"]
    module_data["tenant_module"]=return_modules
    module_data['module']=modules_dict()

     # Return the modified module_data with tenant-specific access information
    logging.info("Final module_data structure: %s", module_data)
    return module_data



def modules_dict():
    logging.info("Starting the retrieval of module mappings.")
    common_utils_database = DB('common_utils', **db_config)
    # Step 1: Retrieve the data
    module_mappings_df = common_utils_database.get_data('module', {}, ['parent_module_name', 'module_name', 'submodule_name'])
    logging.info("Retrieved module mappings data: %s", module_mappings_df)
    
    # Step 2: Create the desired output format
    result = {}
    
    for _, row in module_mappings_df.iterrows():
        parent = row['parent_module_name']
        module = row['module_name']
        submodule = row['submodule_name']
    
        # Initialize the list for the parent module if it doesn't exist
        if parent not in result:
            result[parent] = []
            logging.debug("Initialized list for parent module: %s", parent)
        
        # Include submodule_name if it exists, otherwise include module_name
        if submodule and submodule not in result[parent]:
            result[parent].append(submodule)
            logging.debug("Added submodule: %s to parent module: %s", submodule, parent)
        elif not submodule and module and module not in result[parent]:
            result[parent].append(module)
            logging.debug("Added module: %s to parent module: %s", module, parent)
        # If both module_name and submodule_name are absent, add parent_module_name
        elif not module and not submodule and parent not in result[parent]:
            result[parent].append(parent)
            logging.debug("Added parent module name: %s to itself", parent)

    formatted_result_json = result
    logging.info("Successfully formatted the result: %s", formatted_result_json)
    
    return formatted_result_json


def get_user_module_map(data):
    '''
    Description:Retrieves the module mapping for a user based on their role and tenant information.
    The function checks for existing user-module mappings and falls back to role-based mappings if necessary.
    '''
    # logging.info(f"Request Data: {data}")
    try:
        # Extract username, role, and tenant_name from the input data dictionary
        username = data.get('username', None)
        role = data.get('role', None)
        tenant_name = data.get('tenant_name', None)
        # Initialize the database connection
        tenant_database = data.get('db_name', None)
        database = DB(tenant_database, **db_config)
        common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        return_dict={}
        # Retrieve tenant_id based on tenant_name
        tenant_id=common_utils_database.get_data(
            "tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
        flag=False
        # Fetch the user-module mapping from the database
        user_map=database.get_data(
            "user_module_tenant_mapping",{"user_name":username,"tenant_id":tenant_id
            },["module_names","sub_module","module_features"]).to_dict(orient="records")
        # Check if a user-specific module map exists
        if user_map:
            user_map=user_map[0]
            logging.info("User map found: %s", user_map)
            for key,value in user_map.items():
                if value:
                    flag=True
                    break
            if flag:
                # Rename 'module_names' to 'module'
                return_dict=user_map
                return_dict['module']=user_map['module_names']
                return_dict.pop('module_names')
                logging.info("Returning user-specific module mapping: %s", return_dict)
            else:
                # If the user map is empty, fall back to role-based mapping
                return_dict=database.get_data(
                    "role_module",{"role":role},["module","sub_module","module_features"]
                    ).to_dict(orient="records")
                logging.info("User map is empty; falling back to role-based mapping: %s", return_dict)
        else:
            # If no user-specific map is found, retrieve the role-based module map
            return_dict=database.get_data(
                "role_module",{"role":role},["module","sub_module","module_features"]
                ).to_dict(orient="records")
            logging.info("No user-specific map found; retrieved role-based mapping: %s", return_dict)
        return {'flag':True,'data':return_dict}

    except Exception as e:
        # Handle any exceptions that occur during data retrieval
        logging.exception(f"error in fetching data {e}")
        response={'Flag':False,'data':{}}
        return response

def get_partner_info(data):
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
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
    Partner = data.get('Partner', '')
    ##database connection
    tenant_database = data.get('db_name', None)
    database = DB(tenant_database, **db_config)
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    #role = data.get('role_name', None)
    role = data.get('role_name') or data.get('role') or 'Super Admin'
    modules_list = data.get('modules_list', None)
    parent_module = data.get('parent_module', None)
    sub_parent_module = data.get('sub_parent_module', None)
    
    # # Get tenant's timezone
    tenant_name = data.get('tenant_name', '')
    tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
    tenant_timezone = db.execute_query(tenant_timezone_query, params=[tenant_name])

        # Ensure timezone is valid
    if tenant_timezone.empty or tenant_timezone.iloc[0]['time_zone'] is None:
        raise ValueError("No valid timezone found for tenant.")
        
    tenant_time_zone = tenant_timezone.iloc[0]['time_zone']
    match = re.search(r'\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)', tenant_time_zone)
    if match:
        tenant_time_zone = match.group(1).replace(' ', '')  # Ensure it's formatted correctly
        
        
    try:
        counts={}
        returning_partner_module_data={}

        
        partner=tenant_name
        sub_partner=''
        tenant_df=db.get_data(
            "tenant",{'tenant_name':tenant_name
                      },['parent_tenant_id','id','email_ids','logo','service_provider_to','service_provider_status'])
        parent_tenant_id=tenant_df['parent_tenant_id'].to_list()[0]
        tenant_id=tenant_df['id'].to_list()[0]
        email_id=tenant_df['email_ids'].to_list()[0]
        service_provider_to=tenant_df['service_provider_to'].to_list()[0]
        service_provider_status=tenant_df['service_provider_status'].to_list()[0]
        logo=tenant_df['logo'].to_list()[0]
        if parent_tenant_id:
            sub_partner=tenant_name
            partner=db.get_data(
                "tenant",{'id':parent_tenant_id},['tenant_name'])['tenant_name'].to_list()[0]

        #formating data for partner info
        if 'Partner info' in modules_list:
            try:
                # Load email_id if it's a JSON string
                email_id = json.loads(email_id)
            except Exception as e:
                logging.warning(f"Exception is {e}")

            # Fetch the required tenant information including addresses
            query = '''
                SELECT 
                    billing_address_1, 
                    billing_address_2, 
                    billing_city, 
                    billing_state, 
                    billing_zip_code, 
                    billing_notes, 
                    cross_provider_customer_optimization, 
                    physical_address_1, 
                    physical_addresss_2, 
                    physical_apt_or_suite, 
                    physical_city, 
                    physical_state, 
                    physical_zip_code, 
                    physical_country, 
                    physical_county,
                    time_zone
                FROM 
                    tenant 
                WHERE 
                    tenant_name = %s
                '''
                
            params=[tenant_name]

                # Execute the query with the tenant_name as a parameter
            tenant_info = db.execute_query(query, params=params)

            # Convert the result to a dictionary (assuming tenant_info is a DataFrame)
            tenant_info_dict = tenant_info.to_dict(orient='records')[0] if not tenant_info.empty else {}


            # Prepare the tenant names list
            tenant_names = db.get_data('tenant', {"is_active": True}, ['tenant_name'])['tenant_name'].to_list()

            # Add partner info along with addresses
            returning_partner_module_data['Partner info'] = {
                'partner': partner,
                'sub_partner': sub_partner,
                'email_id': email_id,
                'logo': logo,
                'service_provider_to': service_provider_to,
                'service_provider_status': service_provider_status,
                **tenant_info_dict,  # Merge tenant info dictionary into the partner info
                'tenant_names': tenant_names,
            }

            # Remove "Partner info" from modules_list to avoid redundant processing
            modules_list.remove("Partner info")
            # logging.info(returning_partner_module_data)

        #formating data for athentication
        if 'Partner authentication' in modules_list:
            returning_partner_module_data['Partner authentication']={}
            modules_list.remove("Partner authentication")
            try:
                db = DB('common_utils', **db_config)
                query = '''SELECT * FROM partner_authentication'''
                partner_auth_data = db.execute_query(query, True)

                # Convert the result to a list of dictionaries (key-value pairs)
                partner_auth_dict_list = partner_auth_data.to_dict(orient='records')

                # Ensure the data is sent as an object (not a list)
                if partner_auth_dict_list:
                    partner_auth_dict = partner_auth_dict_list[0]  # Extract the first dictionary (object)
                else:
                    partner_auth_dict = {}
                
                # Add the partner authentication data to the response dictionary as an object
                returning_partner_module_data['Partner authentication'] = partner_auth_dict
                
                

                
                # Add the partner authentication data to the response dictionary
                returning_partner_module_data['Partner authentication'] = partner_auth_dict
                
                # Remove "Partner authentication" from modules_list to avoid redundant processing
                modules_list.remove("Partner authentication")
                
                # logging.info(f"Partner authentication data added: {partner_auth_dict}")
            
            except Exception as e:
                logging.warning(f"Failed to fetch Partner authentication data: {e}")
            # logging.info(returning_partner_module_data)

        for module in modules_list:
            data["module_name"]=module
            if module in data["pages"]: 
                data["mod_pages"]=data["pages"][module]
            module_data,pages=get_module_data(data,True)
            if pages:
                counts[module]={"start":pages["start"],"end":pages["end"],"total":pages["total"]}

            #formating data for Partner module Access 
            if module=="Partner module access":
                module_data=form_Partner_module_access(tenant_id,module_data)
                filtered_roles = [role for role in module_data["role_name"] if role != "Super Admin"]
                role_df = db.get_data('roles', {"tenant_id": tenant_id, "is_active": True}, ['role_name'])
                # Filter role_df to include only roles in filtered_roles
                filtered_role_df = role_df[role_df['role_name'].isin(filtered_roles)]
                # Update module_data with unique role names from the filtered roles
                module_data["role_name"] = list(set(filtered_role_df['role_name']))
                tenant_modules = db.get_data(
                                    table_name='tenant_module',
                                    condition={"tenant_name": tenant_name,"is_active":True }, 
                                    columns=['tenant_id', 'module_name'],
                                    order={'id': 'asc'}
                                ).to_dict(orient='records')
                # Extract module names into a list
                module_list = [module['module_name'] for module in tenant_modules]
                module_list = [module for module in module_list if module != "Super admin"]
                module_data["tenant_module"] = module_list
            #formating data for Customer groups
            if module=="Customer groups":
                module_data_new={}
                module_data_new['customer_names']=[]
                module_data_new['billing_account_number']=[]
                # Loop through each dictionary in the list
                for dic in module_data["customers"]:
                    # Create new dictionaries for each key-value pair
                    first_key, first_value = list(dic.items())[0]
                    second_key, second_value = list(dic.items())[1]
                    # logging.info('second_key',second_key)
                    if first_key =='billing_account_number':
                        if first_value and first_value != "None":
                            module_data_new['billing_account_number'].append(first_value)
                        if second_value and second_value != "None":
                            module_data_new['customer_names'].append(second_value)
                    else:
                        if first_value and first_value != "None":
                            module_data_new['customer_names'].append(first_value)
                        if second_value and second_value != "None":
                            module_data_new['billing_account_number'].append(second_value)
                module_data.update(module_data_new)
                module_data.pop("customers")  
                soc_codes_pop_up_query="SELECT distinct(soc_code) FROM public.mobility_feature where soc_code is not null"
                soc_codes_pop_up_values=database.execute_query(soc_codes_pop_up_query,True)['soc_code'].to_list()
                # Add SOC codes to the module_data
                module_data['soc_code'] = soc_codes_pop_up_values
                billing_account_numbers=database.get_data('sim_management_inventory',{'billing_account_number':"not Null","is_active":True},['billing_account_number'])['billing_account_number'].to_list()
                module_data["billing_account_numbers"] = billing_account_numbers
                module_data["tenant_name"] = sorted(module_data["tenant_name"])
            #formating data for Partner users
            if module=="Partner users":
                result_dict = {}
                
                module_data['role_name']=list(set(module_data['role_name']))
                # Sort module_data['tenant'] by tenant_name
                sorted_tenants = sorted(module_data['tenant'], key=lambda x: x['tenant_name'])
                for d in sorted_tenants:
                    tenant_id = d["id"]
                    tenant_name = d["tenant_name"]
                    parent_id = d["parent_tenant_id"]
                    sub_temp = []
                
                    if parent_id == "None":
                        for di in sorted_tenants:
                            if di["parent_tenant_id"] != "None" and float(di["parent_tenant_id"]) == float(tenant_id):
                                sub_temp.append(di["tenant_name"])
                        
                        # Sort sub_temp in alphabetical order
                        result_dict[tenant_name] = sorted(sub_temp)
                # Update module_data with the sorted result_dict
                module_data['tenant'] = result_dict
                user_df=db.get_data("users",{},['is_active','migrated']).to_dict(orient='records')
                total_count=len(user_df)
                active_user_count=0
                migrated_count=0
                for user in user_df:
                    if user['is_active']:
                        active_user_count=active_user_count+1
                    if user['migrated']:
                        migrated_count=migrated_count+1
                module_data['total_count']=total_count
                module_data['active_user_count']=active_user_count
                module_data['migrated_count']=migrated_count

            #final addition of modules data into the retuting dict
            returning_partner_module_data[module]=module_data
            returning_partner_module_data["pages"]=counts

        #calling get header to get headers mapping    

        headers_map=get_headers_mapping(tenant_database,
            modules_list,role,username,tenant_id,sub_parent_module,parent_module,data,db)
        message=f"partner data fetched successfully"
        returning_partner_module_data = convert_timestamp_data(returning_partner_module_data, tenant_time_zone)
        response = {"flag": True, "data": serialize_data(returning_partner_module_data),
                    "headers_map":headers_map,"message":message}
        
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        try:
            audit_data_user_actions = {"service_name": 'Module Management',"created_date": start_time,
            "created_by": username,
                "status": str(response['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "get_partner_info",
                "request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response
        
    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = "Something went wrong while fetching Partner info"
        response={"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'Module Management',
                          "created_date": start_time,
                          "error_message": message,
                          "error_type": error_type,"users": username,
                          "session_id": session_id,"tenant_name": Partner,
                          "comments": "","module_name": "get_partner_info",
                          "request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response
        
    
def create_module_access_info(data):
    """
    Processes role-based access data to generate a dictionary with module, 
    submodule, and feature information.

    Args:
        data (dict): Dictionary containing roles as keys and their 
        corresponding modules and features as values.

    Returns:
        dict: A dictionary with roles as keys 
        and JSON-encoded strings of modules, submodules, and features as values.
    """
    return_dict={}
    modules_list = []
    submodules_list = {}
    features_dict = {}
    # Iterate over each role and its associated features
    for role,user_features in data.items():
        logging.info("Processing role: %s", role)
        # logging.info(user_features)
        for main_module, content in user_features.items():
            logging.info("Main module: %s, Content: %s", main_module, content)
            modules_list.append(main_module)
            submodules_list[main_module]=content['Module']
            # Initialize the features dictionary for the current main module
            features_dict[main_module]={}
            features=content['Feature']
            # Iterate over each submodule of the main module
            for submodule in content['Module']:
                if submodule in features:
                    features_dict[main_module][submodule]=features[submodule]
         # Store the processed data for the current role
        return_dict[role]={"module":json.dumps(modules_list
        ),"sub_module":json.dumps(submodules_list
                                  ),"module_features":json.dumps(features_dict)}
    logging.info("Final return dictionary: %s", return_dict)
     # Return the final dictionary with role-based access information
    return return_dict



def update_partner_info(data):
    """
    Updates partner information in the database based on provided data and performs
    various operations such as validation, updating, or inserting
    records in different modules.

    Args:
        data (dict): Dictionary containing details such as Partner,
                     request timestamps, session IDs, 
                     access tokens, and other data needed for updating or inserting records.

    Returns:
        dict: Response indicating the success or
        failure of the operation with a message.
    """
    # logging.info(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            logging.warning("Permission check failed: %s", result)
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        logging.warning(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    ##database connection
    # Start time  and date calculation
    start_time = time.time()
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    updated_data_ui = data.get('changed_data', [])
    session_id = data.get('session_id', None)
    where_dict=data.get('where_dict', {})
    role = data.get('role_name', None)
    action = data.get('action', None)
    module_name = data.get('module_name', None)
    module_name=module_name.lower()
    template_name = data.get("template_name", 'Create User')
    # database Connection
    tenant_database = data.get('db_name', None)
    database = DB(tenant_database, **db_config)
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config) 
    try:
        # Fetch tenant ID based on tenant name
        tenant_id=db.get_data(
            "tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
        # Prepare updated data for database operations
        updated_data={}
        for key,value in updated_data_ui.items():
            if type(value) != str:
                updated_data[key]=json.dumps(value)
            elif value == 'None' or value == '':
                updated_data[key]=None
            else:
                updated_data[key]=value
        # Remove 'id' field if present and module_name is not 'customer groups'  
        if "id" in updated_data and module_name !='customer groups':
            updated_data.pop("id")
        # Perform database operations based on the module_name and action
        if module_name=='partner info':
            where_dict={"tenant_name":tenant_name}
            if action in ('update', 'delete'):
                db.update_dict("tenant",updated_data,where_dict)
            elif action == "create":
                tenant_id=db.insert_data(updated_data,"tenant")

        if module_name=="partner module access":
            if action in ('update', 'delete'):
                role_module_acess=create_module_access_info(updated_data_ui)
                for role,value_dict in role_module_acess.items():
                    where_dict={"role":role}
                    database.update_dict("role_module",value_dict,where_dict)
            elif action == "create":
                role_module_acess=create_module_access_info(updated_data_ui)
                for role,value_dict in role_module_acess.items():
                    value_dict['role']=role
                    role_module_id=db.insert_data(value_dict,"role_module")
    
        if module_name=="customer groups":
            if action == "update" or action == "delete":
                where_dict={"id":updated_data['id']}
                database.update_dict("customergroups",updated_data,where_dict)
            elif action == "create":
                if 'id' in updated_data:
                    updated_data.pop('id')
                if 'modified_date' in updated_data:
                    updated_data.pop('modified_date')
                customergroups_id=database.insert_data(updated_data,"customergroups")
                
        if module_name=="partner users":
            if "tenant_id" in updated_data and updated_data['tenant_id']:
                updated_data['tenant_id']=str(int(float(updated_data["tenant_id"])))
                
            if action == "delete":
                db.update_dict("users",updated_data,{"username":updated_data["username"]})

            elif action == "update":
                
                customer_info=updated_data.get('customer_info')
                try:
                    customer_info=json.loads(customer_info)
                except Exception as e:
                    logging.warning(f"Exception is {e}")
                if customer_info:

                    db_customer_info={}
                    for key,value in customer_info.items():
                        # logging.info(value,'value')
                        if value and type(value) != str:
                            db_customer_info[key]=json.dumps(value)
                        elif value == 'None':
                            db_customer_info[key]=None
                        elif value == 'true':
                            db_customer_info[key]=True
                        elif value == 'false':
                            db_customer_info[key]=False
                        else:
                            db_customer_info[key]=value
                    db.update_dict("users",db_customer_info,{
                        "username":customer_info["username"]})

                user_update=updated_data.get('user_info')
                try:
                    user_update=json.loads(user_update)
                except Exception as e:
                    logging.warning(f"Exception is {e}")
                if user_update:

                    db_user_update={}
                    for key,value in user_update.items():
                        if type(value) != str:
                            db_user_update[key]=json.dumps(value)
                        elif value == 'None':
                            db_user_update[key]=None
                        elif str(value).lower() == 'true':
                            db_user_update[key]=True
                        elif str(value).lower() == 'false':
                            db_user_update[key]=False
                        else:
                            db_user_update[key]=value

                    db.update_dict("users",db_user_update,{
                        "username":db_user_update["username"]})

                user_modules_update=updated_data.get('data')
                try:
                    user_modules_update=json.loads(user_modules_update)
                except Exception as e:
                    logging.warning(f"Exception is {e}")
                if user_modules_update:
                    tenant_id=db.get_data(
                        "tenant",{'tenant_name':updated_data["Selected Partner"]
                                  },['id'])['id'].to_list()[0]
                    user_module_acess=create_module_access_info(user_modules_update)
                    # logging.info(user_module_acess,updated_data)
                    for user,value_dict in user_module_acess.items():
                        value_dict['module_names']=value_dict['module']
                        value_dict.pop('module')
                        db.update_dict(
                            "user_module_tenant_mapping",value_dict,{
                                "tenant_id":tenant_id,"user_name":updated_data["Username"]})

            elif action == "create":

                user_update=updated_data.get('user_info')
                try:
                    user_update=json.loads(user_update)
                    tenant=user_update.get('tenant_name')
                    tenant_id=db.get_data(
                        "tenant",{'tenant_name':tenant},['id'])['id'].to_list()[0]
                except Exception as e:
                    logging.warning(f"Exception is {e}")

                tenants_list=[]
                tenants_list.append(user_update["tenant_name"])
                tenants_list.extend(user_update["subtenant_name"])

                for tenant_name in tenants_list:
                    tenant_id=db.get_data(
                        "tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
                    db_user={"tenant_id":tenant_id,"user_name":user_update["username"]}
                    user_module_id=db.insert_data(db_user,"user_module_tenant_mapping")

                if user_update:

                    db_user_update={}
                    for key,value in user_update.items():
                        if type(value) != str:
                            db_user_update[key]=json.dumps(value)
                        elif value == 'None':
                            db_user_update[key]=None
                        else:
                            db_user_update[key]=value
                    db_user_update['tenant_id']=tenant_id
                    user_id=db.insert_data(db_user_update,"users")
                    to_email=db.get_data("users",{"username":username},['email'])['email'].to_list()[0]
                    try:
                        db.update_dict("email_templates", {"last_email_triggered_at": request_received_at}, {"template_name": template_name})
                        
                        # Call send_email and assign the result to 'result'
                        result = send_email(template_name, username=username, user_mail=to_email)
                    except:
                        pass
                        
                    # Check the result and handle accordingly
                    if isinstance(result, dict) and result.get("flag") is False:
                        logging.info(result)
                    else:
                        # Continue with other logic if needed
                        to_emails, cc_emails, subject, body, from_email, partner_name = result
                        #to_emails,cc_emails,subject,body,from_email,partner_name=send_email(template_name,username=username,user_mail=to_email)
                        db.update_dict("email_templates",{"last_email_triggered_at":request_received_at},{"template_name":template_name})
                        query = """
                            SELECT parents_module_name, sub_module_name, child_module_name, partner_name
                            FROM email_templates
                            WHERE template_name = %s
                        """

                        
                        params=[template_name]
                        # Execute the query with template_name as the parameter
                        email_template_data = db.execute_query(query, params=params)
                        if not email_template_data.empty:
                            # Unpack the results
                            parents_module_name, sub_module_name, child_module_name, partner_name = email_template_data.iloc[0]
                        else:
                            # If no data is found, assign default values or log an error
                            parents_module_name = ""
                            sub_module_name = ""
                            child_module_name = ""
                            partner_name = ""

                        try:
                            ##email audit
                            email_audit_data = {"template_name": template_name,"email_type": 'Application',
                            "partner_name": partner_name,
                                "email_status": 'success',
                                "from_email": from_email,
                                "to_email": to_emails,
                                "cc_email": cc_emails,
                                "comments": 'update inventory data',
                                "subject": subject,"body":body,"role":role,
                                "action": "Email triggered",
                                "parents_module_name": parents_module_name,
                                "sub_module_name": sub_module_name,          
                                "child_module_name": child_module_name  
                                    
                            }
                            db.update_audit(email_audit_data, 'email_audit') 
                        except:
                            pass
                    message=f"mail sent sucessfully"
                    # End time calculation
                    end_time = time.time()
                    time_consumed = end_time - start_time
                    try:
                        # Email audit
                        email_audit_data = {
                            "template_name": template_name,
                            "email_type": 'Send Grid',
                            "partner_name": partner_name,
                            "email_status": 'success',
                            "from_email": from_email,
                            "to_email": to_emails,
                            "cc_email": cc_emails,
                            "comments": 'Account creation confirmation',
                            "subject": subject,
                            "body": body,
                            "role": role,
                            "action_performed": "account creation confirmation"
                        }
                        db.update_audit(email_audit_data, 'email_audit')
                    except Exception as e:
                        logging.warning(f"Exception during audit logging: {e}")
        message='Updated sucessfully'
        response = {"flag": True, "message": message}
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        try:
            audit_data_user_actions = {"service_name": 'Module Management',
                                       "created_date": request_received_at,
                                        "created_by": username,
                                        "status": str(response['flag']),
                                        "time_consumed_secs": time_consumed,
                                        "session_id": session_id,
                                        "tenant_name": Partner,
                                        "comments": json.dumps(updated_data_ui),
                                        "module_name": "update_partner_info",
                                        "request_received_at":request_received_at}
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.warning(f"Exception is {e}")
        return response
    except Exception as e:
        logging.exception(F"Something went wrong {e}")
        message = "Something went wrong while updating Partner info"
        response={"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'Module Management',
                          "created_date": request_received_at,
                          "error_message": message,
                          "error_type": error_type,
                          "users": username,
                          "session_id": session_id,
                          "tenant_name": Partner,
                          "comments": message,
                          "module_name": "update_partner_info",
                          "request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response

def get_people_data(data_list,module_name,tenant_id,database):
    #database = DB('altaworx_central', **db_config)
    logging.info("Fetching data for module: %s", module_name)
    # logging.info(tenant_id,"tenant_id")

    if module_name=='Bandwidth Customers':
        band_id=[]
        
        for data_item in data_list['customers']:
            if data_item['bandwidth_customer_id']:
                band_id.append(data_item['bandwidth_customer_id'])
        logging.info("Bandwidth Customer IDs collected: %s", band_id)
                
        if band_id:
            band_id_int = tuple(int(float(i)) for i in band_id)
            logging.info("Executing query to fetch bandwidth customers with IDs: %s", band_id_int)
            
            query = f"""
            SELECT bws.id AS bandwidth_unique_col, 
                   bws.bandwidth_account_id, 
                   bws.bandwidth_customer_name, 
                   c.bandwidth_customer_id
            FROM customers AS c
            JOIN bandwidth_customers AS bws 
              ON bws.id = CAST(CAST(c.bandwidth_customer_id AS FLOAT) AS INTEGER)
            WHERE bws.id IN {band_id_int}
            ORDER BY c.modified_date DESC;
            """
            df = database.execute_query(query, True)
            total=len(df)
            logging.info("Number of bandwidth customers fetched: %d", total)
            bandwidth_account_id=df['bandwidth_account_id'].to_list()[0]
            logging.info("Fetched bandwidth account ID: %s", bandwidth_account_id)
            data_out=df.to_dict(orient='records')
            
            global_account_number_df=database.get_data(
                'bandwidthaccount',{'id':bandwidth_account_id},["id","global_account_number"])
            global_account_number=global_account_number_df['global_account_number'].to_list()[0]
            bandwidthaccount_unique_col=global_account_number_df['id'].to_list()[0]
            logging.info("Global account number: %s, Bandwidth account unique col: %s", 
                             global_account_number, bandwidthaccount_unique_col)
            merged_list=[]
            dict2 = {d['bandwidth_customer_id']: d for d in data_list['customers']}
            for dic_data_out in data_out:
                # Find the corresponding dictionary in dict2
                dic_data_list = dict2.get(dic_data_out['bandwidth_customer_id'], {})
                # Merge dictionaries
                merged_dict = {**dic_data_list, **dic_data_out}
                merged_dict.update({
                    "global_account_number":global_account_number})
                merged_dict.update({
                    "bandwidthaccount_unique_col":bandwidthaccount_unique_col})
                merged_list.append(merged_dict)

            data_list['customers']=merged_list
            logging.info("Successfully merged bandwidth customer data.")
            
        return data_list,total
    elif module_name=='E911 Customers':
        e911_customer_ids = tuple(int(float(i)) for i in data_list['e911_customer_id'])
        #e911_customer_ids=tuple(data_list['e911_customer_id'])
        if e911_customer_ids:
            df=database.get_data(
                "e911customers", {'id':e911_customer_ids},order={"modified_date":"desc"})
            total=len(df)
            data_list['e911_customers']=df.to_dict(orient='records')
            data_list.pop('e911_customer_id')
        return data_list,total
    
    elif module_name=='NetSapiens Customers':
        reseller_ids=database.get_data(
            "customers",{'netsapiens_customer_id':"not Null"},['id'])['id'].to_list()
        total=len(reseller_ids)
        # logging.info(reseller_ids)
        for data_item in data_list['customers']:
            if data_item['id'] in reseller_ids:
                data_item['netsapiens_type']='Reseller'
            else:
                data_item['netsapiens_type']='Domain'
                
        return data_list,total
    

def update_people_data(data):
    '''
    updates module data for a specified module by checking user and tenant 
    to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view,
      handles errors, and logs relevant information.
    '''
    # Start time  and date calculation
    start_time = time.time()
    # print(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
        logging.info("Checking user permissions.")
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            logging.warning("User does not have permission: %s", result)
            return result
        else:
            # Continue with other logic if needed
            pass
    except Exception as e:
        logging.warning(f"got exception in the restriction")
    
    
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    user_name = data.get('user_name', '')
    action = data.get('action','')
    ui_changed_data = data.get('changed_data', {})
    module_name = data.get('module', {})
    ##Database connection
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    dbs = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:
        logging.info("Fetching column mappings for module: %s", module_name)
        df=dbs.get_data(
            'module_column_mappings',{"module_name":module_name
                },["main_update_table","sub_table_mapping","unique_columns"])
        main_update_table=df['main_update_table'].to_list()[0]
        try:
            sun_table_map=json.loads(df['sub_table_mapping'].to_list()[0])
        except:
            sun_table_map={}
        try:
            unique_column=json.loads(df['unique_columns'].to_list()[0])
        except:
            unique_column=df['unique_columns'].to_list()

        changed_data={}
        for key,value in ui_changed_data.items():
            if value and value != None and value != "None":
                changed_data[key]=value
            else:
                changed_data[key]=None
        
        update_dics={}
        for table,cols in sun_table_map.items():
            if table not in update_dics:
                update_dics[table]={}
            for col in cols:
                if col in changed_data and changed_data[col]:
                    update_dics[table][col]=changed_data[col]
                if col in changed_data:
                    changed_data.pop(col)
        if action == 'delete':
            filtered_changed_data = {key: value for key, value in changed_data.items() if value is not None}
            logging.debug("Updating record in main table: %s", main_update_table)
            unique_column_val=filtered_changed_data.pop(unique_column[main_update_table])
            logging.debug("Unique column value for update: %s", unique_column_val)
            logging.debug(main_update_table, "33333333333333333333333333333333333333333")
            
            database.update_dict(main_update_table,filtered_changed_data ,{'id':changed_data['id']})
            logging.debug("Record updated successfully in table: %s", main_update_table)
                # database.execute(delete_query_sub_table)
        elif action == 'update':
            filtered_changed_data = {key: value for key, value in changed_data.items() if value is not None}
            logging.debug("Updating record in main table: %s", main_update_table)
            unique_column_val=filtered_changed_data.pop(unique_column[main_update_table])
            logging.debug("Unique column value for update: %s", unique_column_val)
            
            database.update_dict(main_update_table,filtered_changed_data ,{'id':changed_data['id']})
            logging.debug("Record updated successfully in table: %s", main_update_table)
                
        elif action == 'create':
            logging.debug("Creating new record in main table: %s", main_update_table)
            
            # Check if module is "Bandwidth Customers" and fetch bandwidth_account_id
            if module_name == "Bandwidth Customers":
                # Fetch the id from bandwidth_customers where bandwidth_customer_name matches Partner
                result = database.get_data(
                    'bandwidth_customers', 
                    {"bandwidth_customer_name": Partner}, 
                    ["id"]
                )
                
                # If a matching record is found, use it as the bandwidth_account_id
                if not result.empty:
                    bandwidth_id = result['id'].iloc[0]
                    changed_data['bandwidth_customer_id'] = int(bandwidth_id)
                    logging.debug(f"Assigned bandwidth_account_id: {bandwidth_id} for Partner: {Partner}")

            # Check if module is "NetSapiens Customers" and fetch netsapiens_customer_id
            elif module_name == "NetSapiens Customers":
                # Fetch the territory_id from netsapiens_reseller where partner matches description
                territory_value = changed_data.get("tenant_name", None)
                query_filter = {"territory": territory_value} if territory_value else {"description": data.get("description", "")}

                # Fetch the territory_id based on the selected filter
                result = database.get_data(
                    'netsapiens_reseller', 
                    query_filter, 
                    ["territory_id"]
                )
                logging.debug(result, "3333333333333333332222222222222222222222222222222222222222")
                
                if result.empty:
                    logging.debug("No matching record found for 'territory'. Attempting to query by 'description' instead.")
                    description_value = changed_data.get("tenant_name", "")
                    query_filter = {"description": description_value}

                    # Run the query again using description as the filter
                    result = database.get_data(
                        'netsapiens_reseller',
                        query_filter,
                        ["territory_id"]
                    )
                
                # If a matching record is found, use it as the netsapiens_customer_id
                if not result.empty:
                    territory_id = result['territory_id'].iloc[0]
                    changed_data['netsapiens_customer_id'] = int(territory_id)
                    print(f"Assigned netsapiens_customer_id: {territory_id} for description: {data.get('description', '')}")
                    
                    
            # Clean up changed_data to ensure it only contains relevant fields for the insert
            if 'id' in changed_data:
                changed_data.pop("id")  # Remove 'id' if present, as it may be auto-generated
            if 'created_by' not in changed_data:
                changed_data['created_by'] = user_name  # Ensure created_by is set to the current user
            if 'created_date' not in changed_data:
                changed_data['created_date'] = request_received_at  # Set created_date to the request time
                
            if 'is_deleted' not in changed_data or changed_data['is_deleted'] in (None, "None", ""):
                changed_data['is_deleted'] = False

            logging.debug("Inserting data: %s", changed_data)
            # Insert the new record into the main update table
            inster_id=database.insert_data(changed_data, main_update_table)

            message = f"Record created successfully"
            logging.debug("Create action completed successfully.")
            response = {"flag": True, "message": message}  
                
        # else:
        #     if 'id' in changed_data:
        #         changed_data.pop("id")
        #     if 'bandwidth_customer_id' in changed_data:
        #         changed_data.pop("bandwidth_customer_id")
        #     if 'netsapiens_customer_id' in changed_data:
        #         changed_data.pop("netsapiens_customer_id")
        #         print(changed_data, main_update_table, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
            # database.insert_dict(changed_data, main_update_table)
            
        message = f"Updated sucessfully"
        response = {"flag": True, "message": message}
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        try:
            audit_data_user_actions = {"service_name": 'Module Management',
                                       "created_date": request_received_at,
                                        "created_by": user_name,
                                        "status": str(response['flag']),
                                        "time_consumed_secs": time_consumed,
                                        "session_id": session_id,
                                        "tenant_name": Partner,
                                        "comments": json.dumps(changed_data),
                                        "module_name": "update_people_data",
                                        "request_received_at":request_received_at}
            dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            print(f"Exception is {e}")
        return response          
    except Exception as e:
        print(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_people_data'
                          ,"created_date": request_received_at,
                          "error_message": message
                          ,"error_type": error_type
                          ,"users": user_name
                          ,"session_id": session_id
                          ,"tenant_name": Partner
                          ,"comments":message,
                          "module_name": "Module Managament",
                          "request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            print(f"Exception is {e}")
        return response
    
def get_superadmin_info(data):
    '''
    Description:The get_superadmin_info function retrieves and formats
      superadmin-related data based on user permissions and requested modules, 
    handling various sub-modules and tabs. It performs permission checks, 
    database queries, and error logging, while measuring performance and updating audit records.
    '''
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Retrieving the data
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    ##database connection
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    sub_parent_module=data.get('sub_parent_module', None)
    parent_module=data.get('parent_module', None)
    
    
    # # Get tenant's timezone
    tenant_name = data.get('tenant_name', '')
    tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
    tenant_timezone = db.execute_query(tenant_timezone_query, params=[tenant_name])

        # Ensure timezone is valid
    if tenant_timezone.empty or tenant_timezone.iloc[0]['time_zone'] is None:
        raise ValueError("No valid timezone found for tenant.")
        
    tenant_time_zone = tenant_timezone.iloc[0]['time_zone']
    match = re.search(r'\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)', tenant_time_zone)
    if match:
        tenant_time_zone = match.group(1).replace(' ', '')  # Ensure it's formatted correctly
    # database Connection
    database = DB('altaworx_central', **db_config)
    tenant_database='altaworx_central'
    pages={}
    try:
        role_name = data.get('role_name', None)
        data_dict_all = {}
        # Retrieve the main tenant ID based on the tenant name
        main_tenant_id=db.get_data(
            "tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]
         # Check if the user is a superadmin
        if role_name and role_name.lower() != 'super admin':
            message = 'Module is not enabled for this user since he is not a superadmin'
            response_data = {"flag": True, "message": message}
            # logging.info(response_data)
        else:
            sub_module = data.get('sub_module', None)
            sub_tab = data.get('sub_tab', '')
            # Handle different sub-modules and tabs  
            if sub_module and sub_module.lower() == 'partner api':  
                Environment = data.get('Environment', '')
                Partner = data.get('Selected_Partner', '')
                environment_list = ['SandBox', 'UAT', 'Prod']
                partner_list = get_tenant_list(db)

                if sub_tab.lower() == 'carrier apis':
                    # Fetch carrier API data
                    logging.info("Fetching carrier API data.")
                    carrier_api_data,pages = get_data_and_format(data,
                        db, "carrier_apis", Environment, Partner)
                    data_dict_all = {
                        "Carrier_apis_data": carrier_api_data,
                        "Environment": environment_list,
                        "Partner": partner_list}
                    # Convert timestamps to a consistent format
                    #data_dict_all = convert_timestamps(data_dict_all) 
                    headers_map=get_headers_mapping(tenant_database,["carrier apis"
                        ],role_name,username,main_tenant_id,sub_parent_module,parent_module,data,db)
                elif sub_tab.lower() == 'amop apis':
                    # Fetch Amop API data
                    logging.info("Fetching Amop API data.")
                    amop_api_data,pages = get_data_and_format(data,db, "amop_apis", Environment, Partner)
                    data_dict_all = {
                        "amop_apis_data": amop_api_data,
                        "Environment": environment_list,
                        "Partner": partner_list
                    }
                    # Convert timestamps to a consistent format
                    #data_dict_all = convert_timestamps(data_dict_all)
                    headers_map=get_headers_mapping(tenant_database,["amop apis"
                    ],role_name,username,main_tenant_id,sub_parent_module,parent_module,data,db)
            elif sub_module and sub_module.lower() == 'partner modules':
                logging.info("Fetching partners and sub-partners.")
                # Extract 'sub_partner' and 'Partner' from the data dictionary
                flag = data.get('flag', '')
                # Initialize tenant_dict
                tenant_dict = {}
                # Fetch parent tenants
                parent_tenant_df = db.get_data('tenant', {'parent_tenant_id': "Null", "is_active": True}, ["id", "tenant_name"])
                # Fetch all active tenants
                all_tenants_df = db.get_data('tenant', {"is_active": True}, ["tenant_name", "parent_tenant_id"])
                # Build the dictionary with parent-child relationships
                for _, parent_row in parent_tenant_df.iterrows():
                    parent_tenant_name = parent_row['tenant_name']
                    parent_tenant_id = parent_row['id']
                    
                    # Filter and sort child tenants
                    child_tenants = sorted(
                        all_tenants_df[all_tenants_df['parent_tenant_id'] == parent_tenant_id]['tenant_name'].tolist(),
                        key=lambda name: (not name[0].isdigit(), name)  # False for digits, True for others
                    )
                    # Add to the dictionary
                    tenant_dict[parent_tenant_name] = child_tenants
                # Sort the tenant_dict keys
                tenant_dict = dict(sorted(tenant_dict.items(), key=lambda item: (not item[0][0].isdigit(), item[0])))

                if flag=='withoutparameters':
                    data_dict_all = {
                            "partners_and_sub_partners": tenant_dict

                        }
                else:
                    sub_partner = data.get('sub_partner', '')
                    partner = data.get('Selected_Partner', '')
                    # Determine the value to use for the query
                    query_value = sub_partner if sub_partner else partner

                    if query_value:
                        try:
                            tenant_query_dataframe = db.get_data(
                                "tenant",{"tenant_name":query_value},["id"])
                            tenant_id = tenant_query_dataframe.iloc[0]['id']
                        except Exception as e:
                            # Log the exception for debugging
                            logging.warning(f"An error occurred while executing the query: {e}")
                    else:
                        logging.info("No valid value provided for the query.")
                    ##roles dataframe for the tenant
                    role_dataframe = db.get_data(
                        'roles',{"tenant_id":int(tenant_id)
                    },['id','role_name','is_active','created_by','modified_by','modified_date'
                       ],{"modified_date":"desc"})
                    roles_data = role_dataframe.to_dict(orient='records')  # Convert to dictionary of lists
                    ##tenant_module
                    role_module_dataframe = db.get_data(
                        'tenant_module',{"tenant_id":int(tenant_id)
                    },['id','module_name','is_active','modified_by','modified_date'
                       ],{"modified_date":"desc"})
                    # Convert to dictionary of lists
                    role_module_data = role_module_dataframe.to_dict(orient='records')  
                    
                    data_dict_all = {
                            "roles_data": roles_data,
                            "role_module_data":role_module_data,
                            "partners and sub partners": tenant_dict

                        }
                data_dict_all = convert_timestamp_data(data_dict_all, tenant_time_zone)
                headers_map=get_headers_mapping(tenant_database,["partner module","role partner module"
                            ],role_name,username,main_tenant_id,'',parent_module,data,db)
            elif sub_module and sub_module.lower() == 'partner creation':
                tenant_names=db.get_data('tenant',{"is_active":True},['tenant_name'])['tenant_name'].to_list()
                data_dict_all = {
                            "tenant_names": tenant_names
                        }
                headers_map="headers_map"


        response = {"flag": True, "data": serialize_data(data_dict_all) ,"headers_map":headers_map,"pages":pages}
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
                                       "comments": 'Reports data',
                                       "module_name": "Reports",
                                       "request_received_at": request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except Exception as e:
            logging.exception(f"exception is {e}")
        return response
        
    except Exception as e:
        logging.exception(F"Something went wrong and error is {e}")
        message = f"Something went wrong getting Super admin info {e}"
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'get_superadmin_info',
                          "created_date": request_received_at,
                          "error_message": message,"error_type": error_type,
                          "users": username,"session_id": session_id,
                          "tenant_name": Partner,"comments": "",
                          "module_name": "Module Managament",
                          "request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return {"flag": False, "message": message}

def get_data_and_format(data, database, table_name, Environment=None, Partner=None):
    """
    Fetches data from a specified database table and formats it into a dictionary.

    Parameters:
    - database (DB): The database object used to execute the query.
    - table_name (str): The name of the table to fetch data from.
    - Environment (str, optional): An optional filter for the environment.
    - Partner (str, optional): An optional filter for the partner.

    Returns:
    - dict: A dictionary containing the table name as the key 
      and a list of records as the value.
    """
    logging.info("Fetching data from table: %s", table_name)
    logging.info("Filters applied - Environment: %s, Partner: %s", Environment, Partner)
    
    mod_pages = data.get('mod_pages', {})
    pages = {}

    # Check if mod_pages exists and extract pagination info
    if "mod_pages" in data:
        start = data["mod_pages"].get("start") or 0  # Default to 0 if no value
        end = data["mod_pages"].get("end") or 100   # Default to 100 if no value
        logging.debug(f"Starting page is {start} and ending page is {end}")
        limit = data.get('limit', 100)

        # Calculate pages 
        pages['start'] = start
        pages['end'] = end

        # Build the count query based on filters
        count_query = f"SELECT COUNT(*) FROM {table_name}"  # Directly inject the table_name here
        count_params = []  # Start with an empty list for parameters

        # Add filters to the query if Environment or Partner is provided
        if Environment and Partner:
            count_query += f" WHERE env = '{Environment}' AND partner = '{Partner}'"
        elif Environment:
            count_query += f" WHERE env = '{Environment}'"
            
        elif Partner:
            count_query += f" WHERE partner = '{Partner}'"
            

        # Log the constructed query and parameters
        logging.debug("Executing count query: %s", count_query)
        logging.debug("With parameters: %s", count_params)

        try:
            # Ensure parameters are passed correctly
            count_result = database.execute_query(count_query,True)  # Ensure it's a tuple

            # Log the result of the count query
            logging.debug("Count query result: %s", count_result)

            # Check if the result is a DataFrame or if it returns a scalar value
            if isinstance(count_result, pd.DataFrame):
                if not count_result.empty:
                    # Extract the count from the DataFrame
                    count_value = count_result.iloc[0, 0]
                else:
                    raise ValueError("Count query returned an empty DataFrame.")
            elif isinstance(count_result, bool):  # If it returns a boolean, something went wrong
                logging.error("Received a boolean result from the count query, something went wrong.")
                raise ValueError("Database query failed or returned unexpected result.")
            else:
                # Handle other possible return types (e.g., scalar value)
                logging.debug("Received a non-DataFrame result: %s", type(count_result))
                count_value = count_result  # Assuming count_result is directly the count value

            pages['total'] = int(count_value)  # Store the count value

        except Exception as e:
            # Log the exception and raise a custom error
            logging.error("Error executing count query: %s", e)
            raise ValueError(f"Database query failed: {e}")

    # Fetch data based on the filters or no filters
    try:
        if Environment and Partner:
            logging.info("Fetching data with filters for Environment and Partner.")
            dataframe = database.get_data(
                table_name, {"env": Environment, "partner": Partner},
                None, {"last_modified_date_time": "desc"}, [], None, mod_pages
            )
        elif Environment:
            logging.info("Fetching data with filters for Environment and Partner.")
            dataframe = database.get_data(
                table_name, {"env": Environment},
                None, {"last_modified_date_time": "desc"}, [], None, mod_pages
            )
        elif Partner:
            logging.info("Fetching data with filters for Environment and Partner.")
            dataframe = database.get_data(
                table_name, {"partner": Partner},
                None, {"last_modified_date_time": "desc"}, [], None, mod_pages
            )

        else:
            logging.info("Fetching data without filters.")
            dataframe = database.get_data(
                table_name, None, None, {"last_modified_date_time": "desc"}, [], None, mod_pages
            )

        # Log the shape of the dataframe
        logging.debug("Fetched dataframe shape: %s", dataframe.shape)

        # Convert the dataframe to a list of dictionaries
        data_list = dataframe.to_dict(orient='records')
        logging.info("Retrieved %d record(s) from table %s.", len(data_list), table_name)
        
        # Structure the data in the desired format
        formatted_data = {table_name: data_list}
        try:
            for record in formatted_data['amop_apis']:
                for key, value in record.items():
                    if isinstance(value, pd.Timestamp):  # Check if the value is a Timestamp
                        record[key] = value.strftime('%m-%d-%Y %H:%M:%S')  # Format Timestamp to string
        except:
            pass
            
    except Exception as e:
        logging.error("Error fetching data: %s", e)
        raise ValueError(f"Data fetching failed: {e}")

    return formatted_data, pages


def get_tenant_list(db, include_sub_tenants=False):
    logging.info("Fetching tenant list. Include sub-tenants: %s", include_sub_tenants)
    #common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    if include_sub_tenants:
        tenant_query_dataframe = db.get_data("tenant",{
            "parent_tenant_id":"not Null","is_active":True},["tenant_name"])
    else:
        tenant_query_dataframe = db.get_data("tenant",{
            "parent_tenant_id":"Null","is_active":True},["tenant_name"])
    
    sorted_tenant_names = tenant_query_dataframe['tenant_name'].sort_values().to_list()
    
    return sorted_tenant_names

# Function to convert Timestamps to strings
def convert_timestamps(obj):
    logging.info("Converting timestamps in the object: %s", obj)
    if isinstance(obj, dict):
        logging.info("Processing a dictionary.")
        return {k: convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        logging.info("Processing a list with %d elements.", len(obj))
        return [convert_timestamps(elem) for elem in obj]
    elif isinstance(obj, pd.Timestamp):
        logging.info("Converting pandas Timestamp: %s", obj)
        return obj.strftime('%m-%d-%Y %H:%M:%S')
    logging.info("No conversion applied for object: %s", obj)
    return obj

def update_superadmin_data(data):
    
    '''
    updates module data for a specified module by checking user and tenant to
    get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, 
    handles errors, and logs relevant information.
    '''
    logging.info("Starting update_superadmin_data function.")
    # Start time  and date calculation
    start_time = time.time()
    # logging.info(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    ##Restriction Check for the Amop API's
    try:
        logging.info("Checking permissions for user.")
        # Create an instance of the PermissionManager class
        permission_manager_instance = PermissionManager(db_config)
    
        # Call the permission_manager method with the data dictionary and validation=True
        result = permission_manager_instance.permission_manager(data, validation=True)
    
        # Check the result and handle accordingly
        if isinstance(result, dict) and result.get("flag") is False:
            logging.warning("Permission check failed.")
            return result
        else:
            logging.info("Permission check passed.")
            # Continue with other logic if needed
            pass
    except Exception as e:
        logging.info(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', '')
    changed_data = data.get('changed_data', {})
    # logging.info(changed_data, 'changed_data')
    unique_id = changed_data.get('id', None)
    # logging.info(unique_id, 'unique_id')
    table_name = data.get('table_name', '')
    ##Database connection
    tenant_database = data.get('db_name', '')
    db = DB(tenant_database, **db_config)
    dbs = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    try:
        # Ensure unique_id is available
        if unique_id is not None:
            logging.info(f"Preparing to update data for unique_id: {unique_id}")
            # Prepare the update data
            update_data = {key: value for key, value in changed_data.items() if key != 'unique_col' and key != 'id'}
            # Perform the update operation
            dbs.update_dict(table_name, update_data, {'id': unique_id})
            logging.info('edited successfully')
            message = f"Data Edited Successfully"
            response_data = {"flag": True, "message": message}
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {"service_name": 'Module Management'
                                       ,"created_date": request_received_at,
            "created_by": username,
                "status": str(response_data['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": json.dumps(changed_data),
                "module_name": "update_superadmin_data","request_received_at":request_received_at
            }
            dbs.update_audit(audit_data_user_actions, 'audit_user_actions')
            return response_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = f"Unable to save the data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_superadmin_data'
                          ,"created_date": request_received_at,
                          "error_message": message,
                          "error_type": error_type,
                          "users": username,
                          "session_id": session_id,
                          "tenant_name": Partner,"comments": "comments",
                          "module_name": "Module Managament",
                          "request_received_at":request_received_at}
            dbs.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response     

def dataframe_to_blob(data_frame):
    '''
    Description:The Function is used to convert the dataframe to blob
    '''
    logging.info("Converting DataFrame to blob.")
    # Create a BytesIO buffer
    bio = BytesIO()
    
    # Use ExcelWriter within a context manager to ensure proper saving
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        logging.info("Writing DataFrame to Excel.")
        data_frame.to_excel(writer, index=False)
        logging.info("DataFrame written to Excel successfully.")
    
    # Get the value from the buffer
    bio.seek(0)
    blob_data = base64.b64encode(bio.read())
    logging.info("DataFrame converted to blob successfully.")
    
    return blob_data
     
def export(data, max_rows=500):
    '''
    Description:Exports data into an Excel file. It retrieves data based on the module name from the database,
    processes it, and returns a blob representation of the data if within the allowed row limit.
    '''
    # logging.info the request data for debugging
    # logging.info(f"Request Data: {data}")
    ### Extract parameters from the Request Data
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', None)
    module_name = data.get('module_name', '')
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    user_name = data.get('user_name', '')
    session_id = data.get('session_id', '')
    tenant_database = data.get('db_name', '')
    ids = data.get('ids', '')
    ##database connection for common utilss
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    logging.info(f"Fetching export query for module: {module_name}")
    # Start time  and date calculation
    start_time = time.time()
    try:
        ##databse connenction
        database = DB(tenant_database, **db_config)
        # Fetch the query from the database based on the module name
        module_query_df = db.get_data("export_queries", {"module_name": module_name})
        # logging.info(module_query_df,'module_query_df')
        ##checking the dataframe is empty or not
        if module_query_df.empty:
            return {
                'flag': False,
                'message': f'No query found for module name: {module_name}'
            }
        # Extract the query string from the DataFrame
        query = module_query_df.iloc[0]['module_query']
        if not query:
            logging.warning(f"Unknown module name: {module_name}")
            return {
                'flag': False,
                'message': f'Unknown module name: {module_name}'
            }
        ##params for the specific module
        if module_name in ("inventory status history", "bulkchange status history"):
            params=[ids]
        else:
            params = [start_date, end_date]
        if module_name=='Users':
            data_frame = db.execute_query(query, params=params)
        else:
            data_frame = database.execute_query(query, params=params)
        # Check the number of rows in the DataFrame
        row_count = data_frame.shape[0]
        # logging.info(row_count,'row_count')
        ##checking the max_rows count
        if row_count > max_rows:
            return {
                'flag': False,
                'message': f'Cannot export more than {max_rows} rows.'
            }
        if module_name=='NetSapiens Customers':
            data_frame['NetSapiensType'] = 'Reseller'
        # Capitalize each word and add spaces
        data_frame.columns = [col.replace('_', ' ').capitalize() for col in data_frame.columns]
        data_frame['S.No'] = range(1, len(data_frame) + 1)
        # Reorder columns dynamically to put S.NO at the first position
        columns = ['S.No'] + [col for col in data_frame.columns if col != 'S.No']
        
        # Format specific columns with dollar symbol for "Rate Plan Socs" module
        
        if module_name == "Rate Plan Socs":
            if "Overage dollar mb" in data_frame.columns:
                data_frame["Overage dollar mb"] = data_frame["Overage dollar mb"].apply(lambda x: f"${x}")
            if "Base rate" in data_frame.columns:
                data_frame["Base rate"] = data_frame["Base rate"].apply(lambda x: f"${x}")
                
        if module_name == "Customer Rate Plan":

    # Check if "Id" column exists in data_frame
            if "Id" in data_frame.columns:
                try:
                    # Convert all "Id" values into a list for a single query
                    ids_list = data_frame["Id"].tolist()
                    
                    # Use a single SQL query to get the counts for each ID at once
                    count_query = """
                        SELECT customer_rate_plan_id, COUNT(*) AS count
                        FROM sim_management_inventory
                        WHERE customer_rate_plan_id IN %s
                        GROUP BY customer_rate_plan_id
                    """
                    
                    # Execute the single query with the list of IDs
                    count_results = database.execute_query(count_query, params=(tuple(ids_list),))

                    # Convert the count results to a dictionary for quick lookup
                    tn_counts_dict = {int(row["customer_rate_plan_id"]): row["count"] for _, row in count_results.iterrows()}

                    # Map counts back to the DataFrame
                    data_frame["# of TNs"] = data_frame["Id"].map(tn_counts_dict).fillna(0).astype(int).astype(str)
                    columns.append('# of TNs')
                    columns.remove('Id')

                except Exception as e:
                    logging.exception(f"Failed to retrieve count for Customer Rate Plan: {e}")
                    return {
                        'flag': False,
                        'message': f"An error occurred while fetching count for Customer Rate Plan: {e}"
                    }

                
                
        data_frame = data_frame[columns]
        # Proceed with the export if row count is within the allowed limit
        data_frame = data_frame.astype(str)
        data_frame.replace(to_replace='None', value='', inplace=True)
        logging.info("Converting DataFrame to blob.")

        blob_data = dataframe_to_blob(data_frame)
        # End time calculation
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))

        # Return JSON response
        response = {
            'flag': True,
            'blob': blob_data.decode('utf-8')
        }
        audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": request_received_at,
            "created_by": user_name,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": "",
            "module_name": "export","request_received_at":request_received_at
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
            # Log error to database
            error_data = {
                "service_name": 'Module Management',
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": message,
                "module_name": "export","request_received_at":request_received_at
            }
            db.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception is {e}")
        return response
        


def rate_plan_dropdown_data_optimization_groups(data):
    #print("Starting to retrieve rate plan dropdown data.")
    # logging.info(f"Request Data is {data}")
    tenant_database = data.get('db_name', None)
    database = DB(tenant_database, **db_config)
    service_provider_names =['AT&T - Telegence']
    #print("Retrieved unique service provider names: %s", service_provider_names)
    # Initialize an empty dictionary to store the results
    rate_plans_list = {}
    # Iterate over each service provider name
    for service_provider_name in service_provider_names:
        #print("Processing service provider: %s", service_provider_name)
        # Get the rate plan codes for the current service provider name
        rate_plan_items = database.get_data(
            "carrier_rate_plan", 
            {'service_provider': service_provider_name,"is_active":True}, 
            ["rate_plan_code"]
        )['rate_plan_code'].to_list()
        #print("Retrieved rate plan codes for %s: %s", service_provider_name, rate_plan_items)
        
        # Add the result to the dictionary
        rate_plans_list[service_provider_name] = rate_plan_items
        #print("Final rate plans list: %s", rate_plans_list)
    
    # Return the resulting dictionary
    return {"flag":True,'rate_plans_list':rate_plans_list}
    #return rate_plans_list             

def insert_data_async(history, db):
    try:
        # Perform the insert operation asynchronously
        history_id = db.insert_data(history, 'sim_management_inventory_action_history')
        logging.info(f"History inserted successfully with ID: {history_id}")
    except Exception as e:
        logging.error(f"An error occurred while inserting history data: {e}")

def send_email_async(template_name, unique_id, request_received_at, db, dbs,role):
    try:
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
        logging.error(f"An error occurred during email notification: {e}")




def get_status_history(data):
    '''
    Retrieves the status history of a SIM management inventory item based on the provided ID.

    Parameters:
    - data (dict): Dictionary containing the 'list_view_data_id' for querying the status history.

    Returns:
    - dict: A dictionary containing the status history data, header mapping, 
    and a success message or an error message.
    '''
    # Start time  and date calculation
    start_time = time.time()
    ##fetching the request data
    list_view_data_id=data.get('list_view_data_id','')
    session_id=data.get('session_id','')
    username=data.get('username','')
    Partner=data.get('Partner','')
    role_name = data.get('role_name', '')
    list_view_data_id=int(list_view_data_id)
    iccid=data.get('iccid','')
    request_received_at = data.get('request_received_at', None)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    tenant_database=data.get('db_name','altaworx_central')
    logging.info(f"Received request to get status history for ID: {list_view_data_id}")
    try:
        ##Database connection
        database = DB(tenant_database, **db_config)
         # Fetch status history data from the database
        sim_management_inventory_action_history_dict=database.get_data(
        "sim_management_inventory_action_history",{'iccid':iccid
        },["service_provider","iccid","msisdn","customer_account_number","customer_account_name"
        ,"previous_value","current_value","date_of_change","change_event_type","changed_by"]
        ).to_dict(orient='records')
        logging.info(f"Fetched {len(sim_management_inventory_action_history_dict)} records for ID: {list_view_data_id}")
        # Handle case where no data is returned
        if not sim_management_inventory_action_history_dict:
             ##Fetching the header map for the inventory status history
            headers_map=get_headers_mapping(tenant_database,["inventory status history"
            ],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data,common_utils_database)
            logging.info("Status history data fetched successfully.")
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
        headers_map=get_headers_mapping(tenant_database,["inventory status history"
        ],role_name,"username","main_tenant_id","sub_parent_module","parent_module",data,common_utils_database)
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
    


def reports_data(data):
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
    table = data.get('table_name', '')
    
    # Database connection
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    return_json_data = {}
    pages = {}

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
            module_query_df = common_utils_database.get_data("export_queries", {"module_name": module_name})
            if module_query_df.empty:
                raise ValueError(f'No query found for module name: {module_name}')
            query = module_query_df.iloc[0]['module_query']
            if not query:
                raise ValueError(f'Unknown module name: {module_name}')
            params = [limit, offset]
            # main_query_start_time = time.time()
            df = database.execute_query(query, params=params)
            # Check if the module is "Usage By Line Report" and adjust billing_cycle_end_date
            if module_name == "Usage By Line Report" and "billing_cycle_end_date" in df.columns:
                df["billing_cycle_end_date"] = pd.to_datetime(df["billing_cycle_end_date"])
                df["billing_cycle_end_date"] = df["billing_cycle_end_date"].apply(
                    lambda date: (date - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    if date.time() == pd.Timestamp("00:00:00").time() else date
                )
            
            # Adjust date_activated for "Newly Activated Report" if it ends in "00:00:00"
            if module_name == "Newly Activated Report" and "date_activated" in df.columns:
                df["date_activated"] = pd.to_datetime(df["date_activated"])
                df["date_activated"] = df["date_activated"].apply(
                    lambda date: (date - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    if date.time() == pd.Timestamp("00:00:00").time() else date
                )
            if module_name == "Customer Rate Pool Usage Report" and "billing_period_start_data" in df.columns:
                df["billing_period_start_data"] = pd.to_datetime(df["billing_period_start_data"])
                df["billing_period_start_data"] = df["billing_period_start_data"].apply(
                    lambda date: (date - pd.Timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    if date.time() == pd.Timestamp("00:00:00").time() else date
                )
            
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

            # query_time = time.time() - q_time
            # logging.info(f"Parallel Query execution time: {query_time:.4f} seconds")

        # Preparing the response data
        return_json_data.update({
            'message': 'Successfully generated the report',
            'flag': True,
            'headers_map': headers_map,
            'data': serialize_data(df_dict),
            'pages': pages
        })

        # End time and audit logging
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        audit_data_user_actions = {
            "service_name": 'Module Management',
            "created_date": request_received_at,
            "created_by": username,
            "status": str(return_json_data['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": 'Reports data',
            "module_name": "Reports",
            "request_received_at": request_received_at
        }
        common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
        return return_json_data

    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = "Unable to fetch the reports data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        
        # Error logging
        try:
            error_data = {
                "service_name": 'update_superadmin_data',
                "created_date": request_received_at,
                "error_message": message,
                "error_type": error_type,
                "users": username,
                "session_id": session_id,
                "tenant_name": Partner,
                "comments": "",
                "module_name": "Module Management",
                "request_received_at": request_received_at
            }
            common_utils_database.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception while logging error: {e}")
        return response



    
    
    
    
    




def people_revio_customers_list_view(data):
    # logging.info(f"Request Data: {data}")

    tenant_name = data.get('tenant_name', None)
    role_name = data.get('role_name', None)
    tenant_database = data.get('db_name', '')     
    # Database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

    try:
        return_json_data = {}

        # Fetch tenant_id based on tenant_name
        # tenant_id = common_utils_database.get_data("tenant", {'tenant_name': tenant_name}, ['id'])['id'].to_list()[0]

        # Pagination logic
        start_page = data.get('mod_pages', {}).get('start', 0)
        end_page = data.get('mod_pages', {}).get('end', 100)
        

        
        limit = end_page - start_page
        offset = start_page 
        
        # # Get tenant's timezone
        tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
        tenant_timezone = common_utils_database.execute_query(tenant_timezone_query, params=[tenant_name])

        # Ensure timezone is valid
        if tenant_timezone.empty or tenant_timezone.iloc[0]['time_zone'] is None:
            raise ValueError("No valid timezone found for tenant.")
        
        tenant_time_zone = tenant_timezone.iloc[0]['time_zone']
        match = re.search(r'\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)', tenant_time_zone)
        if match:
            tenant_time_zone = match.group(1).replace(' ', '')  # Ensure it's formatted correctly    
        headers_map = get_headers_mapping(tenant_database,["people_rev_io_customers"],role_name,'','','','',data,common_utils_database)
        # Dropdown filter query
        # dropdown_query = """
        # SELECT description, bill_profile_id
        # FROM revbillprofile
        # WHERE is_active = 'true';
        # """
        # logging.info("Executing dropdown query to fetch bill profiles.")
        # dropdown_result = database.execute_query(dropdown_query, flag=True)

        # Prepare dropdown data
        dropdown_options = [{"label": "All", "value": "all"}]  # Add the "All" option

        # for row in dropdown_result.itertuples():
        #     dropdown_options.append({
        #         "label": f"{row.description} - {row.bill_profile_id}",
        #         "value": row.bill_profile_id
        #     })


        # selected_bill_profile_id = data.get('dropdown_options', {}).get('value', None)

        
        # Assuming the selected_bill_profile_id is set correctly earlier in the function

        # if selected_bill_profile_id == "all":
            # Main query for fetching all customers
        query = """
                        SELECT
                            COUNT(*) OVER() AS total_count,
                            id,
                            partner, 
                            agent_name,  
                            name, 
                            account,
                            customer_bill_period_end_day, 
                            customer_bill_period_end_hour
                        FROM 
                            public.vw_people_revio_customers
                        ORDER BY  
                            modified_date DESC
                        LIMIT %s OFFSET %s;"""
            
        params = [limit, offset]
        logging.info(f"Executing query to fetch all customers with params: {params}")
        # else:
        #     # Main query for fetching customers based on selected bill_profile_id
        #     query = """SELECT 
        #                     id,
        #                     partner, 
        #                     agent_name,  
        #                     name, 
        #                     account,
        #                     customer_bill_period_end_day, 
        #                     customer_bill_period_end_hour
        #                 FROM 
        #                     public.vw_people_revio_customers
        #                 WHERE bill_profile_id = %s  -- Filter by the selected bill profile ID
        #             ORDER BY 
        #                 modified_date DESC
        #             LIMIT %s OFFSET %s;"""
            
        #     params = [selected_bill_profile_id, limit, offset]
        #     logging.info(f"Executing query to fetch customers with bill profile ID: {selected_bill_profile_id} and params: {params}")

        
        # Execute the main query and get results
        start_time = time.time()
        result = database.execute_query(query, params=params)
        query_duration = time.time() - start_time
        logging.info(f"Query executed in {query_duration:.2f} seconds")

        # Now to get the total count based on the same conditions
        # count_query = """SELECT 
        #                     COUNT(*) AS total_count
        #                 FROM 
        #                     public.vw_people_revio_customers"""

        # if selected_bill_profile_id != "all":
        #     count_query += " WHERE bill_profile_id = %s;"  # Add this condition if filtering by bill_profile_id

        # # Prepare the params for the count query
        # count_params = [tenant_id]

        # if selected_bill_profile_id != "all":
        #     count_params.append(selected_bill_profile_id)

        # Execute the count query
        logging.info("Executing count query.")
        # count_result = database.execute_query(count_query, True)
        total_count = int(result.iloc[0]['total_count']) if not result.empty else 0
        
        # Pagination pages info
        pages = {
            "start": start_page,
            "end": end_page,
            "total": total_count
        }
        


        logging.info("Preparing the response data.")
        # Execute each query and store the result
        # result = database.execute_query(query, params=params)
        # df_dict = result.to_dict(orient='records')
        df_dict = convert_timestamp_data(result.to_dict(orient='records'), tenant_time_zone)

            # Set the response data
        if not result.empty:
            return_json_data['flag'] = True
            return_json_data['message'] = 'Data fetched successfully'
            return_json_data['data'] =serialize_data(df_dict)
            return_json_data['headers_map'] = headers_map
            return_json_data['pages'] = pages
            return_json_data['dropdown_options'] = dropdown_options 
            logging.info("Data fetched successfully.")
        else:
            return_json_data['flag'] = False
            return_json_data['message'] = 'No data found or an error occurred'
            return_json_data['data'] = []
            return_json_data['dropdown_options'] = dropdown_options
            logging.warning("No data found.")

        return return_json_data 


    
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        # Error handling
        return_json_data['flag'] = False
        return_json_data['message'] = f"Failed!! Error: {str(e)}"
        return_json_data['data'] = []
        return return_json_data



def add_people_revcustomer_dropdown_data(data):
    '''
    Description: Retrieves add_service_product dropdown data from the database based on unique identifiers and columns provided in the input data.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    # checking the access token valididty
    username = data.get('username', None)
   

    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    module_name = data.get('module_name', None)
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)

    try:
        response_data = {}
        rev_customer_name = data.get('customer_name', None)
        response_message = ""
        # Query 1: Get customer ID
        query1 = """SELECT IA.username FROM integration_authentication IA JOIN revcustomer RC ON IA.integration_id = RC.integration_authentication_id;"""
        logging.info("Executing Query 1 to retrieve customer ID.")
        
        rev_io_account_df = database.execute_query(query1, flag=True)
        # Extract only the 'username' column values as a list
        if isinstance(rev_io_account_df, pd.DataFrame) and not rev_io_account_df.empty:
            rev_io_account = rev_io_account_df['username'].dropna().tolist()
            # Remove duplicates by converting to a set and back to a list
            rev_io_account = list(dict.fromkeys(rev_io_account))

            response_data['rev_io_account'] = rev_io_account
            logging.debug(f"Retrieved rev_io_account: {rev_io_account}")
        else:
            logging.info("No data found or query failed")
        # Retrieve all customer_name values from the 'customers' table
        customer_names_df = database.get_data('customers', {}, ['customer_name'])
        if isinstance(customer_names_df, pd.DataFrame) and not customer_names_df.empty:
            customer_names = customer_names_df['customer_name'].tolist()
            customer_names = list(set(customer_names))
            response_data['customer_names'] = customer_names
            logging.debug(f"Retrieved customer names: {customer_names}")
        else:
            logging.info("No customer names found or query failed")
            response_data['customer_names'] = []
        
        # Query 2: Get Bill Profile
        query2 = """SELECT  description FROM public.revbillprofile where integration_authentication_id = 1 and is_active = 'true' and is_deleted = 'false' order by description ASC"""
        
        logging.info("Executing Query 2 to retrieve bill profiles.")
        rev_io_bill_profile_df = database.execute_query(query2, flag=True)
        # Extract only the 'username' column values as a list
        if isinstance(rev_io_bill_profile_df, pd.DataFrame) and not rev_io_bill_profile_df.empty:
            rev_bill_profile_list = rev_io_bill_profile_df['description'].dropna().unique().tolist()
            response_data['rev_bill_profile'] = rev_bill_profile_list
            logging.debug(f"Retrieved bill profiles: {rev_bill_profile_list}")
        else:
            logging.info("No bill profiles found or query failed")
            response_data['rev_bill_profile'] = []
        
        # Query 3: Get Customer Rate Plan
        query3 = """
                SELECT *
                FROM revcustomer
                WHERE is_active = 'true' 
                AND is_deleted = 'false'
                AND rev_customer_id IS NOT NULL 
                AND rev_customer_id <> ''
                AND integration_authentication_id = 1
                AND status IS NOT NULL 
                AND status <> 'CLOSED'
                ORDER BY customer_name, rev_customer_id;
            """
        logging.info("Executing Query 3 to retrieve customer rate plans.")
        customer_rate_plan = database.execute_query(query3, flag=True)
        # Extract only the 'username' column values as a list
        if isinstance(customer_rate_plan, pd.DataFrame) and not customer_rate_plan.empty:
            # Construct the customer rate plan list
            response_data['customer_rate_plan'] = [
                f"{row['customer_name']} - {row['rev_customer_id']}"  # Adjust column names as needed
                for index, row in customer_rate_plan.iterrows()
            ]
            logging.debug(f"Retrieved customer rate plans: {response_data['customer_rate_plan']}")
        else:
            logging.warning("No customers found or query failed")
            response_data['customer_rate_plan'] = []
        
        message = " data sent sucessfully"
        response = {"flag": True, "message": message, "response_data": response_data}
        return response
    except Exception as e:
        logging.warning(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting add service product"
        # Error Management
        error_data = {"service_name": 'SIM management',
                      "error_messag": message,
                      "error_type": e, "user": username,
                      "session_id": session_id,
                      "tenant_name": tenant_name,
                      "comments": message,
                      "module_name": module_name,}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}



def download_people_bulk_upload_template(data):
    # Construct the SQL query based on the provided logic
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE 
            (table_name = 'revagent' AND column_name = 'agent_name') OR
            (table_name = 'revcustomer' AND column_name IN ('customer_name', 'rev_customer_id')) OR
            (table_name = 'customers' AND column_name IN ('tenant_name','customer_bill_period_end_day', 'customer_bill_period_end_hour'));
    """
    
    # Execute the query to fetch the columns without parameters (set flag=True)
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    logging.info(f"Executing query to fetch column names from the database: {query}")
    columns_df = database.execute_query(query, flag=True)  # Use flag=True since no parameters are required
    
    if isinstance(columns_df, bool):  # Handle case if execution returns True (error scenario)
        
        logging.info("Query execution returned a boolean value instead of a DataFrame.")
        return {
            'flag': False,
            'error': 'Failed to retrieve columns from the database.'
        }
    logging.debug(f"Columns retrieved from the database: {columns_df['column_name'].tolist()}")
    # Remove the 'id' column if it exists
    columns_df = columns_df[columns_df['column_name'] != 'id']
    columns_df['column_name'] = columns_df['column_name'].str.replace('_', ' ').str.capitalize()
    # logging.info(columns_df, "00000000000000000")
    # logging.info(columns_df['column_name'].values, "o99999999999999999999999")

    # Create an empty DataFrame with the column names as columns
    result_df = pd.DataFrame(columns=columns_df['column_name'].values)
    # logging.info(result_df, "88888876543")


    logging.debug(f"Resulting DataFrame structure: {result_df.columns.tolist()}")
    # Convert the DataFrame to blob (binary data) and return it as part of the response
    blob_data = dataframe_to_blob(result_df)
    response = {
        'flag': True,
        'blob': blob_data.decode('utf-8')
    }
    logging.info("Successfully generated the bulk upload template.")
    return response

def convert_booleans(data):
    logging.debug(f"Initial data for conversion: {data}")
    for key, value in data.items():
        if isinstance(value, str) and value.lower() == "true":
            data[key] = True
            logging.debug(f"Converted '{key}' from string 'true' to boolean True")
        elif isinstance(value, str) and value.lower() == "false":
            data[key] = False
            logging.debug(f"Converted '{key}' from string 'false' to boolean False")
        elif isinstance(value, dict):  # Recursively process nested dictionaries
            logging.debug(f"Found nested dictionary at key '{key}', processing recursively.")
            convert_booleans(value)
    return data  # Return the modified dictionary


def submit_update_info_people_revcustomer(data):
    """
    Updates email template data for a specified module by checking user and tenant permissions.
    Constructs and executes SQL queries to fetch and manipulate data, handles errors, and logs relevant information.
    """
    data = convert_booleans(data)
    changed_data = data.get('changed_data', {})
    user_name = data.get('username', '')
    tenant_name = data.get('tenant_name','')
    request_received_at = data.get('request_received_at','')
    new_data = {k: v for k, v in data.get('new_data', {}).items() if v}
    new_payload = {k: v for k, v in data.get('new_payload', {}).items() if v}
    unique_id = changed_data.get('id')
    action = data.get('action', '')
    
    # Database connection setup
    tenant_database = data.get('db_name', '')
    # database Connection
    dbs = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    
    tenant_id = common_utils_database.get_data("tenant", {'tenant_name': tenant_name}, ['id'])['id'].to_list()[0]
    
    try:
        if action == 'create':
            new_data = {k: v for k, v in new_data.items() if v not in [None, "None"]}
            for k, v in new_data.items():
                if isinstance(v, list):
                    if all(isinstance(item, dict) for item in v):
                        new_data[k] = json.dumps(v)  # Convert list of dicts to JSON string
                    else:
                        new_data[k] = ', '.join(str(item) for item in v if item is not None)  # Convert other types to strings
            
            new_payload = {k: v for k, v in new_payload.items() if v not in [None, "None"]}
            for k, v in new_payload.items():
                if isinstance(v, list):
                    if all(isinstance(item, dict) for item in v):
                        new_payload[k] = json.dumps(v)  # Convert list of dicts to JSON string
                    else:
                        new_payload[k] = ', '.join(str(item) for item in v if item is not None)  # Convert other types to strings
            
            # url = 'https://api.revioapi.com/v1/Customers'
            url = os.getenv("PEOPLE_REVIO_CUSTOMER", " ")
            headers = {
                'Ocp-Apim-Subscription-Key': '04e3d452d3ba44fcabc0b7085cdde431',
                'Authorization': 'Basic QU1PUFRvUmV2aW9AYWx0YXdvcnhfc2FuZGJveDpHZW9sb2d5N0BTaG93aW5nQFN0YW5r'
            }
            
            # Pass the new_data as params in the GET request
            params = new_data

            # Call the API and check response
            api_response = requests.get(url, headers=headers, params=params)
            if api_response.status_code == 200:
                # Success, construct and return response with both API and database insert responses
                message = "Add Rev.io product data submitted successfully."
                
                # Call the insert_data function and capture its response
                insert_response = dbs.insert_data(new_payload, 'customers')
                
                # Ensure insert_response is a dictionary for consistency
                if isinstance(insert_response, int):  # if it's an ID or similar, wrap in dict
                    insert_response = {"success": True, "inserted_id": insert_response}
                elif not isinstance(insert_response, dict):
                    insert_response = {"success": False, "message": "Unknown insert response format"}
                
                # Create the response data including both the API response and insert response
                response_data = {
                    "flag": True,
                    "message": message,
                    "api_response": api_response.json(),
                    "insert_response": insert_response
                }
                
                
                
                return response_data
            else:
                # API call failed, return error message
                raise Exception(f'Failed to retrieve data from client API: {api_response.status_code} - {api_response.text}')
            
        elif action == 'update':
            # Remove fields not in 'customers' table from 'changed_data'
                allowed_fields = [
                    "id", "customer_bill_period_end_day", 
                    "customer_bill_period_end_hour", "modified_by", "modified_date"
                ]
                filtered_changed_data = {k: v for k, v in changed_data.items() if k in allowed_fields and v is not None}
                
                # Ensure 'id' is available for WHERE clause
                if "id" in filtered_changed_data:
                    dbs.update_dict('customers', filtered_changed_data, {'id': filtered_changed_data['id']})
                    message = "Update successful."
                    response_data = {"flag": True, "message": message}
                else:
                    message = "Update failed. Missing required ID for update."
                    response_data = {"flag": False, "message": message}
                
                return response_data

        elif action == 'info':
            line_item_count = 0  # Initialize with a default value

            query = """
                SELECT COUNT(dt.customer_id) AS TotalSimCardCount  
                    FROM public.device jd  
                    INNER JOIN public.device_tenant dt ON jd.id = dt.device_id  
                    INNER JOIN public.customers s ON dt.customer_id = s.id  
                    WHERE jd.is_active = 'true' 
                        AND jd.is_deleted = 'false' 
                        AND (dt.customer_id IS NOT NULL  
                            AND s.id IS NOT NULL  
                            AND s.id = %s)  
                        AND dt.tenant_id = %s;
            """
            
            
            params = [data['info_data']['id'], tenant_id]


            # Execute the query
            result = dbs.execute_query(query, params=params)

            if isinstance(result, list) and result:
                line_item_count = result[0].get('line_item_count', 0)
            elif isinstance(result, pd.DataFrame) and not result.empty:
                line_item_count = result.iloc[0].get('line_item_count', 0)

            response_data = {
                "flag": True,
                "message": f"{action} Successfully",
                "line_item_count": str(line_item_count)
            }
            return response_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = "Unable to save the data"
        response = {"flag": False, "message": message}
        
        return response
    
    
def people_bulk_import_data(data):
    username = data.get('username')
    insert_flag = data.get('insert_flag', 'append')
    tenant_database = data.get('db_name', '')
    
    # Initialize the database connection
    database = DB(tenant_database, **db_config)

    # Check if blob data is provided
    blob_data = data.get('blob')
    if not blob_data:
        logging.warning("Blob data not provided")
        return {"flag": False, "message": "Blob data not provided"}

    try:
        # Extract and decode the blob data
        blob_data = blob_data.split(",", 1)[1]
        blob_data += '=' * (-len(blob_data) % 4)  # Padding for base64 decoding
        file_stream = BytesIO(base64.b64decode(blob_data))

        # Read the data into a DataFrame
        uploaded_dataframe = pd.read_excel(file_stream, engine='openpyxl')

        # SQL query for fetching columns dynamically
        query = """
            SELECT 
                C.tenant_name AS tenant_name, 
                RA.agent_name AS agent_name, 
                RC.customer_name AS customer_name, 
                RC.rev_customer_id AS rev_customer_id, 
                C.customer_bill_period_end_day, 
                C.customer_bill_period_end_hour
            FROM 
                revcustomer RC
            LEFT JOIN 
                revagent RA ON RA.rev_agent_id = RC.agent_id
            LEFT JOIN 
                customers C ON C.rev_customer_id = RC.id
            LIMIT 100 OFFSET 0;
        """
        # Fetch the columns and data from the query
        result_df = database.execute_query(query, True)

        uploaded_dataframe.columns = uploaded_dataframe.columns.str.replace(' ', '_').str.lower()
        print(uploaded_dataframe.columns, "333333333333333333")
        # Normalize DataFrame columns
        uploaded_columns = [col.strip().lower() for col in uploaded_dataframe.columns]
        query_columns = [col.strip().lower() for col in result_df.columns]


        # Compare the column names
        if sorted(uploaded_columns) != sorted(query_columns):
            logging.warning("Column mismatch detected")
            return {
                "flag": False,
                "message": "Columns didn't match",
                "uploaded_columns": list(set(uploaded_columns) - set(query_columns)),
                "query_columns": list(set(query_columns) - set(uploaded_columns))
            }

        # Split uploaded dataframe into separate dataframes for each table
        revcustomer_data = uploaded_dataframe[['customer_name', 'rev_customer_id', 'agent_name']]
        revagent_data = uploaded_dataframe[['agent_name']].drop_duplicates()
        customers_data = uploaded_dataframe[['tenant_name', 'customer_name', 'customer_bill_period_end_day', 'customer_bill_period_end_hour']]

        # Perform the insertion
        if insert_flag == 'append':
            database.insert(revcustomer_data, 'revcustomer', if_exists='append', method='multi')
            database.insert(revagent_data, 'revagent', if_exists='append', method='multi')
            database.insert(customers_data, 'customers', if_exists='append', method='multi')
            logging.info("Append operation completed successfully")
            return {"flag": True, "message": "Append operation is done"}

        logging.error("Invalid insert_flag value")
        return {"flag": False, "message": "Invalid insert_flag value"}

    except Exception as e:
        return {"flag": False, "message": f"An error occurred during the import: {str(e)}"}
    
    
def reports_data_with_date_filter(data):
    '''
    This function retrieves report data by executing a query based 
    on the provided module name and parameters,
    converting the results into a dictionary format. 
    It logs the audit and error details to the database 
    and returns the report data along with a success flag.
    '''
    # Start time  and date calculation
    start_time = time.time()
    module_name=data.get('module_name','')
    # logging.info("module_name",module_name)
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', None)
    Partner = data.get('Partner', '')
    mod_pages = data.get('mod_pages', {})
    logging.info("##mod_pages", mod_pages)
    limit = mod_pages.get('end', 100) 
    offset = mod_pages.get('start', 0)
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    ##Database connection
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    common_utils_database = DB('common_utils', **db_config)
    return_json_data={}
    logging.info(f"Report request for module: {module_name}, Username: {username}, Partner: {Partner}")
    try:
        # Fetch the query from the database based on the module name
        module_query_df = common_utils_database.get_data(
            "export_queries",{"module_name": module_name})
        # logging.info(module_query_df,'module_query_df')
        ##checking the dataframe is empty or not
        if module_query_df.empty:
            return {
                'flag': False,
                'message': f'No query found for module name: {module_name}'
            }
        # Extract the query string from the DataFrame
        query = module_query_df.iloc[0]['module_query']
        if not query:
            logging.warning(f"No query defined for module name: {module_name}")
            return {
                'flag': False,
                'message': f'Unknown  module name: {module_name}'
            }
        params = [start_date,end_date,limit,offset]
        ##executing the query
        df = database.execute_query(query, params=params)
        # Convert dataframe to dictionary
        df_dict = df.to_dict(orient='records')
        for record in df_dict:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.strftime('%m-%d-%Y %H:%M:%S')
        headers_map=get_headers_mapping(tenant_database,[module_name
        ],"role_name","username","main_tenant_id","sub_parent_module","parent_module",data,common_utils_database)
        logging.info(f"## Report written Successfully")
        return_json_data.update({
            'message': 'Successfully generated the report',
            'flag': True,
            'headers_map': headers_map,
            'data': df_dict
        })
        try:
            # End time calculation
            end_time = time.time()
            time_consumed=F"{end_time - start_time:.4f}"
            time_consumed = int(float(time_consumed))
            audit_data_user_actions = {"service_name": 'Module Management',
                                       "created_date": request_received_at,
                                       "created_by": username,
                                       "status": str(return_json_data['flag']),
                                       "time_consumed_secs": time_consumed,
                                       "session_id": session_id,
                                       "tenant_name": Partner,
                                       "comments": 'Reports data',
                                       "module_name": "Reports",
                                       "request_received_at": request_received_at}
            common_utils_database.update_audit(audit_data_user_actions, 'audit_user_actions')
            logging.info("Audit log recorded successfully")
        except Exception as e:
            logging.exception(f"exception is {e}")
        return return_json_data
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        message = f"Unable to fetch the reports data"
        response = {"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'update_superadmin_data',
                          "created_date": request_received_at,"error_message": message,
                          "error_type": error_type,"users": username,"session_id": session_id,
                          "tenant_name": Partner,"comments": "","module_name": "Module Managament",
                          "request_received_at":request_received_at}
            common_utils_database.log_error_to_db(error_data, 'error_log_table')
            logging.info("Error details logged successfully")
        except Exception as e:
            logging.exception(f"exception is {e}")
        return response


def get_modules(data):
    #start_time = time.time()
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    session_id = data.get('session_id', None)
    role_name = data.get('role_name', None)
    tenant_database = data.get('db_name', '')
    db = DB('common_utils', **db_config)
    database = DB('altaworx_central', **db_config)
    tenant_id=db.get_data('tenant',{"tenant_name":tenant_name},['id'])['id'].to_list()[0]
    # Step 1: Fetch tenant modules
    tenant_module_query_params = [tenant_name]
    tenant_module_query = '''SELECT module_name
                             FROM tenant_module 
                             WHERE tenant_name = %s AND is_active = TRUE order by id asc;'''
    tenant_module_dataframe = db.execute_query(tenant_module_query, params=tenant_module_query_params)
    
    main_tenant_modules = tenant_module_dataframe["module_name"].to_list()
    logging.debug(f"Tenant modules fetched: {main_tenant_modules}")
    
    # Step 2: Handle modules based on role
    if role_name.lower() == 'super admin':
        # For Super Admin, get all tenant modules
        final_modules = [{"module": mod, "sub_module": []} for mod in main_tenant_modules]  # Ensure final_modules is a list of dicts
    else:
        # Fetch role modules for non-super admin
        role_module_data = database.get_data("role_module", {"role": role_name}, ["module", "sub_module"]).to_dict(orient='records')
        
        # Step 2.1: Deserialize the JSON-like strings if needed
        if role_module_data:
            for item in role_module_data:
                item["module"] = json.loads(item["module"])  # Convert module string to list
                item["sub_module"] = json.loads(item["sub_module"])  # Convert sub_module string to dict
    
        # Step 3: Filter role modules by tenant modules
        filtered_modules = []
        for module in role_module_data:
            # Iterate over the list of modules
            for mod in module["module"]:
                if mod in main_tenant_modules:
                    filtered_modules.append({
                        "module": mod,
                        "sub_module": module["sub_module"].get(mod, [])  # Get sub_modules or empty list if not found
                    })
        
        # Step 4: Add filtered modules to final_modules
        final_modules = filtered_modules
        logging.debug(f"Final modules after filtering: {final_modules}")
    
    # Now `final_modules` will contain the relevant modules based on the user's role.
    
    # Step 5: Fetch user-specific module and sub_module data
    user_module_df = db.get_data(
        "user_module_tenant_mapping", 
        {"user_name": username, "tenant_id": tenant_id}, 
        ["module_names", "sub_module"]
    ).to_dict(orient='records')
    
    if not user_module_df or not user_module_df[0].get("module_names"):
        # If no user-specific modules are found, use final_modules as is
        logging.debug("No user-specific modules found, using final modules.")
        # Now `final_modules` contains either the filtered data or the entire modules if no filtering was applied.
        logging.debug(final_modules,'ghbjklhjgvbjkhvhjlhvbjhb')
        # Transforming the keys
        transformed_data = [
            {
                'parent_module_name': item['module'], 
                'module_name': item['sub_module']
            } 
            for item in final_modules  # Use final_modules instead of data
        ]
        
        # Print the transformed data
        logging.debug('transformed_data',transformed_data)
        module_table_df = db.get_data(
        "module", {"is_active": True}, 
        ["module_name", "parent_module_name", "submodule_name"], 
        {'id': "asc"}
        ).to_dict(orient="records")
        
        
        # Process the data
        formatted_data = format_module_data(transformed_data, module_table_df)
        
        # Output the formatted data
        logging.debug(formatted_data)
    else:
        # Convert module_names to a list
        module_names_list = json.loads(user_module_df[0]['module_names'])

        # Convert sub_module JSON string to a dictionary
        sub_module_dict = json.loads(user_module_df[0]['sub_module'])

        # Prepare the output data
        filtered_output = []

        # Iterate through the module names to filter based on the user data
        for module_name in module_names_list:
            # Get the corresponding sub_modules
            valid_sub_modules = []
            for sub_module in sub_module_dict.get(module_name, []):
                # You can apply any further logic if needed for filtering sub_modules
                valid_sub_modules.append(sub_module)
            
            # Append valid module and sub_modules to the output
            if valid_sub_modules:
                filtered_output.append({'parent_module_name': module_name, 'module_name': valid_sub_modules})

        # Final output
        print(filtered_output)
        filter_data=filtered_output
        modules = []
        module_data = db.get_data('module', {}, ['id', 'parent_module_name', 'module_name', 'submodule_name']).to_dict(orient='records')

        # Initialize lists to store filtered results
        filtered_results = []

        # Loop through the filter data
        for filter_item in filter_data:
            parent_name = filter_item['parent_module_name']
            module_names = filter_item['module_name']
            
            # Check the module_data for matching parent and module names
            for module_item in module_data:
                # If the parent_module_name matches
                if module_item['parent_module_name'] == parent_name:
                    # If there is a match in module_name or submodule_name
                    if module_item['module_name'] in module_names or module_item['submodule_name'] in module_names:
                        filtered_results.append(module_item)
                    # Handle the case where you just want the parent module even if no submodules or modules match
                    elif parent_name == module_item['parent_module_name']:
                        filtered_results.append(module_item)

        # Create structured output for modules with IDs
        modules = []  # Resetting modules to gather results
        for item in filtered_results:
            module_entry = {
                'id': item['id'],  # Use the existing ID from module_data
                'parent_module_name': item['parent_module_name'],
                'module_name': item['module_name'],
                'submodule_name': item['submodule_name']
            }
            modules.append(module_entry)

        # Print the structured modules list
        print("Modules:", modules)
        my_dict=modules
        # Initialize the output structure
        output_structure = []

        # Group by parent_module_name
        for item in my_dict:
            parent_name = item['parent_module_name']
            
            # Check if the parent entry already exists
            parent_entry = next((entry for entry in output_structure if entry['parent_module_name'] == parent_name), None)
            
            if not parent_entry:
                parent_entry = {
                    'parent_module_name': parent_name,
                    'queue_order': item['id'],  # Use id as queue_order for parent
                    'children': []
                }
                output_structure.append(parent_entry)

            # Only add a child if module_name is not None
            if item['module_name'] is not None:
                # Check if the child module already exists
                child_entry = next((child for child in parent_entry['children'] if child['child_module_name'] == item['module_name']), None)
                
                if not child_entry:
                    child_entry = {
                        'child_module_name': item['module_name'],
                        'queue_order': len(parent_entry['children']) + 1,  # Position-based queue_order for children
                        'sub_children': []  # Always include sub_children key
                    }
                    parent_entry['children'].append(child_entry)

                # If there is a submodule, add it to sub_children
                if item['submodule_name']:
                    sub_child_entry = {
                        'sub_child_module_name': item['submodule_name'],
                        'queue_order': len(child_entry['sub_children']) + 1,  # Position-based queue_order for sub-children
                        'sub_children': []  # Always include sub_children key for sub-modules
                    }
                    child_entry['sub_children'].append(sub_child_entry)

        
        # Print the structured output
        #print("Output Structure:")
        #print(json.dumps(output_structure))
        formatted_data=output_structure
    logo=db.get_data("tenant",{'tenant_name':tenant_name},['logo'])['logo'].to_list()[0]
    message = "Module data sent sucessfully"
    response = {"flag": True, "message": message, "Modules": formatted_data,"logo":logo}
    logging.info("get_modules function executed successfully")
    return response

def format_module_data(transformed_data, module_table_df):
    logging.info("Starting format_module_data function")
    # Step 1: Create a mapping for transformed_data to find parent modules easily
    parent_module_map = {item['parent_module_name']: {
        "parent_module_name": item['parent_module_name'],
        "queue_order": 0,  # Placeholder for queue order
        "children": []
    } for item in transformed_data}
    logging.debug(f"Parent module map initialized with {len(parent_module_map)} entries")

    # Step 2: Populate the parent_module_map with child and sub-child modules
    for module in module_table_df:
        parent_name = module['parent_module_name']
        module_name = module['module_name']
        submodule_name = module.get('submodule_name')
        logging.debug(f"Processing module: {module_name} under parent: {parent_name}")

        # Check if the parent module name exists in the transformed_data
        if parent_name in parent_module_map and module_name:  # Ensure module_name is not None or empty
            # Check if the child already exists in the parent's children list
            child_module = next((child for child in parent_module_map[parent_name]['children']
                                 if child['child_module_name'] == module_name), None)

            if not child_module:
                # If the child module doesn't exist, create a new one
                child_module = {
                    "child_module_name": module_name,
                    "queue_order": len(parent_module_map[parent_name]['children']) + 1,
                    "sub_children": []
                }
                # Append the child module to the parent's children
                parent_module_map[parent_name]['children'].append(child_module)
                logging.debug(f"Added child module: {module_name} with queue order {child_module['queue_order']}")

            # If submodule_name exists, add it to sub_children
            if submodule_name:
                sub_child_module = {
                    "sub_child_module_name": submodule_name,
                    "queue_order": len(child_module["sub_children"]) + 1,
                    "sub_children": []
                }
                child_module["sub_children"].append(sub_child_module)
                logging.debug(f"Added sub-child module: {submodule_name} with queue order {sub_child_module['queue_order']}")

    # Step 3: Convert parent_module_map to a list
    structured_data = []
    for index, (parent_name, parent_data) in enumerate(parent_module_map.items()):
        parent_data["queue_order"] = index + 1  # Set queue_order based on the index
        structured_data.append(parent_data)  # Include even if no children
        logging.debug(f"Set queue order {parent_data['queue_order']} for parent module: {parent_name}")

    logging.info("Successfully formatted module data")
    return structured_data

# def format_module_data(transformed_data, module_table_df):
#     # Step 1: Create a mapping for transformed_data to find parent modules easily
#     parent_module_map = {item['parent_module_name']: {
#         "parent_module_name": item['parent_module_name'],
#         "queue_order": 0,  # Placeholder for queue order
#         "children": []
#     } for item in transformed_data}

#     # Step 2: Populate the parent_module_map with child modules
#     for module in module_table_df:
#         parent_name = module['parent_module_name']
#         module_name = module['module_name']
#         submodule_name = module.get('submodule_name')

#         # Check if the parent module name exists in the transformed_data
#         if parent_name in parent_module_map:
#             # Create a child module entry
#             child_module = {
#                 "child_module_name": module_name,
#                 "queue_order": len(parent_module_map[parent_name]['children']) + 1,
#                 "sub_children": []
#             }

#             # If submodule_name exists, add it to sub_children
#             if submodule_name:
#                 child_module["sub_children"].append({
#                     "sub_child_module_name": submodule_name,
#                     "queue_order": 1,  # Set queue order for subchildren
#                     "sub_children": []
#                 })

#             # Append the child module to the parent's children
#             parent_module_map[parent_name]['children'].append(child_module)

#     # Step 3: Convert parent_module_map to a list
#     structured_data = []
#     for index, (parent_name, parent_data) in enumerate(parent_module_map.items()):
#         parent_data["queue_order"] = index + 1  # Set queue_order based on the index
#         structured_data.append(parent_data)

#     return structured_data

def form_modules_dictionary(data, sub_modules, tenant_modules):
    '''
    Description: The form_modules_dict function constructs a nested dictionary that maps parent modules 
    to their respective submodules and child modules. It filters and organizes modules based on the 
    user's tenant permissions and specified submodules.
    '''
    logging.info("Starting form_modules_dictionary function")
    # Initialize an empty dictionary to store the output
    out = {}
    
    # Iterate through the list of modules in the data
    for item in data:
        parent_module = item['parent_module_name']
        logging.debug(f"Processing parent module: {parent_module}")
        
        # Skip modules not assigned to the tenant
        if parent_module not in tenant_modules and parent_module:
            logging.debug(f"Skipping parent module {parent_module} - not in tenant_modules")
            continue
            
        # If there's no parent module, initialize an empty dictionary for the module
        if not parent_module:
            logging.debug(f"Adding top-level module: {item['module_name']}")
            out[item['module_name']] = {}
            continue
        else:
            logging.debug(f"Initializing empty dictionary for parent module {parent_module}")
            out[item['parent_module_name']] = {}
        
        # Iterate through the data again to find related modules and submodules
        for module in data:
            temp = {}
            
            # Skip modules not in the specified submodules
            if (module['module_name'] not in sub_modules and module['submodule_name'] not in sub_modules):
                logging.debug(f"Skipping module {module['module_name']} or submodule {module['submodule_name']} - not in sub_modules")
                continue
            
            # Handle modules without submodules and create a list for them
            if module['parent_module_name'] == parent_module and module['module_name'] and not module['submodule_name']:
                logging.debug(f"Adding module without submodule: {module['module_name']} under {parent_module}")
                temp = {module['module_name']: []}
            # Handle modules with submodules and map them accordingly
            elif module['parent_module_name'] == parent_module and module['module_name'] and module['submodule_name']:
                logging.debug(f"Mapping submodule {module['submodule_name']} to module {module['module_name']} under {parent_module}")
                temp = {module['submodule_name']: [module['module_name']]}
            
            # Update the output dictionary with the constructed module mapping
            if temp:
                for key, value in temp.items():
                    if key in out[item['parent_module_name']]:
                        out[item['parent_module_name']][key].append(value[0])
                        logging.debug(f"Appending {value[0]} to existing submodule {key} in {parent_module}")
                    else:
                        out[item['parent_module_name']].update(temp)
                        logging.debug(f"Adding new submodule {key} to {parent_module}")

    logging.info("Module dictionary formed successfully")
    # Return the final dictionary containing the module mappings                    
    return out


def transform_structure_data_values(input_data):
    '''
    Description: The transform_structure function transforms a nested dictionary 
    of modules into a list of structured dictionaries, each with queue_order to 
    maintain the order of parent modules, child modules, and sub-children.
    '''
    logging.info("Starting transform_structure_data_values function")
    # Initialize an empty list to store the transformed data
    transformed_data = []
    
    # Initialize the queue order for parent modules
    queue_order = 1 
    
    # Iterate over each parent module and its children in the input data
    for parent_module, children in input_data.items():
        logging.debug(f"Processing parent module: {parent_module} with queue_order {queue_order}")
        transformed_children = []
        child_queue_order = 1
        
        # Iterate over each child module and its sub-children
        for child_module, sub_children in children.items():
            logging.debug(f"Processing child module: {child_module} with queue_order {child_queue_order}")
            transformed_sub_children = []
            sub_child_queue_order = 1
            
            # Iterate over each sub-child module
            for sub_child in sub_children:
                logging.debug(f"Adding sub-child module: {sub_child} with queue_order {sub_child_queue_order}")
                transformed_sub_children.append({
                    "sub_child_module_name": sub_child,
                    "queue_order": sub_child_queue_order,
                    "sub_children": []
                })
                sub_child_queue_order += 1
            
            # Append the transformed child module with its sub-children
            transformed_children.append({
                "child_module_name": child_module,
                "queue_order": child_queue_order,
                "sub_children": transformed_sub_children
            })
            child_queue_order += 1
        
        # Append the transformed parent module with its children
        transformed_data.append({
            "parent_module_name": parent_module,
            "queue_order": queue_order,
            "children": transformed_children
        })
        queue_order += 1
    logging.info("Transformation completed successfully")
    
    # Return the list of transformed data
    return transformed_data



def convert_timestamp(data):
    logging.debug(f"Converting data: {data}")
    if isinstance(data, pd.Timestamp):
        logging.debug(f"Converting pd.Timestamp: {data}")
        return data.strftime('%m-%d-%Y %H:%M:%S')
    elif isinstance(data, dict):
        logging.debug("Processing dictionary in convert_timestamp")
        return {k: convert_timestamp(v) for k, v in data.items()}
    elif isinstance(data, list):
        logging.debug("Processing list in convert_timestamp")
        return [convert_timestamp(v) for v in data]
    else:
        logging.debug(f"No conversion needed for type: {type(data)}")
        return data



def carrier_rate_plan_list_view(data):
    '''
    Description: Retrieves emails in list view data from the database.
    Validates the access token and logs the request, then fetches and returns the device history if the token is valid.
    '''
    print("Starting carrier_rate_plan_list_view function")

    role_name = data.get('role_name', '')
    module_name = data.get('module_name', '')
    tenant_database = data.get('db_name', 'altaworx_central')
    database = DB('common_utils', **db_config)

    # Set up pagination defaults
    start_page = data.get('mod_pages', {}).get('start', 0)
    end_page = data.get('mod_pages', {}).get('end', 100)
    limit = 100  # Set the limit to 100
    return_json_data = {}

    try:
        # Connect to the tenant database
        dbs = DB(tenant_database, **db_config)
        
        
        # # Get tenant's timezone
        tenant_name = data.get('tenant_name', '')
        tenant_timezone_query = """SELECT time_zone FROM tenant WHERE tenant_name = %s"""
        tenant_timezone = database.execute_query(tenant_timezone_query, params=[tenant_name])

        # Ensure timezone is valid
        if tenant_timezone.empty or tenant_timezone.iloc[0]['time_zone'] is None:
            raise ValueError("No valid timezone found for tenant.")
        
        tenant_time_zone = tenant_timezone.iloc[0]['time_zone']
        match = re.search(r'\(\w+\s[+\-]?\d{2}:\d{2}:\d{2}\)\s*(Asia\s*/\s*Kolkata)', tenant_time_zone)
        if match:
            tenant_time_zone = match.group(1).replace(' ', '')  # Ensure it's formatted correctly

        # Main data query
        query = """
            SELECT
                id,
                service_provider,
                rate_plan_short_name,
				friendly_name,
                device_type,
                base_rate,
                overage_rate_cost,
                plan_mb,
                data_per_overage_charge,
                allows_sim_pooling,
                is_retired,
                modified_by,
                TO_CHAR(modified_date::date, 'YYYY-MM-DD') AS modified_date
            FROM carrier_rate_plan 
            WHERE 
              friendly_name IS NOT NULL
            ORDER BY modified_date DESC
            LIMIT %s OFFSET %s;
        """
        result = dbs.execute_query(query, params=[limit, start_page])

        # Count query for pagination
        count_query = "SELECT COUNT(*) AS total_count FROM carrier_rate_plan"
        total_count_result = dbs.execute_query(count_query, True)
        total_count = total_count_result['total_count'].iloc[0] if not total_count_result.empty else 0

        # Additional query to count occurrences of each service_provider
        provider_count_query = """
            SELECT service_provider, COUNT(*) as provider_count
            FROM carrier_rate_plan
            GROUP BY service_provider
        """
        provider_count_result = dbs.execute_query(provider_count_query, True)

        # Create a dictionary to check service_provider occurrences
        provider_counts = provider_count_result.set_index('service_provider')['provider_count'].to_dict()

        # Get headers mapping
        headers_map = get_headers_mapping(tenant_database, ["Rate Plan Socs"], role_name, '', '', '', '', data,database)

        # Check if result is empty
        if result.empty:
            logging.info("No data found for carrier_rate_plan")
            return {
                'flag': False,
                'data': [],
                'message': 'No data found for E911 Customers!',
                'headers_map': headers_map,
                'pages': {"start": start_page, "end": end_page, "total": total_count}
            }

        # Convert and format data, add new column for provider occurrence
        df_dict = result.to_dict(orient='records')
        df_dict = convert_timestamp_data(df_dict, tenant_time_zone)
        #df_dict = convert_timestamp(df_dict)

        for record in df_dict:
            provider = record.get('service_provider')
            # Set 'has_multiple_providers' to True if count > 1, else False
            record['assigned'] = "True" if provider_counts.get(provider, 0) > 1 else "False"

        pages = {
            "start": start_page,
            "end": end_page,
            "total": int(total_count)
        }

        # Successful response
        logging.info("Returning data response")
        return {
            'flag': True,
            'message': 'Data fetched successfully',
            'data': {
        'carrier_rate_plan': serialize_data(df_dict)
    },
            'headers_map': headers_map,
            'pages': pages
        }

    except Exception as e:
        logging.exception("An error occurred in carrier_rate_plan_list_view:", e)
        headers_map = get_headers_mapping(tenant_database, [module_name], role_name, '', '', '', '', data,database)
        return {
            "flag": False,
            "message": "Data fetch failed",
            "headers_map": headers_map,
            "pages": {}
        }


# def statuses_inventory(data):
#     logging.info("Request Data recieved")
#     service_provider_id=data.get('service_provider_id','')
#     tenant_database = data.get('db_name', 'altaworx_central')
#     database = DB(tenant_database, **db_config)
#     try:
#         integration_id=database.get_data('serviceprovider',{"id":service_provider_id},['integration_id'])['integration_id'].to_list()[0]
#         statuses=database.get_data('device_status',{"integration_id":integration_id,"is_active":True,"is_deleted":False},['display_name'])['display_name'].to_list()
#         response={"flag":True,"update_status_values":statuses}
#         return response
#     except Exception as e:
#         logging.exception(f"Exception while fetching statuses")
#         response={"flag":True,"update_status_values":[]}
#         return response
    
    
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

def export_to_s3_bucket(data, max_rows=5000000):
    # You might want to configure the S3 bucket name
    S3_BUCKET_NAME = 'searchexcelssandbox'
    '''
    Description: Exports data into an Excel or CSV file, stores it in S3, and returns the URL.
    '''
    # Extract parameters from the Request Data
    Partner = data.get('Partner', '')
    request_received_at = data.get('request_received_at', None)
    module_name = data.get('module_name', '')
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    user_name = data.get('user_name', '')
    session_id = data.get('session_id', '')
    tenant_database = data.get('db_name', '')
    ids = data.get('ids', '')
    
    # database connection for common utils
    db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    
    # #logging.info(f"Fetching export query for module: {module_name}")
    
    # start_time = time.time()
    try:
        database = DB(tenant_database, **db_config)
        # Convert the start_date string to a datetime object
        start_date = datetime.strptime(data["start_date"], "%Y-%m-%d %H:%M:%S")
        # Extract the bill_year and bill_month
        bill_year = start_date.year
        bill_month = start_date.month
        # Assuming you already have the dataframe `billing_periods`
        billing_periods_query = f"""
            select 
                service_provider,
                TO_CHAR(billing_cycle_start_date::date, 'YYYY-MM-DD HH:MI:SS') AS billing_cycle_start_date,
                TO_CHAR(billing_cycle_end_date::date, 'YYYY-MM-DD HH:MI:SS') AS billing_cycle_end_date
            from billing_period 
            where bill_year = '{bill_year}' and bill_month = '{bill_month}'
        """
        
        # Fetch the data from the database and load it into DataFrame
        billing_periods = database.execute_query(billing_periods_query, True)
        
        # Convert the columns to datetime
        billing_periods['billing_cycle_start_date'] = pd.to_datetime(billing_periods['billing_cycle_start_date'])
        billing_periods['billing_cycle_end_date'] = pd.to_datetime(billing_periods['billing_cycle_end_date'])
        
        # Get the earliest billing cycle start date (minimum date)
        earliest_billing_cycle_start_date = billing_periods['billing_cycle_start_date'].min()
        
        # Get the latest billing cycle end date (most recent in the future)
        latest_billing_cycle_end_date = billing_periods['billing_cycle_end_date'].max()
        # Fetch the query from the database based on the module name
        module_query_df = db.get_data("export_queries", {"module_name": module_name})
        
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
        params = [earliest_billing_cycle_start_date, latest_billing_cycle_end_date] if module_name not in ("inventory status history", "bulkchange status history") else [ids]
        
        # Execute query
        data_frame = database.execute_query(query, params=params)
        # Capitalize column names
        data_frame.columns = [col.replace('_', ' ').capitalize() for col in data_frame.columns]

        # Convert to CSV (you can convert to Excel if you prefer)
        csv_buffer = StringIO()
        data_frame.to_csv(csv_buffer, index=False)

        # Upload the CSV file to S3
        file_name = f"exports/Inventory_export.csv"
        csv_buffer.seek(0)  # Move to the start of the StringIO buffer

        # Upload to S3 (public or private based on your needs)
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

        # Generate URL (public URL or pre-signed URL)
        download_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file_name}"
        response = {
            'flag': True,
            'download_url': download_url  # Return the URL where the file is stored in S3
        }
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