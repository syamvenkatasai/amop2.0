"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
# Importing the necessary Libraries
import re
import requests
import os
import pandas as pd
from datetime import time
import time
from io import BytesIO
import json
import base64
import zipfile
import io
import pytds
import base64
from pytz import timezone
import boto3
import concurrent.futures
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    'host': os.environ['HOST'],
    'port': os.environ['PORT'],
    'user': os.environ['USER'],
    'password': os.environ['PASSWORD']
}
logging = Logging(name="optimization")



##first point
def get_optimization_data(data):
    '''
    This function retrieves optimization data by executing a query based 
    on the provided module name and parameters,
    converting the results into a dictionary format. 
    It logs the audit and error details to the database 
    and returns the optimization data along with a success flag.
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
    table = data.get('table_name', 'vw_optimization_instance_summary')
    optimization_type= data.get('optimization_type', '')
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
        count_params = [table]
        count_query = f"SELECT COUNT(*) FROM {table} where optimization_type='{optimization_type}'"                
        # count_start_time = time.time()
        count_result = database.execute_query(count_query, count_params).iloc[0, 0]
        # Set total pages count
        pages['total'] = int(count_result)
        module_query_df = common_utils_database.get_data("module_view_queries", {"module_name": optimization_type})
        if module_query_df.empty:
            raise ValueError(f'No query found for module name: {optimization_type}')
        query = module_query_df.iloc[0]['module_query']
        if not query:
            raise ValueError(f'Unknown module name: {optimization_type}')
        params = [offset, limit]
        # main_query_start_time = time.time()
        df = database.execute_query(query, params=params)
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
        tenant_time_zone=fetch_tenant_timezone()
        # conversion_start_time = time.time()
        df_dict = df.to_dict(orient='records')
        # Convert timestamps
        df_dict = convert_timestamp_data(df_dict, tenant_time_zone)
        # Get headers mapping
        headers_map = get_headers_mapping(
            tenant_database, [optimization_type], role_name, "username", 
            "main_tenant_id", "sub_parent_module", "parent_module", data, common_utils_database
        )
        service_providers=database.get_data('serviceprovider',{"is_active":True},['service_provider_name'])['service_provider_name'].to_list()
        # Sort the list of service providers
        service_providers = sorted(service_providers)
        # Preparing the response data
        return_json_data.update({
            'message': 'Successfully Fetched the optimization data',
            'flag': True,"service_providers":service_providers,
            'headers_map': headers_map,
            'data': df_dict,
            'pages': pages
        })

        # End time and audit logging
        end_time = time.time()
        time_consumed=F"{end_time - start_time:.4f}"
        time_consumed = int(float(time_consumed))
        audit_data_user_actions = {
            "service_name": 'optimization',
            "created_date": request_received_at,
            "created_by": username,
            "status": str(return_json_data['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": 'optimization data',
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
        message = f"Unable to fetch the Optimization data{e}"
        return_json_data = {"flag": True, "message": message,"headers_map":headers_map,'data': []}
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
                "module_name": module_name,
                "request_received_at": request_received_at
            }
            common_utils_database.log_error_to_db(error_data, 'error_log_table')
        except Exception as e:
            logging.exception(f"Exception while logging error: {e}")
        return return_json_data



def get_headers_mapping(tenant_database,module_list,role,user,tenant_id,
                        sub_parent_module,parent_module,data,common_utils_database):
    '''
    Description: The  function retrieves and organizes field mappings,headers,and module features 
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    '''
    feature_module_name=data.get('feature_module_name','')
    user_name = data.get('username') or data.get('user_name') or data.get('user')
    tenant_name = data.get('tenant_name') or data.get('tenant') 
    try:
        tenant_id=common_utils_database.get_data('tenant',{
            "tenant_name":tenant_name}['id'])['id'].to_list()[0]
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
                final_features = get_features_by_feature_name(user_name, tenant_id, 
                                                              feature_module_name,common_utils_database)
                logging.info("Fetched features for user '%s': %s", user_name, final_features)

        except Exception as e:
            logging.info(f"there is some error {e}")
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


def format_timestamp(ts):
    # Check if the timestamp is not None
    if ts is not None:
        # Convert a Timestamp or datetime object to the desired string format
        return ts.strftime("%b %d, %Y, %I:%M %p")
    else:
        # Return a placeholder or empty string if the timestamp is None
        return " "


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



##export button in list view
def export_optimization_data_zip(data):
    '''
    The export_optimization_data_zip function generates a ZIP file containing 
    Excel files grouped by session IDs based on query results retrieved from a 
    database. It processes optimization data using provided filters, encodes the 
    ZIP as a base64 string, and returns it for download, with error logging 
    in case of failures.
    '''
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
    common_utils_database = DB('common_utils', **db_config)

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
        
        # Buffer for the zip file (in memory)
        zip_buffer = io.BytesIO()
        #current_date_str = datetime.now().strftime('%Y%m%d')
        zip_filename = f"Optimization_session.zip"

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Group rows by 'session_id' and process each group as a batch
            grouped = data_frame.groupby('session_id')

            for session_id, group in grouped:
                # Create a folder inside the ZIP for each session_id
                folder_name = f'{session_id}/'
                zipf.writestr(folder_name, '')  # Create folder in ZIP

                # Buffer for the grouped Excel file (in memory)
                excel_buffer = io.BytesIO()

                # Write the entire group as a single Excel file in memory
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    group.to_excel(writer, index=False)

                # Move back to the start of the buffer to read the content
                excel_buffer.seek(0)

                # Add the Excel file to the ZIP archive inside the session_id folder
                zipf.writestr(f'{folder_name}{session_id}.xlsx', excel_buffer.read())

        # Get the content of the zip buffer
        zip_buffer.seek(0)
        zip_blob = zip_buffer.getvalue()
        # Convert to base64 (can be sent to frontend as a blob)
        encoded_blob = base64.b64encode(zip_blob).decode('utf-8')
        return {
            'flag': True,
            'blob': encoded_blob,
            'filename': zip_filename
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


##details for optimization dropdown data
def optimization_dropdown_data(data):
    '''
    The optimization_dropdown_data function fetches dropdown data 
    for service providers, including unique customer names and their
    corresponding billing periods, based on active service providers 
    from the database. It structures the response into two parts:
    service_provider_customers and service_provider_billing_periods,
    handling errors and returning appropriate fallback data in case of an issue.
    '''
    logging.info(f"Request Data Recieved")
    Partner = data.get('Partner', '')
    username = data.get('username', '')
    module_name = data.get('module_name', '')
    request_received_at = data.get('request_received_at', '')
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    
    try:
        # List of service provider names with their ids
        serviceproviders = database.get_data("serviceprovider", {"is_active": True}, ["id", "service_provider_name"])
        service_provider_list = serviceproviders.to_dict(orient='records')  # List of dicts containing both id and service_provider_name
        service_provider_list = sorted(service_provider_list, key=lambda x: x['service_provider_name'])
        # Initialize dictionaries to store separate data
        service_provider_customers = {}
        service_provider_billing_periods = {}
        
        # Iterate over each service provider
        for service_provider in service_provider_list:
            service_provider_id = service_provider['id']
            service_provider_name = service_provider['service_provider_name']
            
            # Get customer data (including possible duplicates)
            query_customers=f"select distinct customer_name,customer_id from optimization_customer_processing where service_provider='AT&T - Cisco Jasper'"
            customers=database.execute_query(query_customers,True)
            # Create a set to filter unique customer names
            unique_customers = set()  # Using a set to store unique customer names
            customer_list = []
            
            # Loop through each customer and add unique ones to the list
            for row in customers.to_dict(orient='records'):
                customer_name = row["customer_name"]
                if customer_name not in unique_customers:  # Check if the customer is already added
                    unique_customers.add(customer_name)
                    customer_list.append({"customer_id": row["customer_id"], "customer_name": customer_name})
            
            # Get billing period data including start date, end date, and ID
            billing_periods = database.get_data(
                "billing_period",
                {'service_provider': service_provider_name, "is_active": True},
                ["id", "billing_cycle_start_date", "billing_cycle_end_date"],
                order={'billing_cycle_end_date': 'desc'}  # Ordering by `id` in descending order
            )
            
            # Initialize a list to hold the formatted billing periods
            formatted_billing_periods = []
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
        
        # Prepare the response in case of an exception
        response = {
            "flag": False,
            "service_provider_customers": {},
            "service_provider_billing_periods": {}
        }
        return response


##to get the pop up details in the optimize button
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





##start button in optimize button in list view
def start_optimization(data):
    '''
    The start_optimization function initiates an optimization process 
    by sending a POST request with tenant and user-specific details to
    1.0 Controller. It retrieves the tenant ID from the database, sets 
    necessary headers, and sends the request body as JSON, returning the 
    API's response or error details in case of failure.
    '''
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
        tenant_id=str(tenant_id)
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
        message=f"exception is {e}"
        response_data={"flag":False,"message":message}
        # Return the status code and response JSON
        return response_data



def rate_plans_by_customer_count(data,database,common_utils_database):
    '''
    The rate_plans_by_customer_count function retrieves customer rate plan 
    counts by executing stored procedures based on the portal type (portal_id) 
    and other input parameters like ServiceProviderId, BillingPeriodId, and tenant_name.
    It dynamically determines customer IDs (RevCustomerIds or AMOPCustomerIds), connects 
    to a database, and processes data through specific stored procedures. 
    The function handles different portal types and logs errors if exceptions occur.
    '''
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
    '''
    The sim_cards_to_optimize_count function calculates the number of 
    SIM cards eligible for optimization based on the provided filters 
    like ServiceProviderId, BillingPeriodId, tenant_name, and optimization_type.
    It retrieves the relevant data by executing a stored procedure on a remote database,
    either summing up SIM card counts across customers or targeting a specific customer.
    Errors are logged if database connection or execution fails.
    '''
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
    '''
    The get_customer_total_sim_cards_count function calculates the total number 
    of SIM cards associated with a specific rev_customer_id. It retrieves the 
    related customer IDs from the customers table and then counts the 
    corresponding SIM cards in the sim_management_inventory table.
    The function uses dynamic SQL queries and handles exceptions by 
    logging errors if any issues arise during execution.
    '''
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
    

##second point
def get_optimization_row_details(data):
    '''
    The get_optimization_row_details function retrieves and paginates
    optimization data for a given session, fetching high-usage customer
    details and/or error-related records based on the fetch_type.
    It dynamically constructs queries, supports pagination, and handles 
    errors gracefully, returning structured responses with total counts and data.
    '''
    session_id = data.get('session_id')
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    # Pagination parameters
    start = data.get('start', 0)
    end = data.get('end', 10)  # Default to 10 if no end parameter provided
    params = [start, end]
    pages = {
        'start': start,
        'end': end
    }
    total_count = 0
    error_details_count = 0
    high_usage_customers_data = []
    error_details = []
    
    try:
        count_params = [session_id]
        
        # Check if we're fetching high usage customers, error details, or both
        fetch_type = data.get('fetch_type', 'both')  # Default to 'both'
        
        if fetch_type == 'both' or fetch_type == 'high_usage':
            # Count query to get total count of high usage customers
            count_query = f'''
                SELECT COUNT(DISTINCT customer_name)
                FROM public.vw_optimization_instance_summary 
                WHERE  session_id = '{session_id}'
            '''
            
            total_count = database.execute_query(count_query, count_params).iloc[0, 0]
            
            # Query for high usage customers
            query = f'''
                SELECT DISTINCT customer_name,
                               device_count,
                               total_overage_charge_amt + total_rate_charge_amt AS total_charges
                FROM public.vw_optimization_instance_summary 
                WHERE  session_id = '{session_id}'
                ORDER BY total_charges DESC, device_count DESC
                OFFSET %s LIMIT %s
            '''
            
            high_usage_customers_data = database.execute_query(query, params=params).to_dict(orient='records')
            
            # Add total count to the pages dictionary for pagination
            pages['high_usage_total'] = int(total_count)
        
        if fetch_type == 'both' or fetch_type == 'error_details':
            # Count query to get total count of error details
            error_details_count_query = f'''
                SELECT COUNT(*)
                FROM vw_optimization_instance_summary vois
                LEFT JOIN optimization_instance oi ON vois.row_uuid = oi.row_uuid
                LEFT JOIN optimization_smi os ON oi.id = os.instance_id
                JOIN optimization_customer_processing ocp ON vois.optimization_session_id = ocp.session_id
                WHERE vois.session_id = '{session_id}'
                AND os.iccid IS NOT NULL;

            '''
            
            error_details_count = database.execute_query(error_details_count_query,True).iloc[0, 0]
            
            # Error details query
            error_details_query = f'''
                select vois.rev_customer_id,os.iccid,os.msisdn,vois.customer_name,ocp.error_message from vw_optimization_instance_summary vois
                left join optimization_instance oi on vois.row_uuid =oi.row_uuid
                left join optimization_smi os on oi.id =os.instance_id
                JOIN optimization_customer_processing ocp ON vois.optimization_session_id = ocp.session_id where vois.session_id='{session_id}' and iccid is not null 
                OFFSET %s LIMIT %s
            '''
            
            error_details = database.execute_query(error_details_query, params=params).to_dict(orient='records')
            
            # Add total count to the pages dictionary for pagination
            pages['error_details_total'] = int(error_details_count)
        
        # Build response
        response = {
            "flag": True,
            "pages": pages
        }
        
        if fetch_type == 'both' or fetch_type == 'high_usage':
            response["high_usage_customers_data"] = high_usage_customers_data
        if fetch_type == 'both' or fetch_type == 'error_details':
            response["error_details"] = error_details
        
        return response
    
    except Exception as e:
        # Handle any exceptions and provide default empty data
        response = {
            "flag": False,
            "high_usage_customers_data": [],
            "error_details": [],
            "pages": pages,
            "error_message": str(e)
        }
        return response
    




# Initialize S3 client
s3_client = boto3.client('s3')
S3_BUCKET_NAME = 'searchexcelssandbox'  # Replace with your actual bucket name

##Optimization details and push charges reports
def get_optimization_details_reports_data(data):
    '''
    The get_optimization_details_reports_data function generates various optimization-related 
    reports (e.g., client list, device management, charge breakdown) based on a specified report
    type and session ID. It executes queries for each report type, generates corresponding CSV 
    files, zips them, and uploads the zip file to an S3 bucket. Upon successful upload, 
    a download URL is returned, or an error message is logged if any issues occur.
    '''
    report_type = data.get('report_type')
    session_id = data.get('session_id')
    optimization_type = data.get('optimization_type')
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)

    # Define the queries and their respective report types
    queries = {
        'client_list_report': f'''
            SELECT session_id, device_count, total_charge_amount 
            FROM public.vw_optimization_instance_summary
            WHERE session_id = '{session_id}' AND optimization_type = '{optimization_type}'
        ''',
        'session_device_management_reports': f'''
            SELECT session_id, 
                    optimization_type, 
                    service_provider, 
                    run_start_time, 
                    run_end_time, 
                    device_count, 
                    total_cost, 
                    rev_customer_id, 
                    customer_name, 
                    billing_period_start_date, 
                    billing_period_end_date, 
                    run_status
            FROM public.vw_optimization_instance_summary
            WHERE session_id = '{session_id}' AND optimization_type = '{optimization_type}'
        ''',
        'device_assignments_by_customer_report': f'''
            SELECT 
                iccid,
                customer_pool,
                carrier_rate_plan,
                cycle_data_usage_mb,
                communication_plan,
                msisdn,
                uses_proration,
                date_activated,
                EXTRACT(DAY FROM (billing_period_end_date - billing_period_start_date)) AS billing_period_duration,
                days_activated_in_billing_period,
                EXTRACT(DAY FROM (days_in_billing_period)) AS days_in_billing_period,
                device_count,
                average_cost,
                total_cost 
            FROM public.vw_optimization_export_device_assignments
            WHERE session_id = '{session_id}' AND optimization_type = '{optimization_type}'
        ''',
        'all_rate_plans_assignment_report': f'''
            SELECT 
                iccid,
                customer_pool,
                carrier_rate_plan,
                cycle_data_usage_mb,
                communication_plan,
                msisdn,
                uses_proration,
                date_activated,
                EXTRACT(DAY FROM (billing_period_end_date - billing_period_start_date)) AS billing_period_duration,
                days_activated_in_billing_period,
                EXTRACT(DAY FROM (days_in_billing_period)) AS days_in_billing_period,
                device_count,
                average_cost,
                total_cost 
            FROM public.vw_optimization_export_device_assignments
            WHERE session_id = '{session_id}' AND optimization_type = '{optimization_type}'
        ''',
        'customer_charge_breakdown_report': f'''
            SELECT 
                rev_account_number,
                customer_name,
                EXTRACT(DAY FROM (billing_period_end_date - billing_period_start_date)) AS billing_period_duration,
                billing_period_end_date,
                billing_period_start_date,
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
                public.vw_optimization_smi_result_customer_charge_queue
            WHERE session_id = '{session_id}'
        ''',
        'error_breakdown_report': f'''
            SELECT 
                rev_account_number,
                customer_name,
                msisdn,
                iccid,
                error_message
            FROM 
                public.vw_optimization_smi_result_customer_charge_queue
            WHERE session_id = '{session_id}'
        '''
    }

    # Helper function to generate CSV file for each query
    def generate_report_to_csv(report_name, query):
        """Function to execute query and generate CSV data"""
        df = database.execute_query(query, True)
        df.columns = [col.replace('_', ' ').capitalize() for col in df.columns]
        
        # Check if the dataframe is empty
        if df.empty:
            df = pd.DataFrame(columns=df.columns)
        
        # Convert dataframe to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        return (report_name, csv_buffer.getvalue())

    # Initialize a zip buffer to store all CSVs
    zip_buffer = io.BytesIO()

    # Use ThreadPoolExecutor to parallelize query execution, CSV generation, and writing
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        if report_type == 'All Reports':
            for report_name, query in queries.items():
                futures.append(executor.submit(generate_report_to_csv, report_name, query))
        elif report_type in queries:
            futures.append(executor.submit(generate_report_to_csv, report_type, queries[report_type]))

        # Write the reports to a zip file in parallel
        for future in concurrent.futures.as_completed(futures):
            report_name, csv_data = future.result()
            with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
                report_name = ' '.join(word.capitalize() for word in report_name.replace('_', ' ').split())
                zip_file.writestr(f'{report_name}.csv', csv_data)

    # Now upload the zip buffer to S3 directly using multipart upload (if necessary)
    try:
        file_name = "optimization_module_uat/Optimization.zip"
        zip_buffer.seek(0)  # Reset the buffer to the beginning
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_name,
            Body=zip_buffer.getvalue(),
            ContentType='application/zip'
        )

        # Generate the download URL (public URL or pre-signed URL)
        download_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{file_name}"

        # Return response with the download URL
        response = {
            'flag': True,
            'download_url': download_url  # Return the URL where the file is stored in S3
        }

        print(f"File successfully uploaded to S3. Download URL: {download_url}")
        return response

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            'flag': False,
            'download_url': ""  # Return empty download URL in case of error
        }
        return response



##third point
def get_optimization_push_charges_data(data):
    '''
    The get_optimization_push_charges_data function retrieves data for push charges
    based on the provided session ID and pagination parameters (start, end, limit).
    It first counts the total number of rows that match the session ID and then fetches
    the corresponding push charges data. If data exists, it returns the data in a 
    paginated format; otherwise, it returns a message indicating no data was found. 
    The function also handles errors gracefully by logging exceptions and returning an 
    error response with a message.
    '''
    session_id = data.get('session_id')
    tenant_database = data.get('db_name', '')
    # database Connection
    database = DB(tenant_database, **db_config)
    start = data.get('start', 0)
    end = data.get('end', 10)  # Default to 10 if no 'end' parameter provided
    limit = data.get('limit', 10)
    params = [start, end]
    pages = {
        'start': start,
        'end': end
    }
    total_count = 0
    
    try:
        # Query to get the total count of rows
        error_details_count_query = '''
           SELECT COUNT(DISTINCT osmi.iccid)
            FROM 
                vw_optimization_instance_summary vos
            JOIN 
                optimization_instance osi ON vos.row_uuid = osi.row_uuid
            JOIN 
                optimization_smi osmi ON osi.id = osmi.instance_id
            LEFT JOIN 
                optimization_customer_processing ocp ON vos.optimization_session_id = ocp.session_id
            WHERE vos.session_id = %s
        '''
        total_count_result = database.execute_query(error_details_count_query, params=[session_id]).iloc[0, 0]
        pages['total_count'] = int(total_count_result)
        
        # Query to get the data for push charges
        push_charges_dataquery = '''
            SELECT distinct osmi.iccid,
            vos.service_provider_id,
            vos.service_provider,
            vos.rev_customer_id,
            vos.customer_name,
            osmi.msisdn,
            vos.run_status,
            osi.id AS instance_id,
            vos.total_charge_amount,ocp.error_message
            FROM 
            vw_optimization_instance_summary vos
            JOIN 
            optimization_instance osi ON vos.row_uuid = osi.row_uuid
            JOIN 
            optimization_smi osmi ON osi.id = osmi.instance_id
            left join optimization_customer_processing ocp on vos.optimization_session_id=ocp.session_id
            WHERE 
                vos.session_id = %s
            OFFSET %s LIMIT %s
        '''
        
        push_charges_data = database.execute_query(push_charges_dataquery, params=[session_id, start, limit])
        #push_charges_data = push_charges_data.astype(str)
        # Check if data is empty
        if push_charges_data.empty:
            # If no data, create an empty list with column names as keys
            columns = [
                "rev_customer_id", 
                "customer_name", 
                "iccid", 
                "msisdn", 
                "run_status", 
                "instance_id", 
                "error_message", 
                "total_charge_amount"
            ]
            # Return an empty list of data with the column names in the response
            push_charges_data = []
            response = {
                "flag": True,
                "message": "No data found for the given parameters",
                "pages": pages,
                "push_charges_data": push_charges_data,
                "columns": columns
            }
        else:
            # If data exists, convert to list of dictionaries
            push_charges_data = push_charges_data.to_dict(orient='records')
            

            response = {
                "flag": True,
                "pages": pages,
                "push_charges_data": push_charges_data
            }

        # Ensure everything is JSON serializable
        return response

    except Exception as e:
        # Log the exception and return a failure response
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": False,
            "error": str(e),
            "message": "An error occurred while fetching the data"
        }
        # Ensure error response is also JSON serializable
        return response

##update button for rate plans and other updates
def update_optimization_actions_data(data):
    '''
    The update_optimization_actions_data function updates the rate plans and 
    other relevant data for a specific ICCID in the sim_management_inventory table.
    It takes the changed_data (the data to be updated) and the iccid 
    (identifier for the SIM) from the request. 
    The function attempts to update the record and returns a success message 
    if the update is successful. If an exception occurs during the update process, 
    it logs the exception and continues.
    '''
    # Database connection
    tenant_database = data.get('db_name', '')
    database = DB(tenant_database, **db_config)
    changed_data=data.get('changed_data','')
    iccid=str(data.get('iccid'))
    try:
        database.update_dict("sim_management_inventory", changed_data, {"iccid": iccid})
        return {"flag":True,"message":"Updated Successfully"}
    except Exception as e:
        logging.exception(f"Exception is {e}")
        return {"flag":False,"message":"Failed to Update!!!"}
        
    
# Function to fetch both service providers and their rate plans
def get_assign_rate_plan_optimization_dropdown_data(data):
    """
    The get_assign_rate_plan_optimization_dropdown_data function fetches 
    the rate plans for a specified service provider, based on the 
    optimization type (either "Customer" or "Carrier)
    """
    service_provider=data.get('service_provider')
    tenant_database = data.get('db_name', '')
    optimization_type = data.get('optimization_type', 'Customer')
    # database Connection
    database = DB(tenant_database, **db_config)
    try:
        #based on the optimization type the rate plans will be fetched
        if optimization_type=='Customer':
            rate_plan_list_data = database.get_data(
            "customerrateplan", 
            {'service_provider_name': service_provider, "is_active": True},  
            ['rate_plan_code']  # Rate plan column
            )['rate_plan_code'].to_list()
        else:
            rate_plan_list_data = database.get_data(
            "carrier_rate_plan", 
            {'service_provider': service_provider, "is_active": True},  
            ['rate_plan_code']  # Rate plan column
            )['rate_plan_code'].to_list()

        response={"flag":True,"message":"assign rate plan data fetched successfully",
                  "rate_plan_list_data":rate_plan_list_data}
        # Return the data as a JSON response,
        return response

    except Exception as e:
        logging
        response={"flag":True,"message":"unable to fetch the assign rate plan data",
                  "rate_plan_list_data":[]}
        # Return the data as a JSON response,
        return response
    


##submit button push charges screen
def push_charges_submit(data):
    '''
    The push_charges_submit function handles the submission of customer charges 
    for a specific session. It builds a POST request payload with session details 
    (SessionId, selectedInstances, selectedUsageInstances, and pushType) and sends
    it to the configured API endpoint for processing. The function dynamically retrieves 
    the tenant_id and uses it in the headers. It returns the API response, 
    including status code and message, while logging errors in case of failure.
    '''
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

