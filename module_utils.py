"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
# Importing the necessary Libraries
import os
import pandas as pd
# from common_utils.email_trigger import send_email
from common_utils.db_utils import DB
from common_utils.permission_manager import PermissionManager
# from common_utils.authentication_check import validate_access_token
# from common_utils.logging_utils import Logging
import datetime
from time import time
from datetime import datetime
from io import BytesIO
import base64
import json

# logging = Logging(name='Module_api')
# Dictionary to store database configuration settings retrieved from environment variables.
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



def get_headers_mapping(tenant_database,module_list,role,user,tenant_id,sub_parent_module,parent_module):
    print(F"###########################################")
    print(module_list,role,user,tenant_id,sub_parent_module,parent_module)

    database = DB(tenant_database, **db_config)
    common_utils_db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

    ret_out={}
    for module_name in module_list:

        out=database.get_data("field_column_mapping",{"module_name":module_name}).to_dict(orient="records")

        pop_up=[]
        general_fields=[]
        table_fileds={}
        for data in out:
            if data["pop_col"]:
                pop_up.append(data)
            elif data["table_col"]:
                table_fileds.update({data["db_column_name"]:[data["display_name"],data["table_header_order"]]})
            else:
                general_fields.append(data)

        headers={}
        headers['general_fields']=general_fields
        headers['pop_up']=pop_up
        headers['header_map']=table_fileds

        try:
            final_features=[]
            if role.lower()== 'super admin':
                all_features=common_utils_db.get_data("module_features",{"module":module_name},['features'])['features'].to_list()
                if all_features:
                    final_features=json.loads(all_features[0])
            else:
                role_features=database.get_data("role_module",{"role":role},['module_features'])['module_features'].to_list()
                if role_features:
                    role_features=json.loads(role_features[0])

                user_features=database.get_data("user_module_tenant_mapping",{"user":user,"tenant_id":tenant_id},['module_features'])['module_features'].to_list()
                if user_features:
                    user_features=json.loads(user_features[0])

                if user_features:
                    print(user_features,'user_features')
                    if not parent_module:
                        final_features=user_features[module_name]
                    elif sub_parent_module:
                        final_features=user_features[parent_module][sub_parent_module][module_name]
                    else:
                        final_features=user_features[parent_module][module_name]

                else:
                    print(role_features,'role_features')
                    if not parent_module:
                        final_features=role_features[module_name]
                    elif sub_parent_module:
                        final_features=role_features[parent_module][sub_parent_module][module_name]
                    else:
                        final_features=role_features[parent_module][module_name]

        except Exception as e:
            print(f"there is some error {e}")
            pass
        print(final_features)
        headers['module_features']=final_features
        ret_out[module_name]=headers

    return ret_out



def get_module_data(data,flag=False,):
    '''
    Retrieves module data for a specified module by checking user and tenant to get the features by querying the database for column mappings and view names.
    It constructs and executes a SQL query to fetch data from the appropriate view, handles errors, and logs relevant information.
    '''

    print(f"Request Data: {data}")
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
        print(f"got exception in the restriction")
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    username = data.get('username', None)
    tenant_name = data.get('tenant_name', None)
    role = data.get('role_name', None)
    module_name=data.get('module_name', None)
    session_id=data.get('session_id', None)
    sub_parent_module=data.get('sub_parent_module', None)
    parent_module=data.get('parent_module', None)
    tenant_database = data.get('db_name', '')
    ##database connection
    db = DB(database="common_utils", host="amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com", user="root", password="AlgoTeam123", port="5432")
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        print("Failed to start ram and time calc")
        pass
    # database Connection
    database = DB('tenant_database', **db_config)
    try:
        tenant_id=db.get_data("tenant",{'tenant_name':tenant_name},['id'])['id'].to_list()[0]

        data_list={}
        pages={}
        if not flag:
            # Create an instance of PermissionManager and call permission manager method
            pm = PermissionManager(db_config)
            # # Retrieving the features for the user
            flag_, features, allowed_sites, allowed_servivceproviders = pm.permission_manager(
                data)
            if not flag_:
                message = "Access Denied"
                return {"flag": False, "message": message}

        # query to find the column mapping for the module
        module_mappings_df = db.get_data('module_column_mappings', {
                                               'module_name': module_name}, ['columns_mapped', 'master_data_flag','tables_mapped','view_name','condition','drop_down_col','main_update_table','order_by','tenant_filter','combine'])
        columns_data = module_mappings_df['columns_mapped'].to_list()[0]
        main_update_table=module_mappings_df['main_update_table'].to_list()[0]
        tenant_filter=module_mappings_df['tenant_filter'].to_list()[0]
        try:
            columns_data=json.loads(columns_data)
        except:
            pass
        master_data_flag = module_mappings_df['master_data_flag'].to_list()[0]
        tables_list = module_mappings_df['tables_mapped'].to_list()[0]
        try:
            tables_list=json.loads(tables_list)
        except:
            pass
        view_name = module_mappings_df['view_name'].to_list()[0]
        condition = module_mappings_df['condition'].to_list()[0]
        try:
            condition=json.loads(condition)
        except:
            condition={}
        drop_down_col = module_mappings_df['drop_down_col'].to_list()[0]
        try:
            drop_down_col=json.loads(drop_down_col)
        except:
            pass
        order_by = module_mappings_df['order_by'].to_list()[0]
        try:
            order_by=json.loads(order_by)
        except:
            pass
        combine_all = module_mappings_df['combine'].to_list()[0]
        try:
            combine_all=json.loads(combine_all)
        except:
            pass

        if tables_list:

            for table in tables_list:

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

                if columns_data and table in columns_data and condition:
                    if table in condition:
                        data_dataframe=database.get_data(table,condition[table],columns_data[table],order,combine)
                    else:
                        data_dataframe=database.get_data(table,{},columns_data[table],order,combine)
                elif columns_data and table in columns_data:
                    data_dataframe=database.get_data(table,{},columns_data[table],order,combine)
                elif condition and table in condition and columns_data:
                    if table in columns_data:
                        data_dataframe=database.get_data(table,condition[table],columns_data[table],order,combine)
                    else:
                        data_dataframe=database.get_data(table,condition[table],None,order,combine)
                else:
                    data_dataframe=database.get_data(table,{},None,order,combine)
                if drop_down_col and columns_data and table in drop_down_col and table in columns_data:
                    for col in columns_data[table]:
                        data_list[col]=data_dataframe[col].to_list()
                else:
                    df=data_dataframe.to_dict(orient='records')
                    print('data after the mod_pages',data)
                    if "mod_pages" in data and table == main_update_table:
                        pages['start']=data["mod_pages"]["start"]
                        pages['end']=data["mod_pages"]["end"]
                        pages['total']=len(df)
                        if len(df)<pages['end']:
                            data_list[table]=df[pages['start']:len(df)-1]
                        else:
                            data_list[table]=df[pages['start']:pages['end']]
                    else:
                        data_list[table]=df


        message = "Data fetched successfully"
        print(data_list ,'data_list')


        #convert all time stamps into str
        new_data={}
        for table,values in data_list.items():
            temp_list=[]
            for item in values:
                if type(item)==str:
                    print(values,'new_data')
                    temp_list=values
                    break
                temp_dic={}
                for key,value in item.items():
                    if key == 'modified_date':
                       value=str(value)
                       value=value.split('.')[0]
                    temp_dic[key]=str(value)
                temp_list.append(temp_dic)
            new_data[table]=temp_list
        print(new_data ,'new_data')

        # Response including pagination metadata
        if not flag:
            #calling get header to get headers mapping
            headers_map=get_headers_mapping(tenant_database,[module_name],role,username,tenant_id,sub_parent_module,parent_module)
            response = {"flag": True,"message":message, "data": new_data, "pages":pages,"features": features ,"headers_map":headers_map}
            # End time calculation
            end_time = time()
            time_consumed = end_time - start_time
            try:
                audit_data_user_actions = {"service_name": 'Module Management',"created_date": date_started,
                "created_by": username,
                    "status": str(response['flag']),
                    "time_consumed_secs": time_consumed,
                    "session_id": session_id,
                    "tenant_name": Partner,
                    "comments": message,
                    "module_name": "get_module_data","request_received_at":request_received_at
                }
                db.update_audit(audit_data_user_actions, 'audit_user_actions')
            except:
                pass
            return response
        else:
            return new_data,pages


    except Exception as e:
        print(F"Something went wrong and error is {e}")
        message = "Something went wrong fetching module data"
        response={"flag": False, "message": message}
        error_type = str(type(e).__name__)
        try:
            # Log error to database
            error_data = {"service_name": 'get_module_data',"created_date": date_started,"error_message": message,"error_type": error_type,"users": username,"session_id": session_id,"tenant_name": Partner,"comments": "","module_name": "Module Managament","request_received_at":request_received_at}
            db.log_error_to_db(error_data, 'error_log_table')
        except:
            pass
        if not flag:
            return response
        else:
            return []