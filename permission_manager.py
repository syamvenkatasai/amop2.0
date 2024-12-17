"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
# Importing the necessary Libraries
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
import json
import os

logging = Logging(name='permission_manager')
# Dictionary to store database configuration settings retrieved from environment variables.
# db_config = {
#     'host': "amopuatpostgresdb.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#     'port': "5432",
#     'user':"root",
#     'password':"AlgoTeam123"
# }
db_config = {
    'host': os.environ['HOST'],
    'port': os.environ['PORT'],
    'user': os.environ['USER'],
    'password': os.environ['PASSWORD']
}


class PermissionManager:
    # Initializing PermissionManager
    def __init__(self, db_config):
        logging.info('Initializing PermissionManager')
        self.db_config = db_config

    def permission_manager(self,data,validation=False):
    
        if validation==True:
            # Restrictions for the EndPoints
            try:
                Partner = data.get('Partner', '')
                path = data.get('path')
                tenant_database = data.get('db_name','')
                # database Connection
                database_con = DB(tenant_database, **db_config)
                common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
                access_Amop_apis_dataframe = common_utils_database.get_data('amop_apis', {"partner": Partner, "api_name": path,"env":"SandBox"},['api_state'])
                api_state_value = access_Amop_apis_dataframe.iloc[0]['api_state']
                if api_state_value==True:
                    response={"flag": True}
                    return response
                else:
                    message=f"Access Denied: You do not have the required permissions to perform this action. Please contact your administrator."
                    response={"flag": False, "message": message}
                    return response
            except Exception as e:
                logging.info(f"The Exception is {e}")
                
        else:
            # logging.info(f"Request Data: {data}")
            # Extract data from the input dictionary
            tenant = data.get('tenant_name', None)
            role = data.get('role', None)
            username=data.get('user_name', None)
            module=data.get('module', None)
            sub_parent_module=data.get('sub_parent_module', None)
            parent_module=data.get('parent_module', None)
            tenant_database = data.get('db_name','')
            # database Connection
            database = DB(tenant_database, **db_config)
            common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

            try:

                module_features=[]
                try:
                    # Step 2: Retrieve module features based on role
                    role_module_df = database.get_data("role_module", {'role':role},['module_features'])
                    module_features = json.loads(role_module_df["module_features"].to_list()[0])
                    if not parent_module:
                        module_features=module_features[module]
                    elif sub_parent_module:
                        module_features=module_features[parent_module][sub_parent_module][module]
                    else:
                        module_features=module_features[parent_module][module]
                except Exception as e:
                    logging.exception(F'Something is wrong here {e}')
                    pass

                allowed_sites=[]

                allowed_servivceproviders=[]

                return True,module_features,allowed_sites,allowed_servivceproviders
            except Exception as e:
                logging.exception(F"Something went wrong and error is {e}")
                message = "Something went wrong while adding user_name"
                return False,[],[],[]