"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
# Importing the necessary Libraries
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
import os
import requests
import datetime
from datetime import datetime
from time import time
import json
import random
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import numpy as np
from urllib.parse import urlparse, parse_qs
import uuid


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
logging = Logging(name="user_authentication")


def get_request_id():
    """
    Retrieves the request ID from an authorization URL by making a GET request with specified parameters.

    Args:
        data (dict): A dictionary containing request-specific data (not used in this function).

    Returns:
        str: The extracted request ID from the URL query parameters, or None if not found.
    """
    try:
        # Retrieve the URL for the authorization endpoint from an environment variable
        url = os.getenv("REQUEST_ID_URL"," ")
        # Retrieve the client ID and redirect URI from environment variables
        client_id = os.getenv("CLIENT_ID"," ")
        redirect_uri = os.getenv("REDIRECT_URL"," ")
        # Define the response type and scope for the authorization request
        response_type = "code"
        scope = "openid email profile offline_access urn:zitadel:iam:user:metadata"
        # Define request headers
        headers = {
            "x-zitadel-login-client": "278680282679256500",
            "Cookie": "_Host-zitadel.useragent=MTcyMzA5OTU3N...",
            "Cache-Control": "no-cache",
            "User-Agent": "PostmanRuntime/7.41.0",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        # Define query parameters for the GET request
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": response_type,
            "scope": scope,
        }
        # Send a GET request
        response = requests.get(url, params=params, headers=headers)
        # Parse the URL and extract the request ID
        parsed_url = urlparse(response.url)
        query_params = parse_qs(parsed_url.query)

        # Extract the request ID from the query parameters
        request_id = query_params.get('authRequest', [None])[0]

        return request_id
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Something went wrong fetching request id"
        return None

def create_user(username, given_name, family_name, display_name, email, phone, password):
    '''
    Creates a new user in the system by sending a POST request to the specified URL with user details.
    Args:
        username (str): The username of the new user.
        given_name (str): The given name of the new user.
        family_name (str): The family name of the new user.
        display_name (str): The display name of the new user.
        email (str): The email address of the new user.
        phone (str): The phone number of the new user.
        password (str): The password for the new user.

    Returns:
        dict: The response from the server in JSON format, or an error message if an exception occurs.
    '''
    try:
        # Retrieve the URL for creating a new user and Authentication token from an environment variable
        url =  os.getenv("CREATE_USER_URL"," ")
        auth_token = os.getenv("AUTH_TOKEN"," ")
        # Define the request payload with user details
        payload = {
            "username": username,
            "profile": {
                "givenName": given_name,
                "familyName": family_name,
                "displayName": display_name,
                "prefferedLanguage": "en",
                "gender": "GENDER_UNSPECIFIED"
            },
            "email": {
                "email": email,
                "isVerified": True
            },
            "phone": {
                "phone": phone,
                "isVerified": True
            },
            "password": {
                "password": password,
                "changeRequired": False
            }
        }

        ## Define the headers for the POST request
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "PostmanRuntime/7.41.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }

        # Send a POST request to create the new use
        response = requests.post(url, json=payload, headers=headers)
        # Return the response JSON
        try:
            return response.json()
        except ValueError:
            # If response is not JSON, logging.info the raw text
            logging.warning(f"Response Text:{response.text}")
            return None
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Something went wrong creating new user in Zitadel"
        response={"flag": False, "message": message}
        return response
    

def authenticate_user(username, password):
    '''
    Authenticates a user by sending a POST request to the authentication endpoint with the username and password.

    Args:
        username (str): The username of the user to authenticate.
        password (str): The password of the user to authenticate.

    Returns:
        tuple: A tuple containing the session ID and session token if successful, or a dictionary with an error message if an exception occurs.
    '''
    try:
        # Retrieve the URL for the authentication endpoint from an environment variable
        url = os.getenv("AUTHENTICATE_USER_URL"," ")
        # Retrieve the authorization token from an environment variable
        auth_token = os.getenv("AUTH_TOKEN"," ")
        # Define the request body with the provided username and password
        data = {
            "checks": {
                "user": {
                    "loginName": username
                },
                "password": {
                    "password": password
                }
            }
        }

        # Define the headers with the provided authorization token
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "PostmanRuntime/7.41.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }

        # Send a POST request
        response = requests.post(url, json=data, headers=headers)
        # Extract sessionId and sessionToken from the response
        response_data = response.json()
        session_id = response_data.get('sessionId')
        session_token = response_data.get('sessionToken')
        return session_id, session_token
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Something went wrong fetching sessionid and sessiontoken using  Zitadel"
        response={"flag": False, "message": message}
        return response




def send_auth_request(auth_request_id, session_id, session_token):
    '''
    Sends an authorization request to the specified endpoint and extracts the authorization code from the response.

    Args:
        auth_request_id (str): The ID for the authentication request.
        session_id (str): The session ID obtained from the authentication process.
        session_token (str): The session token obtained from the authentication process.

    Returns:
        str: The authorization code extracted from the callback URL, or None if an error occurs
    '''
    try:
        # Base URL with the placeholder for authRequestId
        # base_url = os.getenv("SEND_AUTH_REQUEST_URL"," ")
        # Get the base URL (with /v2beta/oidc/auth_requests/) from the environment variable
        base_url_without_authrequestid = os.getenv("SEND_AUTH_REQUEST_URL", " ").rstrip("/")

        # # Set the authRequestId dynamically
        # auth_request_id = "{authRequestId}"

        # Combine the base URL with the authRequestId
        # base_url = f"{base_url_without_authrequestid}/{auth_request_id}"
        base_url = f"{base_url_without_authrequestid}/{auth_request_id}"
        print(base_url, "base url ///////////////////////////////")
        auth_token = os.getenv("AUTH_TOKEN"," ")
        # Construct the full URL by formatting the base URL with the auth_request_id
        url = base_url.format(authRequestId=auth_request_id)

        # Request body
        payload = {
            "session": {
                "sessionId": session_id,
                "sessionToken": session_token
            }
        }

        # Define the headers for the POST request, including the authorization token
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "PostmanRuntime/7.41.0",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }

        # Send the POST request to the constructed URL with the headers and payload
        response = requests.post(url, headers=headers, json=payload)
        try:
            # Attempt to parse the response as JSON
            response_data = response.json()
            # logging.info(f"Response JSON:{response_data}")

            # Extract the callbackUrl from the response
            callback_url = response_data.get('callbackUrl', '')

            # Parse the authorization code from the callbackUrl
            parsed_url = urlparse(callback_url)
            query_params = parse_qs(parsed_url.query)
            auth_code = query_params.get('code', [None])[0]

            return auth_code

        except Exception as e:
            # If response is not JSON, logging.info the raw text
            logging.warning(f"Response Text: {response.text}")
            return None
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Something went wrong fetching auth_code using  Zitadel"
        response={"flag": False, "message": message}
        return response



def get_access_token(auth_code):
    '''Retrieves an access token using the authorization code and request ID.

    Args:
        auth_code (str): The authorization code received after user authentication.
        auth_request_id (str): The ID of the authentication request (not used in this function but included for context).

    Returns:
        str: The access token if the request is successful, or None if an error occurs.
    '''
    try:
        # Define the URL
        url = os.getenv("ACCESS_TOKEN_URL"," ")
        client_id = os.getenv("CLIENT_ID"," ")
        client_secret = os.getenv("CLIENT_SECRET"," ")
        redirect_uri = os.getenv("REDIRECT_URL"," ")
        auth_token = os.getenv("AUTH_TOKEN"," ")

        # Define the form data for x-www-form-urlencoded
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
            "code": auth_code
        }

        # Define the headers (removing Accept-Encoding)
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "PostmanRuntime/7.41.0",
            "Connection": "keep-alive"
        }

        # Send a POST request
        response = requests.post(url, data=data, headers=headers)

        # Check if the response is in JSON format
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            try:
                response_data = response.json()
                access_token = response_data.get('access_token')
                return access_token
            except ValueError:
                # If JSON decoding fails, logging.info the raw text
                logging.info(f"Response Text:  {response.text}")
                return None
        else:
            # Handle non-JSON or binary responses
            logging.warning(f"Response Content : {response.content}")
            return None
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Something went wrong fetching auth_code using  Zitadel"
        response={"flag": False, "message": message}
        return response


def update_user_in_zitadel(user_id, email, phone, password):

    zitadel_domain = os.getenv("zitadel_domain"," ")
    access_token = os.getenv("AUTH_TOKEN"," ")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Update Profile
    # profile_url = f"https://{zitadel_domain}/management/v1/users/{user_id}"
    # profile_payload = {
    #     "profile": profile_data,
    #     "email": {"email": email, "isVerified": False},
    #     "phone": {"phone": phone, "isVerified": False}
    # }
    # profile_response = requests.put(profile_url, headers=headers, json=profile_payload)

    # if profile_response.status_code != 200:
    #     return {"error": "Failed to update profile", "details": profile_response.json()}

    # Update Password
    password_url = f"https://{zitadel_domain}/management/v1/users/{user_id}/password"
    password_payload = {"password": password, "changeRequired": False}
    password_response = requests.post(password_url, headers=headers, json=password_payload)
    print(password_response)
    
    if password_response.status_code != 200:
        return False

    return True



def process_service_account(data):

    try:
        database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

        client_id=data.get('client_id')
        client_secret=data.get('client_secret')
        role=data.get('role')
        tenant=data.get('tenant')
        email=data.get('email')
        phone=data.get('phone')
        tax_profile=data.get('tax_profile')
        flag=data.get('flag')

        if flag=='update':
            user_id = database.get_data("service_accounts", {"client_id": client_id},['user_id'])['user_id'].to_list()[0]
            response_data = update_user_in_zitadel(user_id, email, phone, client_secret)
            
            logging.info(f"reponse from zitadel fro upadting is {response_data}")
            if response_data:
                data={
                        'client_id': client_id,
                        "client_secret": client_secret,
                        "role": role,
                        "tenant": tenant,
                        "email": email,
                        "phone": phone,
                        "tax_profile": tax_profile
                    }
                database.update_dict("service_accounts",data,{"user_id":user_id})

                logging.info("User Updated successfully")
            else:
                raise ValueError('error in Updated account')
            message='Updated successfully'
        else:
            user_id = database.get_data("service_accounts", {"client_id": client_id},['user_id'])['user_id'].to_list()[0]
            data={
                "is_active": "False"
            }
            database.update_dict("service_accounts",data,{"user_id":user_id})
            response_data = update_user_in_zitadel(user_id, email, phone, client_secret)
            message='Deleted successfully'

        response={"flag": True, "message": message}
        return response
       
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "error while processing service account details"
        response={"flag": False, "message": message}
        return response
    


def get_service_account(data):

    try:
        print(f"data is {data}")
        limit=data.get("limit",10)
        start=data.get("offset",0)
        end=start+limit

        database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        response_data = database.get_data("service_accounts",{'is_active':"True"},mod_pages={'start':start,'end':end}).to_dict(orient='records')
        total=len(database.get_data("service_accounts",columns=['user_id'])['user_id'].to_list())

        return {"flag": True, "results": response_data,"totalResults":total}
       
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "error getting service account details"
        response={"flag": False, "message": message}
        return response



def create_service_account(data):

    try:
        database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

        client_id=data.get('client_id')
        client_secret=data.get('client_secret')
        role=data.get('role')
        tenant=data.get('tenant')
        email=data.get('email')
        phone=data.get('phone')
        tax_profile=data.get('tax_profile')

        response_data = create_user(client_id, client_id, client_id, client_id, email, phone, client_secret)
        # Update the database to mark the user as migrated and store the user ID
        user_id = response_data.get('userId')
        logging.info(f"reponse from zitadel fro user creation is {response_data}")
        if user_id:
            data={
                    'client_id': client_id,
                    "client_secret": client_secret,
                    "role": role,
                    "tenant": tenant,
                    "email": email,
                    "phone": phone,
                    "tax_profile": tax_profile,
                    "user_id":user_id,
                    "is_active": "True"
                }
            database.insert_dict(data,"service_accounts")
            message="User created successfully"
            logging.info("User created successfully")
        else:
            message="error in creating account"
            raise ValueError('error in creating account')
        
        return {"flag": True, "message": message}
       
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Something went wrong fwhile creating service account"
        response={"flag": False, "message": message}
        return response
    


def get_auth_token(data):

    try:
        database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

        client_id=data.get('client_id')
        client_secret=data.get('client_secret')

        user_info = database.get_data("service_accounts", {"client_id": client_id},['client_secret'])
        db_client_secret=user_info["client_secret"].to_list()
        logging.info(f"db_client_secret is {db_client_secret}")
        if db_client_secret:
            db_client_secret=db_client_secret[0]
        else:
            return {"flag":False,"msg":"Not registered"}
        if db_client_secret!=client_secret:
            return {"flag":False,"msg":"invalid secret code"}

        auth_request_id = get_request_id()
        logging.info(f"auth_request_id is {auth_request_id}")

        session_id, session_token = authenticate_user(client_id, client_secret)
        logging.info(f"session_id is {session_id}")
        logging.info(f"session_token is {session_token}")

        auth_code = send_auth_request(auth_request_id, session_id, session_token)
        logging.info(f"auth_code is {auth_code}")

        access_token = get_access_token(auth_code)
        logging.info(f"access_token is {access_token}")

        return {"flag": True, "access_token": access_token}
       
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Something went wrong while genrating access token"
        response={"flag": False, "message": message}
        return response



def zitadel_check(data, user_name):
    '''
    Description:
    Handles the process of checking and creating a user in Zitadel and retrieving an access token.

    Args:
        data (dict): Contains user credentials and other relevant information.
        user_name (str): The username of the user.
        auth_code (str, optional): The authorization code, if available. Defaults to None.

    Returns:
        str: The access token if the process is successful, or a dictionary with error details if an exception occurs.
    '''
    try:
        # Database connection
        database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        
        # Extract user credentials from the request data
        username = user_name
        password = data.get('password', '')
        
        # Get the authorization request ID
        auth_request_id = get_request_id()
        
        # Fetch user information from the database
        user_info = database.get_data("users", {"username": username, "is_active": 'true'})
        
        # Extract user details from the fetched data
        given_name = user_info.at[0, 'first_name']
        family_name = user_info.at[0, 'last_name']
        display_name = username
        phone = user_info.at[0, 'phone']
        email = user_info.at[0, 'email']
        
        # Check if the user is migrated
        migrated = user_info.at[0, 'migrated']
        logging.info(f"migrated is {migrated}")
        
        if not migrated:
            response_data = create_user(username, given_name, family_name, display_name, email, phone, password)
            # Update the database to mark the user as migrated and store the user ID
            user_id = response_data.get('userId')
            database.update_dict("users", {"migrated": True, "user_id": user_id}, {"username": username})
            logging.info("User created successfully")
        
        # Authenticate the user and get session ID and token
        session_id, session_token = authenticate_user(username, password)
        
        # Conditionally send the authentication request and get the authorization code if not provided
        auth_code = send_auth_request(auth_request_id, session_id, session_token)
        
        # Exchange the authorization code for an access token
        access_token = get_access_token(auth_code)
        
        # Update the database with the access token
        database.update_dict("users", {"access_token": access_token}, {"username": username})
        
        return access_token
    except Exception as e:
        logging.exception(f"Exception is {e}")
        message = "Zitadel is not responding"
        response = {"flag": False, "message": message}
        return response


def generate_token(data):
    ##used to get the random authenticaton number
    random_number = random.randint(100000, 999999)
    logging.info("Random Numbers:", random_number)
    return random_number

def reset_password_email(data):
    # logging.info(f"Request Data: {data}")
    request_received_at = data.get('request_received_at', None)
    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        logging.warning("Failed to start ram and time calc")
        pass
    ##the function endpoint is used to reset the password
    username=data.get("username",'')
    template_name=data.get("template_name",'Forgot password')
    session_id=data.get("session_id",'')
    Partner=data.get("Partner",'')
    role=data.get("role",'')
    common_utils_database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
    token= generate_token(data)
    update_data={f"temp_password is {token}"}
    # logging.info(F"update_data is {update_data}")
    #db.update_dict("users", update=update_data, where={'id': unique_id}, logic_operator='AND')
    common_utils_database.update_dict("users",update_data,{"username":username})
    to_email=common_utils_database.get_data("users",{"username":username},['email'])['email'].to_list()[0]
    # Call send_email and assign the result to 'result'
    result = send_email(template_name, username=username, user_mail=to_email)
    # Check the result and handle accordingly
    if isinstance(result, dict) and result.get("flag") is False:
        logging.info(result)
        to_emails = result.get('to_emails')
        cc_emails = result.get('cc_emails')
        subject = result.get('subject')
        body = result.get('body')
        from_email = result.get('from_email')
        partner_name = result.get('partner_name')
        try:
            ##email audit
            email_audit_data = {"template_name": template_name,"email_type": 'Application',
            "partner_name": "",
                "email_status": 'failure',
                "from_email": from_email,
                "to_email": to_email,
                "cc_email": cc_emails,
                
                "subject": subject,"body":body,"role":role,"comments": 'Forgot Password Auditing',"parents_module_name":"User authentication","child_module_name":"","sub_module_name":"","template_type":"User Authentication"
                    
            } 
            common_utils_database.update_audit(email_audit_data, 'email_audit') 
        except:
            pass
    else:
        # Continue with other logic if needed
        to_emails, cc_emails, subject, body, from_email, partner_name = result
        #to_emails,cc_emails,subject,body,from_email,partner_name=send_email(template_name,username=username,user_mail=to_email)
        common_utils_database.update_dict("email_templates",{"last_email_triggered_at":request_received_at},{"template_name":template_name})
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
        try:
            ##email audit
            email_audit_data = {"template_name": template_name,"email_type": 'Application',
            "partner_name": partner_name,
                "email_status": 'success',
                "from_email": from_email,
                "to_email": to_emails,
                "cc_email": cc_emails,
                "action": "Email triggered",
                "subject": subject,"body":body,"role":role,"comments": 'Forgot Password Auditing',
                "parents_module_name":parents_module_name,"child_module_name":child_module_name,"sub_module_name":sub_module_name,
                "template_type":"User Authentication"
                    
            } 
            common_utils_database.update_audit(email_audit_data, 'email_audit') 
        except:
            pass
    message=f"mail sent sucessfully"
    response={"flag":True,"message":message}
    try:
        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        ## Auditing
        audit_data = {
            "service_name": 'User_authentication',
            "created_date": date_started,
            "created_by": username,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": session_id,
            "tenant_name": Partner,
            "comments": message,
            "module_name": "user_login","request_received_at":request_received_at
        }
        common_utils_database.update_audit(audit_data, 'audit_user_actions')
        
        return response
    except Exception as e:
        logging.exception(f"Error sending email: {e}")
        message=f"mail not sent"
        return {"flag":False,"message":message}



def token_check(username,reset_token,db):
    try:
        # Connect to the database
        database = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)

        if not username or not reset_token:
            raise ValueError("Username, and reset token must be provided.")

        # Fetch the reset token from the database
        db_reset_token = db.get_data("users", {"username": username}, ['temp_password'])["temp_password"].to_list()

        if not db_reset_token:
            raise ValueError("User not found or reset token is missing.")

        db_reset_token = db_reset_token[0]  # Assuming there's only one token per user

        # Check if the provided reset token matches the one in the database
        if str(reset_token) == str(db_reset_token):
            # Return success message
            return True
        else:
            # Return invalid token message
            return False

    except Exception as e:
        # Handle any other exceptions
        return False
    


def password_reset(data):
    try:
        # Extract parameters from data
        username = data.get("username")
        new_password = data.get("New_password")

        if not username or not new_password:
            raise ValueError("Username, new password must be provided.")

        db = DB("common_utils", **db_config)
        ##changing the password in the Zitadel to
        try:
            user_info = db.get_data("users", {"username": username,"is_active":'true'})
            currentPassword=user_info['password'].to_list()[0]
            user_id=user_info['user_id'].to_list()[0]

            password_Reset_zitadel(user_id,new_password,currentPassword)

            # Update the user's password
            db.update_dict("users",{"password": new_password},{"username":username})
        except:
            pass
        data={}
        data['user_name'] = username
        data['password'] = new_password

        response=login_using_database(data)

        # Return success message
        return response

    except Exception as e:
        # Handle any other exceptions
        return {"flag": False, "message": "An unexpected error occurred: " + str(e)}


def format_tenant_data(tenants,role_tenant_name,role=''):
    # Create a dictionary to hold the hierarchy
    tenant_hierarchy = []

    # First, add all parent tenants with their details
    for tenant in tenants:
        if tenant["parent_tenant_id"] is None and (tenant["tenant_name"] in role_tenant_name or role == 'Super Admin'): 
            tenant_hierarchy.append({
                "id": tenant["id"],
                "name": tenant["tenant_name"],
                "subPartners": []
            })

    # Then, add all sub-tenants to their respective parents
    for tenant in tenants:
        if tenant["parent_tenant_id"] is not None and (tenant["tenant_name"] in role_tenant_name or role == 'Super Admin'):
            parent_id = tenant["parent_tenant_id"]
            
            for parent in tenant_hierarchy:
                if parent["id"] == parent_id:
                    parent["subPartners"].append({
                        "id": tenant["id"],
                        "name": tenant["tenant_name"]
                    })
    return tenant_hierarchy


def login_using_database(data):
    '''
    The login function authenticates a user by validating credentials against a database,
    migrating the user to Zitadel if needed, and generating a session ID, OTP, and access token for successful logins.
    If any step fails, it returns an error message.
    '''
    request_received_at = data.get('request_received_at', None)

    # logging.info(f"Request Data: {data}")
    ##database connection
    db = DB("common_utils", **db_config)
    common_utils_database = DB('common_utils', **db_config)
    input_user_name = data.get('user_name', None)
    params=[input_user_name]
    try:
        username_query = f"SELECT username FROM users WHERE LOWER(username) = LOWER(%s) AND is_active = TRUE;"
        user_name = db.execute_query(username_query,params=params)['username'].to_list()[0]
    except Exception as e:
        response={"flag":True,"message":"User is Inactive"}

    try:
        # Start time  and date calculation
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f'Start time: {date_started}')
    except:
        date_started=0
        start_time=0
        logging.warning("Failed to start ram and time calc")
        pass

    try:
        # session_id = data.get('session_id', None)
        password = data.get('password', None)
        # access_token=''
        data['user_name'] = data['user_name'].lower() 
        access_token=zitadel_check(data,user_name)
        #Check if access_token was successfully retrieved
        if isinstance(access_token, dict) and not access_token.get("flag", True):
            # Return a new exception message if access_token retrieval failed
            return {"flag": False, "message": "Failed to retrieve access token from Zitadel."}
        else:
            # Proceed with the code if access_token is valid
            # Your code logic here that depends on access_token
            pass
        
        login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Generate a new session ID
        session_id = str(uuid.uuid4()) 
        
        
        # Check if the user already has an active session
        try:
            session_record = db.get_data("live_sessions", {'username':user_name},['username','session_id'])['username'].to_list()[0]
        except:
            session_record=None

        # Check if session_record is either a string or a DataFrame
        if isinstance(session_record, pd.DataFrame) and not session_record.empty:
            # If session is active, update the record with new details
            session_data = {
                "access_token": access_token,
                "login": login_time,
                "session_id": session_id,
                "last_request": login_time
            }
            db.update_dict("live_sessions", session_data, {"username": user_name, "status": "active"})
            logging.info("Session updated.")
        elif isinstance(session_record, str) and session_record == user_name:
            # If session is found but returned as a string (i.e., user already exists)
            session_data = {
                "username": user_name,
                "access_token": access_token,
                "session_id": session_id,  # Insert session ID
                "status": "active",
                "login": login_time,
                "last_request": login_time
            }
            db.update_dict('live_sessions', session_data)
            logging.info("Session updated for user found as string.")
        else:
            # If no active session, insert a new record
            logging.info(f"No active session found for {user_name}. Inserting new session record.")
            session_data = {
                "username": user_name,
                "access_token": access_token,
                "status": "active",
                "session_id": session_id,
                "login": login_time,
                "last_request": login_time
            }
            db.insert_dict(session_data, 'live_sessions')

        db.update_dict("users",{"last_login":request_received_at},{"username":user_name})
        
        tenant_names = common_utils_database.get_data("tenant",{"is_active":"true"},['tenant_name','db_name']).to_dict(orient='records')
        db_tenant_names = {tenant['tenant_name']: tenant['db_name'] for tenant in tenant_names}
        if password:
            flag=token_check(user_name,password,db)
            if flag:
                return {"flag": True, "message": "Token is Valid."}

        if not user_name:
            message = "Username not present in request data."
            logging.info(f"message :{message}")
        else:
            # Fetch user information from the database
            user_info = db.get_data("users", {"username": user_name,"is_active":'true'})
            if not user_info.empty:
                role_name=user_info['role'].to_list()[0]
                role=user_info.iloc[0]['role']
                email=user_info.iloc[0]['email']
                # Check if user_info is empty (no user found)
                if user_info.empty:
                    message = "Invalid user credentials."
                    logging.info(f"message : {message}")
                    response={"flag": False, "message": message, "tenant_names": [],"role":''}
                else:
                    # logging.info(f"password is {(user_info.iloc[0]['password'])}")
                    # Check username and password validity
                    if user_info.iloc[0]['username'] != user_name or str(user_info.iloc[0]['password']) != str(password):
                        message = "Invalid user credentials."
                        logging.info(f"message : {message}")
                        response={"flag": False, "message": message, "tenant_names": [],"role":''}
                    else:
                        #getting tenant_ids for all the users
                        if role_name=='Super Admin':
                            tenant_names_all=common_utils_database.get_data("tenant",{"is_active":"true"}).to_dict(orient='records')
                            final_tenants=format_tenant_data(tenant_names_all,[],role_name)
                        else:
                            tenant_ids=db.get_data("user_module_tenant_mapping",{"user_name":user_name},['tenant_id'])['tenant_id'].to_list()
                            role_tenant_ids=db.get_data("roles",{"role_name":role_name,"is_active":True},['tenant_id'])['tenant_id'].to_list()
                            role_tenant_name=common_utils_database.get_data("tenant", {"id": role_tenant_ids},["tenant_name"])['tenant_name'].to_list()
                            # logging.info(f"tenant_id : {tenant_ids} and role_tenant_name are {role_tenant_name}")
                            message = "User authenticated successfully."

                            tenant_all_df = common_utils_database.get_data("tenant", {"id": tenant_ids,"is_active":"true"}).to_dict(orient='records')
                            final_tenants=format_tenant_data(tenant_all_df,role_tenant_name,'')

                        message = "User login is successful."
                        # final_tenants = list(set(final_tenat))
                        # final_tenants.sort()  # Sorts the list in place
                        # logging.info(f"final_tenants : {final_tenants}")
                        response = {"flag": True, "message": message,"db_tenant_names": db_tenant_names, "tenant_names":final_tenants ,"role":role,"access_token":access_token,"email":email,"user_name":user_name,"session_id":session_id}
            else:
                message = "Invalid user credentials."
                logging.info(f"message : {message}")
                response={"flag": False, "message": message, "tenant_names": [],"role":''}

        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time
        try:
            # Example usage
            ## Auditing
            audit_data_user_actions = {
                "service_name": 'User_authentication',
                "created_date": date_started,
                "created_by": user_name,
                "status": str(response['flag']),
                "time_consumed_secs": time_consumed,
                "session_id": session_id,
                "tenant_name": "",
                "comments": message,
                "module_name": "user_login","request_received_at":request_received_at
            }
            db.update_audit(audit_data_user_actions, 'audit_user_actions')
        except:
            pass

        return response

    except Exception as e:
        error_type = type(e).__name__
        # Error handling and logging
        logging.exception(f"Something went wrong and error is {e}")
        message = f"Invalid User Credentials"
        try:
            # Log error to database
            error_data = {
                "service_name": 'User_authentication',
                "created_date": date_started,
                "error_message": message,
                "error_type": error_type,
                "users": user_name,
                "session_id": session_id,
                "tenant_name": "",
                "comments": message,
                "module_name": "User_login","request_received_at":request_received_at
            }
            db.log_error_to_db(error_data, 'error_log_table')
        except:
            pass

        return {"flag": False, "message": message}
    


def impersonate_login_using_database(data):
    request_received_at = data.get('request_received_at', None)
    tenant_name = data.get('tenant_name', '')
    original_user = data.get('username', '')
    impersonated_user_id = data.get('impersonate_data').get('id', '')
    impersonated_user_email = data.get('impersonate_data').get('email', '')
    role = data.get('role_name', '')
    session_id = data.get('session_id', '')

    print(f"Request Data: {data}")
    ##database connection
    db = DB("common_utils", **db_config)

    # Start time and date calculation
    try:
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'Start time: {date_started}')
    except:
        date_started = 0
        start_time = 0
        print("Failed to start time calculation")
        pass

    try:
        # Step 1: Clear session ID for the original user (if any)
        print(f"Clearing session for original user: {original_user}")
        update_data = {"status": "inactive", "session_id": ""}
        db.update_dict("live_sessions", update_data, {"username": original_user})
        print(f"Session for {original_user} cleared.")
        # Audit logging for clearing session
        audit_data_clear_session = {
            "service_name": 'User_authentication',
            "created_date": date_started,
            "created_by": original_user,
            "status": "Cleared",
            "session_id": "",  # Since the session is cleared
            "tenant_name": tenant_name,
            "comments": f"Session for {original_user} cleared",
            "module_name": "user_logout",
            "request_received_at": request_received_at
        }

        # Save audit data to database
        db.update_audit(audit_data_clear_session, 'audit_user_actions')

        # Step 2: Check if impersonated user already has an active session
        impersonated_user_name = db.get_data("users", {'id': impersonated_user_id})['username'].to_list()[0]
        impersonated_tenant_name = db.get_data("users", {'id': impersonated_user_id})['tenant_name'].to_list()[0]
        
        user_data = db.get_data("tenant", {'tenant_name':impersonated_tenant_name })
        

        
        if isinstance(user_data, bool):
            is_10_partner = user_data
        else:
            is_10_partner = user_data['is_10_partner'].to_list()[0] if 'is_10_partner' in user_data.columns else False
            
        
        new_session_id = str(uuid.uuid4())  # Generates a new UUID string

        try:
            session_record = db.get_data("live_sessions", {'username': impersonated_user_name}, ['username', 'session_id'])
        except:
            session_record = None

        # Check if session_record is either a string or a DataFrame
        if isinstance(session_record, pd.DataFrame) and not session_record.empty:
            # If session is active, update the record with new details
            print(f"Updating existing session for impersonated user: {impersonated_user_name}")
            session_data = {
                "login": request_received_at,
                "session_id": new_session_id,
            }
            db.update_dict("live_sessions", session_data, {"username": impersonated_user_name, "status": "active"})
            # Audit logging for session update
            audit_data_update_session = {
                "service_name": 'User_authentication',
                "created_date": date_started,
                "created_by": impersonated_user_name,
                "status": "Updated",
                "session_id": new_session_id,
                "tenant_name": tenant_name,
                "comments": f"Session for impersonate {impersonated_user_name} updated and is_10_partner: {is_10_partner}",
                "module_name": "session_update",
                "request_received_at": request_received_at
            }

            db.update_audit(audit_data_update_session, 'audit_user_actions')
        else:
            # If no active session, insert a new session
            print(f"No active session found for {impersonated_user_name}. Creating new session.")
            session_data = {
                "username": impersonated_user_name,
                "session_id": new_session_id,  
                "status": "active",
                "login": request_received_at,
            }
            db.update_dict(session_data, 'live_sessions')
            # Audit logging for session insert
            audit_data_insert_session = {
                "service_name": 'User_authentication',
                "created_date": date_started,
                "created_by": impersonated_user_name,
                "status": "Inserted",
                "session_id": new_session_id,
                "tenant_name": tenant_name,
                "comments": f"New session created for impersonate user {impersonated_user_name} and is_10_partner : {is_10_partner}",
                "module_name": "session_insert",
                "request_received_at": request_received_at
            }

            db.update_audit(audit_data_insert_session, 'audit_user_actions')

        # Update the last login for the impersonated user
        db.update_dict("users", {"last_login": request_received_at}, {"username": impersonated_user_name})

        # Fetch tenant names
        tenant_names = db.get_data("tenant", {"is_active": "true"}).to_dict(orient='records')
        db_tenant_names = {tenant['tenant_name']: tenant['db_name'] for tenant in tenant_names}

        # Verify user information for the impersonated user
        user_info = db.get_data("users", {"username": impersonated_user_name, "is_active": 'true'})
        if not user_info.empty:
            role_name = user_info['role'].to_list()[0]
            email = user_info.iloc[0]['email']
            
            if role_name == 'Super Admin':
                tenant_names_all = db.get_data("tenant", {"is_active": "true"}).to_dict(orient='records')
                final_tenants = format_tenant_data(tenant_names_all, [], role_name)
            else:
                tenant_ids = db.get_data("user_module_tenant_mapping", {"user_name": impersonated_user_name}, ['tenant_id'])['tenant_id'].to_list()
                tenant_all_df = db.get_data("tenant", {"id": tenant_ids, "is_active": "true"}).to_dict(orient='records')
                final_tenants = format_tenant_data(tenant_all_df, [], '')

            message = "Impersonation login is successful."
            response = {
                "flag": True,
                "message": message,
                "user_impersonate": is_10_partner,
                "db_tenant_names": db_tenant_names,
                "tenant_names": final_tenants,
                "role": role_name,
                "session_id": new_session_id,
                "email": email,
                "user_name": impersonated_user_name
            }
        else:
            message = "Invalid impersonation user credentials."
            response = {"flag": False, "message": message, "tenant_names": [], "role": ''}

        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time

        # Example auditing
        audit_data_user_actions = {
            "service_name": 'User_authentication',
            "created_date": date_started,
            "created_by": impersonated_user_name,
            "status": str(response['flag']),
            "time_consumed_secs": time_consumed,
            "session_id": new_session_id,
            "tenant_name": tenant_name,
            "comments": message,
            "module_name": "user_login",
            "request_received_at": request_received_at
        }
        db.update_audit(audit_data_user_actions, 'audit_user_actions')

        return response

    except Exception as e:
        error_type = type(e).__name__
        # Error handling and logging
        print(f"Something went wrong: {e}")
        message = "Invalid  impersonate User Credentials"
        try:
            # Log error to database
            error_data = {
                "service_name": 'User_authentication',
                "created_date": date_started,
                "error_message": message,
                "error_type": error_type,
                "users": impersonated_user_name,
                "session_id": new_session_id,
                "tenant_name": tenant_name,
                "comments": message,
                "module_name": "User_login",
                "request_received_at": request_received_at
            }
            db.log_error_to_db(error_data, 'error_log_table')
        except:
            pass

        return {"flag": False, "message": message}
    
    


def logout(data):
    try:
        username=data.get('username','')

        db = DB(os.environ['COMMON_UTILS_DATABASE'], **db_config)
        update_data={"access_token":" "}
        db.update_dict('users', update_data, {'username': username})
        
        # Update live_sessions table on logout
        logout_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        session_data = {
            "status": "inactive",
            "logout": logout_time,
            "last_request": logout_time,
            "session_id": "", 
            "access_token":" "
        }
        db.update_dict('live_sessions', session_data, {'username': username})
        
        # Auditing the logout event
        audit_data_user_actions = {
            "service_name": 'User_logout',
            "created_date": logout_time,
            "created_by": username,
            "status": "true",
            "session_id": "",  # Include session ID
            "tenant_name": "",
            "comments": "User logged out successfully",
            "module_name": "user_logout",
            "request_received_at": logout_time
        }
        db.update_audit(audit_data_user_actions, 'audit_user_actions')
        
        message=f"The User has been successfully logged out"
        response={"flag":True,"message":message}
        return response
    except Exception as e:
        message=f"The User has been successfully logged out"
        response={"flag":True,"message":message}
        return response
    






def form_modules_dict(data,sub_modules,tenant_modules,role_name):
    '''
    Description:The form_modules_dict function constructs a nested dictionary that maps parent modules 
    to their respective submodules and child modules. It filters and organizes modules based on the 
    user's role, tenant permissions, and specified submodules.
    '''
    print("Starting to form modules dictionary.")
    # Initialize an empty dictionary to store the output
    out={}
    # Iterate through the list of modules in the data
    for item in data:
        parent_module = item['parent_module_name']
        # print("Processing parent module: %s", parent_module)
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
                print("Skipping parent module: %s (not in tenant modules)", parent_module)
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
    print("Finished forming modules dictionary: %s", out)                  
    return out

def transform_structure(input_data):
    '''
    Description:The transform_structure function transforms a nested dictionary 
    of modules into a list of structured dictionaries,each with queue_order to 
    maintain the order of parent modules, child modules, and sub-children
    '''
    
    print("Starting transformation of input data.")
    
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
    start_time = time()
    # print(f"Request Data: {data}")
    Partner = data.get('Partner', '')
    '''
    if the data is coming from 1.0 then saving and auditing the user  details
    '''
    # Check if "1.0": true exists in the data
    if data.get("1.0"):
        # Add values to the database since "1.0": true is present
        try:
            user_name = data.get("username", '')
            tenant_id = data.get("tenant_id", '')
            session_id = data.get("session_id", '')
            request_received_at = data.get("request_received_at", '')
            tenant_database = data.get("db_name", '')
            auth_code = data.get('auth_code','')


            # Database connection
            database = DB(tenant_database, **db_config)
            db = DB('common_utils', **db_config)
            tenant_name = db.get_data("tenant", {'id':tenant_id},['tenant_name'])['tenant_name'].to_list()[0]

            access_token = get_access_token(auth_code)
            p = {"last_login": request_received_at, "access_token": access_token}
            update_result = db.update_dict("users", p, {"username": user_name})

            
            if update_result:
                message = f"User table successfully updated for user: {user_name}."
                print(message)  # Output message for verification
            else:
                message = f"Failed to update user table for user: {user_name}."
                print(message)
            
            if not user_name:
                message = "Username not present in request data."
                print(f"Message: {message}")
                
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
                print("Session updated.")
            elif isinstance(session_record, str) and session_record == user_name:
                # If session is found but returned as a string (i.e., user already exists)
                session_data = {
                    "access_token": access_token,
                    "login": login_time,
                    "last_request": login_time
                }
                db.update_dict("live_sessions", session_data, {"username": user_name, "status": "active"})
                print("Session updated for user found as string.")
            else:
                # If no active session, insert a new record
                # print(f"No active session found for {user_name}. Inserting new session record.")
                session_data = {
                    "username": user_name,
                    "access_token": access_token,
                    "status": "active",
                    "login": login_time,
                    "last_request": login_time
                }
                db.insert_dict(session_data, 'live_sessions')

            # Audit log for user actions
            end_time = time()
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
            db = DB('common_utils', **db_config)
            db.update_audit(audit_data_user_actions, 'audit_user_actions')

        except Exception as e:
            print(f"Something went wrong: {e}")
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
    request_received_at = data.get('request_received_at', '')
    session_id = data.get('session_id', '')
    ##database connection
    db = DB('common_utils', **db_config)
    # Start time  and date calculation
    start_time = time()
    username = data.get('username', None)
    tenant_name = tenant_name
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
            # print(role_modules_list,role_main_modules_list,'role_modules_list')

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
        # print(final_modules,main_tenant_modules)
        module_table_df=db.get_data(
            "module",{"is_active":True},["module_name","parent_module_name","submodule_name"
                                         ],{'id':"asc"}).to_dict(orient="records")
        return_dict=form_modules_dict(module_table_df,final_modules,main_tenant_modules,role_name)
        return_dict=transform_structure(return_dict)
        # Retrieve tenant logo
        logo=db.get_data("tenant",{'tenant_name':tenant_name},['logo'])['logo'].to_list()[0]
        message = "Module data sent sucessfully"
        response = {"flag": True, "message": message, "Modules": return_dict,"logo":logo, "access_token":access_token}
        # End time calculation
        end_time = time()
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
            print(f"Exception is {e}")
        return response
    except Exception as e:
        print(F"Something went wrong and error is {e}")
        message = "Something went wrong while getting modules"
        # Error Management
        error_data = {"service_name": 'Module_api', "created_date": request_received_at,
                       "error_messag": message, "error_type": e, "user": username,
                      "session_id": session_id, "tenant_name": tenant_name, "comments": message,
                      "module_name": "", "request_received_at": start_time}
        database.log_error_to_db(error_data, 'error_table')
        return {"flag": False, "message": message}