"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""
#Importing the necessary Libraries
import os
from common_utils.logging_utils import Logging
import requests
from requests.auth import HTTPBasicAuth


logging = Logging(name='authentication_check')

db_config = {
    'host': os.environ['HOST'],
    'port': os.environ['PORT'],
    'user': os.environ['USER'],
    'password': os.environ['PASSWORD']
}


def validate_token(token):
    
    client_id = os.getenv("CLIENT_ID"," ")
    client_secret = os.getenv("CLIENT_SECRET"," ")
    zitadel_domain = os.getenv("zitadel_domain","")

    introspect_url = f"https://{zitadel_domain}/oauth/v2/introspect"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {"token": token}

    response = requests.post(
        introspect_url,
        headers=headers,
        data=payload,
        auth=HTTPBasicAuth(client_id, client_secret)
    )

    if response.status_code == 200:
        token_info = response.json()
        return token_info.get("active", False)
    else:
        print("Failed to validate token:", response.status_code, response.text)
        return False