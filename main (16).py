import json
from user_management import delete_user,edit_user
 
def lambda_handler(event, context):

    body = event.get('body')
    if body:
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON in body'})
            }
    else:
        body = {}

    # Extract the HTTP method, path, and query string parameters from the event
    http_method = body.get('httpMethod')
    path = body.get('path')
    query_params = body.get('queryStringParameters', {})
 
    # Route based on the path and method
    if http_method == 'GET' and path == '/delete_user':
        result = delete_user(query_params)
    elif http_method == 'GET' and path == '/edit_user':
        result = edit_user(query_params)
    else:
        result = {'error': 'Invalid path or method'}
 
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
 