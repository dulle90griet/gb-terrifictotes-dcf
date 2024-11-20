import logging
from datetime import datetime
import json

def lambda_handler(event, context):
    some_dict = {"hello": "world"}
    return json.dumps(some_dict)

def lambda_handler_2(event, context):
    some_dict = json.loads(event)
    some_dict['goodbye'] = 'team'
    return some_dict

