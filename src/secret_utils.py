import json


def store_secret(sm_client, secret_id, keys_and_values):

    if isinstance(keys_and_values[0], list):
        key_value_dict = {}
        for key_value_pair in keys_and_values:
            key_value_dict[key_value_pair[0]] = key_value_pair[1]
    else:
        key_value_dict = {keys_and_values[0]: keys_and_values[1]}

    secret_string = json.dumps(key_value_dict)
    response = sm_client.create_secret(Name=secret_id, SecretString=secret_string)
    return response

def retrieve_secret(sm_client, secret_id):
    secret_json = sm_client.get_secret_value(SecretId=secret_id)["SecretString"]
    secret_value = json.loads(secret_json)
    return secret_value

def update_secret(sm_client, secret_id, keys_and_values):
    if isinstance(keys_and_values[0], list):
        key_value_dict = {}
        for key_value_pair in keys_and_values:
            key_value_dict[key_value_pair[0]] = key_value_pair[1]
    else:
        key_value_dict = {keys_and_values[0]: keys_and_values[1]}
    
    secret_string = json.dumps(key_value_dict)
    response = sm_client.update_secret(SecretId=secret_id, SecretString=secret_string)
    return response
