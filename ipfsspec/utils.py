import os
import json


def parse_response(
    response, # Response object
):
    "Parse response object into JSON"
    if response.text.split('\n')[-1] == "":
        try:
            return [json.loads(each) for each in response.text.split('\n')[:-1]]
        except:
            pass
    try:
        return response.json()
    except:
        return response.text
    
    
def parse_error_message(
    response, # Response object from requests
):
    'Parse error message for raising exceptions'
    sc = response.status_code
    try:
        message = response.json()['Message']
    except:
        message = response.text
    return f"Response Status Code: {sc}; Error Message: {message}"


def dict_get(input_dict,keys, default_value=False):
    """
    get keys that are dot seperated (key1.key2.key3) recursively into a dictionary
    """
    if isinstance(keys, str):
        keys = keys.split('.')

    key = keys[0]

    try:

        next_object_list = [input_dict[key]]
        for key in keys[1:]:
            next_object_list += [next_object_list[-1][key]]
        return next_object_list[-1]
    except Exception as e:
        return default_value
    

def dict_put(input_dict,keys, value ):
    """
    insert keys that are dot seperated (key1.key2.key3) recursively into a dictionary
    """
    if isinstance(keys, str):
        keys = keys.split('.')
    key = keys[0]
    if len(keys) == 1:
        assert isinstance(input_dict,dict)
        input_dict[key] = value

    elif len(keys) > 1:
        if key not in input_dict:
            input_dict[key] = {}
        dict_put(input_dict=input_dict[key],
                             keys=keys[1:],
                             value=value)


