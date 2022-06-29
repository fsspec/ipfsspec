import os
import json

GATEWAYS = [
    "http://127.0.0.1:8080",
    # "https://ipfs.io",
    # "https://gateway.pinata.cloud",
    # "https://cloudflare-ipfs.com",
    # "https://dweb.link",
]


def get_default_gateways():
    try:
        return os.environ["IPFSSPEC_GATEWAYS"].split()
    except KeyError:
        return GATEWAYS


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