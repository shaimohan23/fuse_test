# api_call_script.py

import sys
import requests
import urllib.parse

def udf_api_call(bucket_name, file_name, udf):
    params = {
        "f": "json",
        "loginId": "test",
        "password": "test",
        "application": "test",
        "source": "flight",
    }

    encoded_params = urllib.parse.urlencode(params)
    url = f"http://localhost:8010/admin/login?{encoded_params}"

    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()
    user_token = json_data["AdminResponse"]["token"]

    body = {
        "bucketName": bucket_name,
        "objectName": file_name,
        "udf": udf,
        "input": '{"x": 1}',
    }

    headers = {
        "authorization": f"Bearer {user_token}",
    }

    url = "http://127.0.0.1:8010/udf_api"
    response = requests.post(url, json=body, headers=headers)
    response.raise_for_status()
    
    # Write the API response bytes to stdout
    sys.stdout.buffer.write(response.content)

if __name__ == "__main__":
    # Get the command-line arguments
    bucket_name = sys.argv[1]
    file_name = sys.argv[2]
    udf = sys.argv[3]
    
    print (bucket_name, file_name, udf)

    # Call the function and output the result
    udf_api_call(bucket_name, file_name, udf)
