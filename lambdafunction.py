import json
import boto3
import os
from urllib.parse import unquote_plus, parse_qs
 
firehose_client = boto3.client('firehose')
def send_response(message):
    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(message)
    }
    return response

def lambda_handler(event, context):
    
    output_str= event['body'] 
    output_parse=parse_qs(output_str)
    output={key:value
    for key, values in output_parse.items()
    for value in values} 
    output_text= output_text.replace("\n","")
    output_text= output_text.replace('\"','"')
    print(output)
    lambda_client= boto3.client("lambda",region_name="us-east-1")
    print('successfully invoked Lambda_trigger function')
    file1 = open("/tmp/MyFile.txt", "w")
    file1.write(str(output))
    file1.write('\n')
    file1.close()
    
    with open('/tmp/MyFile.txt', 'r') as f:
        for i in range(1):
            line = next(f)
                    Record={'Data': line})
        print("sent to firehose")
    return send_response({"status": "Yamini_krishna"})
    



