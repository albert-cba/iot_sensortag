#from __future__ import print_function
import time
import json
import boto3

print('Loading function')
      
def main(event, context):
    print(event)
    
    client = boto3.client('iot-data', region_name='ap-northeast-1')
    
    isTooDarkState = "false"
    if event['Lux'] <= 10:
        isTooDarkState = "true"
    
    state = '{ "state": { "reported": { "' + event['DeviceName'] + '": { "isTooDark": "' + isTooDarkState + '" } } } }'
    mypayload = json.dumps(state)
    
    response = client.update_thing_shadow(
        thingName='RaspberryPiGateway',
        payload=state
    )
    return "Shadow set completed"        

def lambda_handler(event, context):
    main(event, context)
  
  
if __name__ == '__main__':
    main('blank', 'blank')