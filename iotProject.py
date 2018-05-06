########################################################################
# * Assignment 2 Part 2. File written by Peter Wei pw2428@columbia.edu #
########################################################################

import boto
import boto3
import boto.dynamodb2
import time
from datetime import datetime
import json
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.types import NUMBER
import mraa
import sys
from upm import pyupm_mg811 as sensorObj

DYNAMO_TABLE_NAME ='iotProject'
ACCOUNT_ID =''
IDENTITY_POOL_ID ='' #YOUR IDENTITY POOL ID'
ROLE_ARN ='' #YOUR ROLE_ARN'

sts = boto.connect_sts()
cognito = boto.connect_cognito_identity()
cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
oidc = cognito.get_open_id_token(cognito_id['IdentityId'])
assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])

co2Sensor = sensorObj.MG811(0,2,5.0)
#calibrate CO2 sensor
co2Sensor.setCalibration(.32,.16)
print('calibration set. First reading: ', co2Sensor.ppm())

client_dynamo = boto.dynamodb2.connect_to_region(
        'us-east-1',
        aws_access_key_id=assumedRoleObject.credentials.access_key,
        aws_secret_access_key=assumedRoleObject.credentials.secret_key,
        security_token=assumedRoleObject.credentials.session_token)
table_dynamo = Table(DYNAMO_TABLE_NAME,connection=client_dynamo)

client = boto3.client('sns','us-east-1')
response = client.create_topic(Name = 'iotpj')
topicArn = response['TopicArn']
response = client.subscribe(TopicArn = topicArn, Protocol = 'sms', Endpoint = '+16463312065')

## Read Sensor Values
try:
    dustSensor = mraa.Aio(2)
    gasSensor = mraa.Aio(1)
    tempSensor = mraa.Aio(0)
    dustValue = dustSensor.read()
    gasValue = gasSensor.read()
    temp = tempSensor.read()
except KeyboardInterrupt:
    exit

room = sys.argv[1]

print "Collecting air quality data from room " + str(room)

##Write to Dynamo
while(1):
    try:
        timestamp = str(datetime.now())
        gas = gasSensor.read()
        dust = dustSensor.read()
        co2 = co2Sensor.ppm()
        co2V = co2Sensor.volts()
        
        tempVal = tempSensor.read()
        R = (1023.0/tempVal-1.0) * 100000 
        temperature=1.0/(math.log(R/100000.0)/B+1/298.15)-273.15


        if gas > 38:
            client.publish(TopicArn = topicArn,
                  Message = 'Gas: ' + str(gas),
                  Subject = 'monitoring')

        table_dynamo.put_item(
                       data={
                            'timestamp': timestamp,
                            'GasSensor':gas,
                            'DustSensor': dust,
                            'CO2Sensor': co2,
                            'room': str(room),
                            'CO2Volts': co2V,
                            'temp' : temperature
                        })
        time.sleep(5)
    except:
        time.sleep(5)
