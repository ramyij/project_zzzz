########################################################################
# * Assignment 2 Part 2. File written by Peter Wei pw2428@columbia.edu #
########################################################################

import boto
import boto.dynamodb2
import time
from datetime import datetime
import json
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.types import NUMBER
import mraa
from upm import pyupm_mg811 as sensorObj

DYNAMO_TABLE_NAME ='iotProject'
ACCOUNT_ID ='163430531586'
IDENTITY_POOL_ID ='us-east-1:1a9a4a49-8e4e-43ea-891c-e2b0ddebfb89' #YOUR IDENTITY POOL ID'
ROLE_ARN ='arn:aws:iam::163430531586:role/Cognito_edisonDemoKinesisUnauth_Role' #YOUR ROLE_ARN'

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



## Read Sensor Values
try:
    dustSensor = mraa.Aio(2)
    gasSensor = mraa.Aio(1)
    dustValue = dustSensor.read()
    gasValue = gasSensor.read()
except KeyboardInterrupt:
    exit

room = raw_input("which room are you monitoring? \n")

print "Collecting air quality data from room " + str(room)

##Write to Dynamo
while(1):
    try:
        timestamp = str(datetime.now())
        gas = gasSensor.read()
        dust = dustSensor.read()
        co2 = co2Sensor.ppm()
        co2V = co2Sensor.volts()

        table_dynamo.put_item(
                       data={
                            'timestamp': timestamp,
                            'GasSensor':gas,
                            'DustSensor': dust,
                            'CO2Sensor': co2,
                            'room': str(room),
                            'CO2Volts': co2V
                        })
        print "Dust: " + str(dust)
        print "Gas: " + str(gas)
        print "CO2: " + str(co2)
        print "CO2V: " + str(co2V)
        time.sleep(5)
    except:
        time.sleep(5)
