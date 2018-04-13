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
import pyupm_i2clcd as lcd
from __future__ import print_function
import time, sys, signal, atexit
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


co2Sensor = sensorObj.MG811(0, 6, 5.0)


client_dynamo = boto.dynamodb2.connect_to_region(
        'us-east-1',
        aws_access_key_id=assumedRoleObject.credentials.access_key,
        aws_secret_access_key=assumedRoleObject.credentials.secret_key,
        security_token=assumedRoleObject.credentials.session_token)
table_dynamo = Table(DYNAMO_TABLE_NAME,connection=client_dynamo)


# This function lets you run code on exit
def exitHandler():
    print("Exiting")
    sys.exit(0)

# Register exit handlers
atexit.register(exitHandler)
signal.signal(signal.SIGINT, SIGINTHandler)

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
        table_dynamo.put_item(
                       data={
                            'timestamp':str(datetime.now()),
                            'GasSensor':gasSensor.read(),
                            'DustSensor': dustSensor.read(),
                            'CO2': co2Sensor.ppm()
                            'room': str(room)
                        })
        print "Dust: " + str(dustSensor.read())
        print "Gas: " + str(gasSensor.read())
        print "CO2: " + str(co2Sensor.ppm())
        time.sleep(5)
    except:
        time.sleep(5)
