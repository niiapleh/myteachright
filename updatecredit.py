#/bin/python3!

#perform imports
from azure.servicebus import Message, ServiceBusClient
import requests
import mysql.connector
from mysql.connector import errorcode

import json

import logging


#configure logging format - adding date
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

#from io import StringIO
#import sys

#Delclare Variables
CONNECTION_STR = 'Endpoint=sb://jhs-myteachright.servicebus.windows.net/;SharedAccessKeyName=ipayListen;SharedAccessKey=vCxx1DMExgH/teMkH3ZoLWF0xVjSm6jWUH64sq5tFZc=;EntityPath=ipay'
QUEUE_NAME = 'ipay'

logger=logging.getLogger()


MERCHANT_KEY = 'tk_ee27738a-0f59-11eb-bd0d-f23c9170642f'
#https://manage.ipaygh.com/gateway/json_status_chk?invoice_id=AA123&merchant_key =YOUR_MERCHANT_KEY
IPAY_STATUS = 'https://manage.ipaygh.com/gateway/json_status_chk'

MYSQL_UPDATE = "update tr_user_info_data set data = data + 3 where fieldid=1 and userid = 18;"


#Create functions

def update_credit(amount,userid):
	try:
		cnx = mysql.connector.connect(user='credituser',database='teachright',password='Welcome100')
		if cnx.is_connected():
			print('Connected to MySQL database')
		print('update tr_user_info_data set data = data + {} where fieldid=1 and userid = {};'.format(amount,userid))
		cursor = cnx.cursor()
		cursor.execute('update tr_user_info_data set data = data + {} where fieldid=1 and userid = {};'.format(amount,userid))
#		rows = cursor.fetchall()
#		for row in rows:
#			print(row)
		cnx.commit()
		print("Succesfully credited account")
		cnx.close
	except:
		raise ValueError("An exception occurred")

#	return("update tr_user_info_data set data = data + 3 where fieldid=1 and userid = 18;")
	
logger.warning('Start listening to queue')

def status_url(base_url,invoice_id,key):
	return("{}?invoice_id={}&merchant_key={}".format(base_url,invoice_id,key))


with ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR) as client:

    with client.get_queue_receiver(queue_name=QUEUE_NAME) as receiver:
        for message in receiver:
            print("Message: {}".format(message))
            url = status_url(IPAY_STATUS,message,MERCHANT_KEY)
            r = requests.get(url)
            payload = (r.text)
#            print(payload)
            logger.warning('processing {}'.format(payload))
            try:
             jsonData = json.loads(payload)
            except:
             print("")
#            print(jsonData)
            payment_status = jsonData["{}".format(message)]['status']
            print(payment_status)
            if payment_status == "paid":
             amount = jsonData["{}".format(message)]['amount']
             credit = amount
#             userid=""
#             userid = jsonData["{}".format(message)]['narration']
             userid = str(message).split('_')[2] 
             print(amount,userid)
             try:
                  update_credit(credit,userid)
             except:
                  print("There was an error")
             else:
                  message.complete()
                  print("processed {}".format(jsonData))
                  logger.warning("processed {}".format(jsonData))
