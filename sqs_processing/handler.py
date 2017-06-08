from __future__ import print_function
import os
import sys
import json
import boto3
import requests
import eventlet
eventlet.monkey_patch()

client = boto3.client('sns')

SQL_QUEUE1 = 'queue1'
SNS_ARN = "arn:aws:sns:us-east-1:694231578914:lambdatest"

def sqs_generation(msg_body):

	sqs = boto3.resource('sqs')	

	print ("Saving to SQS ==>" + msg_body)
    	queue = sqs.get_queue_by_name(QueueName = SQL_QUEUE1)
    	response = queue.send_message(MessageBody = msg_body)	
	print (response)

def process_sqs(event, context):

	org_message = event['Records'][0]['Sns']['Message']
	#org_message = '{"DeliveryCount":2,"EndPoint":"https:\/\/apnagujarat.com\/json1.php","Extra":"Extra JSON Parameters"}'

	msg_data  = json.loads(org_message)

	httpurl        = msg_data['EndPoint']
	delivery_count = msg_data['DeliveryCount']

	with eventlet.Timeout(10):
		response = requests.get(httpurl)

	status_code = 301 #response.status_code
	
	#Terminate the Lambda on Successfull Status Code 200
	if status_code == 200:
		print ("returning1")
		return 1
	if status_code != 200 and delivery_count == 5:
		print ("returning2")
		return 1
	
	#If not Process the rest and store to SQS
	delivery_count = delivery_count + 1
	msg_data['DeliveryCount'] = delivery_count
	
	org_message = json.dumps(msg_data)
	print ( org_message )

	sqs_generation(org_message)
		
	return 1

	body = {
		"message": "Go Serverless v1.0! Your function executed successfully!",
		"input": event
	}

	response = {
		"statusCode": 200,
		"body": json.dumps(body)
	}

	return response

	# Use this code if you don't use the http event with the LAMBDA-PROXY integration
	"""
	return {
		"message": "Go Serverless v1.0! Your function executed successfully!",
		"event": event
	}
	"""

def publish_sns(event, context):
	print ("publish_sns")
	org_message = '{"DeliveryCount":1,"EndPoint":"https:\/\/apnagujarat.com\/json1.php","Extra":"Extra JSON Parameters"}'

	client = boto3.client('sns')
	response = client.publish(
	    TopicArn= SNS_ARN,    
	    Message= org_message
	)

	return "message is broadcasted"

def sqsTosns(event, context):
	print ("sqsTosns")
	sqs = boto3.resource('sqs')	
    	queue = sqs.get_queue_by_name(QueueName = SQL_QUEUE1)

	client = boto3.client('sns')

	for message in queue.receive_messages():
		# Get the custom author message attribute if it was set

		body = ''

		if message.body is not None:
			body = message.body
			print (body)
			
			#Send Message to SNS
			response = client.publish(
			    TopicArn= SNS_ARN,    
			    Message= body
			)
			print ("SQS Message is sent to SNS ")
			message.delete()

		#if author_name:
		#	author_text = ' ({0})'.format(author_name)

		# Print out the body and author (if set)
		#print('Hello, {0}!{1}'.format(message.body, author_text))

		# Let the queue know that the message is processed
		

	return 1
