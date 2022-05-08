import json
import boto3
import hashlib
import time

s3 = boto3.client('s3')

def lambda_handler(event, context):
	
	bucket ='inturn-proj'
	data = json.loads(event['Records'][0]['body'])
	print(type(data))
	print(data)
	start = time.time()
	data=data['data']
	final_string=""
	for tupl in data:
		# print("tuple",tupl)
		for key, value in tupl.items():
			# print(value)
			if value is None:
				value = 'n/a'
			if value == "None":
				value = 'n/a'
			if value == "null":
				value = 'n/a'
			if len(str(value)) < 1:
				value = 'n/a'
			tupl[key]=str(value)
		temp_hold= json.dumps(tupl)
		final_string= final_string+(temp_hold+'\n')
	hash_name=hashlib.md5(final_string.encode("UTF-8"))
	file_name=hash_name.hexdigest()
	uploadByteStream = bytes(final_string.encode('UTF-8'))
	s3.put_object(Bucket=bucket, Key=file_name+".json", Body=uploadByteStream)
	print('Put Complete')