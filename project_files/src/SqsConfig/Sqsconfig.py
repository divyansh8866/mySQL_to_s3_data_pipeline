try:
    import boto3
    import os
    import json
    import datetime
    from dotenv import load_dotenv
except Exception as e:
    print(e)


class SqsQue(object):
    def connect_sqs(self) -> None:
        load_dotenv()
        aws_access_key = os.environ.get("AWS_ACCESS_KEY")
        aws_secret_key = os.environ.get("AWS_SECRET_KEY")
        aws_sqs_queue_name = os.environ.get("AWS_SQS_QUEUE_NAME")
        self.resource = boto3.resource(
            "sqs",
            region_name="us-east-1",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )
        self.queue = self.resource.get_queue_by_name(QueueName=aws_sqs_queue_name)

    def send(self, message, find_batch_size=False) -> str:
        message = json.dumps(message, default=datetime_handler)
        if find_batch_size:
            return message
        else:
            print("sending JSON...")
            response = self.queue.send_message(MessageBody=message)
            return response


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")
