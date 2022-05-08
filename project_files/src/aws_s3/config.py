try:
    import boto3
    import os
    import json
    from dotenv import load_dotenv
except Exception as e:
    print(e)


class Aws_s3:
    def connect_s3(self) -> None:
        load_dotenv()
        aws_access_key = os.environ.get("AWS_ACCESS_KEY")
        aws_secret_key = os.environ.get("AWS_SECRET_KEY")
        self.bucket_name = os.environ.get("AWS_S3_BUCKET_NAME")
        self.file_name = os.environ.get("AWS_S3_FILE_NAME")
        self.table_name = os.getenv("TABLE_NAME")
        self.s3 = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
        )

    def get_log(self) -> int:
        try:
            response = self.s3.get_object(
                Bucket="inturn-proj", Key="logs/{}_log.json".format(self.table_name)
            )
            data = json.loads(response["Body"].read().decode("utf-8"))
            print("Resuming process from Index :", data["last_index_value"])
            return int(data["last_index_value"])
        except Exception as e:
            if (
                str(e)
                == "An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist."
            ):
                print("No log file found. Setting Index to 1")
                return 1
            else:
                print(e)

    def put_log(self, index=0):
        try:
            key = "logs/{}_log.json".format(self.table_name)
            data = bytes(
                json.dumps(
                    {"table_name": self.table_name, "last_index_value": index}
                ).encode("UTF-8")
            )
            print(data)
            response = self.s3.put_object(Body=data, Bucket=self.bucket_name, Key=key)
        except Exception as e:
            print(e)
