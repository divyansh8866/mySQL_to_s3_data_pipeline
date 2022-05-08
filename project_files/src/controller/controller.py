try:
    import os
    import sys
    from dotenv import load_dotenv
    from src.DatabaseConfig.DbConfg import SqlData
    from src.SqsConfig.Sqsconfig import SqsQue
    from src.aws_s3.config import Aws_s3
except Exception as e:
    print(e)


class Controller(SqlData, SqsQue, Aws_s3):
    def __init__(self):
        self.batch_size = 20
        self.connect_sql()
        self.connect_sqs()
        self.connect_s3()

    def load_data_to_sqs(self) -> None:
        self.batch_size = self.find_batch_size()
        index = self.get_log()
        status = input("Press ENTER to proceed uploading all datas")

        while True:
            try:
                print("Index:",index)
                data = self.get(top_count=self.batch_size, last_data_key=index)
                if len(data) <= 0:
                    break
                else:
                    temp = int(input("To continue press 1 , else to raise flag press 2 :"))
                    if temp == 2:
                        raise Exception("Loading of data has been interupted")
                    data_json = {"data": data}
                    response = self.send(data_json, find_batch_size=False)
                    index += len(data)
            except Exception as e:
                self.put_log(index)
                print(e)
                break

        print("COMPLETE \n")
        self.close_connection()

    def find_batch_size(self) -> int:
        data = self.get(top_count=self.batch_size, last_data_key=1)
        data_json = {"data": data}
        file_size = sys.getsizeof(self.send(data_json, find_batch_size=True))
        print(file_size)
        over_head_per = 1.15
        size_of_each_rec = int(file_size) / self.batch_size
        final_batch_size = (255000) / (size_of_each_rec * over_head_per)
        print("Optimal batch size : ", int(final_batch_size))
        return int(final_batch_size)
