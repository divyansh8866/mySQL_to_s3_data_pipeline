try:
    from src.controller.controller import Controller
except Exception as e:
    print(e)

sql_to_sql_job = Controller()
sql_to_sql_job.load_data_to_sqs(True)
# sql_to_sql_job.optimal_transmission(automatic_batch_size=True)
