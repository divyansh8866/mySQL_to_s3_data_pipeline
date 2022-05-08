try:
    import pyodbc
    import os
    from dotenv import load_dotenv
    from src.DatabaseConfig.query import Query
    from dotenv import load_dotenv
except Exception as e:
    print(e)


class SqlData:
    def connect_sql(self) -> None:
        try:
            load_dotenv()
            self.server = os.environ.get("SERVER")
            self.database = os.environ.get("DATABASE")
            self.connection = pyodbc.connect(
                """DRIVER={ODBC Driver 17 for SQL Server};
                 SERVER={%s}; 
                 Database={%s}; 
                 UID=DataTeamServiceAccount; 
                 PWD=6puGCc3Cyd3GXFGv;"""
                % (self.server, self.database)
            )
            print("Connection : ESTABLISHED")
        except Exception as e:
            return {"status": -1, "error": {"message": str(e)}}

    def get(self, top_count, last_data_key=0) -> None:
        try:
            print("Quering Database ...")
            cursor = self.connection.cursor()
            cursor.execute(Query.put_query(top_count, last_data_key))
            data = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            data = [dict(zip(columns, item)) for item in data]
            return data
        except Exception as e:
            return {"status": -1, "error": {"message": str(e)}}

    def close_connection(self) -> None:
        self.connection.close()
        print("Connection : TERMINATED")
