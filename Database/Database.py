
import logging

# from sqlalchemy.engine.base import Connection
from sqlalchemy.orm import Session
from utils.config import Config
import sqlalchemy
from sqlalchemy.pool import NullPool
import time
from utils import logs
import os

class Database():

    def __init__(self):
        self._config = Config()
        self._db_data = self._config.get_database_config()
        self.timelog = logs.setup_loggers("Database_time", "time") 
        self._logger = logs.setup_loggers("Database")
        self.global_connect = None  # 221015 Add by peiwen 新增連線的全域變數，每個process有一個連線
        self.reconnect_count = 5  # 221021 Add by peiwen 重連次數

    def connection(self):
        """
        用 pymysql 連線到 iplab_database 這個資料庫，如果資料庫中沒有 iplab_database 請先自己建立一個。建立後連線方法有多項參數可傳入，詳細可參考 https://pymysql.readthedocs.io/en/latest/index.html。這邊用了其中 7 項參數，分別為host、port、user、password、db、charset及cursorclass，前 6 項是用Config物件從config.ini中讀取而來，最後一項則是 pymysql 所提供的class。
        """
        try:
            engine = sqlalchemy.create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8".format(
                user=self._db_data["user"],
                password = self._db_data["password"],
                host = self._db_data["host"],
                port = self._db_data["port"],
                database = self._db_data["db"]
                ) , poolclass=NullPool)
                # 尝试连接
            conn = engine.connect()
            print("Connected to the database.")
            conn.close()
            
            print(engine)
            return engine
        except Exception as e:
            logging.error("[database][connection]::無法連結資料庫")
 
            return None

    def db_session(self):
        """
        用sqlalchemy 創建session 來溝通
        回傳：sqlalchemy session
        """
        db_engine = self.connection()
        return Session(db_engine)
    
    def create_table(self, statement):
        """
        包裝版的非 orm 用法
        參數：sql敘述式
        執行建立 table 的 SQL 述句，若成功建立回傳 True，失敗則回傳False
        """
        create_result = False
        
        if "CREATE TABLE" not in statement:
            logging.error("[database][create_table]::輸入錯誤SQL")
            return create_result
        
        try:
            connect = self.connection().connect()
            connect.execute(statement)
            create_result = True
        except Exception as e:
            logging.error("[database][create_table]::table建立失敗")
        finally:
            connect.close()
            return create_result

    def insert_data(self, statement, *args):
        """
        包裝版的非 orm 用法
        參數：sql敘述式，可以包含敘述式裡的變數
        回傳：boolean 表達insert 的成功與否
        """
        insert_result = False
        
        if "INSERT INTO" not in statement:
            logging.error("[database][insert_data]::輸入錯誤SQL")
            return insert_result
        
        try:
            connect = self.connection().connect()
            connect.execute(statement, args)
            insert_result = True
        except Exception as e:
            logging.error("[database][insert_data]::data新增失敗")
            print(e)
        finally:
            if connect is not None:
                connect.close()
            return insert_result

    def select_data(self, statement, *args):
        """
        包裝版的非 orm 用法
        參數：sql敘述式，可以包含敘述式裡的變數
        回傳：list 裡面包著dict(key為欄位名稱，value為值)
        """
        if "SELECT" not in statement:
            self._logger.error("[database][select_data]::輸入錯誤SQL")
            return None
        selected_data = []
        try:
            # 221015 Add by peiwen 如果全域變數的連線為空的話，呼叫連線的方法
            if self.global_connect == None:
                self.global_connect = self.connection().connect()
            # connect = self.connection().connect()
            start_time = time.time()
            all_record = self.global_connect.execute(statement, args).fetchall()
            end_time = time.time()
            self.timelog.info("[%d][%s][%s] SELECT RESPONSE TIME：%f",os.getpid(), statement, args, end_time - start_time)
            for record in all_record:
                selected_data.append(dict(record))
            # connect.close()
        except Exception as e:
            self._logger.exception("[%d][database][select_data]::data搜尋失敗, [%s][%s]", os.getpid(), statement, args)
            # 221021 add by peiwen 新增重連機制，如果因為斷線或其他原因導致查詢失敗，先將原本的連線段開重連再查詢
            if self.reconnect_count != 0:
                # self.disconnect()
                self.reconnect_count = self.reconnect_count - 1
                time.sleep(10)
                self.create_connection()
                return self.select_data(statement, *args)

        return selected_data

    def update_data(self, statement, *args):
        """
        包裝版的非 orm 用法
        參數：sql敘述式，可以包含敘述式裡的變數
        回傳：boolean， 表示 update 成功與否
        """
        update_result = False
        
        if "UPDATE" not in statement:
            logging.error("[database][update_data]::輸入錯誤SQL", exc_info=True)
            return update_result
        
        try:
            connect = self.connection().connect()
            connect.execute(statement, args)
            update_result = True
        except Exception as e:
            logging.error("[database][update_data]::data新增失敗", exc_info=True)
        finally:
            if connect is not None:
                connect.close()
            return update_result

    def truncate_table(self, statement):
        connect = self.connection().connect()
        connect.execute(statement)
        connect.close()

    # 221015 Add by peiwen 新增連線、中斷連線的方法，避免每次查詢都重新連線造成資料庫負荷過大
    def disconnect(self):
        self.global_connect.close()
        self._logger.info("[%d]DB Disonnection!!", os.getpid())

    def create_connection(self):
        self._logger.info("[%d]Create DB Connection!!", os.getpid())
        self.global_connect = self.connection().connect()

if __name__ == "__main__":
    db = Database()
    db.connection()
 
