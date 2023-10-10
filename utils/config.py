import logging
from configparser import ConfigParser

class Config():

    def __init__(self):
        self._parser = ConfigParser()
        self._parser.read("config.ini", encoding="utf-8")
    
    def get_database_config(self):
        database_config = {}
        database_config["host"]     = str(self._parser["database"]["host"])
        database_config["port"]     = int(self._parser["database"]["port"])
        database_config["user"]     = str(self._parser["database"]["user"])
        database_config["password"] = str(self._parser["database"]["password"])
        database_config["db"]       = str(self._parser["database"]["db"])
        database_config["charset"]  = str(self._parser["database"]["charset"])
        return database_config
    
    def get_config_item(self, section=None, item=None):
        if section is None or item is None:
            logging.error("[Config][get_config_item]::未定義取值的區塊或項目名稱")
            return False
        return self._parser[section][item]

    def get_all_config(self):
        return self._parser

if __name__ == "__main__":
    c = Config()
    # db = c.get_database_config()
    db = c.get_config_item("exchange", "TW").split(",")
    print(db)