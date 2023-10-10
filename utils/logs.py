import logging
import datetime
from os import path
import os
from utils.config import Config


def setup_loggers(logger_name, log_file="default", log_level=logging.INFO):
    logger = logging.getLogger(logger_name)
    config = Config()
    path_to_log_file = config.get_config_item("path", "path_to_log_files")
    # [09-30]chonghua add if statement
    # 若logger未開啟過再創立 否則回傳舊有logger
    # 目的在於阻止建立多個相同logger 導致同一筆log被多個logger一起輸出 引發同一筆log重複寫入的狀況
    if not logger.handlers:
        # logger.setLevel(logging.DEBUG)
        logger.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s %(module)s %(lineno)d %(levelname)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S',)
        # 230414 Add by peiwen 如果log資料夾不存在就新建一個
        if not os.path.exists(path_to_log_file):
            os.makedirs(path_to_log_file)
        
        log_filename = path_to_log_file + datetime.datetime.now().strftime("%Y-%m-%d.log")
        if log_file != "default":
            # log_filename = path_to_log_file + log_file + ".log"
            log_filename = path_to_log_file + log_file + datetime.datetime.now().strftime("_%Y-%m-%d.log")
        
        fileHandler = logging.FileHandler(log_filename, mode='a')
        fileHandler.setLevel(log_level)
        fileHandler.setFormatter(formatter)

        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        streamHandler.setLevel(logging.DEBUG)

        logger.addHandler(fileHandler)
        logger.addHandler(streamHandler)

    return logger
