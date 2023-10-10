import pandas as pd
from sqlalchemy.util.langhelpers import symbol
import win32com.client
from Database.database_components import Stock, Company
from Database.Database import Database
from utils.config import Config
from sqlalchemy import select
import logging
from utils import logs
# 紀錄時間用
# logging.basicConfig(level=logging.WARNING,
#                     format='%(asctime)s %(levelname)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M',
#                     handlers=[logging.FileHandler('D:\log/stock.log', 'a', 'utf-8'), ])
logger = logs.setup_loggers("StockFinal")

logger.warning("start getting data from excel")
# Get the active instance of Excel
xl = win32com.client.GetActiveObject('Excel.Application')
xl_workbook = xl.Workbooks('stock.xlsm')
xl_worksheet = xl_workbook.Worksheets(1)
company_symbol = xl_worksheet.Cells(1.1).Value.split(' ')[0]
print(company_symbol)
# 找到日期的最後一行
last_column = xl_worksheet.Cells(2, xl_worksheet.Columns.Count).End(1).Column
# stock.xlsm 固定欄位是日期、開高低收量，所以列的長度固定為六
# 資料從(2,2)開始
# 先固定欄為一筆資料
config = Config()
db = Database()
db_session = db.db_session()
stock_list = []
for col in range(2,last_column+1):
    stock = []
    for row in range(2,8):
        # 如果該欄位沒有資料，則將其填入none，在mysql 會存成NULL
        if xl_worksheet.Cells(row, col).Value is None:
            stock.append(None)
        else:
            stock.append(xl_worksheet.Cells(row, col).Value)
    company = db_session.execute(select(Company).where(Company.company_symbol == company_symbol)).scalars().first()
    one_stock = Stock(date=str(stock[0])[:10],open=stock[1],low=stock[3],high=stock[2],close=stock[4],volume=stock[5])
    one_stock.company = company
    stock_list.append(one_stock)
# 資料庫連接
logger.warning("start insert data")
db_session.add_all(stock_list)
db_session.commit()
logger.warning("end "+ str(company_symbol))