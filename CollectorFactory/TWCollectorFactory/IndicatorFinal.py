import pandas as pd
from sqlalchemy.util.langhelpers import symbol
import win32com.client
from Database.database_components import Period, SingleIndicator, Stock, Company, FinancialReportIndicatorValue
from Database.Database import Database
from utils.config import Config
from sqlalchemy import select
from datetime import date, datetime
import logging
from utils import logs
# 紀錄時間用
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(levelname)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M',
#                     handlers=[logging.FileHandler('D:\log/stock.log', 'w', 'utf-8'), ])
logger = logs.setup_loggers("IndicatorFinal")

logger.warning("[indicatorFinal]Start getting excel data")
# Get the active instance of Excel
xl = win32com.client.GetActiveObject('Excel.Application')
xl_workbook = xl.Workbooks('indicator.xlsm')
xl_worksheet = xl_workbook.Worksheets(1)
company_symbol = xl_worksheet.Cells(1.1).Value.split(' ')[0]
print(company_symbol)
# 找到日期的最後一行
last_column = xl_worksheet.Cells(2, xl_worksheet.Columns.Count).End(1).Column
# 找到日期的最後一列
last_row = xl_worksheet.UsedRange.Rows.Count
"""
indicator 固定欄位是
    日期、每股盈餘、季底普通股市值、淨負債、稅前息前折舊前淨利、營業收入淨額、
    自由現金流量(D)、負債及股東權益總額、常續性稅後淨利、股東權益總額、每股淨值(B)
    來自營運之現金流量、收盤價(元)、本益比-TSE

"""
# 資料從(2,2)開始
# 先固定欄為一筆資料
config = Config()
db = Database()
db_session = db.db_session()
# 從資料庫撈出是哪一間公司，之後需要用他設定foreign key
company = db_session.execute(select(Company).where(Company.company_symbol == company_symbol)).scalars().first()
# 從資料庫撈出 indicator ，之後需要用他設定foreign key，資料庫的指標順序與tej 的一樣，直接放到list 裡面方便等下設定
indicators_from_db = db_session.execute(select(SingleIndicator).where(SingleIndicator.tw_indicator != None)).scalars()
indicators_from_db_list = []
for indicator in indicators_from_db:
    indicators_from_db_list.append(indicator)
indicator_list = []
# 從資料庫撈出period ，之後設定foreign key
periods_from_db = db_session.execute(select(Period)).scalars()
periods_from_db_list = []
for period in periods_from_db:
    periods_from_db_list.append(period)
# 第一行是名稱，第二行(第一筆資料)好像會是空行，所以就挑過他
for col in range(2,last_column+1):
    indicator_time = str(xl_worksheet.Cells(2, col).Value.date())
    period = Period()
    if xl_worksheet.Cells(2, col).Value.date().month ==3:
        period = periods_from_db_list[0]
    if xl_worksheet.Cells(2, col).Value.date().month ==6:
        period = periods_from_db_list[1]
    if xl_worksheet.Cells(2, col).Value.date().month ==9:
        period = periods_from_db_list[2]
    if xl_worksheet.Cells(2, col).Value.date().month ==12:
        period = periods_from_db_list[3]
    # 第一列是公司代碼，第二列是日期，所以從第三列開始抓取基本面資料，持續到第15列
    # 這裡是寫死的BUG ，修正成動態LAST ROW。by Haley Lee 23/10/05
    for row in range(3, last_row):
        # 抓出indicator
        indicator = xl_worksheet.Cells(row, col).Value
        one_indicator = FinancialReportIndicatorValue(date=indicator_time, indicator_value=indicator)
        # 將foreign key 補上
        one_indicator.company         = company
        one_indicator.period          = period
        one_indicator.singleIndicator = indicators_from_db_list[row-3]
        indicator_list.append(one_indicator)
logger.warning("[IndicatorFinal]start inserting data into db")
# 資料庫連接
db_session.add_all(indicator_list)
db_session.commit()
logger.warning("[IndicatorFinal]end inserting data into db" + company.company_symbol)
