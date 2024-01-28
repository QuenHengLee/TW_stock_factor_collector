import logging
from operator import or_
from utils import logs
from sqlalchemy import select, or_
from CollectorFactory.CollectorAbstractClass import IndicatorCollector
from Database.Database import Database
from utils.config import Config
from Database.database_components import Company, FinancialReportIndicatorValue, Period, SingleIndicator
import win32com, win32api, win32process, win32con, win32gui,win32pdh,win32com.client
import time
import os

class TWIndicatorCollector(IndicatorCollector):
    def __init__(self):
        self._config  = Config()
        self._db      = Database()
        self._logger  = logs.setup_loggers("TWIndicatorCollector") 

    def close_excel_by_force(self, excel):
        # Get the window's process id's
        hwnd = excel.Hwnd
        t, p = win32process.GetWindowThreadProcessId(hwnd)
        # Ask window nicely to close
        win32gui.PostMessage(hwnd, win32con.WM_CLOSE, 0, 0)
        # Allow some time for app to close
        time.sleep(10)
        # If the application didn't close, force close
        try:
            handle = win32api.OpenProcess(win32con.PROCESS_TERMINATE, 0, p)
            if handle:
                win32api.TerminateProcess(handle, 0)
                win32api.CloseHandle(handle)
        except:
            pass

    def get_company(self):
        """
        取得資料庫 Company 中台灣的所有公司
        參數 : 無
        回傳值 : generator
        """
        db_session = self._db.db_session()


        companys = db_session.execute(
            select(Company).where(Company.exchange_name == "TWSE"))
            
        # db_session.close()

        return (company.company_symbol for company in companys.scalars())
    
    def get_data(self, symbol=None):
        """
        取得某支股票的價量資料。
        參數 : 
            symbol : 股票的代號，所有的價量時間區間都是寫在 Excel 裡面由tej抓取
        回傳值 : 無，啟動excel vba 程式之後，會由excel 呼叫另外一支程式進行存檔到資料庫的動作
        """
        if symbol is None:
            self._logger.error(
                "[CollectorFactory][TWCollectorFactory][TWInicatorCollector]::未輸入股票代號")
            return None

        isOpen =True
        try:
            xl = win32com.client.GetActiveObject('Excel.Application')
        except:
            isOpen = False
        if isOpen:
            self._logger.warning("存檔並關掉已經打開的excel")
            # 嘗試將所有的 excel 程式關閉
            for i in range(1,xl.Workbooks.Count+1):
                xl.Workbooks(i).Close(True)
            xl.Quit()
            # 有時候無法正常關閉，找到執行續強制關閉
            self.close_excel_by_force(xl)
        # 有時候關閉沒有那麼及時，所以等待一下再開啟此次需要的 excel
        time.sleep(2)
        # 開啟 Excel
        self._logger.warning("開啟新的excel並執行tej")
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = True
        wb_stock = excel.Workbooks.Open(os.getcwd()+"/data/indicator.xlsm")
        ws = wb_stock.Worksheets('report')
        ws.Cells(1, 1).Value = symbol
        # 開啟TEJ 巨集
        refprop_path = 'C:\TEJWIN\TEJ Smart Wizard\excel03menu.xla'
        wb_tej = excel.Workbooks.Open(refprop_path)
        # 執行巨集程式
        excel.Application.Run("indicator.xlsm!refresh")
        wb_tej.Close()
        
    def get_all_data(self):
        symbols = self.get_company()
        for symbol in symbols:
            # 2014/01/27 EDIT BY QUEN
            # 暫時加上去判斷式，之後要移除
            if int(symbol) > 3581:
                isDone = False
                while not isDone:
                    try:
                        list = win32pdh.EnumObjects(None, None , win32pdh.PERF_DETAIL_WIZARD , 1 )
                        junk, process_list = win32pdh.EnumObjectItems(
                        None, None, "process", win32pdh.PERF_DETAIL_WIZARD)
                    except:
                        self._logger.error("[CollectorFactory][TWCollectorFactory][TWStockCollector]::error getting all process")
                    if "TejRefresh" in process_list:
                        time.sleep(10)
                    else:
                        isDone = True
                self._logger.info("start "+ str(symbol))
                self.get_data(symbol)




if __name__ == "__main__":
    
    tw = TWIndicatorCollector()
 
    tw.get_all_data()
    # 下面是執行特定公司的股價方法
    # taiwan50 = [1101,1216,1301,1303,1326,1402,1590,1605,2002,2207,
    #             2303,2308,2317,2327,2330,2357,2379,2382,2395,2408,
    #             2412,2454,2603,2609,2615,2801,2880,2881,2882,2883,
    #             2884,2885,2886,2887,2890,2891,2892,2912,3008,3034,
    #             3037,3045,3711,4904,4938,5871,5876,5880,6505,9910]
    # # taiwan50 = [1101,1216]
    # companys = tw.get_company()
    # notYet5533 = True
    # for symbol in taiwan50:
    # # for symbol in companys:
    # #     if int(symbol) > 5533:
    # #         notYet5533 = False
    # #     if notYet5533:
    # #         continue
    #     isDone = False
    #     while not isDone:
    #         try:
    #             list = win32pdh.EnumObjects(None, None , win32pdh.PERF_DETAIL_WIZARD , 1 )
    #             junk, process_list = win32pdh.EnumObjectItems(
    #             None, None, "process", win32pdh.PERF_DETAIL_WIZARD)
    #         except:
    #             tw._logger.error("[CollectorFactory][TWCollectorFactory][TWStockCollector]::error getting all process")
    #         if "TejRefresh" in process_list:
    #             time.sleep(10)
    #         else:
    #             isDone = True
    #     tw._logger.info("start "+ str(symbol))
    #     tw.get_data(symbol)
        