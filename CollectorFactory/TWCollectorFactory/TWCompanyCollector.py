from os import name
from Database.database_components import Company
from Database.Database import Database
from CollectorFactory.CollectorAbstractClass import CompanyCollector
from utils.config import Config
import logging
from utils import logs
import pickle
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session
import os

class TWCompanyCollector(CompanyCollector):
    def __init__(self):
        self._config  = Config()
        self._db  = Database()
        self._logger = logs.setup_loggers("TWCompanyCollector")

    def get_company(self):
        """
        取得台灣交易所的所有公司的公司代號。
        參數 : 無
        回傳值 : 正常回傳為list，異常為None
        """
        company_list = []
        company_df = pd.read_csv(os.getcwd() + '/data/tw_companys.csv')
        # 把公司的代碼讀出  並把重複的去除
        company_list = company_df['symbols'].unique().tolist()
        # print(company_list)
        return company_list


    def get_data(self, symbol=None):
        """
        取得一家公司的公司資訊。
        參數 : 公司代號
        回傳值 : 無  直接進去資料庫
        """
        if symbol is None:
            self._logger.error("[CollectorFactory][TWCollectorFactory][TWCompanyCollector]::未輸入欲查找的公司代號")
            return None

        # TEJ CSV 資料
        company_list = []
        # company_df = pd.read_csv(self._share_path+'/tw_companys.csv')
        company_df = pd.read_csv(os.getcwd() + '/data/tw_companys.csv')
        company_df.set_index("symbols", inplace=True)
        company_df.loc[symbol]
        # 資料庫連接
        db_session = self._db.db_session()
        try:
            company_name = company_df.loc[symbol].names
            print(company_name)
            db_session.add(Company(company_symbol=symbol, name=company_name, exchange_name="TWSE", country='TW'))
            db_session.commit()
        except:
            self._logger.error(f"{symbol} not exist",exc_info=True)

    def get_all_data(self):
        """
        取得台灣所有公司的公司資訊。
        參數 : 無
        回傳值 : 無 直接進資料庫
        """

        companys = self.get_company()
        for company in companys:
            try:
                self.get_data(company)
            except:
                self._logger.error("[CollectorFactory][TWCollectorFactory][TWCompanyCollector]::此公司無資料"+str(company))
            

if __name__ == "__main__":
    t = TWCompanyCollector()
    t.get_company()
    # t.get_company('2330')
    # t.get_data(2330)
    t.get_all_data()
    # t.get_company()