
from Database.database_components import Industry, IndustryValue, Company
from sqlalchemy.sql.expression import select
from Database.Database import Database
from CollectorFactory.CollectorAbstractClass import IndustryCollector
import pandas as pd
import os
import pickle
from utils import logs


class TWIndustryCollector(IndustryCollector):
    def __init__(self):
        self._db     = Database()
        self._logger = logs.setup_loggers("TWIndustryCollector")

    def get_all_data(self):
        self._logger.info("讀取 /data/tw_sector.pkl")
        pickle_file = open(os.getcwd() +"/data/tw_sector.pkl", "rb")
        while True:
            try:
                objects = pickle.load(pickle_file)
                
            except EOFError:
                break
        pickle_file.close()
        # 建立資料庫連接
        db_session = self._db.db_session()

        # 先找出所有的industries，並存放成物件到dict 裡面，key為subindustry 名稱、value 為industry 物件
        industry_obj_dict = {}
        db_session = self._db.db_session()
        industry_list = db_session.execute(select(Industry).where(Industry.type == "TWSE")).scalars()
        for industry in industry_list:
            industry_obj_dict[industry.subindustry_name] = industry

        industries_values_list = []
        for sub_industry, value in objects.items():
            self._logger.info("開始"+sub_industry)
            for symbol, name in value.items():
                company_obj = db_session.execute(select(Company).where(Company.company_symbol == symbol)).scalars().first()
                # 如果這間公司在資料庫裡面沒有的話，就跳過他
                if company_obj is None:
                    continue
                single_industry_value = IndustryValue()
                single_industry_value.company = company_obj
                single_industry_value.industry = industry_obj_dict[sub_industry]
                industries_values_list.append(single_industry_value)
        
        db_session.add_all(industries_values_list)
        db_session.commit()

if __name__ == "__main__":
        tw = TWIndustryCollector()
        tw.get_all_data()