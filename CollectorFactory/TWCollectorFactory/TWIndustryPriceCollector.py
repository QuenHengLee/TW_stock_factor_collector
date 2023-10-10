
from decimal import InvalidOperation
from Database.database_components import Industry, IndustryPrice, IndustryValue, Company
from sqlalchemy.sql.expression import select
from Database.Database import Database
from CollectorFactory.CollectorAbstractClass import IndustryPriceCollector
import pandas as pd
import os
import pickle
from utils import logs

class TWIndustryPriceCollector(IndustryPriceCollector):
    def __init__(self):
        self._db = Database()
        self._logger = logs.setup_loggers("TWIndustryPriceCollector")
    def get_all_data(self):
        pickle_file = open("data/Restore_marketValue_weighted_TWSE-listed.pkl", "rb")
        while True:
            try:
                objects = pickle.load(pickle_file)
                
            except EOFError:
                break
        pickle_file.close()

        # 先找出所有的industries，並存放成物件到dict 裡面，key為subindustry 名稱、value 為industry 物件
        industry_obj_dict = {}
        db_session = self._db.db_session()

        industry_list = db_session.execute(select(Industry).where(Industry.type == "TWSE")).scalars()
        for industry in industry_list:
            industry_obj_dict[industry.subindustry_name] = industry


        for sector_symbol, value in objects.items():
            industry_price_list = []

            self._logger.info("開始 "+str(sector_symbol))
            for index, sector_data in value.iterrows():
                single_industry_value = IndustryPrice(date=sector_data['date'], close=sector_data['Close'], volume=sector_data['Volume'], vol_price=sector_data['vol_price'])
                single_industry_value.industry = industry_obj_dict[sector_data['name']]
                industry_price_list.append(single_industry_value)
                
            db_session.add_all(industry_price_list)
            db_session.commit()

if __name__ == "__main__":
    tw = TWIndustryPriceCollector()
    tw.get_all_data()