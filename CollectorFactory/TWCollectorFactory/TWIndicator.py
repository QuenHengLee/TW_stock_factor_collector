from CollectorFactory.CollectorAbstractClass import IndicatorCollector
from Database.Database import Database

class TWIndicatorCollector(IndicatorCollector):
    def __init__(self):
        self._DB = Database()

    def get_company(self):
        """
        取得資料庫 Company 中的所有公司
        參數 : 無
        回傳值 : generator
        """
        statement = "SELECT `company_symbol` FROM `company`"
        return (company["company_symbol"] for company in self._DB.select_data(statement))
    
    def get_data(self, symbol=None):
        pass

    def get_all_data(self):
        company_list = self.get_company()
        