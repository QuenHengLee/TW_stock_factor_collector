from CollectorFactory.CollectorAbstractClass import PortfolioCollector
from Database.Database import Database
from Database.database_components import PortfolioValue, Portfolio, Company
from utils import logs
from utils.config import Config
from sqlalchemy.sql.expression import select


class TWPortfolioCollector(PortfolioCollector):
    def __init__(self):
        self._config = Config()
        self._db = Database()
        self._logger = logs.setup_loggers("TWPortfolioCollector")

    def get_data(self, portfolio: Portfolio):
        """
        抓單一指數的所有公司，並將其對應
        參數：portfolio(指數)，需要將成分股匯入的指數物件
        回傳：無，直接存進資料庫
        """
        insert_list = []
        if portfolio.name == "taiwan50":
            # 台灣50 成分股
            taiwan50 = [2330, 2454, 2317, 2303, 2881, 2308, 1303, 2882, 1301, 2002, 3711,
                        2412, 2891, 2886, 5871, 2603, 2884, 1216, 2885, 1326, 3008, 2615,
                        3034, 1101, 2379, 6415, 2892, 2357, 2327, 5880, 2382, 2880, 2887,
                        2609, 2207, 2395, 3045, 2409, 2912, 5876, 1590, 4938, 6505, 1402,
                        2801, 1102, 4904, 9910, 8046, 2408]

            for company in taiwan50:
                company_obj = self.db_session.execute(
                    select(Company).where(Company.company_symbol == company)
                ).scalars().first()

                if company_obj is None:
                    self._logger.warning(f"taiwan50 成分股 {company} 不存在資料庫")
                    continue

                single_portfoliovalue = PortfolioValue()
                single_portfoliovalue.company = company_obj
                single_portfoliovalue.portfolio = portfolio
                insert_list.append(single_portfoliovalue)

        self.db_session.add_all(insert_list)
        self.db_session.commit()

    def get_all_data(self):
        """
        將所有的指數與其組成公司相呼對應
        作法：先將所有的台股指數抓出來，並使用迴圈執行 get_data()
        """
        self.db_session = self._db.db_session()
        portfolio_list = self.db_session.execute(
            select(Portfolio).where(Portfolio.country == "TW")).scalars()

        for portfolio in portfolio_list:
            try:
                self.get_data(portfolio)
            except:
                self._logger.error(f"資料匯入失敗 {portfolio.name}", exc_info=True)


if __name__ == "__main__":
    us = TWPortfolioCollector()
    db_session = us._db.db_session()
    portfolio = db_session.execute(
        select(Portfolio).where(Portfolio.name == "Taiwan50")).scalars().first()
    db_session.close()
    us.get_data(portfolio)
