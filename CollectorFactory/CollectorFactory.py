from CollectorFactory.TWCollectorFactory.TWFactorCollector import TWFactorCollector
from CollectorFactory.USCollectorFactory.USFactorCollector import USFactorCollector
from CollectorFactory.TWCollectorFactory.TWIndustryPriceCollector import TWIndustryPriceCollector
from CollectorFactory.TWCollectorFactory.TWStockCollector import TWStockCollector
from CollectorFactory.USCollectorFactory.USIndustryCollector import USIndustryCollector
from CollectorFactory.TWCollectorFactory.TWIndustryCollector import TWIndustryCollector
from CollectorFactory.USCollectorFactory.USIndustryPriceCollector import USIndustryPriceCollector
from CollectorFactory.USCollectorFactory.USPortfolioCollector import USPortfolioCollector
from CollectorFactory.TWCollectorFactory.TWPortfolioCollector import TWPortfolioCollector
from CollectorFactory.USCollectorFactory.USStockCollector import USStockCollector
from CollectorFactory.TWCollectorFactory.TWCompanyCollector import TWCompanyCollector
from CollectorFactory.USCollectorFactory.USCompanyCollector import USCompanyCollector
from CollectorFactory.TWCollectorFactory.TWIndicatorCollector import TWIndicatorCollector
from CollectorFactory.USCollectorFactory.USIndicatorCollector import USIndicatorCollector


class CollectorFactory():
    def __init__(self, country):
        self.country = country
    
    def StockCollector(self):
        stock_collector_table = {
            "US": USStockCollector(),
            "TW": TWStockCollector(),
        }
        return stock_collector_table[self.country]

    def CompanyCollector(self):
        company_collector_table = {
            "US": USCompanyCollector(),
            "TW": TWCompanyCollector(),
        }
        return company_collector_table[self.country]

    def IndicatorCollector(self):
        indicator_collector_table = {
            "US": USIndicatorCollector(),
            "TW": TWIndicatorCollector(),
        }
        return indicator_collector_table[self.country]

    def FactorCollector(self):
        factor_collector_table = {
            "US": USFactorCollector(),
            "TW": TWFactorCollector(),
        }
        return factor_collector_table[self.country]

    def IndustryCollecor(self):
        industry_collector_table = {
            "US": USIndustryCollector(),
            "TW": TWIndustryCollector(),
        }
        return industry_collector_table[self.country]
    
    def IndustryPriceCollector(self):
        industryPrice_collector_table = {
            "US": USIndustryPriceCollector(),
            "TW": TWIndustryPriceCollector(),
        }
        return industryPrice_collector_table[self.country]

    def PortfolioCollector(self):
        portfolio_collector_table = {
            "US": USPortfolioCollector(),
            "TW": TWPortfolioCollector()
        }
        return portfolio_collector_table[self.country]

        
if __name__ == "__main__":
    us_c = CollectorFactory("US")
    us_s = us_c.StockCollector()
    res = us_s.get_stock_data("AAPL")
    for x in res:
        print(x.date)
