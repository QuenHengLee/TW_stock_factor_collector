from abc import ABCMeta, abstractmethod

class CompanyCollector(metaclass=ABCMeta):
    @abstractmethod
    def get_company(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def get_all_data(self):
        pass

class StockCollector(metaclass=ABCMeta):
    @abstractmethod
    def get_company(self):
        pass

    @abstractmethod
    def get_data(self):
        pass
    
    @abstractmethod
    def get_all_data(self):
        pass

class IndicatorCollector(metaclass=ABCMeta):
    @abstractmethod
    def get_company(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def get_all_data(self):
        pass

class FactorCollector(metaclass=ABCMeta):
    @abstractmethod
    def get_company(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def get_all_data(self):
        pass

class IndustryCollector(metaclass=ABCMeta):
    @abstractmethod
    def get_all_data(self):
        pass

class IndustryPriceCollector(metaclass=ABCMeta):
    @abstractmethod
    def get_all_data(self):
        pass

class PortfolioCollector(metaclass=ABCMeta):
    @abstractmethod
    def get_data(self):
        pass
    @abstractmethod
    def get_all_data(self):
        pass