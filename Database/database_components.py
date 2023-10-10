from datetime import date, datetime
from pandas.core.arrays import string_
from sqlalchemy.sql.expression import null
from sqlalchemy.sql.operators import collate
from sqlalchemy.sql.sqltypes import JSON, TIMESTAMP
from sqlalchemy.sql.visitors import cloned_traverse
from Database.Database import Database
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String, ForeignKey, DateTime, Float, BigInteger, Date, LargeBinary
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
import logging

mapper_registry = registry()
Base = mapper_registry.generate_base()
metadata = MetaData()


class Company(Base):
    '''
    公司
    註:一間實體公司可以有多項虛擬公司，若他在多個交易所上市，則會有多個虛擬公司
    '''
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_symbol = Column(String(50), nullable=False)
    name = Column(String(400), nullable=True)
    exchange_name = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)
    
    # 第一個參數是你要對應到的class名稱，back_populates 是在他的class 裡面你的變數名稱(通常都是小寫的class名)
    stock = relationship('Stock', back_populates="company")
    # conceptValue = relationship("ConceptValue", back_populates="company")
    # industryValue = relationship("IndustryValue", back_populates="company")
    financialReportIndicatorValue = relationship('FinancialReportIndicatorValue', back_populates="company")
    factorValue = relationship('FactorValue', back_populates="company")
    # portfolioValue  = relationship("PortfolioValue", back_populates="company")

class Period(Base):
    """
    財報所表示的季與年
    """
    __tablename__ = 'period'
    id = Column(Integer, primary_key=True, autoincrement=True)
    period_name = Column(String(10))

    financialReportIndicatorValue = relationship('FinancialReportIndicatorValue', back_populates="period")
    factorValue = relationship('FactorValue', back_populates="period")

class Factor(Base):
    """
    因子，基於公司財報基本面計算出來，用於執行換股策略
    此列表紀錄所使用的因子名稱以及他的計算方式
    """
    __tablename__ = 'factor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    factor_name = Column(String(255))
    factor_formula = Column(String(255))

    factorValue = relationship('FactorValue', back_populates="factor")

class FactorValue(Base):
    """
    因子數值：
    此列表記錄各公司的各個因子的數值，每一筆紀錄一間公司對應的一個因子的數值
    """
    __tablename__ = 'factorvalue'
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id'))
    factor_id = Column(Integer,ForeignKey('factor.id'))
    date = Column(Date, nullable=False)
    period_id = Column(Integer, ForeignKey('period.id'))
    factor_value = Column(Float)

    company = relationship('Company', back_populates="factorValue")
    factor = relationship('Factor', back_populates="factorValue")
    period = relationship('Period', back_populates="factorValue")

class SingleIndicator(Base):
    """
    基本面指標：
    此列表紀錄所有使用到的基本面指標，且該指標對應到各國的名稱
    ex. eps
    """
    __tablename__ = 'singleindicator'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tw_indicator = Column(String(255), nullable=True)
    us_indicator = Column(String(255), nullable=True)
    cn_indicator = Column(String(255), nullable=True)

    financialReportIndicatorValue = relationship('FinancialReportIndicatorValue', back_populates="singleIndicator")

class FinancialReportIndicatorValue(Base):
    """
    基本面指標數值
    各個公司各個指標的數值，單一資料為單一間公司對應到的該指標數值
    ex. 台積電, eps, 10
    """
    __tablename__ = 'financialreportindicatorvalue'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id'))
    date = Column(Date, nullable=False)
    period_id = Column(Integer, ForeignKey('period.id'))
    indicator_id = Column(Integer, ForeignKey('singleindicator.id'))
    indicator_value = Column(Float)

    company = relationship('Company', back_populates="financialReportIndicatorValue")
    period = relationship('Period', back_populates="financialReportIndicatorValue")
    singleIndicator = relationship('SingleIndicator', back_populates="financialReportIndicatorValue")

class Stock(Base):
    """
    價量資訊
    單一間公司在單一日期的開、高、低、收、量
    """
    __tablename__ = 'stock'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    company_id = Column(Integer, ForeignKey('company.id'))

    company = relationship('Company', back_populates="stock")

# class Concept(Base):
#     '''
#     概念股
#     目前只有台股使用到、股票分類的一種
#     '''
#     __tablename__ ='concept'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     concept_name = Column(String(50), nullable=False)

#     conceptValue = relationship("ConceptValue", back_populates="concept")
#     conceptPrice = relationship('ConceptPrice', back_populates="concept")

# class ConceptValue(Base):
#     '''
#     公司對應到的概念股
#     屬於多對多新建之表，公司可以屬於多個概念股類別，一個概念股類別可以有多間公司
#     '''
#     __tablename__ = 'concpetvalue'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     company_id = Column(Integer, ForeignKey('company.id'))
#     concept_id = Column(Integer, ForeignKey('concept.id'))

#     company = relationship("Company", back_populates="conceptValue")
#     concept = relationship("Concept", back_populates="conceptValue")

# class ConceptPrice(Base):
#     """
#     概念股價格：
#     單一概念股裡面所有的成分股所組成的價量資訊
#     """
#     __tablename__ = 'conceptprice'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     date = Column(Date, nullable=False)
#     open = Column(Float)
#     high = Column(Float)
#     low = Column(Float)
#     close = Column(Float)
#     volume = Column(Integer)
#     concept_id = Column(Integer, ForeignKey('concept.id'))

#     concept = relationship('Concept', back_populates="conceptPrice")

# class Industry(Base):
#     """
#     類股(產業):
#     各國家各類股組成方式、並不一定每一個國家都有官方的類股分類法，所以type為記錄是哪一種的類股分類法
#     此類股分類並無與任何分類法的名稱掛勾，在此sector為最大類，其次為indudsty、最後則為subindustry
#     此三層分類目前已夠美股與台股使用
#     """
#     __tablename__ ='industry'
#     id               = Column(Integer, primary_key=True, autoincrement=True)
#     sector_name      = Column(String(50))
#     industry_name    = Column(String(100))
#     subindustry_name = Column(String(100))
#     type             = Column(String(50)) 

#     industryValue = relationship("IndustryValue", back_populates="industry")
#     industryPrice = relationship('IndustryPrice', back_populates="industry")

# class IndustryValue(Base):
#     """
#     公司對應到的類股
#     這是多對多生成的新表，單一筆資料為一間公司對應到的類股，對應到的是最小的subindustry
#     """
#     __tablename__ = 'industryvalue'
#     id          = Column(Integer, primary_key=True, autoincrement=True)
#     company_id  = Column(Integer, ForeignKey('company.id'))
#     industry_id = Column(Integer, ForeignKey('industry.id'))

#     company  = relationship("Company", back_populates="industryValue")
#     industry = relationship("Industry", back_populates="industryValue")

# class IndustryPrice(Base):
#     """
#     類股價量：
#     與概念股相同、為類股的所有公司每天的收盤價相加與「收盤價乘買賣量」相加
#     """
#     __tablename__ = 'industryprice'
#     id          = Column(Integer, primary_key=True, autoincrement=True)
#     date        = Column(Date, nullable=False)
#     close       = Column(Float)
#     volume      = Column(BigInteger)
#     vol_price   = Column(Float)
#     industry_id = Column(Integer, ForeignKey('industry.id'))

#     industry    = relationship('Industry', back_populates="industryPrice")

# class ModelResult(Base):
#     """
#     模型結果：
#     此表儲存實驗室所有的模型計算的結果，方便分析與前端使用
#     """
#     __tablename__ = 'modelresult'
#     id              = Column(Integer, primary_key=True, autoincrement=True)
#     model_id        = Column(Integer, ForeignKey('model.id'))
#     name            = Column(String(50), nullable=False)
#     daily_equity    = Column(JSON)
#     trade_history   = Column(JSON)
#     start_date      = Column(JSON, nullable=True)
#     cagr            = Column(Float, nullable=True)
#     return_by_year  = Column(JSON, nullable=True)
#     return_by_month = Column(JSON, nullable=True)
#     daily_drawdown  = Column(JSON, nullable=True)
#     max_drawdown    = Column(Float, nullable=True)
#     mar             = Column(Float, nullable=True)
#     attribute1      = Column(String(10), nullable=True)
#     attribute2      = Column(String(10), nullable=True)
#     attribute3      = Column(String(10), nullable=True)
#     attribute4      = Column(String(10), nullable=True)
#     attribute5      = Column(String(10), nullable=True)
#     attribute6      = Column(String(10), nullable=True)
#     attribute7      = Column(String(10), nullable=True)
#     attribute8      = Column(String(10), nullable=True)
#     update_at       = Column(Date, nullable=False, onupdate=date.today(), default=date.today())

#     model         = relationship("Model", back_populates="modelResult")

# class Model(Base):
#     """
#     記錄所有系統會用到的模型
#     """
#     __tablename__ = 'model'
#     id          = Column(Integer, primary_key=True, autoincrement=True)
#     name        = Column(String(50), nullable=False)
#     description = Column(String(500), nullable=True)

#     modelResult  = relationship("ModelResult", back_populates="model")

# class Portfolio(Base):
#     """
#     記錄所有會用到股票組合，像是道瓊、台灣50
#     """
#     __tablename__ = 'portfolio'
#     id            = Column(Integer, primary_key=True, autoincrement=True)
#     name          = Column(String(50), nullable=False)
#     description   = Column(String(500), nullable=True)
#     # 目前有紀錄國家的只有 portfolio 與 company 兩張表，之後看要不要將這個屬性獨立分開
#     country       = Column(String(10), nullable=True)

#     portfolioValue  = relationship("PortfolioValue", back_populates="portfolio")

# class PortfolioValue(Base):
#     __tablename__ = 'portfoliovalue'
#     id            = Column(Integer, primary_key=True, autoincrement=True)
#     company_id    = Column(Integer, ForeignKey('company.id'))
#     portfolio_id  = Column(Integer, ForeignKey('portfolio.id'))

#     company   = relationship("Company", back_populates="portfolioValue")
#     portfolio = relationship("Portfolio", back_populates="portfolioValue")

# class TradingDays(Base):
#     """
#     220827 add by peiwen 記錄台美股交易日
#     """
#     __tablename__ = 'tradingdays'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     date = Column(Date, nullable=False)


def createDatabase(myengine):
    Base.metadata.create_all(myengine)

if __name__ == "__main__":
    db = Database()
    myengine = db.connection()
    Base.metadata.create_all(myengine)
