from datetime import date, datetime
from logging import ERROR, debug
from numpy.core.arrayprint import printoptions
from numpy.core.numeric import zeros_like
from numpy.testing._private.utils import print_assert_equal
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.coercions import WhereHavingImpl
from sqlalchemy.sql.expression import false
from sqlalchemy.sql.sqltypes import DateTime, Time
from CollectorFactory.CollectorAbstractClass import FactorCollector
from utils.config import Config
from Database.Database import Database
from Database.database_components import Company, Factor, FactorValue, FinancialReportIndicatorValue, SingleIndicator, Stock
from sqlalchemy import select, and_
import pandas as pd
from utils import logs


class TWFactorCollector(FactorCollector):
    def __init__(self):
        self._config  = Config()
        self._db      = Database()
        self._logger  = logs.setup_loggers("TWFactorCollector")
    
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
    
    def get_data(self, symbol):
        """
        計算出一間公司所需的因子資料
        參數：公司代碼
        回傳值：無，資料直接匯入資料庫
        作法：
        1. 先將台股所有指標名稱抓出來，再將它放在dict 裡面的key。
        2. 選出這間公司所有的財報日期(這麼做是因為有些是新公司不會有更古老以前的財報)
        3. for loop 所有的財報日期並計算每一季的因子數值，append 到data_to_commit 裡面
        4. 最後統一匯入到資料庫(避免頻繁匯入導致速度慢)
        """

        data_dict = {}
        db_session = self._db.db_session()
        # 抓出台股所有的指標名稱列表
        indicator_list = db_session.execute(
            select(SingleIndicator.tw_indicator).where(SingleIndicator.tw_indicator != None)
        ).scalars()
        for indicator in indicator_list:
            data_dict[indicator] = 0 
        # 先選出這間公司有的財報日期
        report_times = db_session.execute(
            select(FinancialReportIndicatorValue).join(Company).where(Company.company_symbol == symbol).group_by(FinancialReportIndicatorValue.date)
        ).scalars()
        print("~report_times: ", report_times)
        factors = db_session.execute(
            select(Factor)
        ).scalars()
        factor_obj_dict = {}
        for factor in factors:
            factor_obj_dict[factor.factor_name] = factor
        print("~factor_obj_dict: ", factor_obj_dict)
        data_to_commit = []

        # 再依據財報日期，將每一季的所有數值抓出來存到data_dict
        for report_time in report_times: 
            # 230110 Modify by peiwen 修改查詢語法，把Company提出，一次join三張表太花時間
            # 先找company object
            company_object = db_session.execute(
                select(Company).where(Company.company_symbol == symbol)
            ).scalars().first()
            indicators = db_session.execute(
                select(FinancialReportIndicatorValue, SingleIndicator).join(SingleIndicator).where(and_(FinancialReportIndicatorValue.company_id == company_object.id, FinancialReportIndicatorValue.date == report_time.date))
            )  
            # indicators = db_session.execute(
            #     select(FinancialReportIndicatorValue, Company, SingleIndicator).join(Company).join(SingleIndicator).where(and_(Company.company_symbol == symbol, FinancialReportIndicatorValue.date == report_time.date))
            # )
            data_dict["time"] = report_time.date
            print("~indicators: ", indicators)
            for row in indicators:
                # company   = row.Company
                company = company_object
                period_id = row.FinancialReportIndicatorValue.period_id
                if row.FinancialReportIndicatorValue.indicator_value is None:
                    data_dict[row.SingleIndicator.tw_indicator] = None
                else:
                    data_dict[row.SingleIndicator.us_indicator] = row.FinancialReportIndicatorValue.indicator_value

            # print("~data_dict: ", data_dict)

            # 備註格式 
            # [factor.name] [factor.factor_formula]: [singleindicator.us_indicator]
            # PB 股價淨值比(P/B): 股價淨值比-TSE
            pb = data_dict["PriceToBookRatio_TSE"]
            data_to_commit.append(self._to_factorValue_obj(company, pb, period_id, factor_obj_dict["PB"], report_time.date))

            # PS 股價營收比(P/S): 股價營收比-TEJ
            ps = data_dict["PriceToRevenueRatio_TEJ"]
            data_to_commit.append(self._to_factorValue_obj(company, ps, period_id, factor_obj_dict["PS"], report_time.date))

            # PE 本益比(P/E): 本益比-TEJ
            pe = data_dict["PE_Ratio_TEJ"]
            data_to_commit.append(self._to_factorValue_obj(company, pe, period_id, factor_obj_dict["PE"], report_time.date))

            # ROA(C) 資產報酬率(稅/息/折舊前)(ROA): ROA(C)稅前息前折舊前
            roa_c = data_dict["ROA_PreTax"]
            data_to_commit.append(self._to_factorValue_obj(company, roa_c, period_id, factor_obj_dict["ROA(C)"], report_time.date))

            # ROA(B) 資產報酬率(稅後/息前折舊前) (ROA): RROA(B)稅後息前折舊前
            roa_b = data_dict["ROA_AfterTax"]
            data_to_commit.append(self._to_factorValue_obj(company, roa_b, period_id, factor_obj_dict["ROA(B)"], report_time.date))

             # ROA(A) 資產報酬率(稅後/息前) (ROA): ROA(A)稅後息前
            roa_a = data_dict["ROA_AfterTax_PreInterest"]
            data_to_commit.append(self._to_factorValue_obj(company, roa_a, period_id, factor_obj_dict["ROA(A)"], report_time.date))
            
            # GPM 營業毛利率: 營業毛利率
            gpm = data_dict["GrossProfitMargin"]
            data_to_commit.append(self._to_factorValue_obj(company, gpm, period_id, factor_obj_dict["GPM"], report_time.date))
                       
            # RGPMS 已實現銷貨毛利率: 已實現銷貨毛利率
            rgpms = data_dict["RealizedGrossProfit"]
            data_to_commit.append(self._to_factorValue_obj(company, rgpms, period_id, factor_obj_dict["RGPMS"], report_time.date))
            
            # OPM 營業利益率: 營業利益率
            opm = data_dict["OperatingProfitMargin"]
            data_to_commit.append(self._to_factorValue_obj(company, opm, period_id, factor_obj_dict["OPM"], report_time.date))
            
            # RIR(A) 稅後常續利益率: 常續利益率－稅後
            rir_a = data_dict["RecurringEarningsMargin_AfterTax"]
            data_to_commit.append(self._to_factorValue_obj(company, rir_a, period_id, factor_obj_dict["RIR(A)"], report_time.date))

            # EBITDA 稅/息/折舊前淨利率: 稅前息前折舊前淨利率
            ebitda = data_dict["PreTaxNetProfitMargin"]
            data_to_commit.append(self._to_factorValue_obj(company, ebitda, period_id, factor_obj_dict["EBITDA"], report_time.date))

            # EBTM 稅前淨利率: 稅前淨利率
            ebtm = data_dict["PreTaxNetProfitRate"]
            data_to_commit.append(self._to_factorValue_obj(company, ebtm, period_id, factor_obj_dict["EBTM"], report_time.date))
            
            # NIM 稅後淨利率: 稅後淨利率
            nim = data_dict["AfterTaxNetProfitRate"]
            data_to_commit.append(self._to_factorValue_obj(company, nim, period_id, factor_obj_dict["NIM"], report_time.date))

            # MV 季底普通股市值: 季底普通股市值
            mv = data_dict["EndofQuarterMarketValueofOrdinaryShares"]
            data_to_commit.append(self._to_factorValue_obj(company, mv, period_id, factor_obj_dict["MV"], report_time.date))
 
            # BETA 系統風險beta: 股價資料庫.股價報酬Beta
            beta = data_dict["CAPM_Beta_ThreeMonths"]
            data_to_commit.append(self._to_factorValue_obj(company, beta, period_id, factor_obj_dict["BETA"], report_time.date))                                                                                
            
            # RGR 營收成長率: 營收成長率
            rgr = data_dict["RevenueGrowthRate"]
            data_to_commit.append(self._to_factorValue_obj(company, rgr, period_id, factor_obj_dict["RGR"], report_time.date))                                                                                
            
            # NV_A 淨值/資產: 淨值/資產
            nv_a = data_dict["EquityAssetRatio"]
            data_to_commit.append(self._to_factorValue_obj(company, nv_a, period_id, factor_obj_dict["NV_A"], report_time.date))                                                                                
            
            # ATR 總資產周轉次數: 總資產週轉次數
            atr = data_dict["EquityAssetRatio"]
            data_to_commit.append(self._to_factorValue_obj(company, atr, period_id, factor_obj_dict["ATR"], report_time.date))                                                                                
            
            # OIE_R 業外收支/營收: 業外收支/營收
            oie_r = data_dict["NonOperatingIncomeandExpendituretoRevenue"]
            data_to_commit.append(self._to_factorValue_obj(company, oie_r, period_id, factor_obj_dict["OIE_R"], report_time.date))                                                                                
                   
            # # PE: 本益比TSE
            # pe = data_dict["本益比-TSE"]
            # data_to_commit.append(self._to_factorValue_obj(company, pe, period_id, factor_obj_dict["PE"], report_time.date))

            # # PB 股價淨值比(P/B): 股價淨值比-TSE
            # try:
            #     ev_ebita = (data_dict["季底普通股市值"]+data_dict["淨負債"])/data_dict["稅前息前折舊前淨利"]
            # except ZeroDivisionError:
            #     ev_ebita = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     ev_ebita = None
            # data_to_commit.append(self._to_factorValue_obj(company, ev_ebita, period_id, factor_obj_dict["EV_EBITDA"], report_time.date))

            # # EV_S: (季底普通股市值 + 淨負債) / 營業收入淨額
            # try:
            #     ev_s = (data_dict["季底普通股市值"]+data_dict["淨負債"])/data_dict["營業收入淨額"]
            # except ZeroDivisionError:
            #     ev_s = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     ev_s = None
            # data_to_commit.append(self._to_factorValue_obj(company, ev_s, period_id, factor_obj_dict["EV_S"], report_time.date))

            # # FCF_P: 自由現金流量(D) / 季底普通股市值
            # try:
            #     fcf_p = data_dict["自由現金流量(D)"]/data_dict["季底普通股市值"]
            # except ZeroDivisionError:
            #     fcf_p = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     fcf_p = None
            # data_to_commit.append(self._to_factorValue_obj(company, fcf_p, period_id, factor_obj_dict["FCF_P"], report_time.date))

            # # CROIC: 自由現金流量(D) / 負債及股東權益總額
            # try:
            #     croic = data_dict["自由現金流量(D)"]/data_dict["負債及股東權益總額"]
            # except ZeroDivisionError:
            #     croic = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     croic = None
            # data_to_commit.append(self._to_factorValue_obj(company, croic, period_id, factor_obj_dict["CROIC"], report_time.date))

            # # FCF_OI: 自由現金流量(D) / 營業收入淨額
            # try:
            #     fcf_oi = data_dict["自由現金流量(D)"]/data_dict["營業收入淨額"]
            # except ZeroDivisionError:
            #     fcf_oi = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     fcf_oi = None
            # data_to_commit.append(self._to_factorValue_obj(company, fcf_oi, period_id, factor_obj_dict["FCF_OI"], report_time.date))

            # # ROE: 常續性稅後淨利 / 股東權益總額
            # try:
            #     roe =data_dict["常續性稅後淨利"]/ data_dict["股東權益總額"]
            # except ZeroDivisionError:
            #     roe = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     roe = None
            # data_to_commit.append(self._to_factorValue_obj(company, roe, period_id, factor_obj_dict["ROE"], report_time.date))

            # # ROIC: 常續性稅後淨利 / 負債及股東權益總額
            # try:
            #     roic = data_dict["常續性稅後淨利"]/data_dict["負債及股東權益總額"]
            # except ZeroDivisionError:
            #     roic = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     roic = None
            # data_to_commit.append(self._to_factorValue_obj(company, roic, period_id, factor_obj_dict["ROIC"], report_time.date))

            # # PB: 收盤價(元) / 每股淨值(B)
            # try:
            #     pb = data_dict["收盤價(元)"]/data_dict["每股淨值(B)"]
            # except ZeroDivisionError:
            #     pb = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     pb = None
            # data_to_commit.append(self._to_factorValue_obj(company, pb, period_id, factor_obj_dict["PB"], report_time.date))

            # # PS: 季底普通股市值 / 營業收入淨額
            # try: 
            #     ps = data_dict["季底普通股市值"]/ data_dict["營業收入淨額"]
            # except ZeroDivisionError:
            #     ps = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     ps = None
            # data_to_commit.append(self._to_factorValue_obj(company, ps, period_id, factor_obj_dict["PS"], report_time.date))

            # # P_IC: 季底普通股市值 / 負債及股東權益總額
            # try:
            #     p_ic = data_dict["季底普通股市值"]/data_dict["負債及股東權益總額"]
            # except ZeroDivisionError:
            #     p_ic = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     p_ic = None
            # data_to_commit.append(self._to_factorValue_obj(company, p_ic, period_id, factor_obj_dict["P_IC"], report_time.date))

            # # OCF_E: 來自營運之現金流量 / 股東權益總額
            # try:
            #     ocf_e = data_dict["來自營運之現金流量"]/ data_dict["股東權益總額"]
            # except ZeroDivisionError:
            #     ocf_e = None
            # except:
            #     self._logger.error("Catch an exception.", exc_info=True)
            #     ocf_e = None
            # data_to_commit.append(self._to_factorValue_obj(company, ocf_e, period_id, factor_obj_dict["OCF_E"], report_time.date))

            # # MOM: (財報發布日收盤價-財報截止日收盤價) / 財報截止日收盤價
            # # mom = self._calculate_mom(report_time.date, symbol, db_session)
            # mom = -1
            # data_to_commit.append(self._to_factorValue_obj(company, mom, period_id, factor_obj_dict["MOM"], report_time.date))



        self._logger.warning("factor 計算結束，資料匯入開始")
        db_session.add_all(data_to_commit)
        db_session.commit()

    def _to_factorValue_obj(self, company, factor_value, period_id, factor_obj, date):
        """
        將指標轉換成資料庫的物件
        傳入參數：公司物件, 因子數值, 年或季, 因子物件, 因子時間
        """
        factor_value = FactorValue(date = date, factor_value=factor_value)
        factor_value.company = company
        factor_value.period_id = period_id
        factor_value.factor = factor_obj
        return factor_value

    def _calculate_mom(self, report_date, symbol, db_session:Session):
        """
        用來計算台股動能面指標 MOM
        計算方式：(財報發布日收盤-財報截止日收盤)/財報截止日收盤
        傳入參數：財報截止日期, 公司代碼, 資料庫session
        回傳值  ：動能因子MOM
        """
        # 對應台股財報截止日的財報發布日，key 會截止日的月份對應到的是發布日的日期
        report_announce_dates = {12:'03-31', 3:'05-15', 6:'08-14', 9:'11-14'}
        month = report_date.month
        year  = report_date.year
        if month == 12:
            year+=1
        try:
            report_announce_date = datetime.strptime(f"{year}-{report_announce_dates[month]}", "%Y-%m-%d")        
            report_day = db_session.execute(
            select(Stock).join(Company).where(and_(Company.company_symbol == symbol, Stock.date <=report_date))
            ).scalars().first()
            report_announce_day = db_session.execute(
            select(Stock).join(Company).where(and_(Company.company_symbol == symbol, Stock.date<= report_announce_date))
            ).scalars().first()
            print("report_announce_day:", report_announce_day)
        # (財報發布日收盤-財報截止日收盤)/財報截止日收盤
        except:
            self._logger.error("財報時間錯誤或是資料遺失", exc_info=True)

        try:    
            mom = (report_announce_day.close-report_day.close) / report_day.close
        except ZeroDivisionError:
            mom = 0
        except:
            self._logger.error("Catch an exception.", exc_info=True)
            mom = 0
        
        return mom

    def get_all_data(self):
        symbols = self.get_company()
        for symbol in symbols:
            print("start",symbol)
            self.get_data(symbol)
if __name__ == "__main__":
    tw = TWFactorCollector()
    # taiwan50 = [2330,2454,2317,2303,2881,2308,1303,2882,1301,2002,3711,
    #             2412,2891,2886,5871,2603,2884,1216,2885,1326,3008,2615,
    #             3034,1101,2379,6415,2892,2357,2327,5880,2382,2880,2887,
    #             2609,2207,2395,3045,2409,2912,5876,1590,4938,6505,1402,
    #             2801,1102,4904,9910,8046,2408]
    stock_list = [1101,1216]
    for symbol in stock_list:
        tw._logger.warning("Start "+ str(symbol))
        tw.get_data(symbol)
    # tw.get_all_data()