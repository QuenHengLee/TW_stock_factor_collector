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
            # datadict[XXXX] XXX必填因子的英文，中文會報錯
            # 因子名稱: 財報資料計算公式
            # 但2024年都是用原始的直接轉換，不須再加工處理，所以前後都一樣

            # 因子代號: CLOSE 
            # 因子計算公式: 收盤價(元) 
            CLOSE = data_dict.get("Close", None)
            data_to_commit.append(self._to_factorValue_obj(company, CLOSE, period_id, factor_obj_dict["CLOSE"], report_time.date))
            
            # 因子代號: 
            # 因子計算公式: 市值(百萬元) 
            CAP = data_dict.get("Market_Cap_Million", None)
            data_to_commit.append(self._to_factorValue_obj(company, CAP, period_id, factor_obj_dict["CAP"], report_time.date))

            # 因子代號: PE_TSE 
            # 因子計算公式: 本益比-TSE
            PE_TSE = data_dict.get("PE_Ratio_TSE", None)
            data_to_commit.append(self._to_factorValue_obj(company, PE_TSE, period_id, factor_obj_dict["PE_TSE"], report_time.date))

            # 因子代號: PE 
            # 因子計算公式: 本益比-TEJ
            PE = data_dict.get("PE_Ratio_TEJ", None)
            data_to_commit.append(self._to_factorValue_obj(company, PE, period_id, factor_obj_dict["PE"], report_time.date))

            # 因子代號: PB_TSE 
            # 因子計算公式: 股價淨值比-TSE
            PB_TSE = data_dict.get("PB_Ratio_TSE", None)
            data_to_commit.append(self._to_factorValue_obj(company, PB_TSE, period_id, factor_obj_dict["PB_TSE"], report_time.date))

            # 因子代號: PB 
            # 因子計算公式: 股價淨值比-TEJ
            PB = data_dict.get("PB_Ratio_TEJ", None)
            data_to_commit.append(self._to_factorValue_obj(company, PB, period_id, factor_obj_dict["PB"], report_time.date))

            # 因子代號: PS 
            # 因子計算公式: 股價營收比-TEJ
            PS = data_dict.get("PS_Ratio_TEJ", None)
            data_to_commit.append(self._to_factorValue_obj(company, PS, period_id, factor_obj_dict["PS"], report_time.date))

            # 因子代號: DY 
            # 因子計算公式: 股利殖利率-TSE
            DY = data_dict.get("Dividend_Yield", None)
            data_to_commit.append(self._to_factorValue_obj(company, DY, period_id, factor_obj_dict["DY"], report_time.date))

            # 因子代號: BETA_1M 
            # 因子計算公式: CAPM_Beta 一月
            BETA_1M = data_dict.get("CAPM_Beta_1_Months", None)
            data_to_commit.append(self._to_factorValue_obj(company, BETA_1M, period_id, factor_obj_dict["BETA_1M"], report_time.date))

            # 因子代號: BETA_3M 
            # 因子計算公式: CAPM_Beta 三月
            BETA_3M = data_dict.get("CAPM_Beta_3_Months", None)
            data_to_commit.append(self._to_factorValue_obj(company, BETA_3M, period_id, factor_obj_dict["BETA_3M"], report_time.date))

            # 因子代號: BETA_6M 
            # 因子計算公式: CAPM_Beta 六月
            BETA_6M = data_dict.get("CAPM_Beta_6_Months", None)
            data_to_commit.append(self._to_factorValue_obj(company, BETA_6M, period_id, factor_obj_dict["BETA_6M"], report_time.date))

            # 因子代號: BETA_1Y 
            # 因子計算公式: CAPM_Beta 一年
            BETA_1Y = data_dict.get("CAPM_Beta_1_Year", None)
            data_to_commit.append(self._to_factorValue_obj(company, BETA_1Y, period_id, factor_obj_dict["BETA_1Y"], report_time.date))

            # 因子代號: BVPS 
            # 因子計算公式: 每股淨值(B)
            BVPS = data_dict.get("Book_Value_Per_Share", None)
            data_to_commit.append(self._to_factorValue_obj(company, BVPS, period_id, factor_obj_dict["BVPS"], report_time.date))

            # 因子代號: EPS 
            # 因子計算公式: 每股盈餘
            EPS = data_dict.get("Earnings_Per_Share", None)
            data_to_commit.append(self._to_factorValue_obj(company, EPS, period_id, factor_obj_dict["EPS"], report_time.date))

            # 因子代號: ROA_C 
            # 因子計算公式: ROA(C)稅前息前折舊前
            ROA_C = data_dict.get("ROA_PreTax", None)
            data_to_commit.append(self._to_factorValue_obj(company, ROA_C, period_id, factor_obj_dict["ROA_C"], report_time.date))

            # 因子代號: ROA_B 
            # 因子計算公式: ROA(B)稅後息前折舊前
            ROA_B = data_dict.get("ROA_AfterTax", None)
            data_to_commit.append(self._to_factorValue_obj(company, ROA_B, period_id, factor_obj_dict["ROA_B"], report_time.date))

            # 因子代號: ROA_A 
            # 因子計算公式: ROA(A)稅後息前
            ROA_A = data_dict.get("ROA_AfterTax_Pre_Interest", None)
            data_to_commit.append(self._to_factorValue_obj(company, ROA_A, period_id, factor_obj_dict["ROA_A"], report_time.date))

            # 因子代號: ROA 
            # 因子計算公式: ROA－綜合損益
            ROA = data_dict.get("ROA_Comprehensive_Income", None)
            data_to_commit.append(self._to_factorValue_obj(company, ROA, period_id, factor_obj_dict["ROA"], report_time.date))

            # 因子代號: GPM 
            # 因子計算公式: 營業毛利率
            GPM = data_dict.get("Gross_Profit_Margin", None)
            data_to_commit.append(self._to_factorValue_obj(company, GPM, period_id, factor_obj_dict["GPM"], report_time.date))

            # 因子代號: RGPMS 
            # 因子計算公式: 已實現銷貨毛利
            RGPMS = data_dict.get("Realized_Gross_Profit", None)
            data_to_commit.append(self._to_factorValue_obj(company, RGPMS, period_id, factor_obj_dict["RGPMS"], report_time.date))

            # 因子代號: OPM 
            # 因子計算公式: 營業利益率
            OPM = data_dict.get("Operating_Profit_Margin", None)
            data_to_commit.append(self._to_factorValue_obj(company, OPM, period_id, factor_obj_dict["OPM"], report_time.date))

            # 因子代號: RIR_A 
            # 因子計算公式: 常續利益率－稅後
            RIR_A = data_dict.get("Recurring_Earnings_Margin_After_Tax", None)
            data_to_commit.append(self._to_factorValue_obj(company, RIR_A, period_id, factor_obj_dict["RIR_A"], report_time.date))

            # 因子代號: EBITDA 
            # 因子計算公式: 稅前息前折舊前淨利率
            EBITDA = data_dict.get("Pre_Tax_Net_Profit_Margin", None)
            data_to_commit.append(self._to_factorValue_obj(company, EBITDA, period_id, factor_obj_dict["EBITDA"], report_time.date))

            # 因子代號: EBTM 
            # 因子計算公式: 稅前淨利率
            EBTM = data_dict.get("Pre_Tax_Net_Profit_Rate", None)
            data_to_commit.append(self._to_factorValue_obj(company, EBTM, period_id, factor_obj_dict["EBTM"], report_time.date))

            # 因子代號: NIM 
            # 因子計算公式: 稅後淨利率
            NIM = data_dict.get("After_Tax_Net_Profit_Rate", None)
            data_to_commit.append(self._to_factorValue_obj(company, NIM, period_id, factor_obj_dict["NIM"], report_time.date))

            # 因子代號: MV 
            # 因子計算公式: 季底普通股市值
            MV = data_dict.get("End_of_Quarter_Market_Value_of_Ordinary_Shares", None)
            data_to_commit.append(self._to_factorValue_obj(company, MV, period_id, factor_obj_dict["MV"], report_time.date))

            # 因子代號: RGR 
            # 因子計算公式: 營收成長率
            RGR = data_dict.get("Revenue_Growth_Rate", None)
            data_to_commit.append(self._to_factorValue_obj(company, RGR, period_id, factor_obj_dict["RGR"], report_time.date))

            # 因子代號: NV_A 
            # 因子計算公式: 淨值/資產
            NV_A = data_dict.get("Equity_Asset_Ratio", None)
            data_to_commit.append(self._to_factorValue_obj(company, NV_A, period_id, factor_obj_dict["NV_A"], report_time.date))

            # 因子代號: ATR 
            # 因子計算公式: 總資產週轉次數
            ATR = data_dict.get("Total_Asset_Turnover", None)
            data_to_commit.append(self._to_factorValue_obj(company, ATR, period_id, factor_obj_dict["ATR"], report_time.date))

            # 因子代號: OIE_R 
            # 因子計算公式: 業外收支/營收
            OIE_R = data_dict.get("Non_Operating_Incomeand_Expenditure_to_Revenue", None)
            data_to_commit.append(self._to_factorValue_obj(company, OIE_R, period_id, factor_obj_dict["OIE_R"], report_time.date))

            # 因子代號: ROE_A 
            # 因子計算公式: ROE(A)－稅後
            ROE_A = data_dict.get("ROE_AfterTax_Pre_Interest", None)
            data_to_commit.append(self._to_factorValue_obj(company, ROE_A, period_id, factor_obj_dict["ROE_A"], report_time.date))

            # 因子代號: ROE_B 
            # 因子計算公式: ROE(B)－常續利益
            ROE_B = data_dict.get("ROE_Sustainable_Income", None)
            data_to_commit.append(self._to_factorValue_obj(company, ROE_B, period_id, factor_obj_dict["ROE_B"], report_time.date))

            # 因子代號: ROE 
            # 因子計算公式: ROE－綜合損益
            ROE = data_dict.get("ROE_Comprehensive_Income", None)
            data_to_commit.append(self._to_factorValue_obj(company, ROE, period_id, factor_obj_dict["ROE"], report_time.date))

            # 因子代號: RD 
            # 因子計算公式: 研究發展費
            RD = data_dict.get("Research_Development_Expenses", None)
            data_to_commit.append(self._to_factorValue_obj(company, RD, period_id, factor_obj_dict["RD"], report_time.date))



            # # PS 股價營收比(P/S): 股價營收比-TEJ
            # ps = data_dict.get("PriceToRevenueRatio_TEJ", None)
            # data_to_commit.append(self._to_factorValue_obj(company, ps, period_id, factor_obj_dict["PS"], report_time.date))

            # # PE 本益比(P/E): 本益比-TEJ
            # pe = data_dict.get("PE_Ratio_TEJ", None)
            # data_to_commit.append(self._to_factorValue_obj(company, pe, period_id, factor_obj_dict["PE"], report_time.date))

            # # ROA(C) 資產報酬率(稅/息/折舊前)(ROA): ROA(C)稅前息前折舊前
            # roa_c = data_dict.get("ROA_PreTax", None)
            # data_to_commit.append(self._to_factorValue_obj(company, roa_c, period_id, factor_obj_dict["ROA(C)"], report_time.date))

            # # ROA(B) 資產報酬率(稅後/息前折舊前) (ROA): RROA(B)稅後息前折舊前
            # roa_b = data_dict.get("ROA_AfterTax", None)
            # data_to_commit.append(self._to_factorValue_obj(company, roa_b, period_id, factor_obj_dict["ROA(B)"], report_time.date))

            # # ROA(A) 資產報酬率(稅後/息前) (ROA): ROA(A)稅後息前
            # roa_a = data_dict.get("ROA_AfterTax_PreInterest", None)
            # data_to_commit.append(self._to_factorValue_obj(company, roa_a, period_id, factor_obj_dict["ROA(A)"], report_time.date))

            # # ROE(A) 股東權益報酬率(稅後) ROE(A)－稅後
            # roe_a = data_dict.get("ROE_AfterTax_PreInterest", None)
            # data_to_commit.append(self._to_factorValue_obj(company, roe_a, period_id, factor_obj_dict["ROE(A)"], report_time.date))

            # # GPM 營業毛利率: 營業毛利率
            # gpm = data_dict.get("GrossProfitMargin", None)
            # data_to_commit.append(self._to_factorValue_obj(company, gpm, period_id, factor_obj_dict["GPM"], report_time.date))

            # # RGPMS 已實現銷貨毛利率: 已實現銷貨毛利率
            # rgpms = data_dict.get("RealizedGrossProfit", None)
            # data_to_commit.append(self._to_factorValue_obj(company, rgpms, period_id, factor_obj_dict["RGPMS"], report_time.date))

            # # OPM 營業利益率: 營業利益率
            # opm = data_dict.get("OperatingProfitMargin", None)
            # data_to_commit.append(self._to_factorValue_obj(company, opm, period_id, factor_obj_dict["OPM"], report_time.date))

            # # RIR(A) 稅後常續利益率: 常續利益率－稅後
            # rir_a = data_dict.get("RecurringEarningsMargin_AfterTax", None)
            # data_to_commit.append(self._to_factorValue_obj(company, rir_a, period_id, factor_obj_dict["RIR(A)"], report_time.date))

            # # EBITDA 稅/息/折舊前淨利率: 稅前息前折舊前淨利率
            # ebitda = data_dict.get("PreTaxNetProfitMargin", None)
            # data_to_commit.append(self._to_factorValue_obj(company, ebitda, period_id, factor_obj_dict["EBITDA"], report_time.date))

            # # EBTM 稅前淨利率: 稅前淨利率
            # ebtm = data_dict.get("PreTaxNetProfitRate", None)
            # data_to_commit.append(self._to_factorValue_obj(company, ebtm, period_id, factor_obj_dict["EBTM"], report_time.date))

            # # NIM 稅後淨利率: 稅後淨利率
            # nim = data_dict.get("AfterTaxNetProfitRate", None)
            # data_to_commit.append(self._to_factorValue_obj(company, nim, period_id, factor_obj_dict["NIM"], report_time.date))

            # # MV 季底普通股市值: 季底普通股市值
            # mv = data_dict.get("EndofQuarterMarketValueofOrdinaryShares", None)
            # data_to_commit.append(self._to_factorValue_obj(company, mv, period_id, factor_obj_dict["MV"], report_time.date))

            # # BETA 系統風險beta: 股價資料庫.股價報酬Beta
            # beta = data_dict.get("CAPM_Beta_ThreeMonths", None)
            # data_to_commit.append(self._to_factorValue_obj(company, beta, period_id, factor_obj_dict["BETA"], report_time.date))

            # # RGR 營收成長率: 營收成長率
            # rgr = data_dict.get("RevenueGrowthRate", None)
            # data_to_commit.append(self._to_factorValue_obj(company, rgr, period_id, factor_obj_dict["RGR"], report_time.date))

            # # NV_A 淨值/資產: 淨值/資產
            # nv_a = data_dict.get("EquityAssetRatio", None)
            # data_to_commit.append(self._to_factorValue_obj(company, nv_a, period_id, factor_obj_dict["NV_A"], report_time.date))

            # # ATR 總資產周轉次數: 總資產週轉次數
            # atr = data_dict.get("TotalAssetTurnover", None)
            # data_to_commit.append(self._to_factorValue_obj(company, atr, period_id, factor_obj_dict["ATR"], report_time.date))

            # # OIE_R 業外收支/營收: 業外收支/營收
            # oie_r = data_dict.get("NonOperatingIncomeandExpendituretoRevenue", None)
            # data_to_commit.append(self._to_factorValue_obj(company, oie_r, period_id, factor_obj_dict["OIE_R"], report_time.date))
                                                                            
                   
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
    # stock_list = [1101,1216,1301,1303,1326,1402,1590,1605,2002,2207,
    #             2303,2308,2317,2327,2330,2357,2379,2382,2395,2408,
    #             2412,2454,2603,2609,2615,2801,2880,2881,2882,2883,
    #             2884,2885,2886,2887,2890,2891,2892,2912,3008,3034,
    #             3037,3045,3711,4904,4938,5871,5876,5880,6505,9910]
    # # stock_list = [1101,1216]
    # for symbol in stock_list:
    #     tw._logger.warning("Start "+ str(symbol))
    #     tw.get_data(symbol)
    tw.get_all_data()