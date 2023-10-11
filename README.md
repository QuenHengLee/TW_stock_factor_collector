# TW_stock_factor_collector

## 執行 財報-->因子:
python -m CollectorFactory.TWCollectorFactory.TWFactorCollector

### Error Msg 處理: 
- 問題: SELECT list is not in GROUP BY clause and contains nonaggregated column .... incompatible with sql_mode=only_full_group_by
- 處理方法: 下SQL語法 SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

