TW_stock_factor_collector (中文版)
===
### **一、 專案簡介**
這個專案是搭配TEJ軟體大量截取台灣股票市場所需的資料，擷取資料內容是作為碩士論文實驗所需。雖然TEJ本身就可以直接擷取股市相關資料，但原始的功能僅能擷取**單一公司長時間**的資料，利用Python與bat腳本延伸TEJ原始增益集功能後，可以一次大量地擷取**多個公司長時間**的資料，並在擷取完畢後自動匯入資料庫伺服器。

### **二、 操作手冊**

#### 1. 下載TEJ
* 下載連結: http://140.115.83.180/download/
* 本安裝程式限 國立中央大學 IP 範圍內使用 (校外可搭配VPN)
* 進入帳號 (USER ID)：NCU ，密碼(PASSWORD)： TEJ
![image](https://hackmd.io/_uploads/Bya15ls2a.png)

#### 2. 下載本專案與相關套件
* 從專案主頁直接點擊下載按鈕
* 或是git clone https://github.com/QuenHengLee/TW_stock_factor_collector.git
* 安裝執行專案所需套件
    * sqlalchemy => conda install sqlalchemy
    * pandas => conda install pandas
    * pymysql => conda install pymysql
    * conda install -c anaconda pywin32

#### 3. 設定Excel增益集
* 確認TEJ已成功安裝完成後
* 至MS Excel 選項/增益集/新增Excel03Menu
![image](https://hackmd.io/_uploads/BJukjxina.png)
![image](https://hackmd.io/_uploads/Bk5Hiei3T.png)

#### 4. 準備擷取股價與財報資料所需的.xlsm檔案
* 股價資料: stock.xlsm
* 財報資料: indicator.xlsm


#### 5. 設定股價擷取檔案 stock.xlsm
* 開啟TEJ設定畫面
![image](https://hackmd.io/_uploads/HJsezbs2T.png)
* TEJ Smart Wizard設定 - 1 
    * 指定抓取的時間區間
    * 指定公司碼存放位置為A1
    * 抓取後自動執行finish巨集![image](https://hackmd.io/_uploads/Bys7zZi2a.png)
![image](https://hackmd.io/_uploads/ByGEfWihp.png)
* TEJ Smart Wizard設定 - 2
    * 格式設定選擇樣式3
    * 匯出至Excel
    * 自動調整
    ![image](https://hackmd.io/_uploads/SJCgmZs3T.png)

* TEJ Smart Wizard設定 - 3
    * 重新命名工作表名稱為report
    * 抓取其他公司的價量資料：
        * 在A1輸入股票代號後點選Enter
        * 點選增益集的更新工作表
![image](https://hackmd.io/_uploads/Bkto9Wi26.png)
* 巨集設定
    * 新增Refresh、finish兩個巨集
    * finish會去執行stock.bat，將資料寫入資料庫
    ![image](https://hackmd.io/_uploads/Bk66cZj26.png)
執行 財報-->因子:
python -m CollectorFactory.TWCollectorFactory.TWFactorCollector

### 三、Error Msg 處理: 
- 問題: SELECT list is not in GROUP BY clause and contains nonaggregated column .... incompatible with sql_mode=only_full_group_by
- 處理方法: 下SQL語法 SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));


TW_stock_factor_collector (English version)
===
### **I. Project Introduction**
This project is designed to capture a large amount of data required from the Taiwanese stock market in conjunction with TEJ software, intended for use in a master's thesis experiment. Although TEJ itself can directly capture stock market-related data, its original functionality is limited to capturing data over a long period for **a single company**. By extending TEJ's original capabilities with Python and batch scripts, it is now possible to capture data over a long period for **multiple companies** at once, and automatically import the captured data into a database server upon completion.

### **II. User Manual**

#### 1. Download TEJ
* Download link: http://140.115.83.180/download/
* This installer is only available within the IP range of National Central University (VPN is required for off-campus use)
* Login account (USER ID): NCU, Password (PASSWORD): TEJ
![image](https://hackmd.io/_uploads/Bya15ls2a.png)

#### 2. Download This Project and Related Packages
* Click the download button directly from the project homepage
* Or clone the repository: git clone https://github.com/QuenHengLee/TW_stock_factor_collector.git
* Install the packages required to run the project:
    * sqlalchemy => conda install sqlalchemy
    * pandas => conda install pandas
    * pymysql => conda install pymysql
    * conda install -c anaconda pywin32

#### 3. Set Up Excel Add-ins
* Ensure TEJ is successfully installed
* Go to MS Excel Options/Add-Ins/Add Excel03Menu
![image](https://hackmd.io/_uploads/BJukjxina.png)
![image](https://hackmd.io/_uploads/Bk5Hiei3T.png)

#### 4. Prepare .xlsm Files Needed to Capture Stock Prices and Financial Data
* Stock price data: stock.xlsm
* Financial data: indicator.xlsm

#### 5. Configure the Stock Price Capture File stock.xlsm
* Open the TEJ setup screen
![image](https://hackmd.io/_uploads/HJsezbs2T.png)
* TEJ Smart Wizard Setup - 1 
   * Specify the time period to capture
   * Specify the company code location as A1
   * Automatically execute the finish macro after capturing image: https://hackmd.io/_uploads/Bys7zZi2a.png image: https://hackmd.io/_uploads/ByGEfWihp.png
   
* TEJ Smart Wizard Setting - 2
   * Select style 3 for format setting
   * Export to Excel
   * Auto adjust image: https://hackmd.io/_uploads/SJCgmZs3T.png
*  TEJ Smart Wizard Setting - 3
   * Rename the worksheet name to report
   * Capture price and volume data of other companies:
      * Enter the stock code in A1 and press Enter
      * Click the Update Worksheet of the add-in image: https://hackmd.io/_uploads/Bkto9Wi26.png
* Macro setting
   * Add two macros, Refresh and finish
   * finish will execute stock.bat to write data to the database image:
https://hackmd.io/_uploads/Bk66cZj26.png Execute Financial Statement-->Factor: python -m CollectorFactory.TWCollectorFactory.TWFactorCollector

### **III. Error Msg Handling**
* Problem: SELECT list is not in GROUP BY clause and contains nonaggregated column .... incompatible with sql_mode=only_full_group_by
* Solution: Execute the following SQL statement SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
