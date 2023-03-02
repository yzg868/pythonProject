import tushare as ts
import pandas as pd
import time
import schedule
import datetime

# 设置Tushare Pro的token
pro = ts.pro_api('e706294a2bc3b8bfe52c4cfbe2fdb4424a6af0b3535e125800fb5b14')

# 获取所有A股代码列表
stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code')

# 定义函数，实现增量下载财务数据
def download_finance_data():
    # 获取当前时间
    now = datetime.datetime.now()
    print("Start downloading finance data at", now)

    # 初始化数据框
    income_data = pd.DataFrame()
    balance_data = pd.DataFrame()
    cashflow_data = pd.DataFrame()
    finance_data = pd.DataFrame()

    # 遍历每支股票，获取财务数据
    for i, stock in stock_list.iterrows():
        if i>5:
            break
        ts_code = stock['ts_code']
        # 获取利润表数据
        income_df = pro.income(ts_code=ts_code, start_date='20220201', end_date=now.strftime('%Y%m%d'))
        income_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
        income_data = pd.concat([income_data, income_df], ignore_index=True)

        # 获取资产负债表数据
        balance_df = pro.balancesheet(ts_code=ts_code, start_date='20220201', end_date=now.strftime('%Y%m%d'))
        balance_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
        balance_data = pd.concat([balance_data, balance_df], ignore_index=True)

        # 获取现金流量表数据
        cashflow_df = pro.cashflow(ts_code=ts_code, start_date='20220201', end_date=now.strftime('%Y%m%d'))
        cashflow_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
        cashflow_data = pd.concat([cashflow_data, cashflow_df], ignore_index=True)

        # 获取财务指标数据
        finance_df = pro.fina_indicator(ts_code=ts_code, start_date='20220101', end_date=now.strftime('%Y%m%d'))
        finance_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
        finance_data = pd.concat([finance_data, finance_df], ignore_index=True)
        time.sleep(0.3)  # 设置请求间隔时间

    # 存储到CSV文件
    income_data.to_csv('C:/gupia/利润表.csv', index=False)
    balance_data.to_csv('C:/gupia/资产负债表.csv', index=False)
    cashflow_data.to_csv('C:/gupia/现金流量表.csv', index=False)
    finance_data.to_csv('C:/gupia/财务指标数据表.csv', index=False)

    print("Finished downloading finance data at", datetime.datetime.now())

# 设置定时任务
schedule.every().day.at('20:30').do(download_finance_data)

while True:
    schedule.run_pending()
    time.sleep(1)

