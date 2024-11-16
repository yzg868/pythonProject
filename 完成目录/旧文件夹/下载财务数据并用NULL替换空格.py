import tushare as ts
import pandas as pd
import time

# 设置Tushare Pro的token
pro = ts.pro_api('e706294a2bc3b8bfe52c4cfbe2fdb4424a6af0b3535e125800fb5b14')

# 获取所有A股代码列表
stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code')

# 初始化数据框
income_data = pd.DataFrame()
balance_data = pd.DataFrame()
cashflow_data = pd.DataFrame()
finance_data = pd.DataFrame()
start_date='202302019'
# 遍历每支股票，获取财务数据
for i, stock in stock_list.iterrows():
#    if i>5:
#       break
    ts_code = stock['ts_code']
    # 获取利润表数据
    income_df = pro.income(ts_code=ts_code, start_date=start_date, end_date='20230223')
    income_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    income_data = pd.concat([income_data, income_df], ignore_index=True)

    # 获取资产负债表数据
    balance_df = pro.balancesheet(ts_code=ts_code, start_date=start_date, end_date='20230223')
    balance_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    balance_data = pd.concat([balance_data, balance_df], ignore_index=True)

    # 获取现金流量表数据
    cashflow_df = pro.cashflow(ts_code=ts_code, start_date=start_date, end_date='20230223')
    cashflow_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    cashflow_data = pd.concat([cashflow_data, cashflow_df], ignore_index=True)

    # 获取财务指标数据
    finance_df = pro.fina_indicator(ts_code=ts_code, start_date=start_date, end_date='20230223')
    finance_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    finance_data = pd.concat([finance_data, finance_df], ignore_index=True)
    time.sleep(0.3)  # 设置请求间隔时间

    print(i / len(stock_list) * 100, '%')

    # 存储到CSV文件
income_data.to_csv('C:/gupia/利润表.csv', index=False)
balance_data.to_csv('C:/gupia/资产负债表.csv', index=False)
cashflow_data.to_csv('C:/gupia/现金流量表.csv', index=False)
finance_data.to_csv('C:/gupia/财务指标数据表.csv', index=False)



