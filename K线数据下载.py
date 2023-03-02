import tushare as ts
import pandas as pd
import time

# 设置Tushare Pro的token
pro = ts.pro_api('e706294a2bc3b8bfe52c4cfbe2fdb4424a6af0b3535e125800fb5b14')

# 获取所有A股代码列表
stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code')

# 初始化数据框
daily_k_data = pd.DataFrame()
weekly_k_data = pd.DataFrame()
monthly_k_data = pd.DataFrame()

#遍历每支股票，获取日K线、周K线、月K线数据
for i, stock in stock_list.iterrows():
    if i>5:
       break
    ts_code = stock['ts_code']

    # 获取日K线数据
    daily_k_df = pro.daily(ts_code=ts_code, start_date='20230225', end_date='20230228')
    daily_k_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    daily_k_df.rename(columns={'change': 'chg'}, inplace=True)  # 将change列重命名为chg
    daily_k_data = pd.concat([daily_k_data, daily_k_df], ignore_index=True)
    print(i / len(stock_list) * 100, '%')

    # 获取周K线数据
    weekly_k_df = pro.weekly(ts_code=ts_code, start_date='20230225', end_date='20230228')
    weekly_k_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    weekly_k_df.rename(columns={'change': 'chg'}, inplace=True)  # 将change列重命名为chg
    weekly_k_data = pd.concat([weekly_k_data, weekly_k_df], ignore_index=True)

    # 获取月K线数据
    monthly_k_df = pro.monthly(ts_code=ts_code, start_date='20230225', end_date='20230228')
    monthly_k_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    monthly_k_df.rename(columns={'change': 'chg'}, inplace=True)  # 将change列重命名为chg
    monthly_k_data = pd.concat([monthly_k_data, monthly_k_df], ignore_index=True)

    time.sleep(0.3)  # 设置请求间隔时间
    print(i / len(stock_list) * 100, '%')

    # 存储到CSV文件
daily_k_data.to_csv('C:/gupia/日K线数据表.csv', index=False)
weekly_k_data.to_csv('C:/gupia/周K线数据表.csv', index=False)
monthly_k_data.to_csv('C:/gupia/月K线数据表.csv', index=False)

