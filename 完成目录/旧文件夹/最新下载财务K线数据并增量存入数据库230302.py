import tushare as ts
import pandas as pd
import time
import datetime
import pymysql
#from datetime import datetime

# 设置Tushare Pro的token
pro = ts.pro_api('e706294a2bc3b8bfe52c4cfbe2fdb4424a6af0b3535e125800fb5b14')

# 获取所有A股代码列表
stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code')

# 读取上次下载日期csv文件中的日期
df_last_download_date = pd.read_csv('c:\gupia\中间文件\上次下载日期.csv')
last_download_date_str = str(df_last_download_date.iloc[0, 0])  # 假设日期保存在第一列第一行
last_download_date = datetime.datetime.strptime(last_download_date_str, '%Y-%m-%d')
delta = datetime.timedelta(days=1)  # 定义一个时间差，表示1天
start_date = last_download_date + delta  # 计算上次下载日期加1天后的日期
print(start_date)


# 获取当前时间
now = datetime.datetime.now()  # 获取当前日期和时间

# start_date晚于now,start_date改为今天
if start_date > now:
    start_date = now

# 日期转成字符格式
start_date_str = start_date.strftime('%Y%m%d')

print("Start downloading finance data at", now)

# 初始化数据框
income_data = pd.DataFrame()
balance_data = pd.DataFrame()
cashflow_data = pd.DataFrame()
finance_data = pd.DataFrame()
daily_k_data = pd.DataFrame()
weekly_k_data = pd.DataFrame()
monthly_k_data = pd.DataFrame()

# 遍历每支股票，获取财务数据
for i, stock in stock_list.iterrows():
    if i>5:
        break
    ts_code = stock['ts_code']
    # 获取利润表数据
    income_df = pro.income(ts_code=ts_code, start_date=start_date_str, end_date=now.strftime('%Y%m%d'))
    income_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    income_data = pd.concat([income_data, income_df], ignore_index=True)

    # 获取资产负债表数据
    balance_df = pro.balancesheet(ts_code=ts_code, start_date=start_date_str, end_date=now.strftime('%Y%m%d'))
    balance_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    balance_data = pd.concat([balance_data, balance_df], ignore_index=True)

    # 获取现金流量表数据
    cashflow_df = pro.cashflow(ts_code=ts_code, start_date=start_date_str, end_date=now.strftime('%Y%m%d'))
    cashflow_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    cashflow_data = pd.concat([cashflow_data, cashflow_df], ignore_index=True)

    # 获取财务指标数据
    finance_df = pro.fina_indicator(ts_code=ts_code, start_date=start_date_str, end_date=now.strftime('%Y%m%d'))
    finance_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    finance_data = pd.concat([finance_data, finance_df], ignore_index=True)

    # 获取日K线数据
    daily_k_df = pro.daily(ts_code=ts_code, start_date=start_date_str, end_date=now.strftime('%Y%m%d'))
    daily_k_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    daily_k_df.rename(columns={'change': 'chg'}, inplace=True)  # 将change列重命名为chg
    daily_k_data = pd.concat([daily_k_data, daily_k_df], ignore_index=True)
    print(i / len(stock_list) * 100, '%')

    # 获取周K线数据
    weekly_k_df = pro.weekly(ts_code=ts_code, start_date=start_date_str, end_date=now.strftime('%Y%m%d'))
    weekly_k_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    weekly_k_df.rename(columns={'change': 'chg'}, inplace=True)  # 将change列重命名为chg
    weekly_k_data = pd.concat([weekly_k_data, weekly_k_df], ignore_index=True)

    # 获取月K线数据
    monthly_k_df = pro.monthly(ts_code=ts_code, start_date=start_date_str, end_date=now.strftime('%Y%m%d'))
    monthly_k_df.fillna('NULL', inplace=True)  # 将空内容替换为NULL
    monthly_k_df.rename(columns={'change': 'chg'}, inplace=True)  # 将change列重命名为chg
    monthly_k_data = pd.concat([monthly_k_data, monthly_k_df], ignore_index=True)

    time.sleep(0.3)  # 设置请求间隔时间

# 存储到CSV文件
income_data.to_csv('C:/gupia/利润表.csv', index=False)
balance_data.to_csv('C:/gupia/资产负债表.csv', index=False)
cashflow_data.to_csv('C:/gupia/现金流量表.csv', index=False)
finance_data.to_csv('C:/gupia/财务指标数据表.csv', index=False)
daily_k_data.to_csv('C:/gupia/日K线数据表.csv', index=False)
weekly_k_data.to_csv('C:/gupia/周K线数据表.csv', index=False)
monthly_k_data.to_csv('C:/gupia/月K线数据表.csv', index=False)

print("Finished downloading finance data at", datetime.datetime.now())

def store_csv_to_mysql(table_name):
    # 读取CSV文件并将第一行作为列名
    df = pd.read_csv(f'c:\\gupia\\{table_name}.csv', header=0)

    # 定义MySQL连接参数
    cnx = pymysql.connect(user='root', password='root',
                          host='127.0.0.1', database='gpsj')

    # 创建MySQL表格
    cursor = cnx.cursor()

    # 将数据写入MySQL表格
    for _, row in df.iterrows():
        values = []
        for col in df.columns:
            value = row[col]
            if isinstance(value, str):
                value = f"'{value}'"
            elif pd.isnull(value):
                value = 'NULL'
            elif "_date" in col:  # 如果列名中包含'_date'，则按照日期格式进行转换
                value = datetime.datetime.strptime(str(value), '%Y%m%d').strftime('%Y-%m-%d')
                value = f"'{value}'"
            else:
                value = str(value)
            values.append(value)
        values_str = ', '.join(values)

        # 根据数据是否已存在判断是否插入新记录
        select_query = f"SELECT * FROM {table_name} WHERE {df.columns[0]}='{row[df.columns[0]]}' AND {df.columns[1]}='{row[df.columns[1]]}'"
        cursor.execute(select_query)
        result = cursor.fetchone()

        if result:
            continue
        else:
            # 如果不存在，则插入新记录
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({values_str})"
            cursor.execute(insert_query)

    # 提交更改并关闭连接
    cnx.commit()
    cursor.close()
    cnx.close()

# 调用函数存储各表数据
store_csv_to_mysql('利润表')
store_csv_to_mysql('财务指标数据表')
store_csv_to_mysql('现金流量表')
store_csv_to_mysql('资产负债表')
store_csv_to_mysql('日K线数据表')
store_csv_to_mysql('周K线数据表')
store_csv_to_mysql('月K线数据表')

print('完成')


# 将本次下载日期保存到csv文件中
df_last_download_date.iloc[0, 0] = now.strftime('%Y-%m-%d')
df_last_download_date.to_csv('c:\gupia\中间文件\上次下载日期.csv', index=False)

