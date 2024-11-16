import tushare as ts
from pymongo import MongoClient
import datetime
import pymongo

# 设置tushare pro的token
ts.set_token('e706294a2bc3b8bfe52c4cfbe2fdb4424a6af0b3535e125800fb5b14')

# 初始化tushare pro的api
pro = ts.pro_api()

# 建立mongodb数据库连接
client = MongoClient('mongodb://localhost:27017/')
db = client['stock']
download_record = db["download_record"]

# 定义初始日期
start_date_init = '20241102'  # 可以是任意日期字符串，也可以是空字符串
if start_date_init:
    start_date = start_date_init
else:
    last_download = download_record.find_one({}, sort=[("_id", -1)])
    if last_download:
        start_date = (last_download["download_date"] + datetime.timedelta(days=1)).strftime('%Y%m%d')
    else:
        start_date = '20241102'  # 如果是第一次下载，则从2024年11月2日开始下载

# 创建所需的集合
income_statement_collection = db['income_statement']
balance_sheet_collection = db['balance_sheet']
cash_flow_collection = db['cash_flow']
financial_indicator_collection = db['financial_indicator']
daily_k_data_collection = db['daily_k_data']
daily_k_data_hfq_collection = db['daily_k_data_hfq']
weekly_k_data_collection = db['weekly_k_data']
monthly_k_data_collection = db['monthly_k_data']


# 定义下载数据并存储的函数
def download_and_save_data():
    stock_codes = pro.stock_basic(exchange='', list_status='L', fields='ts_code')['ts_code'].tolist()
    current_date = datetime.datetime.now().strftime('%Y%m%d')
    today = datetime.datetime.now()

    # 检查上次下载的时间
    weekly_last_download = download_record.find_one({"data_type": "weekly_k_data"})
    monthly_last_download = download_record.find_one({"data_type": "monthly_k_data"})

    # 确定是否应下载周线和月线数据
    should_download_weekly = (not weekly_last_download) or ((today - weekly_last_download["download_date"]).days >= 7)
    should_download_monthly = (not monthly_last_download) or (
                monthly_last_download["download_date"].month != today.month)

    for stock_code in stock_codes:
        print(f"Downloading data for {stock_code}...")

        # 下载财务数据（利润表、资产负债表、现金流量表、财务指标）
        income_statement = pro.income(ts_code=stock_code, start_date=start_date, end_date=current_date)
        if not income_statement.empty:
            try:
                income_statement_collection.insert_many(income_statement.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        balance_sheet = pro.balancesheet(ts_code=stock_code, start_date=start_date, end_date=current_date)
        if not balance_sheet.empty:
            try:
                balance_sheet_collection.insert_many(balance_sheet.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        cash_flow = pro.cashflow(ts_code=stock_code, start_date=start_date, end_date=current_date)
        if not cash_flow.empty:
            try:
                cash_flow_collection.insert_many(cash_flow.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        financial_indicator = pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=current_date)
        if not financial_indicator.empty:
            try:
                financial_indicator_collection.insert_many(financial_indicator.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        # 下载日线数据
        daily_k_data = pro.daily(ts_code=stock_code, start_date=start_date, end_date=current_date)
        if not daily_k_data.empty:
            try:
                daily_k_data_collection.insert_many(daily_k_data.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        # 下载后复权日线数据
        daily_k_data_hfq = ts.pro_bar(ts_code=stock_code, adj='hfq', start_date=start_date, end_date=current_date)
        if not daily_k_data_hfq.empty:
            try:
                daily_k_data_hfq_collection.insert_many(daily_k_data_hfq.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        # 下载周线数据（每周一次）
        if should_download_weekly:
            weekly_k_data = pro.weekly(ts_code=stock_code, start_date=start_date, end_date=current_date)
            if not weekly_k_data.empty:
                try:
                    weekly_k_data_collection.insert_many(weekly_k_data.to_dict('records'))
                except pymongo.errors.BulkWriteError as e:
                    print("有重复记录：", e.details)

        # 下载月线数据（每月一次）
        if should_download_monthly:
            monthly_k_data = pro.monthly(ts_code=stock_code, start_date=start_date, end_date=current_date)
            if not monthly_k_data.empty:
                try:
                    monthly_k_data_collection.insert_many(monthly_k_data.to_dict('records'))
                except pymongo.errors.BulkWriteError as e:
                    print("有重复记录：", e.details)

    # 更新 download_record 集合中的 download_date
    if should_download_weekly:
        download_record.update_one(
            {"data_type": "weekly_k_data"},
            {"$set": {"download_date": today}},
            upsert=True
        )
    if should_download_monthly:
        download_record.update_one(
            {"data_type": "monthly_k_data"},
            {"$set": {"download_date": today}},
            upsert=True
        )


# 执行下载
download_and_save_data()

# 将当前日期插入到 download_record 集合中
download_record.insert_one({"data_type": "daily", "download_date": datetime.datetime.now()})

# 关闭数据库连接
client.close()


