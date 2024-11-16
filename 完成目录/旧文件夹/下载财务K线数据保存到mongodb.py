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

# 创建名为stock的数据库
db = client['stock']
download_record = db["download_record"]

# 定义初始日期
start_date_init = '20241102'  # 可以是任意日期字符串，也可以是空字符串
if start_date_init:
    start_date = start_date_init
else:
    # 获取上次下载的日期
    last_download = download_record.find_one({}, sort=[("_id", -1)])
    if last_download:
        start_date = (last_download["download_date"] + datetime.timedelta(days=1)).strftime('%Y%m%d')
        print(start_date)
        exit()
    else:
        start_date = '20241102'  # 如果是第一次下载，则从2024年11月2日开始下载


# 创建名为income_statement、balance_sheet、cash_flow、financial_indicator、daily_k_data、weekly_k_data、monthly_k_data的集合
income_statement_collection = db['income_statement']
balance_sheet_collection = db['balance_sheet']
cash_flow_collection = db['cash_flow']
financial_indicator_collection = db['financial_indicator']
daily_k_data_collection = db['daily_k_data']
daily_k_data_hfq_collection = db['daily_k_data_hfq']
weekly_k_data_collection = db['weekly_k_data']
monthly_k_data_collection = db['monthly_k_data']


# 定义一个函数来下载并存储数据
def download_and_save_data():
    # 获取所有A股的股票代码
    stock_codes = pro.stock_basic(exchange='', list_status='L', fields='ts_code')['ts_code'].tolist()
    i=0
    for stock_code in stock_codes:
       #if i>5:
       #     break
        #i=i+1
        print(f"Downloading data for {stock_code}...")

        # 获取该股票的利润表数据
        income_statement = pro.income(ts_code=stock_code, start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))
        #print(income_statement)

        # 如果不是空数据，将数据存储到mongodb中的income_statement集合中
        if not income_statement.empty:
            #income_statement_collection.insert_many(income_statement.to_dict('records'))
            try:
                # 将数据存储到mongodb中的income_statement集合中
                income_statement_collection.insert_many(income_statement.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        # 获取该股票的资产负债表数据
        balance_sheet = pro.balancesheet(ts_code=stock_code, start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))

        # 如果不是空数据，将数据存储到mongodb中的balance_sheet集合中
        if not balance_sheet.empty:
            #balance_sheet_collection.insert_many(balance_sheet.to_dict('records'))
            try:
                # 将数据存储到mongodb中的balance_sheet集合中
                balance_sheet_collection.insert_many(balance_sheet.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)


        # 获取该股票的现金流量表数据
        cash_flow = pro.cashflow(ts_code=stock_code, start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))

        # 如果不是空数据，将数据存储到mongodb中的cash_flow集合中
        if not cash_flow.empty:
            #cash_flow_collection.insert_many(cash_flow.to_dict('records'))
            try:
                # 将数据存储到mongodb中的cash_flow集合中
                cash_flow_collection.insert_many(cash_flow.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)


        # 获取该股票的财务指标数据
        financial_indicator = pro.fina_indicator(ts_code=stock_code, start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))

        # 如果不是空数据，将数据存储到mongodb中的financial_indicator集合中
        if not financial_indicator.empty:
            #financial_indicator_collection.insert_many(financial_indicator.to_dict('records'))
            try:
                # 将数据存储到mongodb中的financial_indicator集合中
                financial_indicator_collection.insert_many(financial_indicator.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)


        # 获取该股票的日K线数据
        daily_k_data = pro.daily(ts_code=stock_code, start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))

        # 如果不是空数据，将数据存储到mongodb中的daily_k_data集合中
        if not daily_k_data.empty:
            #daily_k_data_collection.insert_many(daily_k_data.to_dict('records'))
            try:
                # 将数据存储到mongodb中的daily_k_data集合中
                daily_k_data_collection.insert_many(daily_k_data.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        # 获取该股票的后复权日K线数据
        daily_k_data_hfq = ts.pro_bar(ts_code=stock_code,  adj='hfq', start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))

        # 如果不是空数据，将数据存储到mongodb中的daily_k_data集合中
        if not daily_k_data_hfq.empty:
            #daily_k_data_collection.insert_many(daily_k_data.to_dict('records'))
            try:
                # 将数据存储到mongodb中的daily_k_data集合中
                daily_k_data_hfq_collection.insert_many(daily_k_data_hfq.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)

        # 获取该股票的周K线数据
        weekly_k_data = pro.weekly(ts_code=stock_code, start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))

        # 如果不是空数据，将数据存储到mongodb中的weekly_k_data集合中
        if not weekly_k_data.empty:
            #weekly_k_data_collection.insert_many(weekly_k_data.to_dict('records'))
            try:
                # 将数据存储到mongodb中的weekly_k_data集合中
                weekly_k_data_collection.insert_many(weekly_k_data.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)


        # 获取该股票的月K线数据
        monthly_k_data = pro.monthly(ts_code=stock_code, start_date=start_date, end_date=datetime.datetime.now().strftime('%Y%m%d'))

        # 如果不是空数据，将数据存储到mongodb中的monthly_k_data集合中
        if not monthly_k_data.empty:
            #monthly_k_data_collection.insert_many(monthly_k_data.to_dict('records'))
            try:
                # 将数据存储到mongodb中的monthly_k_data集合中
                monthly_k_data_collection.insert_many(monthly_k_data.to_dict('records'))
            except pymongo.errors.BulkWriteError as e:
                print("有重复记录：", e.details)


download_and_save_data()

# 将下载日期存储到mongodb中的download_record集合中
download_record.insert_one({"download_date": datetime.datetime.now()})


# 关闭数据库连接
client.close()




