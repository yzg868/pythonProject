import pymongo

# 连接 MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# 获取 stock 数据库
db = client["stock"]

# 获取利润表集合
income_statement_collection = db['income_statement']
balance_sheet_collection = db['balance_sheet']
cash_flow_collection = db['cash_flow']
financial_indicator_collection = db['financial_indicator']
daily_k_data_collection = db['daily_k_data']
weekly_k_data_collection = db['weekly_k_data']
monthly_k_data_collection = db['monthly_k_data']

# 删除所有记录
income_statement_collection.delete_many({})
balance_sheet_collection.delete_many({})
cash_flow_collection.delete_many({})
financial_indicator_collection.delete_many({})
daily_k_data_collection.delete_many({})
weekly_k_data_collection.delete_many({})
monthly_k_data_collection.delete_many({})


# 创建联合唯一索引
income_statement_collection.create_index(
    [
        ("ts_code", pymongo.ASCENDING),
        ("ann_date", pymongo.DESCENDING),
        ("f_ann_date", pymongo.DESCENDING),
        ("end_date", pymongo.DESCENDING),
        ("update_flag", pymongo.ASCENDING),
    ],
    unique=True
)

balance_sheet_collection.create_index(
    [
        ("ts_code", pymongo.ASCENDING),
        ("ann_date", pymongo.DESCENDING),
        ("f_ann_date", pymongo.DESCENDING),
        ("end_date", pymongo.DESCENDING),
        ("update_flag", pymongo.ASCENDING),
    ],
    unique=True
)

cash_flow_collection.create_index(
    [
        ("ts_code", pymongo.ASCENDING),
        ("ann_date", pymongo.DESCENDING),
        ("f_ann_date", pymongo.DESCENDING),
        ("end_date", pymongo.DESCENDING),
        ("update_flag", pymongo.ASCENDING),
    ],
    unique=True
)

financial_indicator_collection.create_index(
    [
        ("ts_code", pymongo.ASCENDING),
        ("ann_date", pymongo.DESCENDING),
        ("end_date", pymongo.DESCENDING),
    ],
    unique=True
)

daily_k_data_collection.create_index(
    [
        ("ts_code", pymongo.ASCENDING),
        ("trade_date", pymongo.DESCENDING),
    ],
    unique=True
)

weekly_k_data_collection.create_index(
    [
        ("ts_code", pymongo.ASCENDING),
        ("trade_date", pymongo.DESCENDING),
    ],
    unique=True
)

monthly_k_data_collection.create_index(
    [
        ("ts_code", pymongo.ASCENDING),
        ("trade_date", pymongo.DESCENDING),
    ],
    unique=True
)

# 关闭数据库连接
client.close()


