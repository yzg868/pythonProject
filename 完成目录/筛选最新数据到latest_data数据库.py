from pymongo import MongoClient

# 连接到 MongoDB 数据库
client = MongoClient("mongodb://localhost:27017/")

# 删除 latest_data 数据库以确保数据更新
client.drop_database("latest_data")

# 重新连接到 stock 数据库和最新数据数据库
db = client["stock"]
latest_db = client["latest_data"]

# 定义需要筛选的集合和排序字段
collections = {
    "balance_sheet": {"collection": db["balance_sheet"], "date_field": "end_date"},
    "income_statement": {"collection": db["income_statement"], "date_field": "end_date"},
    "cash_flow": {"collection": db["cash_flow"], "date_field": "end_date"},
    "financial_indicator": {"collection": db["financial_indicator"], "date_field": "end_date"},
    "daily_k_data": {"collection": db["daily_k_data"], "date_field": "trade_date"},
    "daily_k_data_hfq": {"collection": db["daily_k_data_hfq"], "date_field": "trade_date"},
    "weekly_k_data": {"collection": db["weekly_k_data"], "date_field": "trade_date"},
    "monthly_k_data": {"collection": db["monthly_k_data"], "date_field": "trade_date"}
}

# 遍历每个集合并提取每只股票的最新数据
for name, info in collections.items():
    collection = info["collection"]
    date_field = info["date_field"]

    # 使用聚合管道获取每只股票的最新数据
    pipeline = [
        {"$sort": {date_field: -1}},  # 按 date_field 降序排列
        {"$group": {
            "_id": "$ts_code",  # 按股票代码 ts_code 分组
            "latest_record": {"$first": "$$ROOT"}  # 获取每组中 date_field 最新的记录
        }}
    ]
    results = collection.aggregate(pipeline)

    # 提取最新记录并准备插入新集合
    latest_data = [record["latest_record"] for record in results]
    latest_collection = latest_db[f"{name}_latest"]  # 新集合名称加后缀 "_latest"

    # 插入数据到新集合
    if latest_data:
        latest_collection.insert_many(latest_data)
        print(f"{name} 集合的最新数据已保存到 latest_data 数据库中的集合 {name}_latest")
    else:
        print(f"{name} 集合中未找到最新数据。")



