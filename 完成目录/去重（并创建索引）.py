from pymongo import MongoClient, ASCENDING

# 连接到 MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['stock']

# 要去重的集合列表及对应字段
collections = [
    ("daily_k_data", ["ts_code", "trade_date"]),
    ("daily_k_data_hfq", ["ts_code", "trade_date"]),
    ("weekly_k_data", ["ts_code", "trade_date"]),
    ("monthly_k_data", ["ts_code", "trade_date"]),
    ("income_statement", ["ts_code", "end_date", "update_flag"]),
    ("balance_sheet", ["ts_code", "end_date", "update_flag"]),
    ("cash_flow", ["ts_code", "end_date", "update_flag"]),
    ("financial_indicator", ["ts_code", "end_date", "update_flag"])
]


def remove_duplicates_and_create_index(collection_name, fields):
    collection = db[collection_name]

    # 查找重复记录并保留最新的一条
    pipeline = [
        {"$group": {
            "_id": {field: f"${field}" for field in fields},
            "count": {"$sum": 1},
            "ids": {"$push": "$_id"}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]

    duplicates = list(collection.aggregate(pipeline))

    # 删除多余的重复记录，只保留一条
    for dup in duplicates:
        ids_to_delete = dup["ids"][1:]  # 保留第一个ID，删除后续重复项
        collection.delete_many({"_id": {"$in": ids_to_delete}})

    # 创建唯一索引
    #index_fields = [(field, ASCENDING) for field in fields]
    #collection.create_index(index_fields, unique=True)
    #print(f"已为集合 {collection_name} 创建唯一索引: {fields}")


# 处理每个集合
for collection_name, fields in collections:
    remove_duplicates_and_create_index(collection_name, fields)

print("去重并创建索引完成")

