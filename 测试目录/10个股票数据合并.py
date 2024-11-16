from pymongo import MongoClient
import pandas as pd

# 连接到 MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stock"]

# 获取日K线数据和基本每股收益数据
daily_k_data = pd.DataFrame(
    list(db["daily_k_data_hfq"].find({"trade_date": {"$gte": "20200101", "$lte": "20241110"}}))
)
income_data = pd.DataFrame(list(db["income_statement"].find({"ann_date": {"$gte": "20200101"}})))

# 筛选出前 10 只股票
selected_stocks = daily_k_data["ts_code"].unique()[:10]
daily_k_data = daily_k_data[daily_k_data["ts_code"].isin(selected_stocks)]
income_data = income_data[income_data["ts_code"].isin(selected_stocks)]

# 转换日期格式
daily_k_data["trade_date"] = pd.to_datetime(daily_k_data["trade_date"], format="%Y%m%d")
income_data["ann_date"] = pd.to_datetime(income_data["ann_date"], format="%Y%m%d")
income_data["next_ann_date"] = income_data.groupby("ts_code")["ann_date"].shift(-1).fillna("20241110")
income_data["next_ann_date"] = pd.to_datetime(income_data["next_ann_date"], format="%Y%m%d")

# 数据合并
merged_data = pd.merge(daily_k_data, income_data, on="ts_code", how="inner")
merged_data = merged_data[
    (merged_data["trade_date"] >= merged_data["ann_date"]) &
    (merged_data["trade_date"] < merged_data["next_ann_date"])
]

# 提取并重命名字段，保留 ts_code
merged_data = merged_data[["ts_code", "trade_date", "open", "high", "low", "close", "vol", "basic_eps"]]
merged_data = merged_data.rename(columns={
    "trade_date": "datetime",
    "vol": "volume",
    "basic_eps": "eps"
})

# 批量插入合并数据
if not merged_data.empty:
    merged_collection = db["merged_data1"]
    merged_collection.delete_many({})  # 清空旧数据
    merged_collection.insert_many(merged_data.to_dict("records"))
    print("数据合并完成，并存储到 merged_data1 集合中！")
else:
    print("合并数据为空，请检查数据源。")
