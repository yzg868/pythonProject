from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# 连接到MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["latest_data"]

# 读取最新利润表和资产负债表集合
income_collection = db["income_statement_latest"]
balance_collection = db["balance_sheet_latest"]

# 查询基本每股收益大于0.3的数据
income_data = income_collection.find({"basic_eps": {"$gt": 0.3}})
income_df = pd.DataFrame(list(income_data))

# 查询期末总股本小于2亿的数据
balance_data = balance_collection.find({"total_share": {"$lt": 2 * 10**8}})
balance_df = pd.DataFrame(list(balance_data))

# 检查是否包含'ts_code'列
if "ts_code" not in income_df.columns or "ts_code" not in balance_df.columns:
    raise KeyError("Error: 'ts_code' column not found in one or both DataFrames.")

# 合并两个DataFrame
result_df = pd.merge(income_df, balance_df, on="ts_code")

# 获取当前日期，用于文件命名
current_date = datetime.now().strftime("%Y%m%d")
file_path = f"C:/stock/筛选结果_{current_date}.xlsx"

# 保存到Excel文件
result_df.to_excel(file_path, index=False)
print(f"筛选结果已保存到 {file_path}")



