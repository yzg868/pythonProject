from pymongo import MongoClient
import backtrader as bt
import pandas as pd
from datetime import datetime

# 连接到 MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stock"]

# 获取日K线数据和基本每股收益数据
daily_k_data = pd.DataFrame(
    list(db["daily_k_data_hfq"].find({"trade_date": {"$gte": "20200101", "$lte": "20241110"}}))
)
income_data = pd.DataFrame(list(db["income_statement"].find({"ann_date": {"$gte": "20200101"}})))

# 使用列名 'basic_eps' 筛选基本每股收益大于 2 的数据
income_data = income_data[income_data["basic_eps"] > 2]

# 对基本每股收益数据进行公告日期的区间填充
income_data = income_data.sort_values(by=["ts_code", "ann_date"])
income_data["next_ann_date"] = income_data.groupby("ts_code")["ann_date"].shift(-1).fillna("20241110")

# 将日K线数据和基本每股收益合并
merged_data = []

for code in daily_k_data["ts_code"].unique():
    k_data = daily_k_data[daily_k_data["ts_code"] == code]
    inc_data = income_data[income_data["ts_code"] == code]

    for _, row in inc_data.iterrows():
        # 筛选当前利润表记录对应的时间区间
        period_data = k_data[
            (k_data["trade_date"] >= row["ann_date"]) & (k_data["trade_date"] < row["next_ann_date"])
        ]
        if not period_data.empty:  # 检查 period_data 是否为空
            period_data = period_data.copy()  # 显式创建副本以避免链式赋值警告
            period_data.loc[:, "eps"] = row["basic_eps"]  # 使用当前区间的 basic_eps
            merged_data.append(period_data)

if merged_data:
    merged_data = pd.concat(merged_data)
    merged_data = merged_data.reset_index(drop=True)
else:
    print("未生成合并数据，检查输入数据。")
    merged_data = pd.DataFrame()

# 确保有数据再执行后续操作
if not merged_data.empty:
    # 将合并后的数据存储到合并数据集合中
    merged_collection = db["merged_data"]
    merged_collection.delete_many({})  # 清空旧数据
    merged_collection.insert_many(merged_data.to_dict("records"))

    # 回测系统
    class EPS2Strategy(bt.Strategy):
        def __init__(self):
            self.close = self.datas[0].close

        def next(self):
            # 策略逻辑：如果 eps > 2，且当前未持仓，则买入；否则卖出
            if self.data.eps[0] > 2:
                if not self.position:
                    self.buy(size=100)  # 买入100股
            elif self.position:
                self.sell(size=100)  # 卖出100股

    # 将 merged_data 转换为 backtrader 数据格式
    data = bt.feeds.PandasData(
        dataname=merged_data,
        datetime=None,
        openinterest=-1,
        fromdate=datetime(2020, 1, 1),
        todate=datetime(2024, 11, 10),
    )

    # 创建回测系统
    cerebro = bt.Cerebro()
    cerebro.addstrategy(EPS2Strategy)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000)  # 初始资金
    cerebro.run()  # 运行回测
    cerebro.plot()  # 绘制回测结果
else:
    print("合并数据为空，无法进行回测。")
