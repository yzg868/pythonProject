from pymongo import MongoClient
import backtrader as bt
import pandas as pd
from datetime import datetime, timedelta

# 连接到 MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["stock"]

# 从 merged_data1 集合获取数据
merged_data = pd.DataFrame(list(db["merged_data1"].find()))

# 转换日期格式
merged_data["datetime"] = pd.to_datetime(merged_data["datetime"])

# 确保字段符合 backtrader 格式
merged_data = merged_data.rename(columns={
    "datetime": "datetime",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "eps": "eps"
})

merged_data = merged_data.sort_values(by=["datetime"]).reset_index(drop=True)

# 定义策略类
class EPSHoldStrategy(bt.Strategy):
    params = dict(hold_period=100)  # 持股周期，单位为交易日

    def __init__(self):
        self.eps = self.datas[0].eps
        self.buy_date = None  # 用于记录买入日期

    def next(self):
        # 如果 EPS > 2 且没有持仓，则买入
        if self.eps[0] > 2 and not self.position:
            self.buy(size=100)  # 假设每次买入 100 股
            self.buy_date = self.data.datetime.date(0)
            print(f"买入日期: {self.buy_date}, 价格: {self.data.close[0]}")
        # 如果持仓且已达到持股周期，则卖出
        elif self.position:
            holding_days = (self.data.datetime.date(0) - self.buy_date).days
            if holding_days >= self.params.hold_period:
                self.sell(size=100)
                print(f"卖出日期: {self.data.datetime.date(0)}, 价格: {self.data.close[0]}")

# 准备数据
data = bt.feeds.PandasData(
    dataname=merged_data,
    datetime="datetime",
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
    openinterest=-1,
    fromdate=datetime(2020, 1, 1),
    todate=datetime(2024, 11, 10),
)

# 创建回测系统
cerebro = bt.Cerebro()
cerebro.addstrategy(EPSHoldStrategy)
cerebro.adddata(data)
cerebro.broker.setcash(100000)  # 初始资金 10 万元
cerebro.broker.setcommission(commission=0.001)  # 假设交易手续费为 0.1%

# 运行回测
print("初始资金: %.2f" % cerebro.broker.getvalue())
cerebro.run()
print("回测结束资金: %.2f" % cerebro.broker.getvalue())

# 绘制回测结果
cerebro.plot()
