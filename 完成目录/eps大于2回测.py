from pymongo import MongoClient
import backtrader as bt
import pandas as pd
from datetime import datetime
import os
import matplotlib.pyplot as plt
import matplotlib

# 设置 Matplotlib 使用支持中文的字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体显示中文
plt.rcParams['axes.unicode_minus'] = False   # 正常显示负号

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
merged_data = merged_data.sort_values(by=["ts_code", "datetime"]).reset_index(drop=True)

# 自定义数据源类，添加 eps 字段
class CustomPandasData(bt.feeds.PandasData):
    lines = ('eps',)  # 新增 eps 行
    params = (('eps', -1),)  # 默认 eps 列索引

# 定义策略类
class TopEPSStrategy(bt.Strategy):
    params = dict(
        start_date=datetime(2022, 1, 1),  # 策略开始日期
        top_n=2,  # 选取 EPS 排名前 N 的股票
        rebalance_days=30,  # 调仓周期（以天为单位）
    )

    def __init__(self):
        self.stocks = {}  # 保存当前持仓股票
        self.last_rebalance = None  # 上次调仓日期
        self.rebalance_log = []  # 调仓记录
        self.portfolio_values = []  # 账户价值记录
        self.dates = []  # 日期记录

    def next(self):
        current_date = self.data.datetime.date(0)

        # 记录当前账户价值
        self.portfolio_values.append(self.broker.getvalue())
        self.dates.append(current_date)

        # 跳过策略开始日期之前的时间
        if current_date < self.params.start_date.date():
            return

        # 检查是否到达调仓日
        if self.last_rebalance and (current_date - self.last_rebalance).days < self.params.rebalance_days:
            return

        # 更新上次调仓日期
        self.last_rebalance = current_date
        print(f"调仓日期: {current_date}")

        # 获取当前日期所有股票的 EPS 数据
        data_by_stock = {}
        for data in self.datas:
            if data.datetime.date(0) == current_date:
                data_by_stock[data._name] = data.eps[0]

        # 根据 EPS 排序选取前 N 名股票
        sorted_stocks = sorted(data_by_stock.items(), key=lambda x: x[1], reverse=True)
        top_stocks = [stock for stock, eps in sorted_stocks[:self.params.top_n]]

        # 初始化调仓记录
        rebalance_record = {"date": current_date, "buy": [], "sell": []}

        # 卖出不在新排名中的股票
        for stock in list(self.stocks.keys()):
            if stock not in top_stocks:
                self.close(data=self.getdatabyname(stock))
                del self.stocks[stock]
                print(f"卖出: {stock}")
                rebalance_record["sell"].append(stock)

        # 买入排名前 N 的股票
        for stock in top_stocks:
            if stock not in self.stocks:
                cash_per_stock = self.broker.getcash() * 0.5 / self.params.top_n  # 50% 资金分配给 top N
                data = self.getdatabyname(stock)
                size = int(cash_per_stock // data.close[0])
                if size > 0:
                    self.buy(data=data, size=size)
                    self.stocks[stock] = size
                    print(f"买入: {stock}, 持仓: {size} 股")
                    rebalance_record["buy"].append(stock)

        # 如果无买卖操作，不保存记录
        if rebalance_record["buy"] or rebalance_record["sell"]:
            self.rebalance_log.append(rebalance_record)

    def stop(self):
        # 创建目标目录
        output_dir = "c:/stock/调仓记录"
        os.makedirs(output_dir, exist_ok=True)

        # 保存调仓记录
        rebalance_df = pd.DataFrame(self.rebalance_log)
        rebalance_df.to_csv(os.path.join(output_dir, "../测试目录/rebalance_log.csv"), index=False)
        print(f"调仓记录已保存到 {os.path.join(output_dir, '../测试目录/rebalance_log.csv')}")

        # 保存账户价值记录
        portfolio_df = pd.DataFrame({
            "date": self.dates,
            "portfolio_value": self.portfolio_values
        })
        portfolio_df.to_csv(os.path.join(output_dir, "portfolio_values.csv"), index=False)
        print(f"账户价值已保存到 {os.path.join(output_dir, 'portfolio_values.csv')}")

# 准备回测系统
cerebro = bt.Cerebro()

# 添加每只股票的数据
for stock in merged_data["ts_code"].unique():
    stock_data = merged_data[merged_data["ts_code"] == stock]
    data = CustomPandasData(
        dataname=stock_data,
        datetime="datetime",
        open="open",
        high="high",
        low="low",
        close="close",
        volume="volume",
        eps="eps",
        openinterest=-1,
    )
    cerebro.adddata(data, name=stock)

# 添加策略
cerebro.addstrategy(TopEPSStrategy)

# 设置初始资金、手续费等
cerebro.broker.setcash(1000000)  # 初始资金 100 万元
cerebro.broker.setcommission(commission=0.001)  # 假设交易手续费为 0.1%

# 添加分析器
cerebro.addanalyzer(bt.analyzers.PyFolio, _name="pyfolio")
cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe")
cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name="annual_return")
cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")

# 运行回测
print("初始资金: %.2f" % cerebro.broker.getvalue())
results = cerebro.run()
print("回测结束资金: %.2f" % cerebro.broker.getvalue())

# 提取分析结果
strategy = results[0]
sharpe_ratio = strategy.analyzers.sharpe.get_analysis()
drawdown = strategy.analyzers.drawdown.get_analysis()

# 计算总收益率和年化收益率
initial_cash = 1000000
ending_cash = cerebro.broker.getvalue()
total_return = (ending_cash - initial_cash) / initial_cash  # 总收益率

# 计算回测年数
start_date = merged_data["datetime"].min()
end_date = merged_data["datetime"].max()
backtest_years = (end_date - start_date).days / 365.25

# 按年数计算年化收益率
annual_return_value = (1 + total_return) ** (1 / backtest_years) - 1

# 绘制收益曲线并显示指标
plt.figure(figsize=(12, 8))
plt.plot(strategy.dates, strategy.portfolio_values, label="账户价值", color='blue')
plt.title("回测收益曲线", fontsize=16)
plt.xlabel("日期", fontsize=12)
plt.ylabel("账户价值", fontsize=12)
plt.legend()

# 添加文本信息
plt.text(strategy.dates[0], ending_cash * 0.9,
         f"初始资金: {initial_cash:.2f}\n"
         f"回测结束资金: {ending_cash:.2f}\n"
         f"总收益率: {total_return * 100:.2f}%\n"
         f"年化收益率: {annual_return_value * 100:.2f}%\n"
         f"夏普比率: {sharpe_ratio['sharperatio']:.2f}\n"
         f"最大回撤: {drawdown['max']['drawdown']:.2f}%",
         fontsize=10, color="black", bbox=dict(facecolor='white', alpha=0.5))

plt.grid()
plt.show()