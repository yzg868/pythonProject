import matplotlib.pyplot as plt

# 提取账户价值随时间变化的数据
results = cerebro.run()
strategy = results[0]  # 获取第一个策略
portfolio_values = strategy.analyzers.pyfolio.get_analysis()['portfolio_value']
dates = list(portfolio_values.keys())
values = list(portfolio_values.values())

# 绘制收益曲线
plt.figure(figsize=(10, 6))
plt.plot(dates, values, label="Account Value")
plt.title("Strategy Performance Over Time")
plt.xlabel("Date")
plt.ylabel("Account Value")
plt.legend()
plt.grid()
plt.show()
