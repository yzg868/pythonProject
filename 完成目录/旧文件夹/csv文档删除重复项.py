import pandas as pd

# 读取CSV文件
df = pd.read_csv('c:/gupia/利润表.csv')

# 删除重复行
#df.drop_duplicates(keep='first', inplace=True)
df.drop_duplicates(subset=['ts_code', 'ann_date','f_ann_date','end_date'], keep='first', inplace=True)


# 将数据写回CSV文件
df.to_csv('c:/gupia/利润表.csv', index=False)
