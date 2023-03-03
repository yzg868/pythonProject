import pandas as pd
import pymysql

# 读取cwsj.csv文件并将其存储为pandas DataFrame对象
df = pd.read_csv('c:\\gupia\\cwsj2.csv')

# 连接到MySQL数据库
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='root',
                             database='gpsj')

# 创建游标对象
cursor = connection.cursor()

# 创建表
create_table_query = """
CREATE TABLE IF NOT EXISTS cwsj2 (
    ts_code VARCHAR(12),
    ann_date DATE,
    end_date DATE,
    eps FLOAT,
    PRIMARY KEY(ts_code, ann_date),
    UNIQUE(ts_code, ann_date)
)"""

cursor.execute(create_table_query)

# 将DataFrame数据插入到MySQL表中
for _, row in df.iterrows():
    sql_query = f"INSERT IGNORE INTO cwsj2 (ts_code, ann_date, end_date, eps) VALUES ('{row['ts_code']}', '{row['ann_date']}', '{row['end_date']}', {row['eps']})"
    cursor.execute(sql_query)
    connection.commit()

# 关闭连接
connection.close()
print('完成')


