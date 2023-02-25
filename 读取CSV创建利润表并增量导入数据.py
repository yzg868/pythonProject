import pandas as pd
import pymysql
from datetime import datetime

# 读取CSV文件并将第一行作为列名
df = pd.read_csv('c:\\gupia\\利润表.csv', header=0)

# 定义MySQL连接参数
cnx = pymysql.connect(user='root', password='root',
                      host='127.0.0.1', database='gpsj')

# 定义要创建的表格名称
table_name = '利润表'

# 创建MySQL表格
cursor = cnx.cursor()

# 判断表格是否存在
cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
result = cursor.fetchone()
if result:
    print(f"Table '{table_name}' already exists.")
else:
    # 创建表格的列和类型
    columns = []
    for col in df.columns:
        if col == df.columns[0]:
            columns.append(f"{col} varchar(12)")
        elif "_date" in col:  # 如果列名中包含'_date'，则设置为date类型
            columns.append(f"{col} date")
        else:
            columns.append(f"{col} float")

    # 组合列和类型
    columns_str = ', '.join(columns)

    # 创建表格的SQL语句
    create_table_query = f"CREATE TABLE {table_name} ({columns_str})"

    # 执行SQL语句
    cursor.execute(create_table_query)

# 将数据写入MySQL表格
for _, row in df.iterrows():
    values = []
    for col in df.columns:
        value = row[col]
        if isinstance(value, str):
            value = f"'{value}'"
        elif pd.isnull(value):
            value = 'NULL'
        elif col in [df.columns[1], df.columns[2], df.columns[3]]:
            value = datetime.strptime(str(value), '%Y%m%d').strftime('%Y-%m-%d')
            value = f"'{value}'"
        else:
            value = str(value)
        values.append(value)
    values_str = ', '.join(values)

    # 根据数据是否已存在判断是否插入新记录
    select_query = f"SELECT * FROM {table_name} WHERE {df.columns[0]}='{row[df.columns[0]]}' AND {df.columns[1]}='{row[df.columns[1]]}'"
    cursor.execute(select_query)
    result = cursor.fetchone()

    if result:
        continue
    else:
        # 如果不存在，则插入新记录
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({values_str})"
        cursor.execute(insert_query)


# 提交更改并关闭连接
cnx.commit()
cursor.close()
cnx.close()

print ('完成')
