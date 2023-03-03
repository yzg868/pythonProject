import pandas as pd
import pymysql
from datetime import datetime

def insert_data_to_mysql(filename):
    # 读取csv文件并将其存储为pandas DataFrame对象
    df = pd.read_csv(f'c:\\gupia\\{filename}.csv')

    # 连接到MySQL数据库
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='root',
                                 database='gpsj')

    # 创建游标对象
    cursor = connection.cursor()

    # 获取列名
    cols = ', '.join([f'`{col}`' for col in df.columns])

    for _, row in df.iterrows():
        values = []
        for col in df.columns:
            value = row[col]
            if isinstance(value, str):
                value = f"'{value}'"
            elif pd.isnull(value):
                value = 'NULL'
            elif "_date" in col:
                value = datetime.strptime(str(value), '%Y%m%d').strftime('%Y-%m-%d')
                value = f"'{value}'"
            else:
                value = str(value)
            values.append(value)
        values_str = ', '.join(values)
        sql_query = f"INSERT IGNORE INTO {filename} ({cols}) VALUES ({values_str})"
        cursor.execute(sql_query)

    # 提交更改
    connection.commit()

insert_data_to_mysql('利润表')

