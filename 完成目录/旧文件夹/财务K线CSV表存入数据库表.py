import pandas as pd
import pymysql
from datetime import datetime

def store_csv_to_mysql(table_name):
    # 读取CSV文件并将第一行作为列名
    df = pd.read_csv(f'c:\\gupia\\{table_name}.csv', header=0)

    # 定义MySQL连接参数
    cnx = pymysql.connect(user='root', password='root',
                          host='127.0.0.1', database='gpsj')

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
            elif col == 'change':  # 修改列名
                columns.append("chg float")  # 修改列名对应的类型
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
            elif "_date" in col:  # 如果列名中包含'_date'，则按照日期格式进行转换
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

# 调用函数存储各表数据
store_csv_to_mysql('利润表')
store_csv_to_mysql('财务指标数据表')
store_csv_to_mysql('现金流量表')
store_csv_to_mysql('资产负债表')
store_csv_to_mysql('日K线数据表')
store_csv_to_mysql('周K线数据表')
store_csv_to_mysql('月K线数据表')

print('完成')