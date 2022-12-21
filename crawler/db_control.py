import pymysql
import config

# db연결
def connect_db():
    db_config = config.DATABASE_CONFIG
    conn = pymysql.connect(host = db_config['host'],
                     database = db_config['database'],
                     user = db_config['user'],
                     password = db_config['password'])
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor

# db연결 끊기
def disconnect_db(conn, cursor):
    cursor.close()
    conn.close()

# table select문 실행
def select_table(table_name: str, column_list: list) -> list:
    conn, cursor = connect_db()
    sql_column = ""
    for column in column_list:
        if column == column_list[-1]:
            sql_column += column
        else:
            sql_column += column + ", "

    #select 쿼리문
    select_sql = "SELECT " + sql_column + " FROM " + table_name + ";"
    cursor.execute(select_sql)
    selected_data = cursor.fetchall()
    disconnect_db(conn, cursor)

    return selected_data

# table insert문 실행
def insert_table(table_name: str, insert_data_list: list):
    conn, cursor = connect_db()
    # 요소가 dict형태인 list만 받음
    dict_value_list = sorted(insert_data_list[0].keys())
    values = ""
    for value in dict_value_list:
        if value == dict_value_list[-1]:
            values += "%(" + value + ")s);"
        else:
            values += "%(" + value + ")s,"

    #insert 쿼리문
    insert_sql = "INSERT INTO " + table_name + " VALUES(" + values

    # pk중복시 안멈추고 실행
    for data in insert_data_list:
        try:
            cursor.execute(insert_sql, data)
            conn.commit()
        except:
            continue

    disconnect_db(conn, cursor)

# table update문 실행
def update_table(table_name: str, set_column_list: list, where_column_list: list , update_data_list: list):
    conn, cursor = connect_db()
    for data in update_data_list:
        set_data = ""
        where_data = ""
        for column in set_column_list:
            if column == set_column_list[-1]:
                set_data += f"'{column}' = {data[column]}"
            else:
                set_data += f"'{column}' = {data[column]},"
        for column in where_column_list:
            if column == where_column_list[-1]:
                where_data += f"'{column}' = {data[column]}"
            else:
                where_data += f"'{column}' = {data[column]},"
        update_sql = "UPDATE " + table_name + " SET " + set_data + " WHERE " + where_data + ";"
        cursor.execute(update_sql)
        conn.commit()

    disconnect_db(conn, cursor)





