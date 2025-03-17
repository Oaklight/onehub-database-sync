import sqlite3

# 替换为你的 SQLite 数据库文件路径
sqlite_db_file = "one-hub/api_dir/data/one-api.db"

try:
    conn = sqlite3.connect(sqlite_db_file)
    cursor = conn.cursor()
    print("Successfully connected to SQLite database.")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = sorted(cursor.fetchall(), key=lambda x: x[0])

    print("\nTables in the database:")
    for table in tables:
        table_name = table[
            0
        ]  #  `fetchall()` 返回的是一个列表，每个元素是一个元组，元组的第一个元素是表名
        print(f"- {table_name}")

    for table in tables:
        table_name = table[0]
        print(f"\nDetails for table '{table_name}':")

        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = sorted(cursor.fetchall(), key=lambda x: x[1])

        # 打印表头
        print(
            f"{'Column Name':<20} {'Type':<15} {'Not Null':<8} {'PK':<3}"
        )  # <20 表示左对齐，占20个字符宽度
        print("-" * 50)

        for column in columns:
            # column 的结构是： (cid, name, type, notnull, dflt_value, pk)
            col_name = column[1]
            col_type = column[2]
            col_notnull = "YES" if column[3] == 1 else "NO"
            col_pk = "YES" if column[5] == 1 else "NO"

            print(f"{col_name:<20} {col_type:<15} {col_notnull:<8} {col_pk:<3}")

    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        foreign_keys = cursor.fetchall()

        if foreign_keys:
            print(f"\nForeign Keys in table '{table_name}':")
            for fk in sorted(foreign_keys, key=lambda x: x[3]):
                # fk 的结构是： (id, seq, table, from, to, on_update, on_delete, match)
                fk_id = fk[0]
                fk_from_col = fk[3]
                fk_to_table = fk[2]
                fk_to_col = fk[4]
                fk_on_update = fk[5]
                fk_on_delete = fk[6]

                print(
                    f"  - ID: {fk_id}, From: {fk_from_col}, To Table: {fk_to_table}, To Column: {fk_to_col}, "
                    f"ON UPDATE: {fk_on_update}, ON DELETE: {fk_on_delete}"
                )
except sqlite3.Error as e:
    print(f"Error connecting to SQLite: {e}")
    exit()  # 连接失败则退出程序


finally:  # 用finally来确保连接一定会被关闭
    if conn:
        conn.close()
        print("\nSQLite database connection closed.")
