import sqlite3
import psycopg
from psycopg import sql
import tomllib

# 读取配置文件
with open("config.toml", "rb") as f:
    config = tomllib.load(f)

# 数据库连接配置
sqlite_db_file = config["database"]["sqlite_file"]
pg_db_config = {
    "dbname": config["postgresql"]["cloud"]["dbname"],
    "user": config["postgresql"]["cloud"]["user"],
    "password": config["postgresql"]["cloud"]["password"],
    "host": config["postgresql"]["cloud"]["host"],
    "port": int(config["postgresql"]["cloud"]["port"]),
}

# 基于实际schema的显式类型映射
SCHEMA_MAPPING = {
    "channels": {"only_chat": "BOOLEAN", "status": "BIGINT", "type": "BIGINT"},
    "logs": {"is_stream": "BOOLEAN"},
    "user_groups": {"public": "BOOLEAN", "enable": "BOOLEAN"},
    "tokens": {"unlimited_quota": "BOOLEAN", "chat_cache": "BOOLEAN"},
    "payments": {
        "enable": "BOOLEAN",
        "fixed_fee": "NUMERIC(10,2)",
        "percent_fee": "NUMERIC(10,2)",
    },
    "users": {"access_token": "VARCHAR(32)"},
}


def convert_type(sqlite_type, table_name, col_name):
    """根据schema映射表转换数据类型"""
    # 首先检查是否有显式映射
    if table_name in SCHEMA_MAPPING and col_name in SCHEMA_MAPPING[table_name]:
        return SCHEMA_MAPPING[table_name][col_name]

    # 默认类型映射
    sqlite_type = sqlite_type.lower().strip()
    if sqlite_type in ["integer", "bigint"]:
        return "BIGINT"
    if sqlite_type in ["real", "float", "double", "numeric", "decimal"]:
        return "NUMERIC(10,2)"
    if sqlite_type in ["text", "varchar", "char"]:
        # 提取长度信息
        if "(" in sqlite_type:
            length = sqlite_type.split("(")[1].split(")")[0]
            return f"VARCHAR({length})"
        return "TEXT"
    if sqlite_type in ["datetime", "timestamp"]:
        return "TIMESTAMP WITH TIME ZONE"
    if sqlite_type == "date":
        return "DATE"
    if sqlite_type == "time":
        return "TIME"
    if sqlite_type == "blob":
        return "BYTEA"
    if sqlite_type in ["boolean", "bool"]:
        return "BOOLEAN"
    if sqlite_type in ["json"]:
        return "JSONB"

    return "TEXT"


def migrate_table_structure(sqlite_conn, pg_conn):
    """迁移表结构"""
    sqlite_cursor = sqlite_conn.cursor()

    # 获取 SQLite 中的所有表
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in sqlite_cursor.fetchall()]

    # 排除不需要迁移的表
    tables_to_exclude = ["sqlite_sequence"]
    tables = [table for table in tables if table not in tables_to_exclude]

    for table in tables:
        # 为每个表创建使用独立连接
        with pg_conn.cursor() as pg_cursor:
            try:
                # 开始新的事务
                pg_cursor.execute("BEGIN;")

                # 获取表结构
                sqlite_cursor.execute(f"PRAGMA table_info({table});")
                columns = sqlite_cursor.fetchall()

                # 构建 CREATE TABLE 语句
                column_defs = []
                for col in columns:
                    col_name = f'"{col[1]}"' if col[1].lower() == "group" else col[1]
                    col_type = convert_type(col[2], table, col[1])
                    # 特殊处理 users 表的 access_token 列
                    if table == "users" and col[1] == "access_token":
                        col_type = "VARCHAR(32)"
                    not_null = " NOT NULL" if col[3] else ""
                    default = f" DEFAULT '{col[4]}'" if col[4] else ""
                    column_defs.append(f"{col_name} {col_type}{not_null}{default}")

                # 添加主键
                sqlite_cursor.execute(f"PRAGMA table_info({table});")
                pk_columns = [col[1] for col in columns if col[5]]
                if pk_columns:
                    column_defs.append(f"PRIMARY KEY ({', '.join(pk_columns)})")

                # 如果表存在则先删除
                pg_cursor.execute(f"DROP TABLE IF EXISTS {table};")

                # 特殊处理 abilities 表的主键
                if table == "abilities":
                    column_defs = [
                        col for col in column_defs if not col.startswith("PRIMARY KEY")
                    ]
                    column_defs.append('PRIMARY KEY ("group", model, channel_id)')

                # 创建表
                create_table_sql = (
                    f"CREATE TABLE {table} (\n    "
                    + ",\n    ".join(column_defs)
                    + "\n);"
                )
                pg_cursor.execute(create_table_sql)
                pg_cursor.execute("COMMIT;")
                print(f"Created table {table}")
            except Exception as e:
                pg_cursor.execute("ROLLBACK;")
                print(f"Error creating table {table}: {e}")

def migrate_data(sqlite_conn, pg_conn):
    """迁移数据"""
    sqlite_cursor = sqlite_conn.cursor()

    # 获取 SQLite 中的所有表
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in sqlite_cursor.fetchall()]

    # 排除不需要迁移的表
    tables_to_exclude = ["sqlite_sequence"]
    tables = [table for table in tables if table not in tables_to_exclude]

    for table in tables:
        # 为每个表创建使用独立连接
        with pg_conn.cursor() as pg_cursor:
            try:
                # 开始新的事务
                pg_cursor.execute("BEGIN;")

                # 获取列名
                sqlite_cursor.execute(f"PRAGMA table_info({table});")
                columns = [col[1] for col in sqlite_cursor.fetchall()]

                # 查询数据
                sqlite_cursor.execute(f"SELECT * FROM {table};")
                rows = sqlite_cursor.fetchall()

                # 获取列类型信息
                sqlite_cursor.execute(f"PRAGMA table_info({table});")
                col_info = sqlite_cursor.fetchall()

                # 获取 PostgreSQL 列类型
                pg_cursor.execute(
                    f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;",
                    (table,),
                )
                pg_col_types = {col[0]: col[1] for col in pg_cursor.fetchall()}

                # 转换数据
                converted_rows = []
                for row in rows:
                    converted_row = []
                    for i, value in enumerate(row):
                        col_name = columns[i]
                        col_type = col_info[i][2].lower()
                        pg_type = pg_col_types.get(col_name, "").lower()

                        # 处理 boolean 类型
                        if pg_type == "boolean":
                            # 将各种可能的boolean表示转换为True/False
                            if value in [1, "1", "true", "True", "TRUE", "t", "T"]:
                                converted_row.append(True)
                            elif value in [0, "0", "false", "False", "FALSE", "f", "F"]:
                                converted_row.append(False)
                            else:
                                converted_row.append(None)
                        # 处理 numeric 类型
                        elif (
                            col_type in ["numeric", "decimal", "real"]
                            or pg_type == "numeric"
                        ):
                            # 确保 numeric 值被正确转换为 Decimal
                            try:
                                converted_row.append(
                                    float(value) if value is not None else None
                                )
                            except (ValueError, TypeError):
                                converted_row.append(None)
                        # 处理 integer 类型
                        elif col_type in ["integer", "bigint"]:
                            converted_row.append(
                                int(value) if value is not None else None
                            )
                        else:
                            # 特殊处理 users 表的 access_token 列
                            if table == "users" and col_name == "access_token":
                                converted_row.append(str(value)[:32])
                            else:
                                converted_row.append(value)
                    converted_rows.append(tuple(converted_row))

                # 插入数据
                insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                )
                pg_cursor.executemany(insert_sql, converted_rows)
                pg_cursor.execute("COMMIT;")
                print(f"Migrated {len(converted_rows)} rows to table {table}")
            except Exception as e:
                pg_cursor.execute("ROLLBACK;")
                print(f"Error migrating data to table {table}: {e}")

def main():
    # 连接数据库
    try:
        sqlite_conn = sqlite3.connect(sqlite_db_file)
        pg_conn = psycopg.connect(**pg_db_config)

        # 迁移表结构
        migrate_table_structure(sqlite_conn, pg_conn)

        # 迁移数据
        migrate_data(sqlite_conn, pg_conn)

        # 提交事务
        pg_conn.commit()

    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        if sqlite_conn:
            sqlite_conn.close()
        if pg_conn:
            pg_conn.close()

if __name__ == "__main__":
    main()
