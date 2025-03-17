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
    "dbname": config["postgresql"]["dbname"],
    "user": config["postgresql"]["user"],
    "password": config["postgresql"]["password"],
    "host": config["postgresql"]["host"],
    "port": config["postgresql"]["port"],
}

# 数据类型映射
TYPE_MAPPING = {
    "INTEGER": "BIGINT",
    "REAL": "NUMERIC",
    "TEXT": "TEXT",
    "varchar": "VARCHAR",
    "char": "CHAR",
    "datetime": "TIMESTAMP WITH TIME ZONE",
    "numeric": "NUMERIC",
    "JSON": "JSONB",
}


def convert_type(sqlite_type):
    """将 SQLite 数据类型转换为 PostgreSQL 数据类型"""
    # 处理 boolean 类型
    if sqlite_type.lower() in ["boolean", "bool", "numeric"]:
        return "BOOLEAN"

    for key, value in TYPE_MAPPING.items():
        if key in sqlite_type:
            return value
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
                    col_type = convert_type(col[2])
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

                # 转换数据
                converted_rows = []
                for row in rows:
                    converted_row = []
                    for i, value in enumerate(row):
                        col_type = col_info[i][2].lower()
                        if (
                            col_type in ["boolean", "bool", "numeric"]
                            and value is not None
                        ):
                            converted_row.append(bool(value))
                        else:
                            # 特殊处理 users 表的 access_token 列
                            if table == "users" and columns[i] == "access_token":
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
