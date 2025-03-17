import argparse
import sqlite3
import psycopg
import tomllib
from psycopg import sql


def get_pg_config(config, db_type):
    """获取 PostgreSQL 配置"""
    return {
        "dbname": config[f"postgresql.{db_type}"]["dbname"],
        "user": config[f"postgresql.{db_type}"]["user"],
        "password": config[f"postgresql.{db_type}"]["password"],
        "host": config[f"postgresql.{db_type}"]["host"],
        "port": config[f"postgresql.{db_type}"]["port"],
    }


# 数据类型映射
TYPE_MAPPING = {
    "bigint": "INTEGER",
    "numeric": "REAL",
    "text": "TEXT",
    "varchar": "TEXT",
    "char": "TEXT",
    "timestamp with time zone": "TEXT",
    "boolean": "INTEGER",
    "jsonb": "TEXT",
}


def convert_type(pg_type):
    """将 PostgreSQL 数据类型转换为 SQLite 数据类型"""
    for key, value in TYPE_MAPPING.items():
        if key in pg_type.lower():
            return value
    return "TEXT"


def sync_table(pg_conn, sqlite_conn, table):
    """同步单个表"""
    with pg_conn.cursor() as pg_cursor, sqlite_conn.cursor() as sqlite_cursor:
        try:
            # 开始事务
            sqlite_conn.execute("BEGIN;")

            # 获取表结构
            pg_cursor.execute(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;",
                (table,),
            )
            columns = pg_cursor.fetchall()

            # 创建表（如果不存在）
            column_defs = []
            for col in columns:
                col_name = col[0]
                col_type = convert_type(col[1])
                column_defs.append(f"{col_name} {col_type}")

            create_table_sql = (
                f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(column_defs)});"
            )
            sqlite_cursor.execute(create_table_sql)

            # 删除目标表数据
            sqlite_cursor.execute(f"DELETE FROM {table};")

            # 查询源表数据
            pg_cursor.execute(f"SELECT * FROM {table};")
            rows = pg_cursor.fetchall()

            # 插入数据
            placeholders = ",".join(["?"] * len(columns))
            insert_sql = f"INSERT INTO {table} ({','.join([col[0] for col in columns])}) VALUES ({placeholders})"
            sqlite_cursor.executemany(insert_sql, rows)
            sqlite_conn.commit()
            print(f"Synced {len(rows)} rows to table {table}")
        except Exception as e:
            sqlite_conn.rollback()
            print(f"Error syncing table {table}: {e}")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Sync PostgreSQL to SQLite")
    parser.add_argument(
        "source", choices=["cloud", "local"], help="Source database: cloud or local"
    )
    args = parser.parse_args()

    # 读取配置文件
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)

    # 获取 PostgreSQL 配置
    pg_config = get_pg_config(config, args.source)

    # 连接数据库
    pg_conn = psycopg.connect(**pg_config)
    sqlite_conn = sqlite3.connect(config["database"]["target_sqlite"])

    try:
        # 获取所有表名
        with pg_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
            """
            )
            tables = [row[0] for row in cursor.fetchall()]

        # 同步每个表
        for table in tables:
            sync_table(pg_conn, sqlite_conn, table)

    finally:
        pg_conn.close()
        sqlite_conn.close()


if __name__ == "__main__":
    main()
