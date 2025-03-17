import argparse
import psycopg
import tomllib
from psycopg import sql


def get_db_config(config, db_type):
    """获取数据库配置"""
    return {
        "dbname": config[f"postgresql.{db_type}"]["dbname"],
        "user": config[f"postgresql.{db_type}"]["user"],
        "password": config[f"postgresql.{db_type}"]["password"],
        "host": config[f"postgresql.{db_type}"]["host"],
        "port": config[f"postgresql.{db_type}"]["port"],
    }


def sync_table(src_conn, dst_conn, table):
    """同步单个表"""
    with src_conn.cursor() as src_cursor, dst_conn.cursor() as dst_cursor:
        try:
            # 开始事务
            dst_cursor.execute("BEGIN;")

            # 删除目标表数据
            dst_cursor.execute(f"TRUNCATE TABLE {table};")

            # 查询源表数据
            src_cursor.execute(f"SELECT * FROM {table};")
            rows = src_cursor.fetchall()

            # 获取列名
            src_cursor.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = %s;",
                (table,),
            )
            columns = [row[0] for row in src_cursor.fetchall()]

            # 插入数据
            insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            )
            dst_cursor.executemany(insert_sql, rows)
            dst_cursor.execute("COMMIT;")
            print(f"Synced {len(rows)} rows to table {table}")
        except Exception as e:
            dst_cursor.execute("ROLLBACK;")
            print(f"Error syncing table {table}: {e}")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Sync PostgreSQL databases")
    parser.add_argument(
        "direction",
        choices=["cloud-to-local", "local-to-cloud"],
        help="Sync direction: cloud-to-local or local-to-cloud",
    )
    args = parser.parse_args()

    # 读取配置文件
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)

    # 确定源和目标数据库
    if args.direction == "cloud-to-local":
        src_config = get_db_config(config, "cloud")
        dst_config = get_db_config(config, "local")
    else:
        src_config = get_db_config(config, "local")
        dst_config = get_db_config(config, "cloud")

    # 连接数据库
    src_conn = psycopg.connect(**src_config)
    dst_conn = psycopg.connect(**dst_config)

    try:
        # 获取所有表名
        with src_conn.cursor() as cursor:
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
            sync_table(src_conn, dst_conn, table)

    finally:
        src_conn.close()
        dst_conn.close()


if __name__ == "__main__":
    main()
