#!/usr/bin/env python3
"""
Script to migrate data from SQLite to PostgreSQL database.
Handles table structure, data migration, and sequence synchronization.
"""

import os
import sys
import sqlite3
import psycopg2
from psycopg2 import sql
import tomllib

# 数据类型映射配置
SCHEMA_MAPPING = {
    "channels": {"only_chat": "BOOLEAN", "status": "BIGINT", "type": "BIGINT"},
    "logs": {"is_stream": "BOOLEAN"},
    "user_groups": {
        "public": "BOOLEAN", 
        "enable": "BOOLEAN",
        "promotion": "BOOLEAN"  # 修复：标记为BOOLEAN而不是NUMERIC
    },
    "tokens": {"unlimited_quota": "BOOLEAN", "chat_cache": "BOOLEAN"},
    "payments": {
        "enable": "BOOLEAN",
        "fixed_fee": "NUMERIC(10,2)",
        "percent_fee": "NUMERIC(10,2)",
    },
    "prices": {
        "locked": "BOOLEAN"  # 修复：标记为BOOLEAN而不是NUMERIC
    },
    "users": {"access_token": "VARCHAR(32)"},
}

def load_config():
    """
    加载配置文件，返回配置字典。
    首先尝试从配置文件加载，如果失败则尝试从环境变量获取。
    """
    try:
        # 先尝试从配置文件加载
        config_path = os.environ.get("CONFIG_PATH", "config.toml")
        print(f"Loading configuration from: {config_path}")
        
        if not os.path.exists(config_path):
            print(f"Warning: Configuration file not found at {config_path}")
            # 尝试从环境变量构建配置
            return load_config_from_env()
            
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
        
        # 验证必要的配置项
        if "database" not in config or "postgresql" not in config:
            print("Error: Invalid configuration file structure")
            return load_config_from_env()
            
        # 验证PostgreSQL配置中是否有占位符值
        pg_config = config["postgresql"]["cloud"]
        if (pg_config["user"] == "your_cloud_user" or 
            pg_config["password"] == "your_cloud_password" or
            pg_config["dbname"] == "your_cloud_db"):
            print("Warning: PostgreSQL configuration contains placeholder values")
            print("Attempting to use environment variables instead")
            return load_config_from_env()
            
        return config
    except Exception as e:
        print(f"Error loading configuration from file: {e}")
        return load_config_from_env()

def load_config_from_env():
    """
    从环境变量中加载配置
    """
    print("Loading configuration from environment variables")
    try:
        # 确保必要的环境变量已设置
        required_vars = ["PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DATABASE", "SQLITE_PATH"]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
            print("Required variables are: PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE, SQLITE_PATH")
            return None
            
        # 构建配置字典
        config = {
            "database": {
                "sqlite_file": os.environ.get("SQLITE_PATH")
            },
            "postgresql": {
                "cloud": {
                    "host": os.environ.get("PG_HOST"),
                    "port": os.environ.get("PG_PORT"),
                    "user": os.environ.get("PG_USER"),
                    "password": os.environ.get("PG_PASSWORD"),
                    "dbname": os.environ.get("PG_DATABASE")
                }
            }
        }
        
        return config
    except Exception as e:
        print(f"Error loading configuration from environment variables: {e}")
        return None

def convert_type(sqlite_type, table_name, col_name):
    """
    根据schema映射表转换数据类型
    """
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

def test_pg_connection(pg_config):
    """
    测试PostgreSQL连接是否可用
    """
    try:
        print(f"Testing PostgreSQL connection to {pg_config['host']}:{pg_config['port']} as {pg_config['user']}")
        conn = psycopg2.connect(**pg_config)
        conn.close()
        print("PostgreSQL connection test successful")
        return True
    except Exception as e:
        print(f"PostgreSQL connection test failed: {e}")
        # 提供更详细的故障排除建议
        print("\nTroubleshooting suggestions:")
        print("1. Check if your PostgreSQL credentials are correct")
        print("2. Ensure the database server is running and accessible from your network")
        print("3. Verify your user has the necessary permissions")
        print("4. If using Supabase, check your project settings and connection string")
        print("5. Try setting the environment variables manually:")
        print("   export PG_HOST=your_host")
        print("   export PG_PORT=your_port")
        print("   export PG_USER=your_user")
        print("   export PG_PASSWORD=your_password")
        print("   export PG_DATABASE=your_database")
        print("   export SQLITE_PATH=path_to_sqlite_file")
        print("   Then run the script again")
        return False

def format_default_value(col_type, default_value):
    """
    根据列类型格式化默认值
    """
    if default_value is None or default_value == "":
        return ""
        
    # 对于布尔类型，转换特殊的默认值
    if col_type == "BOOLEAN":
        if default_value.lower() in ["true", "t", "1", "yes", "y"]:
            return " DEFAULT TRUE"
        elif default_value.lower() in ["false", "f", "0", "no", "n"]:
            return " DEFAULT FALSE"
    
    # 对于数值类型，确保默认值是数字
    if "NUMERIC" in col_type or "INT" in col_type:
        try:
            # 尝试将值转换为数字
            float(default_value)
            return f" DEFAULT {default_value}"
        except (ValueError, TypeError):
            # 如果不是有效的数字，则不设置默认值
            return ""
            
    # 其他类型的默认值按原样处理
    return f" DEFAULT '{default_value}'"

def migrate_table_structure(sqlite_conn, pg_conn):
    """
    迁移表结构从SQLite到PostgreSQL
    """
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
                    not_null = " NOT NULL" if col[3] else ""
                    
                    # 使用新的格式化函数处理默认值
                    default = format_default_value(col_type, col[4]) if col[4] else ""
                    
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
                print(f"SQL was: {create_table_sql if 'create_table_sql' in locals() else 'Not available'}")

def validate_numeric_data(sqlite_cursor, table, columns):
    """
    验证并修正数值数据，确保符合PostgreSQL的数值范围
    """
    # 获取 PostgreSQL 列类型
    sqlite_cursor.execute(f"PRAGMA table_info({table});")
    col_info = sqlite_cursor.fetchall()

    # 检查每个数值列
    for col in col_info:
        col_name = col[1]
        col_type = col[2].lower()

        # 处理 numeric 类型
        if col_type in ["numeric", "decimal", "real"]:
            # 更新超出范围的数值
            sqlite_cursor.execute(
                f"UPDATE {table} SET {col_name} = 99999999.99 "
                f"WHERE {col_name} > 99999999.99;"
            )

def migrate_data(sqlite_conn, pg_conn):
    """
    迁移数据从SQLite到PostgreSQL
    """
    sqlite_cursor = sqlite_conn.cursor()

    # 获取 SQLite 中的所有表
    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in sqlite_cursor.fetchall()]

    # 排除不需要迁移的表
    tables_to_exclude = ["sqlite_sequence"]
    tables = [table for table in tables if table not in tables_to_exclude]

    # 验证并修正数值数据
    for table in tables:
        sqlite_cursor.execute(f"PRAGMA table_info({table});")
        columns = [col[1] for col in sqlite_cursor.fetchall()]
        validate_numeric_data(sqlite_cursor, table, columns)

    for table in tables:
        # 为每个表创建使用独立连接
        with pg_conn.cursor() as pg_cursor:
            try:
                # 检查表是否存在
                pg_cursor.execute("SELECT to_regclass(%s);", (table,))
                if pg_cursor.fetchone()[0] is None:
                    print(f"Table {table} does not exist in PostgreSQL, skipping data migration")
                    continue
                
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
                        is_pk = col_info[i][5]  # 检查是否是主键列

                        # 处理主键列，确保不为空
                        if is_pk and value is None:
                            raise ValueError(
                                f"Primary key column {col_name} cannot be null"
                            )

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
                            # 对于主键列，确保值被正确转换
                            if is_pk:
                                converted_row.append(int(value))
                            else:
                                converted_row.append(
                                    int(value) if value is not None else None
                                )
                        else:
                            # 特殊处理 users 表的 access_token 列
                            if table == "users" and col_name == "access_token":
                                converted_row.append(str(value)[:32] if value else None)
                            else:
                                converted_row.append(value)
                    converted_rows.append(tuple(converted_row))

                # 插入数据
                if converted_rows:  # 只有当有数据时才执行插入操作
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

def sync_sequences(pg_conn):
    """
    同步所有表的序列值，确保自增ID从正确的值开始
    """
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE column_default LIKE 'nextval%'
            """
            )
            sequences = cursor.fetchall()

            for table, column in sequences:
                # 检查表是否存在
                cursor.execute("SELECT to_regclass(%s);", (table,))
                if cursor.fetchone()[0] is None:
                    print(f"Table {table} does not exist, skipping sequence synchronization")
                    continue
                    
                try:
                    cursor.execute(
                        f"""
                        SELECT setval(pg_get_serial_sequence('{table}', '{column}'),
                        COALESCE((SELECT MAX({column}) FROM {table}), 0) + 1, false)
                    """
                    )
                    print(f"Synchronized sequence for {table}.{column}")
                except Exception as e:
                    print(f"Error synchronizing sequence for {table}.{column}: {e}")
    except Exception as e:
        print(f"Error in sync_sequences: {e}")

def main():
    """
    Main function to handle the database migration process
    """
    # 初始化连接变量
    sqlite_conn = None
    pg_conn = None
    
    try:
        # 加载配置
        config = load_config()
        if not config:
            print("Failed to load configuration. Exiting.")
            sys.exit(1)
        
        # 设置连接参数
        sqlite_db_file = config["database"]["sqlite_file"]
        pg_db_config = {
            "dbname": config["postgresql"]["cloud"]["dbname"],
            "user": config["postgresql"]["cloud"]["user"],
            "password": config["postgresql"]["cloud"]["password"],
            "host": config["postgresql"]["cloud"]["host"],
            "port": int(config["postgresql"]["cloud"]["port"]),
        }
        
        # 打印连接信息（不显示密码）
        print(f"SQLite database: {sqlite_db_file}")
        print(f"PostgreSQL host: {pg_db_config['host']}, port: {pg_db_config['port']}, database: {pg_db_config['dbname']}, user: {pg_db_config['user']}")
        
        # 连接SQLite数据库
        try:
            sqlite_conn = sqlite3.connect(sqlite_db_file)
            print("SQLite connection established successfully.")
        except Exception as e:
            print(f"Error connecting to SQLite database: {e}")
            sys.exit(1)
        
        # 测试PostgreSQL连接
        if not test_pg_connection(pg_db_config):
            print("PostgreSQL connection test failed. Exiting.")
            sys.exit(1)
            
        # 连接PostgreSQL数据库
        try:
            pg_conn = psycopg2.connect(**pg_db_config)
            print("PostgreSQL connection established successfully.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            sys.exit(1)
        
        # 执行迁移过程
        migrate_table_structure(sqlite_conn, pg_conn)
        migrate_data(sqlite_conn, pg_conn)
        sync_sequences(pg_conn)
        
        print("Migration completed successfully.")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        sys.exit(1)
    finally:
        # 关闭连接
        if sqlite_conn:
            sqlite_conn.close()
            print("SQLite connection closed.")
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    main()
