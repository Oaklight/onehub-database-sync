import psycopg

import tomllib

# 读取配置文件
with open("config.toml", "rb") as f:
    config = tomllib.load(f)

# 获取 PostgreSQL 配置
pg_db_config = {
    "dbname": config["postgresql"]["dbname"],
    "user": config["postgresql"]["user"],
    "password": config["postgresql"]["password"],
    "host": config["postgresql"]["host"],
    "port": config["postgresql"]["port"],
}

try:
    # 连接到 PostgreSQL 数据库
    conn = psycopg.connect(**pg_db_config)
    cursor = conn.cursor()
    print("Successfully connected to PostgreSQL database.")

    # 获取所有表名
    cursor.execute(
        """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """
    )
    tables = cursor.fetchall()

    print("\nTables in the database:")
    for table in tables:
        table_name = table[0]
        print(f"- {table_name}")

    # 遍历每个表，获取详细信息
    for table in tables:
        table_name = table[0]
        print(f"\nDetails for table '{table_name}':")

        # 获取列信息
        cursor.execute(
            f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s;
        """,
            (table_name,),
        )
        columns = cursor.fetchall()

        # 打印表头
        print(f"{'Column Name':<20} {'Data Type':<15} {'Nullable':<8} {'Default':<15}")
        print("-" * 60)

        for column in columns:
            col_name = column[0]
            col_type = column[1]
            col_nullable = "YES" if column[2] == "YES" else "NO"
            col_default = column[3] if column[3] else "None"

            print(f"{col_name:<20} {col_type:<15} {col_nullable:<8} {col_default:<15}")

        # 获取主键信息
        cursor.execute(
            f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
        """,
            (table_name,),
        )
        primary_keys = [pk[0] for pk in cursor.fetchall()]

        if primary_keys:
            print(f"\nPrimary Keys: {', '.join(primary_keys)}")

        # 获取外键信息
        cursor.execute(
            f"""
            SELECT conname, confrelid::regclass, a.attname AS column_name, af.attname AS referenced_column
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
            JOIN pg_attribute af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
            WHERE c.contype = 'f' AND c.conrelid::regclass::text = %s;
        """,
            (table_name,),
        )
        foreign_keys = cursor.fetchall()

        if foreign_keys:
            print("\nForeign Keys:")
            for fk in foreign_keys:
                fk_name = fk[0]
                fk_to_table = fk[1]
                fk_from_col = fk[2]
                fk_to_col = fk[3]

                print(
                    f"  - Name: {fk_name}, From: {fk_from_col}, To Table: {fk_to_table}, To Column: {fk_to_col}"
                )

except psycopg.Error as e:
    print(f"Error connecting to PostgreSQL: {e}")
