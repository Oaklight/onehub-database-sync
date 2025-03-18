import argparse
import subprocess
import tomllib
import psycopg
from psycopg import sql


def get_db_config(config, db_type):
    """获取数据库配置"""
    return {
        "dbname": config["postgresql"][f"{db_type}"]["dbname"],
        "user": config["postgresql"][f"{db_type}"]["user"],
        "password": config["postgresql"][f"{db_type}"]["password"],
        "host": config["postgresql"][f"{db_type}"]["host"],
        "port": config["postgresql"][f"{db_type}"]["port"],
    }


def check_postgresql_tools():
    """检查 PostgreSQL 工具是否可用"""
    for tool in ["pg_dump", "pg_restore"]:
        try:
            subprocess.run(
                [tool, "--version"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError:
            raise Exception(
                f"{tool} not found. Please install PostgreSQL client tools (postgresql-client)"
            )


def clean_target_db(dst_config):
    """跳过清理操作，因为目标数据库为空"""
    print("✅ Skipping cleanup: target database is assumed to be empty")


def replicate_db(src_config, dst_config):
    """使用 pg_dump 和 pg_restore 复制数据库（带详细错误输出）"""

    dump_cmd = [
        "pg_dump",
        "--format=custom",
        "--no-acl",
        "--no-owner",
        f"--dbname=postgresql://{src_config['user']}:{src_config['password']}@{src_config['host']}:{src_config['port']}/{src_config['dbname']}",
    ]

    restore_cmd = [
        "pg_restore",
        "--clean",
        "--if-exists",
        "--no-acl",
        "--no-owner",
        f"--dbname=postgresql://{dst_config['user']}:{dst_config['password']}@{dst_config['host']}:{dst_config['port']}/{dst_config['dbname']}",
    ]

    try:
        # 执行 pg_dump 和 pg_restore
        with subprocess.Popen(
            dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as dump_process:
            with subprocess.Popen(
                restore_cmd, stdin=dump_process.stdout, stderr=subprocess.PIPE
            ) as restore_process:
                dump_process.stdout.close()
                stdout, dump_err = dump_process.communicate()
                _, restore_err = restore_process.communicate()

                # 检查返回码
                if dump_process.returncode != 0:
                    print(f"❌ pg_dump failed with error:\n{dump_err.decode()}")
                    raise Exception("❌ pg_dump failed")

                if restore_process.returncode != 0:
                    print(f"❌ pg_restore failed with error:\n{restore_err.decode()}")
                    raise Exception("❌ pg_restore failed")

        print("✅ Database replication completed successfully")

    except Exception as e:
        raise Exception(f"❌ Database replication failed: {e}")


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Replicate PostgreSQL databases")
    parser.add_argument(
        "direction",
        choices=["cloud-to-local", "local-to-cloud"],
        help="Replication direction: cloud-to-local or local-to-cloud",
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

    # 清理目标数据库并执行复制
    clean_target_db(dst_config)
    replicate_db(src_config, dst_config)

    print("✅ PostgreSQL database replication completed successfully!")


if __name__ == "__main__":
    main()
