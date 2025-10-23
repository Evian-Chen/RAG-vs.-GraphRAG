# load_tables_to_pg.py
# pip3 install pandas sqlalchemy psycopg2-binary python-dotenv
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PG_URI   = os.getenv("PG_URI")                 # postgresql://...sslmode=require
SCHEMA   = os.getenv("PG_SCHEMA", "public")    # 預設 public
DATA_DIR = os.getenv("DATA_DIR", "data/tables")
CHUNK_SIZE = int(os.getenv("CSV_CHUNK_SIZE", "10000"))  # 每塊10000列，可依機器調大/調小


engine = create_engine(PG_URI, pool_pre_ping=True)

def ensure_pk(conn, schema: str, table: str, pk_name: str = None):
    """
    若表沒有主鍵：
    1) 確保存在 id 欄位
    2) 回填 id（僅補 NULL）
    3) 建立序列，設定預設值
    4) 加上 PRIMARY KEY (id)
    """
    if pk_name is None:
        pk_name = f"{table}_pk"

    # fully-quoted identifiers
    tbl_q = f'"{schema}"."{table}"'
    seq_name = f'{table}_id_seq'
    seq_q = f'"{schema}"."{seq_name}"'

    # 已有主鍵就跳過
    has_pk = conn.execute(text("""
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = :s AND table_name = :t AND constraint_type='PRIMARY KEY'
        LIMIT 1
    """), {"s": schema, "t": table}).fetchone()
    if has_pk:
        return

    # 1) 確保有 id 欄位（先不設 identity，避免舊資料 NULL 影響加 PK）
    conn.execute(text(f'''
        ALTER TABLE {tbl_q}
        ADD COLUMN IF NOT EXISTS id BIGINT
    '''))

    # 2) 回填 id（對 NULL 的列補唯一序號）
    conn.execute(text(f'''
        WITH seq AS (
          SELECT ctid, ROW_NUMBER() OVER () AS rn
          FROM {tbl_q}
        )
        UPDATE {tbl_q} t
        SET id = s.rn
        FROM seq s
        WHERE t.ctid = s.ctid AND t.id IS NULL
    '''))

    # 3) 建立序列（若不存在），並把 id 預設綁到序列上（後續 INSERT 自動遞增）
    conn.execute(text(f'''
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind='S' AND c.relname = :seq_name AND n.nspname = :schema
          ) THEN
            EXECUTE 'CREATE SEQUENCE {seq_q} OWNED BY {tbl_q}.id';
          END IF;
        END $$;
    '''), {"seq_name": seq_name, "schema": schema})

    # 將序列設到目前最大 id 後
    conn.execute(text(f'''
        SELECT setval('{seq_q}', COALESCE((SELECT MAX(id) FROM {tbl_q}), 0))
    '''))

    # 設定 id 預設值為 nextval(seq)
    conn.execute(text(f'''
        ALTER TABLE {tbl_q}
        ALTER COLUMN id SET DEFAULT nextval('{seq_q}')
    '''))

    # 4) 加主鍵（若約束已存在會丟錯，包 try 以避免重複加）
    try:
        conn.execute(text(f'''
            ALTER TABLE {tbl_q}
            ADD CONSTRAINT "{pk_name}" PRIMARY KEY (id)
        '''))
    except Exception:
        # 可能已經有同名約束或其他情況，略過即可
        pass

def load_one_csv(csv_path: Path):
    table = csv_path.stem  # 以檔名為表名
    print(f"[LOAD] {csv_path.name} -> {SCHEMA}.{table}")

    # 串流讀取大檔：每塊 CHUNK_SIZE 列
    first = True
    total_rows = 0
    for i, df in enumerate(pd.read_csv(
        csv_path,
        chunksize=CHUNK_SIZE,
        low_memory=False,           # 降低型別猜測的記憶體抖動
        dtype_backend="pyarrow" if hasattr(pd, "options") else None  # 新版 pandas 可更省記憶體（可忽略）
    ), start=1):
        total_rows += len(df)
        df.to_sql(
            table,
            engine,
            schema=SCHEMA,
            if_exists="replace" if first else "append",
            index=False,
            method="multi",         # 批量 insert，速度較快
            chunksize=5000,         # 傳給 SQL 的批大小
        )
        print(f"  - chunk {i} inserted: {len(df)} rows (cumulative: {total_rows})")
        first = False

    # 若檔案可能是空的，total_rows==0 時跳過後續步驟
    if total_rows == 0:
        print(f"[SKIP] {table}: file is empty.")
        return

    # 加主鍵（若沒有）
    with engine.begin() as conn:
        ensure_pk(conn, SCHEMA, table)

    print(f"[OK]   {table}: {total_rows} rows")


def main():
    data_dir = Path(DATA_DIR)
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {data_dir.resolve()}")
        return

    for p in csv_files:
        # 更改這邊的檔名可以選擇要載入的檔案
        if "SessionLength" in p.name:
            load_one_csv(p)

    # 驗證
    with engine.begin() as conn:
        res = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :s
            ORDER BY 1
        """), {"s": SCHEMA})
        tables = [r[0] for r in res]
    print("tables:", tables)

if __name__ == "__main__":
    main()
