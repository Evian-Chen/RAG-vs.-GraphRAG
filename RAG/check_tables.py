#!/usr/bin/env python3
# check_tables.py - 檢查資料庫中的實際表名

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect

load_dotenv()
PG_URI = os.getenv("PG_URI")

if not PG_URI:
    print("Error: PG_URI not found in environment variables")
    exit(1)

try:
    engine = create_engine(PG_URI, pool_pre_ping=True)
    
    # 使用 inspector 查看表名
    insp = inspect(engine)
    table_names = insp.get_table_names(schema="public")
    
    print("=== Tables found in database ===")
    for i, table in enumerate(table_names, 1):
        print(f"{i}. '{table}'")
    
    print(f"\nTotal tables: {len(table_names)}")
    
    # 檢查是否有 SessionActive 相關的表
    session_tables = [t for t in table_names if 'session' in t.lower()]
    if session_tables:
        print(f"\nSession related tables: {session_tables}")
    
    # 測試查詢第一個表的結構
    if table_names:
        first_table = table_names[0]
        print(f"\n=== Sample from '{first_table}' ===")
        with engine.begin() as conn:
            result = conn.execute(text(f'SELECT * FROM public."{first_table}" LIMIT 3'))
            rows = result.fetchall()
            if rows:
                # 取得欄位名稱
                columns = result.keys()
                print(f"Columns: {list(columns)}")
                for i, row in enumerate(rows, 1):
                    print(f"Row {i}: {dict(zip(columns, row))}")
            else:
                print("No data found")

except Exception as e:
    print(f"Error connecting to database: {e}")
