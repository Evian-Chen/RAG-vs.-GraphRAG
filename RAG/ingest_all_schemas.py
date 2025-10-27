# ingest_all_schemas.py
# 完整處理所有 schema CSV 的攝取腳本

import os
import time
import hashlib
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi
from sentence_transformers import SentenceTransformer

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())
col = client[os.getenv("MONGO_DB","ragdb")][os.getenv("MONGO_COL","cards")]
emb = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def upsert(doc):
    col.update_one(
        {"type": doc["type"], "title": doc["title"]},
        {"$set": doc}, upsert=True
    )

def to_text(table, columns_info):
    lines = [f"# Table: {table}", "## Columns", ""]
    for col_info in columns_info:
        name = col_info.get("name", "")
        data_type = col_info.get("data_type", "")
        desc = col_info.get("description", "")
        lines.append(f"- {name} ({data_type}) – {desc}")
    return "\n".join(lines)

def parse_schema_description_csv(csvp):
    """解析 schema 描述格式的 CSV（如 SessionActive, GameConsume 等）"""
    try:
        # 讀取所有行
        with open(csvp, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        if len(lines) < 3:
            return None, [], []
        
        # 第一行通常是表名
        table_line = lines[0].strip()
        table = csvp.stem.replace(" ", "").strip()
        
        # 第二行是欄位標題，找到 name 和 data type 的欄位位置
        header_line = lines[1].strip()
        headers = [h.strip() for h in header_line.split(',')]
        
        name_idx = -1
        dtype_idx = -1
        desc_idx = -1
        
        for i, h in enumerate(headers):
            if 'name' in h.lower():
                name_idx = i
            elif 'data type' in h.lower() or 'datatype' in h.lower():
                dtype_idx = i
            elif 'details' in h.lower():
                desc_idx = i
        
        if name_idx == -1:
            return None, [], []
        
        columns = []
        columns_info = []
        
        # 處理數據行
        for line in lines[2:]:
            if line.strip():
                parts = [p.strip().strip('"') for p in line.split(',')]
                if len(parts) > name_idx:
                    col_name = parts[name_idx] if name_idx < len(parts) else ""
                    col_type = parts[dtype_idx] if dtype_idx != -1 and dtype_idx < len(parts) else ""
                    col_desc = parts[desc_idx] if desc_idx != -1 and desc_idx < len(parts) else ""
                    
                    if col_name and col_name.lower() != 'name':
                        columns.append(col_name)
                        columns_info.append({
                            "name": col_name,
                            "data_type": col_type,
                            "description": col_desc
                        })
        
        return table, columns, columns_info
        
    except Exception as e:
        print(f"Error parsing schema description CSV {csvp}: {e}")
        return None, [], []

def parse_dimension_csv(csvp):
    """解析維度表格式的 CSV（如 DimChannel 等）"""
    try:
        df = pd.read_csv(csvp)
        table = csvp.stem
        columns = df.columns.tolist()
        
        columns_info = []
        for col in columns:
            # 對於維度表，我們可以從數據推斷一些信息
            sample_data = df[col].dropna().iloc[0] if not df[col].dropna().empty else ""
            
            # 簡單的類型推斷
            data_type = "varchar"
            if pd.api.types.is_numeric_dtype(df[col]):
                data_type = "int"
            
            desc = f"{col} 欄位"
            if 'key' in col.lower() or 'id' in col.lower():
                desc = f"{col} 識別碼"
            elif 'name' in col.lower():
                desc = f"{col} 名稱"
            
            columns_info.append({
                "name": col,
                "data_type": data_type,
                "description": desc
            })
        
        return table, columns, columns_info
        
    except Exception as e:
        print(f"Error parsing dimension CSV {csvp}: {e}")
        return None, [], []

def detect_csv_type(csvp):
    """檢測 CSV 文件類型"""
    try:
        with open(csvp, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            second_line = f.readline().strip()
        
        # 如果第二行包含 "name" 和 "data type"，判斷為 schema 描述格式
        if 'name' in second_line.lower() and 'data type' in second_line.lower():
            return "schema_description"
        else:
            return "dimension"
    except:
        return "dimension"  # 默認當作維度表處理

# 手動定義的特殊 schema（從之前分析得出）
special_schemas = {
    "SessionActive": [
        {"name": "ProDate", "data_type": "int", "description": "資料傳送日期,轉檔日期, 格式: yyyyMMdd (UTC+8) Taiwan Zone"},
        {"name": "LoginDate", "data_type": "int", "description": "登入日期,帳號登入日期,取Client連線登入遊戲時間格式: yyyyMMdd"},
        {"name": "SessionID", "data_type": "varchar(50)", "description": "SessionID"},
        {"name": "UserID", "data_type": "int", "description": "玩家自動ID(帳號)"},
        {"name": "LoginTime", "data_type": "bigint", "description": "登入時間,帳號登入日期, Unix timestamp (單位:1000000分之一秒)"},
        {"name": "UDID", "data_type": "nvarchar(36)", "description": "設備識別ID,記錄設備的唯一識別ID"},
        {"name": "SysType", "data_type": "int", "description": "操作系統"},
        {"name": "Country", "data_type": "char(2)", "description": "所在地區(國別),ISO 3166-1 alpha-2"},
        {"name": "Region", "data_type": "nvarchar(20)", "description": "所在地區(省市)"},
        {"name": "Channel", "data_type": "int", "description": "上架平台/渠道,用戶下載遊戲的來源平台代碼"},
        {"name": "PublishVer", "data_type": "varchar(20)", "description": "遊戲版本,遊戲發行的版本代碼或編號"},
        {"name": "DEV", "data_type": "nvarchar(100)", "description": "機型"},
        {"name": "SysVer", "data_type": "nvarchar(20)", "description": "操作系統版本"},
        {"name": "Resolution", "data_type": "varchar(20)", "description": "解析度/分辨率"},
        {"name": "Network", "data_type": "int", "description": "聯網方式"},
        {"name": "LV", "data_type": "int", "description": "目前等級,玩家等級"},
        {"name": "VipLV", "data_type": "int", "description": "目前VIP等級,玩家等級代碼"},
        {"name": "LoginTimeTs", "data_type": "int", "description": "登入時間 10位數,帳號登入日期, Unix timestamp"},
        {"name": "IP", "data_type": "varchar(50)", "description": "IP"},
        {"name": "IDFA", "data_type": "varchar(40)", "description": "IDFA,iOS裝置識別"},
        {"name": "IDFV", "data_type": "varchar(40)", "description": "IDFV,iOS 裝置Vindor標示符"},
        {"name": "IMEI", "data_type": "varchar(16)", "description": "IMEI,Android裝置識別"},
        {"name": "AAID", "data_type": "varchar(36)", "description": "AAID,Google Advertising ID"},
        {"name": "AndroidID", "data_type": "varchar(16)", "description": "AndroidID,Google Android ID"},
        {"name": "GtDeviceID", "data_type": "nvarchar(128)", "description": "GT裝置識別碼,線上(GT)共用的裝置識別碼"},
        {"name": "OpenType", "data_type": "int", "description": "第三方驗證者,第三方綁定"},
        {"name": "OpenID", "data_type": "varchar(30)", "description": "第三方驗證使用者ID"}
    ]
}

def process_csv_file(csvp):
    """處理單個 CSV 文件"""
    table_name = csvp.stem.replace(" ", "").strip()
    
    # 如果有手動定義的 schema，優先使用
    if table_name in special_schemas:
        table = table_name
        columns_info = special_schemas[table_name]
        columns = [col["name"] for col in columns_info]
        print(f"Using predefined schema for {table}")
    else:
        # 自動檢測和解析
        csv_type = detect_csv_type(csvp)
        
        if csv_type == "schema_description":
            table, columns, columns_info = parse_schema_description_csv(csvp)
        else:
            table, columns, columns_info = parse_dimension_csv(csvp)
        
        if not table or not columns_info:
            print(f"Failed to parse {csvp}")
            return False
    
    # 生成文本描述
    text = to_text(table, columns_info)
    vec = emb.encode([text], normalize_embeddings=True)[0].tolist()
    
    doc = {
        "type": "schema",
        "title": f"schema::{table}",
        "text": text,
        "meta": {"table": table, "columns": columns},
        "source_path": str(csvp),
        "content_hash": hashlib.sha256(text.encode()).hexdigest(),
        "embedding": vec,
        "updatedAt": time.time()
    }
    
    upsert(doc)
    print(f"✓ Ingested schema: {table} ({len(columns)} columns)")
    return True

def main():
    """處理所有 schema CSV 文件"""
    schema_dir = Path("../../data/schema_csv")
    success_count = 0
    total_count = 0
    
    for csvp in schema_dir.glob("*.csv"):
        total_count += 1
        if process_csv_file(csvp):
            success_count += 1
        else:
            print(f"✗ Failed to process {csvp}")
    
    print(f"\n=== Summary ===")
    print(f"Total files: {total_count}")
    print(f"Successfully processed: {success_count}")
    print(f"Failed: {total_count - success_count}")

if __name__ == "__main__":
    main()
