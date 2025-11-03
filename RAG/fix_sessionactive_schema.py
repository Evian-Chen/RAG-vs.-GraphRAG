# fix_sessionactive_schema.py
# 專門處理 SessionActive schema 的修正腳本

import os
import time
import hashlib
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi
from sentence_transformers import SentenceTransformer

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())
col = client[os.getenv("MONGO_DB","ragdb")][os.getenv("MONGO_COL","cards")]
emb = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# SessionActive 的欄位資訊 (從你的 CSV 手動提取)
sessionactive_columns = [
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

def create_sessionactive_doc():
    table = "SessionActive"
    lines = [f"# Table: {table}", "## Columns", ""]
    
    for col_info in sessionactive_columns:
        name = col_info["name"]
        data_type = col_info["data_type"]
        desc = col_info["description"]
        lines.append(f"- {name} ({data_type}) – {desc}")
    
    text = "\n".join(lines)
    vec = emb.encode([text], normalize_embeddings=True)[0].tolist()
    
    doc = {
        "type": "schema",
        "title": f"schema::{table}",
        "text": text,
        "meta": {
            "table": table, 
            "columns": [col["name"] for col in sessionactive_columns]
        },
        "source_path": "data/schema_csv/SessionActive .csv",
        "content_hash": hashlib.sha256(text.encode()).hexdigest(),
        "embedding": vec,
        "updatedAt": time.time()
    }
    
    # 更新到 MongoDB
    col.update_one(
        {"type": "schema", "title": f"schema::{table}"},
        {"$set": doc}, 
        upsert=True
    )
    
    print(f"Updated SessionActive schema with {len(sessionactive_columns)} columns")
    return doc

if __name__ == "__main__":
    doc = create_sessionactive_doc()
    print(f"Text preview:\n{doc['text'][:500]}...")
