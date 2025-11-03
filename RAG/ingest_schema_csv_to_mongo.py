# ingest_schema_csv_to_mongo.py
# pip install pymongo certifi python-dotenv sentence-transformers pandas
import os, time, hashlib, pandas as pd
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
        {"type": doc["type"], "title": doc["title"], "source_path": doc["source_path"]},
        {"$set": doc}, upsert=True
    )

def to_text(table, df, columns_info):
    lines = [f"# Table: {table}", "## Columns", ""]
    for col_info in columns_info:
        if isinstance(col_info, dict):
            c = col_info.get("name", "")
            t = col_info.get("data_type", "")
            d = col_info.get("description", "")
            lines.append(f"- {c} ({t}) – {d}")
        else:
            # 簡單的欄位名稱
            lines.append(f"- {col_info}")
    return "\n".join(lines)

def parse_schema_csv(csvp):
    """解析 schema CSV，支援兩種格式：
    1. 標準格式：直接有 column_name 等欄位
    2. Schema 描述格式：第一行是表名，第二行是欄位標題，後續是欄位描述
    """
    try:
        # 先嘗試讀取原始格式
        df_raw = pd.read_csv(csvp)
        
        # 檢查是否是標準格式（有 column_name 或類似欄位）
        if "column_name" in df_raw.columns:
            table = df_raw["table_name"].iloc[0] if "table_name" in df_raw.columns else csvp.stem
            columns = df_raw["column_name"].tolist()
            columns_info = []
            for _, r in df_raw.iterrows():
                columns_info.append({
                    "name": str(r.get("column_name", "")),
                    "data_type": str(r.get("data_type", "")),
                    "description": str(r.get("description", ""))
                })
            return table, columns, columns_info
        
        # 檢查是否是 SessionActive 這類特殊格式
        # 第一行可能是表名，第二行是欄位標題
        if len(df_raw) > 1:
            # 檢查第二行是否包含 "name", "data type" 等標題
            first_row = df_raw.iloc[0].tolist()
            if any("name" in str(cell).lower() for cell in first_row):
                try:
                    # 重新讀取，跳過第一行，使用第二行作為 header
                    df = pd.read_csv(csvp, skiprows=1, quoting=1)  # 加入 quoting 處理引號
                    
                    # 清理欄位名稱
                    df.columns = [str(col).strip() for col in df.columns]
                    
                    # 表名從檔案名取得，移除空格
                    table = csvp.stem.replace(" ", "").strip()
                    
                    columns = []
                    columns_info = []
                    
                    for _, r in df.iterrows():
                        col_name = str(r.get("name", "")).strip()
                        if col_name and col_name != "name" and col_name != "nan" and col_name:  # 跳過標題行和空值
                            columns.append(col_name)
                            col_desc = str(r.get("Details", "")).strip()
                            col_type = str(r.get("data type(MSSQL)", "")).strip()
                            if col_type == "nan" or not col_type:
                                col_type = str(r.get("data type(mongo)", "")).strip()
                            
                            columns_info.append({
                                "name": col_name,
                                "data_type": col_type if col_type != "nan" else "",
                                "description": col_desc if col_desc != "nan" else ""
                            })
                    
                    return table, columns, columns_info
                except Exception as e:
                    print(f"Warning: Failed to parse schema format for {csvp}: {e}")
                    # 繼續使用回退邏輯
        
        # 回退：簡單的維度表格式
        table = csvp.stem
        columns = df_raw.columns.tolist()
        columns_info = [{"name": col, "data_type": "", "description": ""} for col in columns]
        return table, columns, columns_info
        
    except Exception as e:
        print(f"Error parsing {csvp}: {e}")
        # 最後的回退
        table = csvp.stem
        return table, [], []

for csvp in Path("data/schema_csv").glob("*.csv"):
    try:
        table, columns, columns_info = parse_schema_csv(csvp)
        text = to_text(table, None, columns_info)
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
        print(f"Ingested schema: {table} ({len(columns)} columns)")
        
    except Exception as e:
        print(f"Error processing {csvp}: {e}")
        continue
