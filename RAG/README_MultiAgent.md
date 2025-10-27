# RAG-vs.-GraphRAG - Multi-Agent Collaborative Pipeline

## 🚀 概述

這是一個基於多代理協作的智能資料查詢與分析系統，能夠自動化處理從使用者查詢到資料分析的完整流程。

## 🔄 Multi-Agent Architecture (多代理架構)

### 系統流程圖
```
使用者輸入查詢 → Rewrite agent 改寫查詢 → table_decide_agent 選表  
→ table_process_agent 處理資料 → data_analysis_agent 分析結果 → Result 輸出
```

### 📡 代理間通訊與回饋機制

各代理之間透過 `AgentMessage` 進行通訊，支援：
- 查詢改寫回饋
- 錯誤處理與重試
- 計畫驗證與調整
- 結果品質評估

## 🤖 Five Core Agents (五個核心代理)

### 1. 🗃️ **Db Agent (資料庫代理)**
- **職責**: 與 PostgreSQL 互動，提供資料庫結構資訊
- **功能**:
  - 掃描資料庫 schema
  - 執行安全的 SQL 查詢
  - 提供資料表統計資訊
- **輸出**: 資料庫結構概覽與樣本資料

### 2. ✏️ **Rewrite Agent (查詢改寫代理)**
- **職責**: 改寫和優化使用者查詢
- **功能**:
  - 語意理解與查詢補強
  - 結合參考資料 (Reference) 優化查詢
  - 根據回饋進行查詢精煉
- **輸出**: 結構化的查詢意圖 JSON

### 3. 📋 **Table Decide Agent (資料表決策代理)**
- **職責**: 決定查詢所需的資料表與欄位
- **功能**:
  - 分析查詢意圖選擇相關表格
  - 定義 JOIN 關係與過濾條件
  - 驗證計畫可行性
- **輸出**: 資料表選擇計畫

### 4. ⚙️ **Table Process Agent (資料表處理代理)**
- **職責**: 處理資料表，生成並執行 SQL
- **功能**:
  - 根據計畫生成 PostgreSQL 查詢
  - 執行查詢並處理結果
  - 資料後處理 (如日期格式化)
- **輸出**: 處理後的資料集

### 5. 🔍 **Data Analysis Agent (資料分析代理)**
- **職責**: 分析資料並生成報告
- **功能**:
  - 統計分析與關鍵發現
  - 生成繁體中文報告
  - 提供後續建議
- **輸出**: 最終分析報告

## 🛠️ 技術架構

### Core Dependencies
```bash
pip install pymongo[srv] sentence-transformers sqlalchemy psycopg2-binary python-dotenv openai
```

### 環境變數設定
```bash
# OpenAI API
OPENAI_API_KEY=your_openai_key
OPENAI_CHAT_MODEL=gpt-4o-mini

# PostgreSQL (Neon)
PG_URI=postgresql://user:pass@host/db

# MongoDB Atlas (選用，用於 Reference 向量搜尋)
MONGO_URI=mongodb+srv://...
MONGO_DB=ragdb
MONGO_COL=cards
MONGO_VECTOR_INDEX=cards_env
```

## 🚦 Usage (使用方法)

### 基本用法
```python
from ask import ask

# 執行查詢
result = ask("請給我 2024-10-01 到 2024-10-31 台灣(TW) 的 SessionActive 筆數與每日趨勢")
print(result)
```

### 進階用法 - 直接使用代理協調器
```python
from ask import AgentCoordinator, pg_engine

# 創建協調器
coordinator = AgentCoordinator(pg_engine)

# 執行流程並取得詳細 context
context = coordinator.execute_pipeline("你的查詢", "參考資料")

# 檢視代理通訊記錄
for msg in context.agent_messages:
    print(f"{msg.sender} → {msg.receiver}: {msg.message_type}")
```

## 📊 輸出格式

系統會產生詳細的執行報告，包含：

```
🤖 == Multi-Agent Pipeline Execution Report == 🤖

📡 [Agent Communications]      # 代理間通訊記錄
✏️  [Query Rewrite Result]     # 查詢改寫結果  
📋 [Table Selection Plan]      # 資料表選擇計畫
💾 [Generated SQL]             # 生成的 SQL 語句
📊 [Data Results]              # 資料結果預覽
🔍 [Analysis Report]           # 分析報告
📚 [Reference Hits]            # 參考資料命中
📈 [Execution Summary]         # 執行摘要
```

## 🔧 特色功能

### 1. **智能回饋循環**
- 代理間可互相回饋，改善查詢品質
- 自動錯誤檢測與重試機制
- 計畫驗證與動態調整

### 2. **安全性保障**
- SQL 注入防護
- 僅允許 SELECT 查詢
- 資料庫權限控制

### 3. **多語言支援**
- 繁體中文查詢理解
- 國際化日期格式處理
- 地區代碼標準化 (如 TW, US)

### 4. **向量搜尋整合**
- MongoDB Atlas Vector Search
- 語意相似度檢索
- 參考資料增強

## 🧪 Testing (測試)

執行測試腳本：
```bash
python test_agents.py
```

測試涵蓋：
- 代理通訊機制
- 完整流程執行
- 錯誤處理

## 📝 實用 SQL 查詢範例

### 查詢表格總列數
```sql
SELECT COUNT(*) FROM public."iap_orders";
```

### 多表列數統計
```sql
SELECT 'iap_orders' AS table_name, COUNT(*) AS row_count FROM public."iap_orders"
UNION ALL
SELECT 'session_active', COUNT(*) FROM public."session_active";
```

### 快速表格統計 (使用 PostgreSQL 統計資訊)
```sql
SELECT relname AS table_name,
       n_live_tup AS estimated_rows
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
```

## 🤝 Contributing

歡迎提交 Issues 和 Pull Requests！

## 📄 License

請參見 [LICENSE](LICENSE) 文件。

---

**注意**: 此系統需要有效的 OpenAI API Key 和 PostgreSQL 資料庫連接才能正常運作。
