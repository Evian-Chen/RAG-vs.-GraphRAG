# ask.py — Multi-agent collaborative pipeline
# 智能代理協作流程：Db agent -> Rewrite agent -> table_decide_agent -> table_process_agent -> data_analysis_agent
# 支援代理間回饋循環與資訊共享機制
# pip install pymongo[srv] sentence-transformers sqlalchemy psycopg2-binary python-dotenv openai

import os, json, re, math
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from pymongo import MongoClient
import certifi

# ---------- Agent Communication Protocol ----------
@dataclass
class AgentMessage:
    """代理間通訊訊息格式"""
    sender: str
    receiver: str
    message_type: str
    content: Dict[str, Any]
    timestamp: float = 0.0

@dataclass 
class PipelineContext:
    """Pipeline 執行上下文，用於代理間資訊共享"""
    user_query: str
    reference_context: str = ""
    db_overview: Dict[str, Any] = None
    rewritten_query: Dict[str, Any] = None
    table_plan: Dict[str, Any] = None
    processed_data: List[Dict[str, Any]] = None
    sql_query: str = ""
    analysis_result: str = ""
    agent_messages: List[AgentMessage] = None
    
    def __post_init__(self):
        if self.agent_messages is None:
            self.agent_messages = []

# ---------- Load env ----------
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")

PG_URI  = os.getenv("PG_URI")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB", "ragdb")
MONGO_COL = os.getenv("MONGO_COL", "cards")
MONGO_VECTOR_INDEX = os.getenv("MONGO_VECTOR_INDEX", "cards_env")

if not PG_URI:
    raise RuntimeError("PG_URI is required (Neon connection string).")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is required.")
if not MONGO_URI:
    print("[warn] MONGO_URI not set; reference retrieval will be disabled.")

# ---------- OpenAI client ----------
from openai import OpenAI
oai = OpenAI(api_key=OPENAI_API_KEY)

def chat(messages, model=OPENAI_CHAT_MODEL, temperature=0.1, max_tokens=800):
    resp = oai.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content.strip()

# ---------- Postgres engine ----------
pg_engine: Engine = create_engine(PG_URI, pool_pre_ping=True)

# ---------- Mongo (reference) ----------
def mongo_cards_collection():
    if not MONGO_URI:
        return None
    client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where())
    return client[MONGO_DB][MONGO_COL]

def reference_search(query: str, k: int = 6) -> List[Dict[str, Any]]:
    """
    Vector search over Mongo Atlas Vector Search ($vectorSearch).
    Returns list of {title, type, text, meta, score}
    """
    col = mongo_cards_collection()
    if col is None:
        return []
    
    try:
        # 使用 $vectorSearch 前提：你已建立 Vector 索引，field=embedding(384, cosine)
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        qv = model.encode([query], normalize_embeddings=True)[0].tolist()

        pipeline = [
            {
                "$vectorSearch": {
                    "index": MONGO_VECTOR_INDEX,
                    "path": "embedding",
                    "queryVector": qv,
                    "numCandidates": max(100, k*20),
                    "limit": k
                }
            },
            {"$project": {
                "title": 1, "type": 1, "text": 1, "meta": 1,
                "score": {"$meta": "vectorSearchScore"}
            }}
        ]
        return list(col.aggregate(pipeline))
    except Exception as e:
        print(f"[warn] MongoDB vector search failed: {e}")
        return []

# ---------- Utility ----------
def clamp(n, lo, hi): return max(lo, min(hi, n))

def build_ref_context(cards: List[Dict[str, Any]], max_chars: int = 9000) -> str:
    """Turn retrieved cards into a compact context string."""
    out = []
    used = 0
    for d in cards:
        block = f"# {d.get('title','')}\n[type={d.get('type','')}] score={round(float(d.get('score',0)),4)}\n{d.get('text','')}\n"
        if used + len(block) > max_chars:
            break
        out.append(block); used += len(block)
    return "\n---\n".join(out)

def allowlist_from_pg(engine: Engine) -> Dict[str, List[str]]:
    """Fallback allowlist by introspecting PG public schema."""
    insp = inspect(engine)
    al: Dict[str, List[str]] = {}
    for t in insp.get_table_names(schema="public"):
        cols = [c["name"] for c in insp.get_columns(t, schema="public")]
        al[t] = cols
    return al

# ---------- Agent 1: Db Agent (資料庫代理) ----------
class DbAgent:
    """負責與 PostgreSQL 互動，提供資料庫結構資訊與資料擷取"""
    
    def __init__(self, engine: Engine):
        self.engine = engine
        self.name = "DbAgent"
    
    def scan_schema(self, sample_rows: int = 5) -> Dict[str, Any]:
        """掃描資料庫結構並提供樣本資料"""
        insp = inspect(self.engine)
        info: Dict[str, Any] = {
            "tables": [],
            "scan_timestamp": math.floor(os.times().elapsed * 1000),
            "total_tables": 0
        }
        
        table_names = insp.get_table_names(schema="public")
        info["total_tables"] = len(table_names)
        
        for t in table_names:
            cols = insp.get_columns(t, schema="public")
            col_defs = [{"name": c["name"], "type": str(c["type"])} for c in cols]

            # 取得樣本資料
            with self.engine.begin() as conn:
                try:
                    rows = conn.execute(text(f'SELECT * FROM public."{t}" LIMIT :n'), {"n": sample_rows}).mappings().all()
                    rows = [dict(r) for r in rows]
                    row_count = conn.execute(text(f'SELECT COUNT(*) as cnt FROM public."{t}"')).scalar()
                except Exception as e:
                    rows = []
                    row_count = 0
                    
            info["tables"].append({
                "name": t, 
                "columns": col_defs, 
                "sample": rows,
                "total_rows": row_count
            })
        return info
    
    def execute_query(self, sql: str, max_rows: int = 20000) -> Tuple[List[Dict[str, Any]], str]:
        """安全執行 SQL 查詢"""
        if not re.match(r"^(with|select)\b", sql.strip(), re.IGNORECASE):
            return [], "Refused: not a SELECT/WITH statement."
        try:
            with self.engine.begin() as conn:
                res = conn.execute(text(sql))
                rows = res.mappings().fetchmany(size=max_rows)
                rows = [dict(r) for r in rows]
            return rows, ""
        except Exception as e:
            return [], f"{type(e).__name__}: {e}"
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """取得特定資料表的統計資訊"""
        try:
            with self.engine.begin() as conn:
                # 基本統計
                stats = {}
                stats["row_count"] = conn.execute(text(f'SELECT COUNT(*) FROM public."{table_name}"')).scalar()
                
                # 欄位統計
                cols_info = conn.execute(text(f"""
                    SELECT column_name, data_type, is_nullable 
                    FROM information_schema.columns 
                    WHERE table_name = :tname AND table_schema = 'public'
                """), {"tname": table_name}).mappings().all()
                stats["columns"] = [dict(c) for c in cols_info]
                
                return stats
        except Exception as e:
            return {"error": str(e)}

# ---------- Agent 2: Rewrite Agent (查詢改寫代理) ----------
class RewriteAgent:
    """負責改寫和優化使用者查詢，結合參考資料進行語意補強"""
    
    def __init__(self):
        self.name = "RewriteAgent"
        self.system_prompt = """You are a rewrite agent for data questions. 
Given a user query, database schema, and references, output a clarified task in VALID JSON format.

CRITICAL: Only use column names that ACTUALLY exist in the provided database schema.

Schema Validation Rules:
- ALWAYS check the provided db_schema for actual table and column names
- NEVER invent or assume column names that don't exist in the schema
- If a column name seems logical but doesn't exist, suggest alternatives from available columns
- LoginDate/PlayDate are integers YYYYMMDD (not date strings)
- Country uses ISO-2 codes like 'TW','US'

Return ONLY valid JSON (no markdown, no extra text):
{
  "goal": "Clear description of the analysis objective",
  "available_tables": ["List actual tables from schema"],
  "available_columns": {"table1": ["col1", "col2"], "table2": ["col3", "col4"]},
  "filters": {
    "LoginDate": {"start": 20241001, "end": 20241007},
    "Country": "TW"
  },
  "metrics": ["Only use columns that exist in schema"],
  "hints": ["Specific guidance based on actual schema"],
  "confidence": 0.8
}"""
    
    def rewrite_query(self, context: PipelineContext) -> Dict[str, Any]:
        """改寫使用者查詢"""
        # 提取詳細的 schema 信息
        schema_summary = {}
        if context.db_overview and "tables" in context.db_overview:
            for table in context.db_overview["tables"]:
                schema_summary[table["name"]] = [col["name"] for col in table["columns"]]
        
        prompt = f"""<reference>
{context.reference_context}
</reference>
<query>{context.user_query}</query>
<actual_db_schema>
Available tables and their columns:
{json.dumps(schema_summary, ensure_ascii=False, indent=2)}
</actual_db_schema>
<schema_details>
{json.dumps(context.db_overview.get('tables', [])[:5], ensure_ascii=False) if context.db_overview else '{}'}
</schema_details>
IMPORTANT: Only reference tables and columns that exist in the actual_db_schema above.
Follow the system rules and output JSON only."""
        
        out = chat([
            {"role":"system","content":self.system_prompt},
            {"role":"user","content":prompt}
        ], max_tokens=600)
        
        try:
            result = json.loads(out)
            # 添加代理資訊
            result["rewrite_agent"] = {
                "version": "1.0",
                "timestamp": math.floor(os.times().elapsed * 1000)
            }
            return result
        except Exception as e:
            return {
                "goal": context.user_query, 
                "available_tables": list(schema_summary.keys()),
                "available_columns": schema_summary,
                "filters": {}, 
                "metrics": [], 
                "hints": [out],
                "confidence": 0.3,
                "error": str(e)
            }
    
    def refine_query(self, context: PipelineContext, feedback: Dict[str, Any]) -> Dict[str, Any]:
        """根據回饋精煉查詢"""
        refinement_prompt = f"""
Original query: {context.user_query}
Previous rewrite: {json.dumps(context.rewritten_query, ensure_ascii=False)}
Feedback from downstream agents: {json.dumps(feedback, ensure_ascii=False)}

Please refine the query based on the feedback. Output JSON only.
"""
        
        out = chat([
            {"role":"system","content":self.system_prompt + "\nYou are now refining based on feedback."},
            {"role":"user","content":refinement_prompt}
        ], max_tokens=400)
        
        try:
            result = json.loads(out)
            result["refined"] = True
            result["refinement_timestamp"] = math.floor(os.times().elapsed * 1000)
            return result
        except Exception:
            return context.rewritten_query  # 回傳原始改寫結果

# ---------- Agent 3: Table Decide Agent (資料表決策代理) ----------
class TableDecideAgent:
    """負責根據改寫後的查詢決定使用哪些資料表與欄位"""
    
    def __init__(self):
        self.name = "TableDecideAgent"
        self.system_prompt = """You are a table selection agent with STRICT schema validation.
You receive (1) rewritten intent JSON and (2) a DB schema overview.

CRITICAL SCHEMA VALIDATION:
- ONLY use tables and columns that ACTUALLY exist in the provided db_overview
- NEVER assume or invent column names
- If a logical column doesn't exist, find the closest match or suggest alternatives
- Validate ALL column references against the actual schema

Rules:
- Check db_overview["tables"] for exact table names and column lists
- LoginDate/PlayDate are integers YYYYMMDD if they exist
- Country is two-letter code if it exists
- If a required column is missing, mark it in alternatives

Return STRICT JSON with schema validation:
{
 "tables": [{"name":"ActualTableName","columns":["ActualCol1","ActualCol2"], "priority": 1}],
 "joins": [{"left":"Table1.ActualCol","right":"Table2.ActualCol","type":"inner"}],
 "filters": {"ActualColumn":"value", "date_col":"ActualDateColumn", "start":20241001, "end":20241031},
 "limit": 100000,
 "confidence": 0.9,
 "reason": "Explain which actual tables/columns were found",
 "schema_issues": ["List any missing or assumed columns"],
 "alternatives": ["Suggest actual columns if ideal ones don't exist"]
}"""
    
    def decide_tables(self, context: PipelineContext) -> Dict[str, Any]:
        """決定需要使用的資料表"""
        prompt = f"""<intent_json>
{json.dumps(context.rewritten_query, ensure_ascii=False)}
</intent_json>
<db_overview>
{json.dumps(context.db_overview, ensure_ascii=False)}
</db_overview>
Output the strict JSON schema specified by the system."""
        
        out = chat([
            {"role":"system","content":self.system_prompt},
            {"role":"user","content":prompt}
        ], max_tokens=900)
        
        # 清理可能的 markdown 包裝
        clean_out = out.strip()
        if clean_out.startswith("```json"):
            clean_out = clean_out[7:]
        if clean_out.startswith("```"):
            clean_out = clean_out[3:]
        if clean_out.endswith("```"):
            clean_out = clean_out[:-3]
        clean_out = clean_out.strip()
        
        try:
            plan = json.loads(clean_out)
            # 添加決策元資訊
            plan["decision_agent"] = {
                "version": "1.0", 
                "timestamp": math.floor(os.times().elapsed * 1000)
            }
            return plan
        except Exception as e:
            return {
                "tables": [], 
                "joins": [], 
                "filters": {}, 
                "limit": 100000, 
                "confidence": 0.1,
                "reason": str(e),
                "error": out
            }
    
    def validate_plan(self, plan: Dict[str, Any], db_overview: Dict[str, Any]) -> Dict[str, Any]:
        """驗證資料表選擇計畫的可行性"""
        validation_result = {
            "valid": True,
            "issues": [],
            "suggestions": []
        }
        
        available_tables = {t["name"] for t in db_overview.get("tables", [])}
        
        # 檢查表格是否存在
        for table_info in plan.get("tables", []):
            table_name = table_info.get("name")
            if table_name not in available_tables:
                validation_result["valid"] = False
                validation_result["issues"].append(f"Table '{table_name}' not found in database")
        
        # 檢查 JOIN 的可行性
        for join_info in plan.get("joins", []):
            left_parts = join_info.get("left", "").split(".")
            right_parts = join_info.get("right", "").split(".")
            
            if len(left_parts) != 2 or len(right_parts) != 2:
                validation_result["issues"].append(f"Invalid join format: {join_info}")
        
        return validation_result

# ---------- Agent 4: Table Process Agent (資料表處理代理) ----------
class TableProcessAgent:
    """負責處理選定的資料表，包括SQL生成、執行與資料處理"""
    
    def __init__(self, db_agent: DbAgent):
        self.name = "TableProcessAgent"
        self.db_agent = db_agent
        self.system_prompt = """You are a SQL composer and data processor. 
Generate ONE single SELECT (or WITH ... SELECT) for PostgreSQL.
CONSTRAINTS:
- Only read. NO DDL/DML. NO semicolon in the middle.
- If date range present and date column is integer YYYYMMDD (e.g., LoginDate/PlayDate), use numeric BETWEEN.
- Country must compare to ISO-2 string exactly (e.g., Country='TW').
- Add LIMIT at the end (use provided limit or 1000).
- Prefer simple aggregates for trend (GROUP BY day integer).
- Add meaningful column aliases for better readability.
- IMPORTANT: Always wrap table names and column names with double quotes for PostgreSQL (e.g., FROM public."TableName", SELECT "ColumnName").
Return the SQL only.
"""
    
    def generate_sql(self, context: PipelineContext, error_feedback: str = "") -> str:
        """根據計畫生成SQL語句，支援錯誤回饋修正"""
        base_prompt = f"""<plan>
{json.dumps(context.table_plan, ensure_ascii=False)}
</plan>
<original_query>
{context.user_query}
</original_query>
<db_schema_detail>
{json.dumps([{"name": t["name"], "columns": [c["name"] for c in t["columns"]]} for t in context.db_overview.get("tables", [])], ensure_ascii=False)}
</db_schema_detail>"""

        if error_feedback:
            prompt = f"""{base_prompt}
<error_feedback>
Previous SQL failed with error: {error_feedback}
Please fix the SQL by:
1. Using only columns that exist in the schema
2. Correcting table names and column references
3. Fixing syntax issues
</error_feedback>
Generate corrected SQL now."""
        else:
            prompt = f"""{base_prompt}
Generate the SQL now."""
        
        sql = chat([
            {"role":"system","content":self.system_prompt},
            {"role":"user","content":prompt}
        ], max_tokens=600)
        
        # 清理 markdown 格式
        sql = sql.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip().rstrip(";")
        
        # 安全性檢查：只允許 SELECT/WITH
        if not re.match(r"^(with|select)\b", sql, re.IGNORECASE):
            sql = "-- invalid_non_select\n" + sql
        
        # 確保有 LIMIT
        if re.search(r"\blimit\b", sql, re.IGNORECASE) is None:
            limit = context.table_plan.get("limit", 1000) if context.table_plan else 1000
            sql += f"\nLIMIT {limit}"
        
        return sql
    
    def execute_and_process(self, context: PipelineContext) -> Tuple[List[Dict[str, Any]], str]:
        """執行SQL並處理結果資料"""
        sql = context.sql_query
        rows, error = self.db_agent.execute_query(sql)
        
        if error:
            return [], error
        
        # 資料後處理
        processed_rows = self._post_process_data(rows, context)
        return processed_rows, ""
    
    def _post_process_data(self, rows: List[Dict[str, Any]], context: PipelineContext) -> List[Dict[str, Any]]:
        """對查詢結果進行後處理"""
        if not rows:
            return rows
        
        processed = []
        for row in rows:
            processed_row = {}
            for key, value in row.items():
                # 處理 Decimal 類型
                from decimal import Decimal
                if isinstance(value, Decimal):
                    value = float(value)
                
                # 日期格式轉換
                if key.lower().find('date') != -1 and isinstance(value, int) and len(str(value)) == 8:
                    processed_row[key] = value
                    processed_row[f"{key}_formatted"] = f"{str(value)[:4]}-{str(value)[4:6]}-{str(value)[6:8]}"
                else:
                    processed_row[key] = value
            processed.append(processed_row)
        
        return processed
    
    def validate_sql(self, sql: str) -> Dict[str, Any]:
        """驗證SQL語句的安全性和正確性"""
        validation = {
            "safe": True,
            "issues": [],
            "warnings": []
        }
        
        # 基本安全檢查
        dangerous_keywords = ['drop', 'delete', 'insert', 'update', 'alter', 'truncate', 'create']
        sql_lower = sql.lower()
        
        for keyword in dangerous_keywords:
            if keyword in sql_lower:
                validation["safe"] = False
                validation["issues"].append(f"Dangerous keyword detected: {keyword}")
        
        # 檢查是否為 SELECT 語句
        if not re.match(r"^(with|select)\b", sql.strip(), re.IGNORECASE):
            validation["safe"] = False
            validation["issues"].append("Not a SELECT or WITH statement")
        
        return validation

# ---------- Agent 5: Data Analysis Agent (資料分析代理) ----------
class DataAnalysisAgent:
    """負責對處理後的資料進行分析，產生最終報告"""
    
    def __init__(self):
        self.name = "DataAnalysisAgent"
        self.system_prompt = """You are a data analysis agent. 
Given the user question, the final SQL, and query rows (may be truncated), produce:
- key findings (bulleted in traditional Chinese)
- one-sentence conclusion (in traditional Chinese)
- if row count is 0, propose next steps (e.g., date col mismatch, value mismatch)
- suggest follow-up questions if relevant
Keep it concise and actionable. Respond in Traditional Chinese.
"""
    
    def analyze_data(self, context: PipelineContext) -> str:
        """分析資料並產生報告"""
        sample = context.processed_data[:50] if context.processed_data else []
        
        payload = {
            "question": context.user_query,
            "sql": context.sql_query,
            "preview_rows": sample,
            "preview_count": len(sample),
            "total_rows": len(context.processed_data) if context.processed_data else 0,
            "agent_messages": [
                {"agent": msg.sender, "type": msg.message_type, "summary": str(msg.content)[:100]}
                for msg in context.agent_messages[-3:] if context.agent_messages
            ]
        }
        
        out = chat([
            {"role":"system","content":self.system_prompt},
            {"role":"user","content":json.dumps(payload, ensure_ascii=False)}
        ], max_tokens=600)
        
        return out
    
    def generate_feedback_to_agents(self, context: PipelineContext) -> Dict[str, Any]:
        """生成給其他代理的回饋訊息"""
        feedback = {
            "data_quality": "good" if context.processed_data and len(context.processed_data) > 0 else "poor",
            "row_count": len(context.processed_data) if context.processed_data else 0,
            "suggestions": []
        }
        
        # 根據結果品質提供建議
        if feedback["row_count"] == 0:
            feedback["suggestions"].append("Consider adjusting filters or date ranges")
            feedback["suggestions"].append("Verify table names and column mappings")
        elif feedback["row_count"] < 10:
            feedback["suggestions"].append("Results may be too limited, consider broader criteria")
        
        return feedback

# ---------- Agent Coordinator (代理協調器) ----------
class AgentCoordinator:
    """協調所有代理的執行與溝通"""
    
    def __init__(self, engine: Engine):
        self.db_agent = DbAgent(engine)
        self.rewrite_agent = RewriteAgent()
        self.table_decide_agent = TableDecideAgent()
        self.table_process_agent = TableProcessAgent(self.db_agent)
        self.data_analysis_agent = DataAnalysisAgent()
        
    def execute_pipeline(self, user_query: str, ref_context: str = "") -> PipelineContext:
        """執行完整的多代理流程"""
        # 初始化 context
        context = PipelineContext(
            user_query=user_query,
            reference_context=ref_context
        )
        
        # Step 1: 資料庫代理掃描結構
        context.db_overview = self.db_agent.scan_schema(sample_rows=3)
        self._add_message(context, "DbAgent", "System", "schema_scan", 
                         {"tables_found": context.db_overview["total_tables"]})
        
        # Step 2: 改寫代理處理查詢
        context.rewritten_query = self.rewrite_agent.rewrite_query(context)
        self._add_message(context, "RewriteAgent", "TableDecideAgent", "query_rewritten",
                         {"confidence": context.rewritten_query.get("confidence", 0.5)})
        
        # Step 3: 資料表決策代理
        context.table_plan = self.table_decide_agent.decide_tables(context)
        validation = self.table_decide_agent.validate_plan(context.table_plan, context.db_overview)
        
        # 如果計畫有問題，回饋給改寫代理
        if not validation["valid"]:
            feedback = {"issues": validation["issues"], "db_available": list(context.db_overview.keys())}
            refined_query = self.rewrite_agent.refine_query(context, feedback)
            context.rewritten_query = refined_query
            context.table_plan = self.table_decide_agent.decide_tables(context)
        
        self._add_message(context, "TableDecideAgent", "TableProcessAgent", "plan_decided",
                         {"tables_count": len(context.table_plan.get("tables", []))})
        
        # Step 4: 資料表處理代理 (支援重試機制)
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            if retry_count == 0:
                context.sql_query = self.table_process_agent.generate_sql(context)
            else:
                # 重試時提供錯誤回饋
                context.sql_query = self.table_process_agent.generate_sql(context, error)
                self._add_message(context, "TableProcessAgent", "System", "sql_retry",
                                 {"retry_attempt": retry_count, "previous_error": error})
            
            context.processed_data, error = self.table_process_agent.execute_and_process(context)
            
            if not error:
                # 成功執行
                self._add_message(context, "TableProcessAgent", "DataAnalysisAgent", "data_ready",
                                 {"rows_processed": len(context.processed_data), "retries_used": retry_count})
                break
            else:
                retry_count += 1
                if retry_count <= max_retries:
                    self._add_message(context, "TableProcessAgent", "TableDecideAgent", "execution_error_retry",
                                     {"error": error, "retry_count": retry_count})
                    
                    # 如果是 schema 相關錯誤，重新生成 table plan
                    if "does not exist" in error or "UndefinedTable" in error or "UndefinedColumn" in error:
                        error_feedback = {
                            "sql_error": error,
                            "available_tables": [t["name"] for t in context.db_overview.get("tables", [])],
                            "suggestion": "Use only tables and columns that exist in schema"
                        }
                        context.table_plan = self.table_decide_agent.decide_tables(context)
                else:
                    # 最終失敗
                    self._add_message(context, "TableProcessAgent", "DataAnalysisAgent", "execution_failed",
                                     {"final_error": error, "total_retries": retry_count - 1})
        
        # Step 5: 資料分析代理
        context.analysis_result = self.data_analysis_agent.analyze_data(context)
        
        # 生成回饋給其他代理
        feedback = self.data_analysis_agent.generate_feedback_to_agents(context)
        self._add_message(context, "DataAnalysisAgent", "System", "analysis_complete", feedback)
        
        return context
    
    def _add_message(self, context: PipelineContext, sender: str, receiver: str, 
                    msg_type: str, content: Dict[str, Any]):
        """添加代理間通訊訊息"""
        message = AgentMessage(
            sender=sender,
            receiver=receiver, 
            message_type=msg_type,
            content=content,
            timestamp=math.floor(os.times().elapsed * 1000)
        )
        context.agent_messages.append(message)

# ---------- 初始化全域代理協調器 ----------
coordinator = AgentCoordinator(pg_engine)

# ---------- Top-level ask() function ----------
def ask(user_query: str) -> str:
    """主要的查詢入口點，使用多代理協作流程"""
    
    # 0) Reference retrieval (Mongo Vector Search)
    ref_cards = reference_search(user_query, k=6)
    ref_context = build_ref_context(ref_cards, max_chars=9000)
    
    # 1) 執行多代理協作流程
    context = coordinator.execute_pipeline(user_query, ref_context)
    
    # 2) 生成詳細輸出報告
    lines = []
    lines.append("🤖 == Multi-Agent Pipeline Execution Report == 🤖\n")
    
    # 代理通訊記錄
    if context.agent_messages:
        lines.append("📡 [Agent Communications]")
        for msg in context.agent_messages:
            lines.append(f"  {msg.sender} → {msg.receiver}: {msg.message_type}")
            if msg.content:
                content_summary = str(msg.content)[:100] + "..." if len(str(msg.content)) > 100 else str(msg.content)
                lines.append(f"    Content: {content_summary}")
        lines.append("")
    
    # 查詢改寫結果
    lines.append("✏️  [Query Rewrite Result]")
    if context.rewritten_query:
        lines.append(json.dumps(context.rewritten_query, ensure_ascii=False, indent=2))
    lines.append("")
    
    # 資料表選擇計畫
    lines.append("📋 [Table Selection Plan]")
    if context.table_plan:
        lines.append(json.dumps(context.table_plan, ensure_ascii=False, indent=2))
    lines.append("")
    
    # SQL 查詢語句
    lines.append("💾 [Generated SQL]")
    lines.append(context.sql_query if context.sql_query else "No SQL generated")
    lines.append("")
    
    # 資料結果預覽
    lines.append(f"📊 [Data Results] {min(len(context.processed_data), 10) if context.processed_data else 0} of {len(context.processed_data) if context.processed_data else 0} rows")
    if context.processed_data:
        for i, row in enumerate(context.processed_data[:10]):
            lines.append(f"  {i+1}: {str(row)}")
    else:
        lines.append("  No data returned")
    lines.append("")
    
    # 資料分析結果
    lines.append("🔍 [Analysis Report]")
    lines.append(context.analysis_result if context.analysis_result else "No analysis available")
    lines.append("")
    
    # 參考資料命中
    if ref_cards:
        lines.append("📚 [Reference Hits]")
        titles = [d.get("title","") for d in ref_cards]
        scores = [round(float(d.get("score",0)),4) for d in ref_cards]
        for t, s in zip(titles, scores):
            lines.append(f"  - {s} · {t}")
        lines.append("")
    
    # 執行摘要
    lines.append("📈 [Execution Summary]")
    lines.append(f"  • Total agents involved: 5")
    lines.append(f"  • Messages exchanged: {len(context.agent_messages)}")
    lines.append(f"  • Tables analyzed: {context.db_overview['total_tables'] if context.db_overview else 0}")
    lines.append(f"  • Rows processed: {len(context.processed_data) if context.processed_data else 0}")
    
    return "\n".join(lines)

if __name__ == "__main__":
    # 🎯 10個核心分析問題 - 基於實際 schema 結構優化，使用存在的欄位
    analysis_questions = [
        # 玩家活躍度分析 (使用 SessionActive 表的實際欄位)
        "使用 SessionActive 表中的 LoginDate、VipLV、Country 欄位，分析 2024-10 月台灣地區(Country='TW')各 VIP 等級的登入活躍度分佈，計算每個 VIP 等級的總登入次數和平均每日活躍度，找出最活躍的 VIP 群體並分析其登入趨勢",
        
        # 設備與平台分析 (使用多表實際欄位)
        "整合 SessionActive 和 SessionLength 表中的 SysType、Country、VipLV、Channel 欄位，分析不同系統類型的玩家分佈特徵、平台活躍程度對比、各系統的 VIP 轉化率，以及設備類型對遊戲行為的影響",
        
        # 消費轉化分析 (使用 _p_GameConsume 表的實際欄位)
        "運用 _p_GameConsume 表中的 VipLV、VipLVBefore、VipLVAfter、BuyNumberActual、CreateDate 欄位，分析玩家 VIP 等級升級軌跡，計算從低等級到高等級的消費轉化率、升級所需的平均消費額，以及 VIP 升級對後續消費行為的影響",
        
        # 玩家留存分析 (使用 SessionActive 表的實際欄位)
        "利用 SessionActive 表中的 UDID、LoginDate、Country、VipLV、SessionID 欄位，追蹤 2024-10-01 到 2024-10-07 期間新登入玩家的 7 日留存情況，分析不同 VIP 等級新玩家的留存率差異和流失規律",
        
        # 整合業務分析 (使用跨表實際欄位)
        "綜合運用 SessionActive 表的 UDID、LoginDate、VipLV 和 _p_GameConsume 表的 UDID、BuyNumberActual、CreateDate 欄位，通過 UDID 關聯分析，建立玩家價值評分模型，結合登入頻率、消費金額、VIP 等級變化，識別最有價值的玩家群體特徵和行為模式",
        
        "分析 2024 年 10 月各遊戲的平均遊戲時長分佈，根據實際的遊戲記錄資料（包含遊戲代碼、會話時長、日期、地區等真實欄位），計算每款遊戲的總遊戲時數、平均會話時長與玩家參與度，找出最受歡迎且黏性最高的遊戲類型。",
        
        "以實際的玩家消費紀錄資料為基礎（包含 VIP 等級、購買次數、金額、日期、地區、銷售類型等真實欄位），深入分析不同 VIP 等級玩家的消費模式，觀察總消費金額、購買頻率、偏好的銷售類型與時間分佈，建立高價值客群的行為輪廓。",
        
        "結合玩家的登入與消費等真實資料（含地區、VIP 等級、渠道、區域等欄位），比較台灣（TW）與美國（US）玩家在 VIP 等級分佈、消費習慣、渠道偏好上的差異，識別地區性遊戲行為特徵與文化差異。",
        
        "根據實際的遊戲活動紀錄（含遊戲代碼、日期、地區、系統類型、VIP 等級等真實欄位），找出 2024 年 10 月最受歡迎的前 10 款遊戲，並分析不同系統平台與 VIP 等級對遊戲偏好的影響。",
        
        "利用真實的玩家登入與活動時間資料（包含登入日期、時段、地區、VIP 等級等欄位），觀察 2024 年 10 月每日玩家活躍度的變化趨勢，比較工作日與週末的登入模式差異，並分析不同 VIP 等級玩家的時間偏好與活躍波動。"
    ]
    
    import sys
    if len(sys.argv) > 1:
        try:
            # 使用命令列參數選擇問題編號 (1-20)
            question_id = int(sys.argv[1]) - 1
            if 0 <= question_id < len(analysis_questions):
                q = analysis_questions[question_id]
                print(f"🎯 執行問題 #{question_id + 1}: {q}\n")
            else:
                print(f"❌ 問題編號應該在 1-{len(analysis_questions)} 之間")
                sys.exit(1)
        except ValueError:
            # 如果不是數字，就當作自定義問題
            q = sys.argv[1]
            print(f"🎯 執行自定義問題: {q}\n")
    else:
        # 默認執行第一個問題
        q = analysis_questions[0]
        print(f"🎯 執行預設問題: {q}\n")
        print("💡 提示：使用 'python ask.py <問題編號1-10>' 來執行其他問題")
        print("💡 提示：使用 'python ask.py \"您的問題\"' 來執行自定義問題\n")
    
    print(ask(q))
