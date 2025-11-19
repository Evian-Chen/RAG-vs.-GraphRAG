import os
import asyncio
from dotenv import load_dotenv
load_dotenv(dotenv_path="./graphrag_index/.env")
import pandas as pd
import tiktoken
from graphrag.query.context_builder.entity_extraction import EntityVectorStoreKey
from graphrag.query.indexer_adapters import (
    read_indexer_entities,
    read_indexer_relationships,
    read_indexer_reports,
    read_indexer_text_units,
)
from graphrag.query.input.loaders.dfs import (
    store_entity_semantic_embeddings,
)
from graphrag.query.llm.oai.chat_openai import ChatOpenAI
from graphrag.query.llm.oai.embedding import OpenAIEmbedding
from graphrag.query.llm.oai.typing import OpenaiApiType
from graphrag.query.structured_search.local_search.mixed_context import (
    LocalSearchMixedContext,
)
from graphrag.query.structured_search.local_search.search import LocalSearch
from graphrag.vector_stores import MilvusVectorStore

# ==================== 載入 GraphRAG 索引 ==================== #
print("🔄 載入 GraphRAG 索引資料...")

index_root = os.path.join(os.getcwd(), 'graphrag_index')
output_dir = os.path.join(index_root, "output")
subdirs = [os.path.join(output_dir, d) for d in os.listdir(output_dir)]
latest_subdir = max(subdirs, key=os.path.getmtime)
INPUT_DIR = os.path.join(latest_subdir, "artifacts")

COMMUNITY_LEVEL = 2

# 載入各類資料表
entity_df = pd.read_parquet(f"{INPUT_DIR}/create_final_nodes.parquet")
entity_embedding_df = pd.read_parquet(f"{INPUT_DIR}/create_final_entities.parquet")
relationship_df = pd.read_parquet(f"{INPUT_DIR}/create_final_relationships.parquet")
report_df = pd.read_parquet(f"{INPUT_DIR}/create_final_community_reports.parquet")
text_unit_df = pd.read_parquet(f"{INPUT_DIR}/create_final_text_units.parquet")

# 轉換為 GraphRAG 格式
entities = read_indexer_entities(entity_df, entity_embedding_df, COMMUNITY_LEVEL)
relationships = read_indexer_relationships(relationship_df)
reports = read_indexer_reports(report_df, entity_df, COMMUNITY_LEVEL)
text_units = read_indexer_text_units(text_unit_df)

# 設置向量資料庫
description_embedding_store = MilvusVectorStore(
    collection_name="entity_description_embeddings",
)
description_embedding_store.connect(uri="./milvus.db")
entity_description_embeddings = store_entity_semantic_embeddings(
    entities=entities, vectorstore=description_embedding_store
)

print(f"✅ 載入完成: {len(entities)} 個實體, {len(relationships)} 個關係")

# ==================== 設置 LLM 和搜尋引擎 ==================== #
api_key = os.environ["GRAPHRAG_API_KEY"]
llm_model = "gpt-4o-mini"  # 使用較便宜的模型
embedding_model = "text-embedding-3-small"

llm = ChatOpenAI(
    api_key=api_key,
    model=llm_model,
    api_type=OpenaiApiType.OpenAI,
    max_retries=20,
)

token_encoder = tiktoken.get_encoding("cl100k_base")

text_embedder = OpenAIEmbedding(
    api_key=api_key,
    api_base=None,
    api_type=OpenaiApiType.OpenAI,
    model=embedding_model,
    deployment_name=embedding_model,
    max_retries=20,
)

context_builder = LocalSearchMixedContext(
    community_reports=reports,
    text_units=text_units,
    entities=entities,
    relationships=relationships,
    covariates=None,
    entity_text_embeddings=description_embedding_store,
    embedding_vectorstore_key=EntityVectorStoreKey.ID,
    text_embedder=text_embedder,
    token_encoder=token_encoder,
)

local_context_params = {
    "text_unit_prop": 0.5,
    "community_prop": 0.1,
    "conversation_history_max_turns": 5,
    "conversation_history_user_turns_only": True,
    "top_k_mapped_entities": 10,
    "top_k_relationships": 10,
    "include_entity_rank": True,
    "include_relationship_weight": True,
    "include_community_rank": False,
    "return_candidate_context": False,
    "embedding_vectorstore_key": EntityVectorStoreKey.ID,
    "max_tokens": 12_000,
}

llm_params = {
    "max_tokens": 2000,
    "temperature": 0.0,
}

search_engine = LocalSearch(
    llm=llm,
    context_builder=context_builder,
    token_encoder=token_encoder,
    llm_params=llm_params,
    context_builder_params=local_context_params,
    response_type="multiple paragraphs",
)

# ==================== 主查詢函數 ==================== #
async def ask_question(question: str):
    """
    向 GraphRAG 提問並獲取答案
    
    Args:
        question: 要詢問的問題
    
    Returns:
        搜尋結果
    """
    print(f"\n📝 問題: {question}")
    print("🔍 搜尋中...")
    
    result = await search_engine.asearch(question)
    
    print(f"\n✅ 回答:\n{result.response}")
    print(f"\n⏱️  搜尋時間: {result.completion_time:.2f} 秒")
    print(f"💬 LLM 呼叫次數: {result.llm_calls}")
    print(f"📊 使用的 tokens: {result.prompt_tokens}")
    
    return result

async def ask_multiple_questions(questions: list):
    """
    批量提問並獲取答案
    
    Args:
        questions: 問題列表
    
    Returns:
        結果列表
    """
    results = []
    total_time = 0
    total_llm_calls = 0
    total_tokens = 0
    
    for i, question in enumerate(questions, 1):
        print(f"\n{'=' * 80}")
        print(f"處理第 {i}/{len(questions)} 個問題")
        print(f"{'=' * 80}")
        
        result = await ask_question(question)
        results.append({
            'question': question,
            'answer': result.response,
            'completion_time': result.completion_time,
            'llm_calls': result.llm_calls,
            'prompt_tokens': result.prompt_tokens
        })
        
        total_time += result.completion_time
        total_llm_calls += result.llm_calls
        total_tokens += result.prompt_tokens
    
    # 顯示總體統計
    print(f"\n{'=' * 80}")
    print(f"📊 總體統計")
    print(f"{'=' * 80}")
    print(f"總問題數: {len(questions)}")
    print(f"總搜尋時間: {total_time:.2f} 秒")
    print(f"平均搜尋時間: {total_time/len(questions):.2f} 秒")
    print(f"總 LLM 呼叫次數: {total_llm_calls}")
    print(f"總使用 tokens: {total_tokens}")
    
    return results

def read_questions_from_file(file_path: str):
    """
    從文件中讀取問題列表
    
    Args:
        file_path: 問題文件路徑
    
    Returns:
        問題列表
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            questions = [line.strip() for line in f if line.strip()]
        print(f"✅ 成功從 {file_path} 讀取 {len(questions)} 個問題")
        return questions
    except FileNotFoundError:
        print(f"❌ 找不到文件: {file_path}")
        return []

# ==================== 執行查詢 ==================== #
if __name__ == "__main__":
    # 從 question_list.txt 讀取問題
    question_file = "question_list.txt"
    questions = read_questions_from_file(question_file)
    
    if not questions:
        print("❌ 沒有找到問題，請檢查 question_list.txt 文件")
    else:
        # 執行批量查詢
        results = asyncio.run(ask_multiple_questions(questions))
        
        # 將結果保存到文件
        output_file = "multiple_questions_result.md"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"GraphRAG 批量查詢結果\n")
            f.write(f"問題總數: {len(results)}\n")
            f.write(f"{'=' * 80}\n\n")
            
            for i, result in enumerate(results, 1):
                f.write(f"問題 {i}: {result['question']}\n")
                f.write(f"{'-' * 80}\n")
                f.write(f"回答:\n{result['answer']}\n\n")
                f.write(f"搜尋時間: {result['completion_time']:.2f} 秒\n")
                f.write(f"LLM 呼叫次數: {result['llm_calls']}\n")
                f.write(f"使用 tokens: {result['prompt_tokens']}\n")
                f.write(f"{'=' * 80}\n\n")
            
            # 添加總體統計
            total_time = sum(r['completion_time'] for r in results)
            total_llm_calls = sum(r['llm_calls'] for r in results)
            total_tokens = sum(r['prompt_tokens'] for r in results)
            
            f.write(f"\n{'=' * 80}\n")
            f.write(f"總體統計\n")
            f.write(f"{'=' * 80}\n")
            f.write(f"總問題數: {len(results)}\n")
            f.write(f"總搜尋時間: {total_time:.2f} 秒\n")
            f.write(f"平均搜尋時間: {total_time/len(results):.2f} 秒\n")
            f.write(f"總 LLM 呼叫次數: {total_llm_calls}\n")
            f.write(f"總使用 tokens: {total_tokens}\n")
        
        print(f"\n💾 所有結果已保存至: {output_file}")
