import os, time, hashlib
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from markdown import markdown
from bs4 import BeautifulSoup
import certifi

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")  

client = MongoClient(
    MONGO_URI,
    tls=True,                      # 或 ssl=True（pymongo 新版用 tls）
    tlsCAFile=certifi.where(),     # 關鍵：指定可信 CA
    serverSelectionTimeoutMS=30000 # 30s
)
db = client["ragdb"]
col = db["cards"]

# 384 維 MiniLM，務必 cosine + normalize
emb = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def sha256(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def md_to_chunks(md_text: str, doc_title: str):
    html = markdown(md_text)
    soup = BeautifulSoup(html, "html.parser")
    chunks, buff, cur = [], [], doc_title

    def flush():
        if buff:
            text = "\n\n".join(buff).strip()
            if text:
                chunks.append({"title": cur, "text": text})
        buff.clear()

    for el in soup.recursiveChildGenerator():
        if el.name in ("h1", "h2"):
            flush(); cur = el.get_text().strip()
        elif el.name in ("p", "li"):
            t = el.get_text(" ", strip=True)
            if t: buff.append(t)
    flush()

    # 粗略長度控制（可換 token-based）
    MAXC, MINC = 1200, 200
    final = []
    for c in chunks:
        t = c["text"]
        if len(t) > 3*MAXC:
            for i in range(0, len(t), MAXC):
                final.append({"title": c["title"], "text": t[i:i+MAXC]})
        else:
            final.append(c)
    # 合併太短片段
    merged = []
    for c in final:
        if merged and len(c["text"]) < MINC:
            merged[-1]["text"] += "\n\n" + c["text"]
        else:
            merged.append(c)
    return merged

def upsert_card(doc):
    col.update_one(
        {
            "type": doc["type"],
            "title": doc["title"],
            "source_path": doc["source_path"],
            "content_hash": doc["content_hash"]
        },
        {"$set": doc},
        upsert=True
    )

def ingest_one(md_path: Path, card_type="doc"):
    md = md_path.read_text(encoding="utf-8")
    base_title = md_path.stem
    chunks = md_to_chunks(md, base_title)
    vecs = emb.encode([c["text"] for c in chunks], normalize_embeddings=True).tolist()

    for i, c in enumerate(chunks):
        content = f"# {c['title']}\n\n{c['text']}"
        doc = {
            "type": card_type,
            "title": f"{base_title}::chunk_{i:03d}",
            "text": content,
            "meta": {"doc": base_title, "chunk_id": i, "tags": ["betting","metrics"]},
            "source_path": str(md_path),
            "content_hash": sha256(content),
            "embedding": vecs[i],
            "updatedAt": time.time()
        }
        upsert_card(doc)
    print(f"Ingested {md_path.name}: {len(chunks)} chunks")

if __name__ == "__main__":
    # 你附件的 md 檔（改成你的實際路徑）
    md_file = Path("data/game_bet_analysis.md")
    if not md_file.exists():
        md_file = Path("data/md/game_bet_analysis.md")
    ingest_one(md_file)
