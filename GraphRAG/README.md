## How to run

1. 建立索引資料與文本
```bash=
python3 graphrag_script.py
```

應該會看到
```bash=
graphrag_index/
├── input/
│   └── davinci.txt
```

2. 初始化並建立索引
```bash=
python -m graphrag.index --init --root ./graphrag_index
```
在 graphrag_index/ 底下的 .env 中加入 OpenAI API KEY
然後建立正式索引
```bash=
python -m graphrag.index --root ./graphrag_index
```

3. 查詢
```bash=
python3 search.py
```

## reference
[GraphRAG Explained: Enhancing RAG with Knowledge Graphs](https://medium.com/@zilliz_learn/graphrag-explained-enhancing-rag-with-knowledge-graphs-3312065f99e1)