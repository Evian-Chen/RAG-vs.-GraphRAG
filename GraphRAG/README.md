## How to run

0. 建立虛擬環境
進入 GraphRAG 的資料夾

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

2. 初始化並建立索引(indexing)
```bash=
python -m graphrag.index --init --root ./graphrag_index
```
在 graphrag_index/ 底下的 .env 中加入 OpenAI API KEY

建立正式索引：
```bash=
python -m graphrag.index --root ./graphrag_index
```

3. 查詢，這裡可以生成 csv 節點報告
```bash=
python3 search.py
```

4. 查看 Graph
將含有 node, edges 的 output folder 複製到 show_graph.py 的 node_df edges_df 的路徑，之後執行：
```bash=
python3 show_graph.py
```
結束後，再執行：
```bash=
open graphrag_network.html
```
查看圖表

## 詢問問題
1. 更改 question_list.txt 裡面的問題
2. 執行 python3 ask_single_question.py

## 更換文本
修改 graphrag_script.py
```bash
# 將 URL 改為新的文本來源
url = "你的新文本URL"
file_path = os.path.join(index_root, 'input', 'your_new_article.txt')
```
必須確保是純文字，至少有 3000 tokens 以上，確保文本夠長

更換後，需要：
1. 清理快取：刪除 cache/ 目錄內容
2. 清理輸出：刪除 output/ 目錄內容
3. 重新建立索引：python -m graphrag.index --root ./graphrag_index

## reference
[GraphRAG Explained: Enhancing RAG with Knowledge Graphs](https://medium.com/@zilliz_learn/graphrag-explained-enhancing-rag-with-knowledge-graphs-3312065f99e1)