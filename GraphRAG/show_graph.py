import pandas as pd
import networkx as nx
from pyvis.network import Network
import os
from datetime import datetime

# 自動找到最新的輸出目錄
output_dir = "./graphrag_index/output"
subdirs = [os.path.join(output_dir, d) for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))]
latest_subdir = max(subdirs, key=os.path.getmtime)
INPUT_DIR = os.path.join(latest_subdir, "artifacts")

print(f"使用資料目錄: {INPUT_DIR}")

# 載入 GraphRAG 輸出
nodes_df = pd.read_parquet(f"{INPUT_DIR}/create_final_nodes.parquet")
edges_df = pd.read_parquet(f"{INPUT_DIR}/create_final_relationships.parquet")

print(f"載入了 {len(nodes_df)} 個節點和 {len(edges_df)} 條邊")

# 建立 NetworkX graph
G = nx.from_pandas_edgelist(
    edges_df,
    source="source",
    target="target",
    edge_attr="weight",
    create_using=nx.Graph()
)

# 加入節點屬性（使用 title 而不是 id）
for _, row in nodes_df.iterrows():
    node_title = row["title"]
    if node_title in G.nodes:  # 確保節點存在於圖中
        G.nodes[node_title]["label"] = node_title
        G.nodes[node_title]["community"] = row.get("community", None)
        G.nodes[node_title]["degree"] = row.get("degree", 0)
        G.nodes[node_title]["description"] = row.get("description", "")
        G.nodes[node_title]["entity_type"] = row.get("entity_type", "")

# 使用 spring layout 計算節點位置（增加間距）
print("正在計算節點佈局...")
pos = nx.spring_layout(G, k=2, iterations=50, seed=42)  # k 值越大，節點越分散

# 建立簡單的 PyVis 視覺化
net = Network(notebook=False, directed=False, height="750px", width="100%")

# 設定物理引擎為靜態（禁用節點移動）
net.set_options("""
{
  "physics": {
    "enabled": false
  },
  "interaction": {
    "dragNodes": true,
    "dragView": true,
    "zoomView": true
  }
}
""")

# 設定顏色
colors = ["#ff6b6b", "#4ecdc4", "#45b7d1", "#96ceb4", "#feca57", "#ff9ff3", "#54a0ff", "#5f27cd"]

# 計算節點的連接度（degree）
degrees = dict(G.degree())
max_degree = max(degrees.values()) if degrees else 1
min_degree = min(degrees.values()) if degrees else 1

print(f"最高連接度: {max_degree}, 最低連接度: {min_degree}")

# 手動添加節點（使用預先計算的位置）
for node, data in G.nodes(data=True):
    community = data.get('community')
    if community:
        try:
            community_idx = int(community) % len(colors)
            color = colors[community_idx]
        except:
            color = "#cccccc"
    else:
        color = "#cccccc"
    
    # 清理標題（移除引號）
    clean_title = node.strip('"')
    
    # 獲取預先計算的位置並放大（讓節點更分散）
    x, y = pos[node]
    x *= 1500  # 放大 X 座標
    y *= 1500  # 放大 Y 座標
    
    # 根據連接度計算節點大小（連接越多越大）
    node_degree = degrees.get(node, 0)
    # 標準化大小：10-50 之間
    if max_degree > min_degree:
        normalized_size = 10 + (node_degree - min_degree) / (max_degree - min_degree) * 40
    else:
        normalized_size = 20
    
    # 如果是高連接度節點（top 10%），加上金色邊框
    border_width = 0
    border_color = color
    if node_degree >= max_degree * 0.7:  # 前 30% 的重要節點
        border_width = 3
        border_color = "#FFD700"  # 金色邊框
    
    net.add_node(
        node, 
        label=clean_title,
        title=f"社群: {community}\n類型: {data.get('entity_type', '')}\n連接數: {node_degree}\n重要度: {'⭐⭐⭐ 高' if node_degree >= max_degree * 0.7 else '⭐⭐ 中' if node_degree >= max_degree * 0.4 else '⭐ 低'}\n描述: {data.get('description', '')[:100]}...",
        color=color,
        size=normalized_size,
        x=x,
        y=y,
        borderWidth=border_width,
        borderWidthSelected=border_width + 2,
        font={'size': int(normalized_size * 0.8)},  # 字體大小也跟著節點大小調整
        shape='dot' if node_degree >= max_degree * 0.7 else 'dot'
    )

# 手動添加邊
for source, target, data in G.edges(data=True):
    weight = data.get('weight', 1)
    net.add_edge(source, target, width=min(weight * 2, 10))

print(f"圖形包含 {len(G.nodes)} 個節點和 {len(G.edges)} 條邊")

# 保存為 HTML
try:
    net.write_html("graphrag_network.html", open_browser=False, notebook=False)
    print("✅ 視覺化完成！請開啟 graphrag_network.html 查看結果")
    
    # 顯示一些統計資訊
    print(f"\n📊 圖形統計:")
    print(f"- 節點數量: {len(G.nodes)}")
    print(f"- 邊數量: {len(G.edges)}")
    print(f"- 社群數量: {len(set(data.get('community') for _, data in G.nodes(data=True) if data.get('community')))}")
    
    # 顯示最重要的節點
    degrees = dict(G.degree())
    top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\n🔝 連接度最高的節點 (⭐⭐⭐ 最重要):")
    for node, degree in top_nodes:
        clean_node_name = node.strip('"')
        importance = "⭐⭐⭐" if degree >= max_degree * 0.7 else "⭐⭐" if degree >= max_degree * 0.4 else "⭐"
        print(f"- {clean_node_name}: {degree} 個連接 {importance}")
    
    # 顯示連接度分佈
    print(f"\n📈 重要度分佈:")
    high_importance = sum(1 for d in degrees.values() if d >= max_degree * 0.7)
    medium_importance = sum(1 for d in degrees.values() if max_degree * 0.4 <= d < max_degree * 0.7)
    low_importance = sum(1 for d in degrees.values() if d < max_degree * 0.4)
    print(f"- ⭐⭐⭐ 高重要度 (金色邊框): {high_importance} 個節點")
    print(f"- ⭐⭐ 中重要度: {medium_importance} 個節點")
    print(f"- ⭐ 低重要度: {low_importance} 個節點")
    
    # ==================== 匯出到 Excel ==================== #
    print(f"\n📝 正在生成 Excel 報告...")
    
    # 準備節點資料
    node_data = []
    for node, data in G.nodes(data=True):
        node_degree = degrees.get(node, 0)
        importance_level = "⭐⭐⭐ 高" if node_degree >= max_degree * 0.7 else "⭐⭐ 中" if node_degree >= max_degree * 0.4 else "⭐ 低"
        
        node_data.append({
            '節點名稱': node.strip('"'),
            '社群編號': data.get('community', ''),
            '實體類型': data.get('entity_type', ''),
            '連接數': node_degree,
            '重要度': importance_level,
            '描述': data.get('description', '')[:200] + '...' if len(data.get('description', '')) > 200 else data.get('description', '')
        })
    
    nodes_export_df = pd.DataFrame(node_data)
    # 按連接數排序
    nodes_export_df = nodes_export_df.sort_values('連接數', ascending=False)
    
    # 準備社群資料
    community_data = []
    communities = set(data.get('community') for _, data in G.nodes(data=True) if data.get('community'))
    for community in sorted(communities):
        community_nodes = [node for node, data in G.nodes(data=True) if data.get('community') == community]
        community_edges = [(u, v) for u, v in G.edges() if 
                          G.nodes[u].get('community') == community and 
                          G.nodes[v].get('community') == community]
        
        # 找出社群中最重要的節點
        community_degrees = {node: degrees.get(node, 0) for node in community_nodes}
        top_community_node = max(community_degrees.items(), key=lambda x: x[1])[0] if community_degrees else ''
        
        community_data.append({
            '社群編號': community,
            '節點數量': len(community_nodes),
            '內部連接數': len(community_edges),
            '最重要節點': top_community_node.strip('"'),
            '最重要節點連接數': community_degrees.get(top_community_node, 0) if top_community_node else 0,
            '社群密度': len(community_edges) / (len(community_nodes) * (len(community_nodes) - 1) / 2) if len(community_nodes) > 1 else 0
        })
    
    communities_df = pd.DataFrame(community_data)
    # 按節點數量排序
    communities_df = communities_df.sort_values('節點數量', ascending=False)
    
    # 準備邊（關係）資料
    edges_data = []
    for source, target, data in G.edges(data=True):
        edges_data.append({
            '來源節點': source.strip('"'),
            '目標節點': target.strip('"'),
            '權重': data.get('weight', 1),
            '來源社群': G.nodes[source].get('community', ''),
            '目標社群': G.nodes[target].get('community', '')
        })
    
    edges_export_df = pd.DataFrame(edges_data)
    # 按權重排序
    edges_export_df = edges_export_df.sort_values('權重', ascending=False)
    
    # 準備統計資料
    stats_data = {
        '統計項目': [
            '總節點數',
            '總邊數',
            '社群數量',
            '最大連接度',
            '最小連接度',
            '平均連接度',
            '高重要度節點數 (⭐⭐⭐)',
            '中重要度節點數 (⭐⭐)',
            '低重要度節點數 (⭐)',
            '圖密度',
            '生成時間'
        ],
        '數值': [
            len(G.nodes),
            len(G.edges),
            len(communities),
            max_degree,
            min_degree,
            f"{sum(degrees.values()) / len(degrees):.2f}",
            high_importance,
            medium_importance,
            low_importance,
            f"{nx.density(G):.4f}",
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ]
    }
    stats_df = pd.DataFrame(stats_data)
    
    # 準備 Top 節點資料
    top_nodes_data = []
    for i, (node, degree) in enumerate(sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:20], 1):
        importance = "⭐⭐⭐ 高" if degree >= max_degree * 0.7 else "⭐⭐ 中" if degree >= max_degree * 0.4 else "⭐ 低"
        node_info = G.nodes[node]
        top_nodes_data.append({
            '排名': i,
            '節點名稱': node.strip('"'),
            '連接數': degree,
            '重要度': importance,
            '社群編號': node_info.get('community', ''),
            '實體類型': node_info.get('entity_type', ''),
            '描述': node_info.get('description', '')[:150] + '...' if len(node_info.get('description', '')) > 150 else node_info.get('description', '')
        })
    
    top_nodes_df = pd.DataFrame(top_nodes_data)
    
    # 匯出到 CSV（多個檔案）
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_folder = f"graphrag_analysis_{timestamp}"
    
    # 建立輸出資料夾
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # 儲存各個 CSV 檔案
    stats_df.to_csv(f"{output_folder}/01_統計總覽.csv", index=False, encoding='utf-8-sig')
    top_nodes_df.to_csv(f"{output_folder}/02_Top20_重要節點.csv", index=False, encoding='utf-8-sig')
    communities_df.to_csv(f"{output_folder}/03_社群分析.csv", index=False, encoding='utf-8-sig')
    nodes_export_df.to_csv(f"{output_folder}/04_所有節點.csv", index=False, encoding='utf-8-sig')
    edges_export_df.to_csv(f"{output_folder}/05_所有關係.csv", index=False, encoding='utf-8-sig')
    
    # 建立 README 說明檔
    readme_content = f"""# GraphRAG 分析報告
生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## � 檔案說明

1. **01_統計總覽.csv**
   - 圖形的整體統計資訊
   - 包含節點數、邊數、社群數量等

2. **02_Top20_重要節點.csv**
   - 連接度最高的前 20 個節點
   - 依連接數排序

3. **03_社群分析.csv**
   - 所有社群的詳細資訊
   - 包含每個社群的節點數、內部連接數、密度等

4. **04_所有節點.csv**
   - 完整的節點列表 ({len(nodes_export_df)} 筆)
   - 包含節點名稱、社群、連接數、重要度等

5. **05_所有關係.csv**
   - 完整的關係列表 ({len(edges_export_df)} 筆)
   - 包含來源節點、目標節點、權重等

## 📊 快速統計

- 總節點數: {len(G.nodes)}
- 總邊數: {len(G.edges)}
- 社群數量: {len(communities)}
- 最大連接度: {max_degree}
- 平均連接度: {sum(degrees.values()) / len(degrees):.2f}

## 🔝 Top 5 重要節點

"""
    for i, (node, degree) in enumerate(sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:5], 1):
        clean_name = node.strip('"')
        readme_content += f"{i}. {clean_name} - {degree} 個連接\n"
    
    with open(f"{output_folder}/README.md", 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"✅ CSV 報告已生成至資料夾: {output_folder}/")
    print(f"   包含 5 個 CSV 檔案:")
    print(f"   - 01_統計總覽.csv")
    print(f"   - 02_Top20_重要節點.csv")
    print(f"   - 03_社群分析.csv")
    print(f"   - 04_所有節點.csv ({len(nodes_export_df)} 筆)")
    print(f"   - 05_所有關係.csv ({len(edges_export_df)} 筆)")
    print(f"   - README.md (說明文件)")
        
except Exception as e:
    print(f"❌ 保存失敗: {e}")
    import traceback
    traceback.print_exc()
