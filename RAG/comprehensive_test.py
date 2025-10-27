#!/usr/bin/env python3
# comprehensive_test.py - 完整測試所有20個分析問題

import sys
import os
import time
from ask import ask

def test_question(question_id: int, question: str):
    """測試單個分析問題"""
    print("="*80)
    print(f"🎯 測試問題 #{question_id}: {question}")
    print("="*80)
    
    try:
        start_time = time.time()
        result = ask(question)
        end_time = time.time()
        
        # 檢查結果是否有關鍵指標
        success_indicators = [
            "📊 [Data Results]" in result,
            "🔍 [Analysis Report]" in result,
            "📈 [Execution Summary]" in result
        ]
        
        has_data = "rows processed: 0" not in result
        execution_time = end_time - start_time
        
        print(f"✅ 問題 #{question_id} 執行成功!")
        print(f"   執行時間: {execution_time:.2f} 秒")
        print(f"   有資料返回: {'是' if has_data else '否'}")
        
        if has_data:
            # 嘗試提取處理的行數
            import re
            rows_match = re.search(r"rows processed: (\d+)", result)
            if rows_match:
                rows_count = rows_match.group(1)
                print(f"   處理行數: {rows_count}")
        
        return True, execution_time, has_data
        
    except Exception as e:
        print(f"❌ 問題 #{question_id} 執行失敗: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, False

def main():
    """主測試函數 - 測試所有20個問題"""
    
    # 所有20個分析問題
    analysis_questions = [
        # 玩家活躍度與留存分析
        "請分析 2024-10 月台灣玩家的登入週期模式，比較週末與工作日的 SessionActive 差異",
        "請給我 2024-10-01 到 2024-10-07 期間首次登入的新玩家，分析他們首日遊戲時長與後續 7 日留存的關係", 
        "比較不同 VIP 等級玩家在 2024-10 月的平均 SessionLength 和登入頻次，找出最活躍的 VIP 群體",
        
        # 商業營收與押注分析
        "請找出 2024-10 月押注金額前 1% 的玩家，分析他們的遊戲偏好和時間分佈模式",
        "分析各個遊戲類別在 2024-10 月的總押注量和玩家數，找出最有價值的遊戲品類",
        "分析玩家儲值後 24 小時內的押注行為變化，計算儲值轉換率",
        
        # 遊戲體驗與平衡性  
        "計算各個遊戲在 2024-10 月的實際 RTP（Return to Player），找出玩家最容易獲勝的遊戲",
        "分析不同遊戲的平均 SessionLength 與單局押注金額的相關性",
        "找出各個熱門遊戲（如愛麗絲、宙斯、KOF'97）的玩家高峰時段分佈",
        
        # 地區與渠道分析
        "比較台灣(TW)和美國(US)玩家的遊戲偏好、押注習慣和平均遊戲時長",
        "分析不同 Channel 的玩家品質，比較各渠道玩家的 LTV（生命週期價值）",
        "分析不同國家/地區玩家最偏愛的遊戲類型和押注段位分佈",
        
        # 玩家分群與行為模式
        "分析不同押注段位的玩家特徵：遊戲時長、VIP等級、地區分佈的關聯性",
        "識別連續 7 天未登入但曾經活躍的玩家，分析他們流失前的行為特徵", 
        "比較玩家在週末和工作日的遊戲選擇、押注金額和遊戲時長差異",
        
        # 時間序列與趨勢分析
        "追蹤新上線遊戲從發佈到穩定期的玩家數和押注量變化趨勢",
        "分析特殊節日（如國慶連假）對不同類型遊戲玩家活躍度和押注行為的影響",
        "分析 24 小時內不同時段的 SessionActive 分佈，找出最佳營運時間窗口",
        
        # 精準營運與優化
        "基於玩家歷史遊戲偏好和押注習慣，分析哪些遊戲組合最容易提升玩家黏性",
        "結合 SessionActive、押注記錄和儲值行為，建立玩家價值評分模型並進行客群細分"
    ]
    
    print("🚀 開始完整測試所有20個分析問題...")
    print(f"測試時間: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    results = []
    total_time = 0
    success_count = 0
    data_count = 0
    
    for i, question in enumerate(analysis_questions, 1):
        success, exec_time, has_data = test_question(i, question)
        
        results.append({
            'id': i,
            'question': question[:50] + "..." if len(question) > 50 else question,
            'success': success,
            'time': exec_time,
            'has_data': has_data
        })
        
        if success:
            success_count += 1
            total_time += exec_time
            if has_data:
                data_count += 1
        
        # 在問題之間暫停，避免API限制
        if i < len(analysis_questions):
            print("\n⏱️  暫停 3 秒避免API限制...\n")
            time.sleep(3)
    
    # 生成測試報告
    print("\n" + "="*80)
    print("📊 完整測試報告")
    print("="*80)
    
    print(f"總問題數: {len(analysis_questions)}")
    print(f"成功執行: {success_count}")
    print(f"失敗執行: {len(analysis_questions) - success_count}")
    print(f"有數據返回: {data_count}")
    print(f"成功率: {success_count/len(analysis_questions)*100:.1f}%")
    print(f"數據返回率: {data_count/success_count*100:.1f}%" if success_count > 0 else "數據返回率: 0%")
    print(f"平均執行時間: {total_time/success_count:.2f} 秒" if success_count > 0 else "平均執行時間: N/A")
    
    print("\n📋 詳細結果:")
    print("-"*80)
    for result in results:
        status = "✅" if result['success'] else "❌"
        data_status = "📊" if result['has_data'] else "🚫"
        time_str = f"{result['time']:.1f}s" if result['success'] else "N/A"
        print(f"{status} #{result['id']:2d} {data_status} {time_str:>6s} | {result['question']}")
    
    # 失敗問題列表
    failed_questions = [r for r in results if not r['success']]
    if failed_questions:
        print(f"\n⚠️  失敗問題 ({len(failed_questions)} 個):")
        for result in failed_questions:
            print(f"   #{result['id']}: {result['question']}")
    
    # 無資料問題列表  
    no_data_questions = [r for r in results if r['success'] and not r['has_data']]
    if no_data_questions:
        print(f"\n⚠️  無資料返回問題 ({len(no_data_questions)} 個):")
        for result in no_data_questions:
            print(f"   #{result['id']}: {result['question']}")
    
    # 總結
    if success_count == len(analysis_questions):
        print(f"\n🎉 所有 {len(analysis_questions)} 個問題都執行成功！")
    elif success_count >= len(analysis_questions) * 0.8:
        print(f"\n👍 大部分問題執行成功 ({success_count}/{len(analysis_questions)})，系統運作良好！")
    else:
        print(f"\n⚠️  部分問題執行失敗 ({success_count}/{len(analysis_questions)})，需要檢查問題。")
    
    return success_count == len(analysis_questions)

if __name__ == "__main__":
    main()
