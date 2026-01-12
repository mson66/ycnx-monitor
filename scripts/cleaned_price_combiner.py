import json
import os
from datetime import datetime

def load_json_file(file_path):
    """加載JSON文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"錯誤：找不到文件 {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"錯誤：JSON解析失敗 {file_path}: {e}")
        return None

def save_json_file(data, file_path):
    """保存JSON文件"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"成功保存文件：{file_path}")
    except Exception as e:
        print(f"錯誤：保存文件失敗 {file_path}: {e}")

def normalize_weekdays(weekdays):
    """標準化星期幾的格式（排序）"""
    if not weekdays:
        return []
    return sorted(weekdays)

def is_same_rule(rule1, rule2):
    """判斷兩個規則是否相同（僅比較weekdays、periodStart、periodEnd）"""
    # 比較星期幾（排序後比較）
    weekdays1 = normalize_weekdays(rule1.get('weekdays', []))
    weekdays2 = normalize_weekdays(rule2.get('weekdays', []))
    
    # 比較時間段
    period_start1 = rule1.get('periodStart', '')
    period_start2 = rule2.get('periodStart', '')
    period_end1 = rule1.get('periodEnd', '')
    period_end2 = rule2.get('periodEnd', '')
    
    return (weekdays1 == weekdays2 and 
            period_start1 == period_start2 and 
            period_end1 == period_end2)

def rule_exists_in_list(rule, rule_list):
    """檢查規則是否已存在於列表中"""
    for existing_rule in rule_list:
        if is_same_rule(rule, existing_rule):
            return True
    return False

def merge_park_data(gov_park, desc_park):
    """合併單個停車場的數據"""
    # 創建深拷貝，避免修改原始數據
    merged_park = json.loads(json.dumps(gov_park))
    
    desc_private_car = desc_park.get('privateCar', {})
    
    # 確保privateCar存在
    if 'privateCar' not in merged_park:
        merged_park['privateCar'] = {}
    
    # 合併hourlyCharges
    gov_hourly = merged_park['privateCar'].get('hourlyCharges', [])
    desc_hourly = desc_private_car.get('hourlyCharges', [])
    
    hourly_added = 0
    hourly_skipped = 0
    
    for desc_rule in desc_hourly:
        # 檢查是否已存在相同規則
        if not rule_exists_in_list(desc_rule, gov_hourly):
            gov_hourly.append(desc_rule)
            hourly_added += 1
        else:
            hourly_skipped += 1
    
    if gov_hourly:
        merged_park['privateCar']['hourlyCharges'] = gov_hourly
    
    # 合併dayNightParks
    gov_daynight = merged_park['privateCar'].get('dayNightParks', [])
    desc_daynight = desc_private_car.get('dayNightParks', [])
    
    daynight_added = 0
    daynight_skipped = 0
    
    for desc_rule in desc_daynight:
        # 檢查是否已存在相同規則
        if not rule_exists_in_list(desc_rule, gov_daynight):
            gov_daynight.append(desc_rule)
            daynight_added += 1
        else:
            daynight_skipped += 1
    
    if gov_daynight:
        merged_park['privateCar']['dayNightParks'] = gov_daynight
    
    return merged_park, {
        'hourly_added': hourly_added,
        'hourly_skipped': hourly_skipped,
        'daynight_added': daynight_added,
        'daynight_skipped': daynight_skipped
    }

def generate_report(stats):
    """生成詳細報告"""
    report = []
    report.append("=" * 80)
    report.append("JSON文件合併報告")
    report.append("=" * 80)
    report.append(f"合併時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"總處理停車場數量：{stats['total_parks']}")
    report.append(f"成功匹配並合併的停車場：{stats['matched_count']}")
    report.append(f"未匹配的停車場：{stats['unmatched_count']}")
    report.append("")
    
    report.append("詳細統計：")
    report.append("-" * 40)
    report.append(f"hourlyCharges規則：")
    report.append(f"  新增規則：{stats['total_hourly_added']}")
    report.append(f"  跳過規則（已存在）：{stats['total_hourly_skipped']}")
    report.append(f"dayNightParks規則：")
    report.append(f"  新增規則：{stats['total_daynight_added']}")
    report.append(f"  跳過規則（已存在）：{stats['total_daynight_skipped']}")
    report.append("")
    
    if stats['matched_parks']:
        report.append("已合併的停車場詳細情況：")
        report.append("-" * 40)
        for park_name, park_stats in stats['matched_parks'].items():
            report.append(f"  {park_name}:")
            report.append(f"    hourlyCharges - 新增:{park_stats['hourly_added']}, 跳過:{park_stats['hourly_skipped']}")
            report.append(f"    dayNightParks - 新增:{park_stats['daynight_added']}, 跳過:{park_stats['daynight_skipped']}")
    
    if stats['unmatched_names']:
        report.append("")
        report.append("未匹配的停車場（park_Id不匹配）：")
        report.append("-" * 40)
        for name in stats['unmatched_names'][:20]:  # 只顯示前20個
            report.append(f"  {name}")
        if len(stats['unmatched_names']) > 20:
            report.append(f"  ... 還有 {len(stats['unmatched_names']) - 20} 個")
    
    return "\n".join(report)

def main():
    # 修改這裡的文件路徑以指向 data/ 目錄
    gov_file = os.path.join('data', 'gov_cleaned.json')
    desc_file = os.path.join('data', 'description-rules.json')
    output_file = os.path.join('data', 'liberay_merged.json')
    report_file = os.path.join('data', 'merge_report.txt')
    
    print("開始合併JSON文件...")
    print(f"基礎文件：{gov_file}")
    print(f"補充文件：{desc_file}")
    
    # 加載文件
    gov_data = load_json_file(gov_file)
    desc_data = load_json_file(desc_file)
    
    if gov_data is None or desc_data is None:
        print("加載文件失敗，程序退出。")
        return
    
    print(f"基礎文件停車場數量：{len(gov_data.get('results', []))}")
    print(f"補充文件停車場數量：{len(desc_data)}")
    
    # 初始化統計數據
    stats = {
        'total_parks': len(gov_data.get('results', [])),
        'matched_count': 0,
        'unmatched_count': 0,
        'total_hourly_added': 0,
        'total_hourly_skipped': 0,
        'total_daynight_added': 0,
        'total_daynight_skipped': 0,
        'matched_parks': {},
        'unmatched_names': []
    }
    
    # 創建description-rules.json的park_Id索引
    desc_by_id = {}
    for desc_park in desc_data:
        park_id = desc_park.get('park_Id', '').strip()
        if park_id:
            desc_by_id[park_id] = desc_park
    
    print(f"補充文件中有效的park_Id數量：{len(desc_by_id)}")
    
    # 合併數據
    merged_results = []
    
    # 遍歷gov數據中的每個停車場
    for gov_park in gov_data.get('results', []):
        gov_park_id = gov_park.get('park_Id', '').strip()
        gov_park_name = gov_park.get('name', '')
        
        # 在desc數據中查找匹配的停車場（僅通過park_Id）
        if gov_park_id in desc_by_id:
            # 找到匹配，合併數據
            desc_park = desc_by_id[gov_park_id]
            merged_park, park_stats = merge_park_data(gov_park, desc_park)
            merged_results.append(merged_park)
            
            # 更新統計
            stats['matched_count'] += 1
            stats['total_hourly_added'] += park_stats['hourly_added']
            stats['total_hourly_skipped'] += park_stats['hourly_skipped']
            stats['total_daynight_added'] += park_stats['daynight_added']
            stats['total_daynight_skipped'] += park_stats['daynight_skipped']
            
            # 記錄詳細統計
            park_display_name = f"{gov_park_name} (ID:{gov_park_id})"
            stats['matched_parks'][park_display_name] = park_stats
            
            print(f"✓ 已合併: {gov_park_name} (ID:{gov_park_id})")
        else:
            # 未找到匹配，保留原始數據
            merged_results.append(gov_park.copy())
            stats['unmatched_count'] += 1
            stats['unmatched_names'].append(f"{gov_park_name} (ID:{gov_park_id})")
            print(f"✗ 未匹配: {gov_park_name} (ID:{gov_park_id})")
    
    # 構建合併後的數據結構
    merged_data = {
        "metadata": {
            "source_files": [gov_file, desc_file],
            "merged_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "merge_rule": "僅通過park_Id匹配，僅比較weekdays/periodStart/periodEnd判斷規則是否重複"
        },
        "results": merged_results
    }
    
    # 保存合併後的JSON文件
    save_json_file(merged_data, output_file)
    
    # 生成並保存報告
    report = generate_report(stats)
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n合併完成！")
    print(f"輸出文件：{output_file}")
    print(f"報告文件：{report_file}")
    
    # 顯示簡要摘要
    print("\n" + "=" * 80)
    print("合併摘要：")
    print(f"• 總處理停車場：{stats['total_parks']}")
    print(f"• 成功匹配並合併：{stats['matched_count']}")
    print(f"• 未匹配：{stats['unmatched_count']}")
    print(f"• 新增hourlyCharges規則：{stats['total_hourly_added']}")
    print(f"• 跳過hourlyCharges規則：{stats['total_hourly_skipped']}")
    print(f"• 新增dayNightParks規則：{stats['total_daynight_added']}")
    print(f"• 跳過dayNightParks規則：{stats['total_daynight_skipped']}")
    print("=" * 80)

    with open("data/step_status.log", "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] STEP 7: 最終合併數據庫完成。\n")

if __name__ == "__main__":
    main()