import json
import os
from datetime import datetime

def extract_descriptions_from_json(file_path):
    # ... (保持原函數邏輯不變) ...
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        descriptions = []
        for result in data.get('results', []):
            if 'privateCar' in result and 'privileges' in result['privateCar']:
                for privilege in result['privateCar']['privileges']:
                    if 'description' in privilege:
                        descriptions.append({
                            'park_Id': result.get('park_Id', 'N/A'),
                            'name': result.get('name', 'N/A'),
                            'description': privilege['description']
                        })
        return descriptions
    except Exception as e:
        print(f"錯誤: {str(e)}")
        return []

def main():
    # 路徑設置
    input_file = os.path.join("data", "gov_cleaned.json")
    output_file = os.path.join("data", "description.json")
    
    print(f"🚀 正在從 {input_file} 提取描述...")
    descriptions = extract_descriptions_from_json(input_file)
    
    if descriptions:
        # 1. 保存 JSON 文件
        os.makedirs("data", exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(descriptions, f, ensure_ascii=False, indent=2)
        print(f"✅ 已成功提取 {len(descriptions)} 條描述至 {output_file}")

        # 2. --- 新增日誌記錄 (追加模式 'a') ---
        log_path = os.path.join("data", "step_status.log")
        with open(log_path, "a", encoding="utf-8") as log_f:
            log_f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 3: 提取完成，共 {len(descriptions)} 條描述。\n")
        # -----------------------------------
        
    else:
        # 如果提取失敗，也可以記錄一條錯誤日誌
        with open("data/step_status.log", "a", encoding="utf-8") as log_f:
            log_f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 3: 警告 - 未能提取任何描述內容。\n")
        print("⚠️ 未找到任何 description 內容。")

if __name__ == "__main__":
    main()