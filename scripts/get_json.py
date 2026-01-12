import json
import os
import re
import requests
from datetime import datetime


def clean_height_info(text):
    """
    增強版：徹底移除所有高度限制相關信息
    """
    if not text:
        return ""
    
    cleaned = text
    
    # 第一階段：處理各種高度限制表述
    height_patterns = [
        # 處理重複模式：「高度限制: 高度限制1.7米」
        (r'高度限制\s*[:：]\s*高度限制\s*\d*\.?\d+\s*[mM米]?\s*', ''),
        # 處理「高度限制: 1.7米」或「高度限制1.7米」
        (r'高度限制\s*\d*\.?\d+\s*[mM米]?\s*', ''),
        # 處理「高度限制:」前綴
        (r'高度限制\s*[:：]\s*', ''),
        # 處理簡寫
        (r'高度\s*[:：]?\s*\d*\.?\d+\s*[mM米]?\s*', ''),
        (r'限高\s*[:：]?\s*\d*\.?\d+\s*[mM米]?\s*', ''),
        # 處理可能的中英文混雜
        (r'[Hh]eight\s*[:：]?\s*\d*\.?\d+\s*[mM]?\s*', ''),
    ]
    
    for pattern, replacement in height_patterns:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
    
    # 第二階段：清理換行和特殊字符
    cleaned = re.sub(r'[\n\r]+', ' ', cleaned)  # 換行符轉空格
    
    # 第三階段：清理開頭的特殊符號
    opening_symbols = r'^[\*\-\:・\.\s\/\\\<\>\"\']+'
    while re.match(opening_symbols, cleaned):
        cleaned = re.sub(opening_symbols, '', cleaned)
    
    # 第四階段：清理HTML標籤（簡單處理）
    cleaned = re.sub(r'<br\s*/?>', ' ', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'<[^>]+>', ' ', cleaned)  # 移除所有HTML標籤
    
    # 最後：合併多餘空格並修剪
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned

def extract_height_value(text):
    """
    從文本中提取高度數值，返回浮點數
    """
    if not text:
        return None
    
    # 嘗試提取數字部分
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:米|m)?', str(text))
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    
    return None

def clean_height_string(text):
    """
    清理高度字符串，去除重複的「高度限制:」前綴
    """
    if not text:
        return ""
    
    # 移除多餘的空格和換行
    cleaned = re.sub(r'\s+', ' ', text).strip()
    
    # 處理重複的「高度限制:」前綴
    cleaned = re.sub(r'高度限制\s*[:：]\s*(?:[\n\r]+\s*)?高度限制\s*[:：]\s*', '高度限制: ', cleaned)
    
    # 處理多個「高度限制:」連續出現的情況
    while '高度限制: 高度限制:' in cleaned:
        cleaned = cleaned.replace('高度限制: 高度限制:', '高度限制: ')
    
    # 清理開頭的無用字符
    cleaned = re.sub(r'^[:\s\n\r]+', '', cleaned)
    
    return cleaned

def extract_height_for_private_car(remark):
    """
    從remark中提取適用於私家車的高度限制
    """
    if not remark:
        return None
    
    # 移除HTML標籤和換行符
    cleaned = re.sub(r'<br\s*/?>', '\n', remark, flags=re.IGNORECASE)
    cleaned = re.sub(r'<[^>]+>', ' ', cleaned)
    cleaned = re.sub(r'\\u003Cbr\\u003E', '\n', cleaned)
    
    # 按行分割
    lines = cleaned.split('\n')
    
    for line in lines:
        line = line.strip()
        # 檢查是否包含私家車/客貨車關鍵詞
        if '私家車' in line or '客貨車' in line:
            # 提取高度數值
            height_value = extract_height_value(line)
            if height_value:
                return height_value
    
    return None

def is_pure_height_info(text):
    """
    判斷文本是否為純高度限制信息（不含收費信息）
    """
    if not text:
        return False
    
    # 檢查是否包含收費關鍵詞
    charge_keywords = ["收費", "元", "$", "時", "泊", "費", "優惠", "免費", "價", "HKD", "小時", "每小時", 
                      "日泊", "夜泊", "月租", "時租", "星期一", "星期二", "星期三", "星期四", 
                      "星期五", "星期六", "星期日", "公眾假期", "電單車", "的士", "私家車"]
    
    # 如果包含任何收費關鍵詞，則不是純高度信息
    for keyword in charge_keywords:
        if keyword in text:
            return False
    
    # 檢查是否包含高度相關信息
    height_keywords = ["高度限制", "限高", "高度", "米", "m"]
    for keyword in height_keywords:
        if keyword in text.lower():
            return True
    
    return False

def clean_html_tags(text):
    """
    清理HTML標籤，將<br>轉換為換行符
    """
    if not text:
        return ""
    
    # 替換<br>為換行符
    cleaned = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    # 移除其他HTML標籤
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    # 處理unicode的br標籤
    cleaned = re.sub(r'\\u003Cbr\\u003E', '\n', cleaned)
    
    return cleaned

def clean_height_remark_advanced(text):
    """
    高級清洗高度限制remark：
    1. 移除所有收費相關信息
    2. 只保留純高度限制信息
    3. 如果沒有高度信息，返回空字符串
    """
    if not text:
        return ""
    
    cleaned = text
    
    # 1. 清理HTML標籤
    cleaned = clean_html_tags(cleaned)
    
    # 2. 按行分割，過濾每行
    lines = cleaned.split('\n')
    filtered_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # 檢查是否為高度相關信息
        if '高度限制' in line or '限高' in line or ('高度' in line and ('米' in line or 'm' in line)):
            # 這可能是高度信息，檢查是否包含收費相關內容
            charge_keywords = ["元", "$", "時租", "日泊", "夜泊", "月租", "收費", "泊車", "停車", "費用"]
            has_charge = any(keyword in line for keyword in charge_keywords)
            
            if not has_charge:
                # 進一步清理，只保留高度數字和單位
                height_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:米|m)', line)
                if height_match:
                    height_value = height_match.group(1)
                    unit = '米' if '米' in line else 'm'
                    filtered_lines.append(f"高度限制: {height_value}{unit}")
                else:
                    filtered_lines.append(line)
    
    # 3. 重新組合成文本
    result = '\n'.join(filtered_lines)
    
    # 4. 如果結果為空，檢查原始文本是否有高度數字
    if not result:
        height_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:米|m)', text)
        if height_match:
            height_value = height_match.group(1)
            unit = '米' if '米' in text else 'm'
            result = f"高度限制: {height_value}{unit}"
    
    # 5. 應用加強的去重規則
    result = clean_height_string(result)
    
    return result

def extract_charges_description(remark):
    """
    從remark中提取收費信息，用於privileges.description
    1. 移除高度限制信息
    2. 保留收費信息
    3. 清理HTML標籤，將<br>轉換為換行符
    """
    if not remark:
        return ""
    
    # 1. 清理HTML標籤，保留換行
    cleaned = clean_html_tags(remark)
    
    # 2. 按行分割
    lines = cleaned.split('\n')
    charge_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # 檢查是否為收費信息（包含收費關鍵詞且不主要為高度信息）
        charge_keywords = ["元", "$", "時租", "日泊", "夜泊", "月租", "收費", "泊車", "停車", "費用", 
                          "優惠", "免費", "每小時", "小時", "私家車", "電單車", "的士", "星期一",
                          "星期二", "星期三", "星期四", "星期五", "星期六", "星期日", "公眾假期"]
        
        # 檢查是否為高度信息
        height_keywords = ["高度限制", "限高", "高度:"]
        is_height_line = any(keyword in line for keyword in height_keywords)
        
        # 檢查是否包含收費信息
        has_charge = any(keyword in line for keyword in charge_keywords)
        
        if has_charge and not is_height_line:
            charge_lines.append(line)
        elif not is_height_line and line and len(line) > 5:  # 不是高度信息且不是空行
            # 進一步檢查是否包含數字+元/$的收費模式
            if re.search(r'[$＄]?\d+', line):
                charge_lines.append(line)
    
    # 3. 重新組合
    result = '\n'.join(charge_lines)
    
    # 4. 清理開頭結尾的空白和特殊字符
    result = re.sub(r'^[\n\s]+', '', result)
    result = re.sub(r'[\n\s]+$', '', result)
    
    return result

# =============================================================================
# 2. 核心數據處理邏輯（完全保留 auto-clean.py 的 preprocess_parking_data）
# =============================================================================

def process_and_save_data(raw_data, output_path):
    """
    接收原始 API 數據並執行完整的清洗流程
    """
    try:
        # 兼容 API 結構
        items = raw_data.get('results', raw_data) if isinstance(raw_data, dict) else raw_data
        processed_list = []

        # 收費關鍵詞：用於判斷 Remark 是否包含有價值的收費信息
        charge_keywords = ["收費", "元", "$", "時", "泊", "費", "優惠", "免費", "價", "HKD", "小時", "每小時"]

        print(f"🚀 開始處理 {len(items)} 條原始記錄...")

        for item in items:
            # 建立核心靜態字段
            clean_item = {
                "park_Id": str(item.get("park_Id", "")),
                "name": item.get("name", ""),
                "displayAddress": item.get("displayAddress", ""),
                "latitude": item.get("latitude"),
                "longitude": item.get("longitude"),
                "contactNo": item.get("contactNo", ""),
                "website": item.get("website", ""),
                "facilities": item.get("facilities", []),
                "heightLimits": item.get("heightLimits", []),  # 保留此字段
                "privateCar": item.get("privateCar", {})
            }

            # 標記是否通過數據救援填入了privateCar
            is_auto_filled = False

            # --- 邏輯 A: 數據救援 (從高度限制 remark 提取收費) ---
            if not clean_item["privateCar"] or len(clean_item["privateCar"]) == 0:
                height_limits = item.get("heightLimits", [])
                rescue_content = ""
                for hl in height_limits:
                    remark = hl.get("remark", "")
                    # 檢查是否包含收費信息
                    if any(k in remark for k in charge_keywords):
                        # 1. 提取收費信息（移除高度信息）
                        rescue_content = extract_charges_description(remark)
                        
                        # 2. 清理高度限制的remark（只保留純高度信息）
                        if remark and remark.strip():
                            hl["remark"] = clean_height_remark_advanced(remark)
                        
                        if rescue_content:
                            print(f"✅ 數據救援: {clean_item.get('park_Id')} - 提取收費信息")
                            break
                
                if rescue_content:
                    # 創建privileges數組，包含add_source字段
                    privilege_item = {
                        "weekdays": ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN", "PH"],
                        "description": rescue_content,
                        "periodStart": "00:00",
                        "periodEnd": "24:00",
                        "add_source": "py-auto-fill"  # 在privilege內部添加add_source
                    }
                    
                    clean_item["privateCar"] = {
                        "privileges": [privilege_item]
                    }
                    is_auto_filled = True
                else:
                    # 如果沒有提取到收費信息，但原始remark有高度信息，則清理heightLimits
                    for hl in height_limits:
                        remark = hl.get("remark", "")
                        if remark:
                            hl["remark"] = clean_height_remark_advanced(remark)

            # --- 邏輯 B: 私家車數據深度清洗 ---
            if isinstance(clean_item.get("privateCar"), dict) and clean_item["privateCar"]:
                pc = clean_item["privateCar"]
                
                # 1. 刪除所有動態車位字段 (包含 'space' 的 key)
                keys_to_del = [k for k in pc.keys() if 'space' in k.lower()]
                for k in keys_to_del:
                    del pc[k]

                # 2. 強化 hourlyCharges 中的 Remark 識別
                if "hourlyCharges" in pc:
                    for entry in pc["hourlyCharges"]:
                        remark = entry.get("remark", "")
                        
                        # 清理remark中的HTML標籤
                        if remark:
                            entry["remark"] = clean_html_tags(remark)
                        
                        # 根據type字段設置billing_unit
                        type_field = entry.get("type", "").lower()
                        if type_field == "half-hourly":
                            entry["billing_unit"] = "half_hour"
                        elif type_field == "hourly":
                            entry["billing_unit"] = "hour"
                        # 其它type值不設置billing_unit

            # --- 邏輯 C: 處理heightLimits中的高度信息 ---
            if clean_item.get("heightLimits"):
                for hl in clean_item["heightLimits"]:
                    # 1. 清理height字段，確保是數值
                    if "height" in hl:
                        current_height = hl["height"]
                        if current_height:
                            # 如果height是字符串，提取數值
                            if isinstance(current_height, str):
                                height_value = extract_height_value(current_height)
                                if height_value:
                                    hl["height"] = height_value
                                    print(f"🔄 轉換height為數值: {clean_item.get('park_Id')} - {current_height} -> {height_value}")
                                else:
                                    hl["height"] = 0  # 無法轉換，設為0
                            # 如果已經是數值，確保是浮點數
                            elif isinstance(current_height, (int, float)):
                                hl["height"] = float(current_height)
                    
                    # 2. 從remark中提取高度信息
                    if "remark" in hl and hl["remark"]:
                        current_remark = hl["remark"]
                        
                        # 提取適用於私家車的高度限制
                        private_car_height = extract_height_for_private_car(current_remark)
                        if private_car_height:
                            hl["height"] = private_car_height
                            print(f"📏 提取高度: {clean_item.get('park_Id')} - {private_car_height}")
                        
                        # 3. 清理remark文本（移除重複、清理格式）
                        cleaned_remark = clean_height_remark_advanced(current_remark)
                        
                        # 4. 如果清理後的remark有高度信息但height為0，從remark提取高度
                        if cleaned_remark and (not hl.get("height") or hl["height"] == 0):
                            # 嘗試從清理後的remark提取高度
                            height_from_cleaned = extract_height_value(cleaned_remark)
                            if height_from_cleaned:
                                hl["height"] = height_from_cleaned
                                print(f"📏 從清理後remark提取高度: {clean_item.get('park_Id')} - {height_from_cleaned}")
                        
                        # 5. 應用最終的remark
                        hl["remark"] = cleaned_remark
                        
                        # 6. 如果remark為空且有高度值，可以設置一個默認remark
                        if not cleaned_remark and hl.get("height") and hl["height"] > 0:
                            hl["remark"] = f"高度限制: {hl['height']}米"
            
            # --- 邏輯 D: 最後檢查heightLimits ---
            if clean_item.get("heightLimits"):
                for hl in clean_item["heightLimits"]:
                    # 確保height字段存在且為數值
                    if "height" not in hl:
                        hl["height"] = 0
                    elif hl["height"] and not isinstance(hl["height"], (int, float)):
                        # 如果height不是數值，嘗試轉換
                        height_value = extract_height_value(str(hl["height"]))
                        hl["height"] = height_value if height_value else 0
                    
                    # 確保remark沒有重複的「高度限制:」
                    if "remark" in hl and hl["remark"]:
                        hl["remark"] = clean_height_string(hl["remark"])
                        
                        # 如果remark為空但有高度值，設置一個默認remark
                        if not hl["remark"] and hl.get("height") and hl["height"] > 0:
                            hl["remark"] = f"高度限制: {hl['height']}米"

            processed_list.append(clean_item)

        # 確保 data 目錄存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 寫入清洗後的數據
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({"results": processed_list}, f, ensure_ascii=False, indent=4)

        # ... 清洗邏輯完成後 ...
        log_msg = f"成功獲取政府數據，共 {len(processed_list)} 條，已保存至 gov_cleaned.json"
        # 覆蓋寫入新的 log 文件
        with open("data/step_status.log", "w", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%H:%M:%S')}] STEP 1: {log_msg}\n")

        print(f"✅ 處理完成！")
        print(f"📁 輸出文件：{output_path}")
        print(f"📊 最終條數：{len(processed_list)} 條")

    except json.JSONDecodeError:
        print("❌ 錯誤: JSON 格式損壞，請檢查原始文件。")
    except Exception as e:
        with open("data/step_status.log", "w", encoding="utf-8") as log_f:
            log_f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 1 ERROR: {str(e)}\n")
        print(f"❌ 發生未知錯誤: {str(e)}")

# =============================================================================
# 3. 主函數：下載並處理數據
# =============================================================================

def main():
    # ✅ GitHub Actions 適配：明確工作目錄為倉庫根目錄
    # 獲取腳本所在目錄的父目錄（即項目根目錄）
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    os.chdir(project_root)  # 確保工作目錄正確
    
    print(f"📍 當前工作目錄: {os.getcwd()}")
    
    API_URL = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=info&vehicleTypes=privateCar&lang=zh_TW"
    OUTPUT_FILE = os.path.join("data", "gov_cleaned.json")
    
    print(f"🌐 正在請求政府 API...")
    try:
        # 設置超時防止掛起
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(API_URL, headers=headers, timeout=60)
        response.raise_for_status()  # 檢查 HTTP 狀態碼
        
        raw_json = response.json()
        print("📥 數據下載完成，開始執行清洗...")
        
        # 處理並保存數據
        process_and_save_data(raw_json, OUTPUT_FILE)
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API 請求失敗: {e}")
        print(f"   請檢查網絡連接或API地址是否正確")
        exit(1)  # ✅ GitHub Actions 適配：明確退出碼
    except Exception as e:
        print(f"❌ 執行失敗: {e}")
        exit(1)  # ✅ GitHub Actions 適配：明確退出碼

if __name__ == "__main__":
    main()