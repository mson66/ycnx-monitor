import json
import os
import sys
import re
import time
from groq import Groq  # 根據你的要求使用 Groq 庫
from datetime import datetime

# === 配置部分 ===
# 注意：如果你使用的是 Moonshot 官方 API，BASE_URL 應設為 https://api.moonshot.cn/v1
API_KEY = os.environ.get("GROQ_API_KEY")
MODEL_NAME = os.environ.get("LLM_MODEL", "moonshotai/kimi-k2-instruct-0905")

if not API_KEY:
    print("錯誤: 未找到環境變量 GROQ_API_KEY")
    sys.exit(1)

# 初始化客戶端
client = Groq(
    api_key=API_KEY
)

# 定義 System Prompt：嚴格要求輸出格式以適配後續的 cleaned_price_combiner.py
SYSTEM_PROMPT = """
# Role: 停車場數據結構化專家

# Task: 
讀取停車場的 "description" 字段信息，提取私家車的時租、日泊、夜泊、全日泊收費規則，並將其轉換為結構化的 JSON 格式。

# Output Format:
輸出必須是一個包含單個 JSON 對象的數組，每個對象結構如下：
{
  "park_Id": "從輸入提取",
  "name": "從輸入提取",
  "description": "原始描述內容",
  "privateCar": {
    "hourlyCharges": [],
    "dayNightParks": []
  }
}

## 1. hourlyCharges 字段規範 (時租/半小時租)
- **weekdays**: 數組，可選值 [MON, TUE, WED, THU, FRI, SAT, SUN, PH]。PH代表公眾假期
- **excludePublicHoliday**: boolean，若描述提及「公眾假期除外」則為 true。
- **type**: 字符串，固定為 "hourly" 或 "half-hourly"。
- **price**: 數值 (Number)。
- **periodStart / periodEnd**: 格式 HH:mm (如 08:00, 23:59)。
- **remark**: 標準備注：[日子段] + [時段] + [單價] + [階梯收費說明]。
- **add_source**: 固定為 "py-ai-fill-rule"。
- **billing_unit**: 固定為 "hour" 或 "half_hour"。
- **covered**: 留空 ""。

## 2. dayNightParks 字段規範 (日泊/夜泊/全日泊)
- **type**: "day-park" (含全日泊), "night-park"。
- **全日泊處理**: 歸類為 "day-park"，時段設為 00:00-24:00。
- **validUntil**: 留空 ""。
- **billing_unit**: 沒有這個字段。
- 其餘字段 (weekdays, price, periodStart/End, remark, add_source) 邏輯同 hourlyCharges。

# Rules:
1. 只輸出純 JSON 內容，禁止包含 Markdown 標記 (如 ```json) 或任何解釋文字。
2. 確保數值類型正確 (price 必須是數字)。
3. 如果描述中沒有相關信息，對應數組應為空 []。
4. 嚴格遵守 24 小時制時間格式。
"""

def extract_json_from_text(text):
    """
    從 LLM 返回的文本中提取 JSON 部分
    """
    try:
        # 嘗試直接解析
        return json.loads(text)
    except json.JSONDecodeError:
        # 如果包含 Markdown 代碼塊，嘗試提取
        match = re.search(r'```json\s*([\s\S]*?)\s*```', text)
        if match:
            try:
                return json.loads(match.group(1))
            except:
                pass
        
        # 嘗試尋找第一個 { 和最後一個 }
        match = re.search(r'(\{[\s\S]*\})', text)
        if match:
            try:
                return json.loads(match.group(1))
            except:
                pass
                
        return None

def process_single_item(item):
    """
    處理單個停車場數據
    """
    park_id = item.get('park_Id', 'N/A')
    description = item.get('description', '')
    
    if not description:
        return None

    user_msg = f"停車場 ID: {park_id}\n原始描述: {description}\n請提取規則並輸出 JSON。"

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg}
            ],
            temperature=0.1,
            max_completion_tokens=4096,
            stream=False
        )
        
        # === 核心修復：萬能提取邏輯 ===
        response_text = ""
        try:
            # 1. 嘗試物件格式 (本地常用)
            response_text = completion.choices[0].message.content
        except (AttributeError, TypeError):
            try:
                # 2. 嘗試字典格式 (Actions 環境可能出現)
                # 報錯 "list indices must be integers" 通常發生在對列表使用了 ['message']
                # 這裡確保我們先定位到 choices[0]
                choice = completion.choices[0]
                if isinstance(choice, dict):
                    response_text = choice['message']['content']
                else:
                    # 如果 choices[0] 是對象但沒有 .message 屬性
                    response_text = choice.get('message', {}).get('content', '')
            except Exception as e:
                print(f"⚠️ 解析結構失敗 {park_id}: {e}")
                return None

        if not response_text:
            return None
            
        json_data = extract_json_from_text(response_text)
        
        if json_data:
            json_data['park_Id'] = park_id
            return json_data
        return None

    except Exception as e:
        print(f"❌ API 調用失敗 {park_id}: {str(e)}")
        return None

def main():
    if len(sys.argv) < 3:
        print("Usage: python ai_fill.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print(f"🤖 AI 處理開始 (Model: {MODEL_NAME})")
    print(f"📂 輸入: {input_file}")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        results = []
        total = len(data)
        
        # 遍歷處理
        for i, item in enumerate(data):
            # 進度顯示
            print(f"[{i+1}/{total}] 處理 ID: {item.get('park_Id')}...", end="\r")
            
            result = process_single_item(item)
            if result:
                results.append(result)
            
            # 避免 API 速率限制 (根據你的 API 等級調整)
            time.sleep(0.5) 

        print(f"\n✅ 處理完成。成功提取: {len(results)}/{total}")

        # 1. 保存結果 JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
            
        print(f"💾 已保存至: {output_file}")

        # 2. --- 新增日誌記錄 (追加模式 'a') ---
        # 這裡動態顯示輸入文件名，方便區分 S1 和 S2
        log_path = os.path.join("data", "step_status.log")
        with open(log_path, "a", encoding="utf-8") as log_f:
            log_f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 5 ({input_file}): AI 提取完成，成功 {len(results)}/{total} 條。\n")
        # -----------------------------------

    except Exception as e:
        # 錯誤時也記錄到 Log
        with open("data/step_status.log", "a", encoding="utf-8") as log_f:
            log_f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 5 ERROR ({input_file}): {str(e)}\n")
        print(f"\n❌ 腳本執行錯誤: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()