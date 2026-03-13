import requests
import json
import os
import time
import math

# --- 1. 配置與路徑初始化 ---
APPID = os.environ.get('WX_APPID', '').strip()
APPSECRET = os.environ.get('WX_APPSECRET', '').strip()
ENV_ID = os.environ.get('WX_ENV_ID', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
COLLECTION_NAME = "mall_offers"

# 數據存儲路徑
DATA_URL = "https://raw.githubusercontent.com/mson66/ycnx-monitor/main/data/hkmallparkings.json"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(os.path.dirname(BASE_DIR), "data")
JSON_FILE_PATH = os.path.join(DATA_FOLDER, "hkmallparkings.json")

# --- 2. 坐標轉換工具 (採用用戶提供的驗證算法) ---
def wgs84_to_gcj02(lng, lat):
    """
    將 WGS84 坐標轉換為 GCJ02 (火星坐標系)
    精確到 0.000000 以確保導航無偏差
    """
    if not lng or not lat:
        return [lng, lat]
    
    PI = 3.1415926535897932384626
    a = 6378245.0
    ee = 0.00669342162296594323

    def transform_lat(x, y):
        ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(y * PI) + 40.0 * math.sin(y / 3.0 * PI)) * 2.0 / 3.0
        ret += (160.0 * math.sin(y / 12.0 * PI) + 320 * math.sin(y * PI / 30.0)) * 2.0 / 3.0
        return ret

    def transform_lng(x, y):
        ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(x * PI) + 40.0 * math.sin(x / 3.0 * PI)) * 2.0 / 3.0
        ret += (150.0 * math.sin(x / 12.0 * PI) + 300.0 * math.sin(x / 30.0 * PI)) * 2.0 / 3.0
        return ret

    rad_lat = lat / 180.0 * PI
    magic = math.sin(rad_lat)
    magic = 1 - ee * magic * magic
    sqrt_m = math.sqrt(magic)
    
    d_lat = transform_lat(lng - 105.0, lat - 35.0)
    d_lng = transform_lng(lng - 105.0, lat - 35.0)
    
    dl = (d_lat * 180.0) / ((a * (1 - ee)) / (magic * sqrt_m) * PI)
    dg = (d_lng * 180.0) / (a / sqrt_m * math.cos(rad_lat) * PI)
    
    return [round(lng + dg, 6), round(lat + dl, 6)]

# --- 3. 基礎功能函數 ---
def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    try:
        res = requests.get(url, timeout=20).json()
        return res.get('access_token')
    except Exception as e:
        print(f"❌ 獲取 Token 異常: {e}")
        return None

def call_gemini_api(prompt):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.2,
            "maxOutputTokens": 8192
        }
    }
    try:
        res = requests.post(url, json=payload, timeout=120).json()
        text_content = res['candidates'][0]['content']['parts'][0]['text'].strip()
        if text_content.startswith("```json"):
            text_content = text_content.split("```json")[1].split("```")[0].strip()
        return json.loads(text_content)
    except Exception as e:
        print(f"⚠️ AI 數據解析失敗: {e}")
        return []

def repair_truncated_json(json_str):
    """強力修復截斷的 JSON 字符串"""
    json_str = json_str.strip()
    if json_str.startswith('```json'):
        json_str = re.sub(r'^```json\s*|\s*```$', '', json_str, flags=re.MULTILINE)
    
    # 嘗試直接解析
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        print("🛠️ 檢測到 JSON 截斷，正在嘗試結構化修復...")
        # 移除結尾可能殘留的半個字串或鍵名
        json_str = re.sub(r',?\s*"[^"]*"?\s*:\s*[^,}]*$', '', json_str)
        # 補齊大括號和中括號
        open_brackets = json_str.count('[') - json_str.count(']')
        open_braces = json_str.count('{') - json_str.count('}')
        
        fixed_str = json_str + ('"}' * open_braces) + (']' * open_brackets)
        try:
            return json.loads(fixed_str)
        except:
            return []

def fetch_malls_incremental():
    print("\n" + "="*60)
    print("🚀 啟動 Gemini 2.5 Flash 增量採集 + 精確坐標修正")
    print("="*60)
    
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    current_malls = []
    if os.path.exists(JSON_FILE_PATH):
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            current_malls = json.load(f)
        print(f"📁 已讀取本地數據: {len(current_malls)} 筆")

    # 分批抓取以防截斷
    mall_targets = [
        "信和集團 (奧海城、屯門市廣場、中港城、荃新天地、黃金海岸、尖沙咀中心、藍灣廣場、朗壹廣場)",
        "新鴻基地產 (apm、新城市廣場、MOKO、V city、YOHO MALL、IFC、V Walk、新達廣場)",
        "新世界/恆基 (K11 MUSEA、K11 Art Mall、D·PARK、MCP 新都城、MOSTown 新港城)",
        "恆隆/太古系 (太古城中心、Citygate 東薈城、Fashion Walk、康怡廣場、雅蘭中心)",
        "領展 Link (樂富廣場、赤柱廣場、T Town、慈雲山中心、黃大仙中心、及各區旗艦)",
        "大型獨立地標 (海港城、時代廣場、Elements 圓方、朗豪坊、新翠商場、iSQUARE)"
    ]
    
    mall_dict = {m['id']: m for m in current_malls}
    total_added = []
    total_updated = []

    for idx, target in enumerate(mall_targets):
        print(f"\n📦 [批次 {idx+1}/{len(mall_targets)}] 採集重點: {target}")
        existing_summary = [f"{m.get('name')}({m.get('id')})" for m in list(mall_dict.values())]
        
        prompt = f"""
    你是一名香港商業地產與跨境交通專家。請執行深度搜索，整理 2026 年最新香港商場泊車優惠。
        
        【參考現有清單 (避免重複)】: {", ".join(existing_summary[:8])} ...
        【當前採集重點】: {target}

    【搜索任務要求】:
    1. 羅列所有香港帶停車場商場，**重點比對上述清單，優先補充名單中缺失的商場**。
    2. 提取必須涵蓋全港至少 50 個商場（包含新增與更新），包括不限於：
       - 信和集團: 奧海城、屯門市廣場、中港城、荃新天地。
       - 新鴻基: V city、YOHO MALL、apm、MOKO、新城市廣場、IFC、V Walk。
       - 恆隆: Fashion Walk、家樂坊、荷李活商業中心。
       - 其他: 圓方 Elements、海港城、時代廣場、東薈城、領展主要商場、太古城中心。
    3. 深挖泊車政策和推廣活動，提取泊車有收費政策，包括免費停車，泊車禮遇，積分泊車優惠等。
    4. 重點提取「粵車南下」專屬禮遇（如FT車牌額外免停、專屬禮包）。
    5. **查缺補漏邏輯**：如果上述已存在數據中的商場政策已過期，請提供更新版；否則請專注於尋找新商場。

    【輸出技術規範 - 極重要】
    1. 格式：必須輸出「純 JSON 數組」，嚴禁任何前導說明、後置總結或 Markdown 代碼塊（不要 ```json）。
    2. 轉義處理：所有文本字段（尤其 description）中的雙引號 (") 必須轉義為 \\\"，禁止使用換行符，請使用 \\n 代替。
    3. 嚴禁斷尾：確保每個 JSON 對象閉合。如果資訊過長，請優先精確化內容而非堆砌字數。
    4. 禁止多餘逗號：數組最後一個對象後嚴禁出現逗號。

    【字段定義】
    - id: 唯一標識, 如海港城為harbourcity, 請確保同一個商場在不同次生成時使用相同的 id。
    - name: 商場中文全稱 （智能校對去重）
    - lat/lng: WGS-84 座標系下的精確經緯度
    - isSouthbound: 若有針對「粵車南下」特有優惠禮遇則為 true，否則 false
    - parking: 這個描述非常重要。應描述無條件獲得免費泊車優惠，以及粵車南下專屬額外免費停車優惠。要求量化小時數，如果都沒有則描述最低消費的免費泊車時數（例1：免費停車1小時，例2:粵車南下額外2小時，例3:消費滿$100，免費停車1小時）
    - spending: 描述最低消費免費泊車門檻（例：消費滿$200，或積分兌換，優惠停车1小时）
    - presents: 消費獎賞與禮品回贈，需要描述具體內容等
    - description: 優先抄官網政策对核心泊車條款（長文本， 1. 2. 3. ... ）
    - link: 官方或可靠活動網址，具體精準指向泊車優惠頁面
    - update_time: （格式：yyyymmdd）根據官方條款中的優惠期起點日期
    - end_time: （格式：yyyymmdd）根據官方條款中的優惠期終止日期，沒有定義則留空不填寫。
        """

        new_batch = call_gemini_api(prompt)
        
        for mall in new_batch:
            # 執行坐標轉換與精確度優化
            try:
                raw_lng = float(mall.get('lng', 0))
                raw_lat = float(mall.get('lat', 0))
                if raw_lng != 0:
                    fixed_coords = wgs84_to_gcj02(raw_lng, raw_lat)
                    mall['lng'] = fixed_coords[0]
                    mall['lat'] = fixed_coords[1]
            except: pass

            m_id, m_name = mall.get('id'), mall.get('name', '未知')
            if m_id in mall_dict:
                mall_dict[m_id] = mall
                total_updated.append(m_name)
            else:
                mall_dict[m_id] = mall
                total_added.append(m_name)
        
        time.sleep(5)

    final_list = list(mall_dict.values())
    print("\n" + "-"*60)
    print(f"📊 採集總結: 新增 {len(total_added)} 個, 更新 {len(total_updated)} 個")
    print(f"📚 總計: {len(final_list)} 筆數據")
    print("-"*60)

    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)
    
    return final_list

def sync_batch_to_wechat(malls, batch_size=5, sleep_time=3):
    token = get_access_token()
    if not token: return
    print(f"\n🌐 同步至微信雲數據庫...")
    
    QUERY_API = f"[https://api.weixin.qq.com/tcb/databasequery?access_token=](https://api.weixin.qq.com/tcb/databasequery?access_token=){token}"
    ADD_API = f"[https://api.weixin.qq.com/tcb/databaseadd?access_token=](https://api.weixin.qq.com/tcb/databaseadd?access_token=){token}"
    UPDATE_API = f"[https://api.weixin.qq.com/tcb/databaseupdate?access_token=](https://api.weixin.qq.com/tcb/databaseupdate?access_token=){token}"

    for i in range(0, len(malls), batch_size):
        batch = malls[i : i + batch_size]
        for item in batch:
            try:
                check_q = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).get()"
                res = requests.post(QUERY_API, json={"env": ENV_ID, "query": check_q}).json()
                exists = len(res.get('data', [])) > 0
                data_str = json.dumps(item, ensure_ascii=False).replace('\\', '\\\\')
                
                query = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).update({{ data: {data_str} }})" if exists \
                        else f"db.collection('{COLLECTION_NAME}').add({{ data: {data_str} }})"
                
                requests.post(UPDATE_API if exists else ADD_API, json={"env": ENV_ID, "query": query})
            except: pass
        time.sleep(sleep_time)

if __name__ == "__main__":
    final_data = fetch_malls_incremental()
    if final_data:
        sync_batch_to_wechat(final_data)