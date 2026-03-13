import requests
import json
import os
import time
import math
import re

# --- 1. 配置與路徑初始化 ---
APPID = os.environ.get('WX_APPID', '').strip()
APPSECRET = os.environ.get('WX_APPSECRET', '').strip()
ENV_ID = os.environ.get('WX_ENV_ID', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
COLLECTION_NAME = "mall_offers"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 修正路徑邏輯，確保與您的 Project 結構一致
DATA_FOLDER = os.path.join(os.path.dirname(BASE_DIR), "data")
JSON_FILE_PATH = os.path.join(DATA_FOLDER, "hkmallparkings.json")

# --- 2. 坐標轉換工具 (驗證過的 GCJ-02 算法) ---
def wgs84_to_gcj02(lng, lat):
    if not lng or not lat: return [lng, lat]
    PI = 3.1415926535897932384626
    a, ee = 6378245.0, 0.00669342162296594323
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

# --- 3. 穩定 JSON 解析 ---
def safe_json_loads(text):
    text = text.strip()
    text = re.sub(r'^```json\s*|\s*```$', '', text, flags=re.MULTILINE)
    try:
        return json.loads(text)
    except:
        return []

def call_gemini_api(prompt):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.2,
            "maxOutputTokens": 4096 
        }
    }
    try:
        res = requests.post(url, json=payload, timeout=60).json()
        raw_text = res['candidates'][0]['content']['parts'][0]['text']
        return safe_json_loads(raw_text)
    except Exception as e:
        print(f"⚠️ AI 請求失敗: {e}")
        return []

def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    try:
        res = requests.get(url).json()
        return res.get('access_token')
    except:
        return None

def sync_to_wechat(item, token):
    if not token: return
    ADD_API = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    UPDATE_API = f"https://api.weixin.qq.com/tcb/databaseupdate?access_token={token}"
    QUERY_API = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"
    
    try:
        check_q = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).get()"
        res = requests.post(QUERY_API, json={"env": ENV_ID, "query": check_q}).json()
        exists = len(res.get('data', [])) > 0
        data_str = json.dumps(item, ensure_ascii=False).replace('\\', '\\\\')
        
        query = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).update({{ data: {data_str} }})" if exists \
                else f"db.collection('{COLLECTION_NAME}').add({{ data: {data_str} }})"
        
        requests.post(UPDATE_API if exists else ADD_API, json={"env": ENV_ID, "query": query})
    except: pass

def fetch_malls_infinite_loop():
    print("\n" + "="*60)
    print("🚀 啟動 流式增量採集模式")
    print("📍 採集規則：每批 5 個 / 間隔 5 秒")
    print("="*60)

    if not os.path.exists(DATA_FOLDER): os.makedirs(DATA_FOLDER)
    
    mall_dict = {}
    if os.path.exists(JSON_FILE_PATH):
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            mall_dict = {m['id']: m for m in data}
    
    token = get_access_token()
    # 定義搜索維度
    groups = ["信和集團", "新鴻基地產", "恆隆/太古", "領展Link", "新世界/恆基", "九龍倉/其他大型商場"]
    
    batch_count = 0
    empty_streak = 0 

    while True:
        batch_count += 1
        current_focus = groups[batch_count % len(groups)]
        # 為了 Prompt 穩定，只給 AI 看最近 20 個已有的 ID
        exclude_ids = list(mall_dict.keys())[-20:]
        
        print(f"\n🔄 [批次 {batch_count}] 正在搜索 {current_focus}...")
        
        prompt = f"""
    你是一名香港商業地產與跨境交通專家。請執行深度搜索，整理 2026 年最新香港商場泊車優惠。
        【排外清單】: {", ".join(exclude_ids)}
        針對「{current_focus}」，找出 5 個清單中沒有的商場。
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
        
        if not new_batch:
            empty_streak += 1
            print(f"⚠️ 本批次未獲取數據。")
            if empty_streak >= 3: break
        else:
            empty_streak = 0
            for mall in new_batch:
                m_id = mall.get('id')
                if m_id and m_id not in mall_dict:
                    # 座標轉換
                    lng, lat = wgs84_to_gcj02(float(mall.get('lng', 0)), float(mall.get('lat', 0)))
                    mall['lng'], mall['lat'] = lng, lat
                    
                    mall_dict[m_id] = mall
                    sync_to_wechat(mall, token)
                    print(f"   ✅ 新增: {mall.get('name')} (座標已修復)")

            # 保存本地備份
            with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(list(mall_dict.values()), f, ensure_ascii=False, indent=2)

        # --- 核心改動：每批次間隔 ---
        if batch_count >= 15: # 安全退出條件，防止 GitHub Actions 超時
            print("\n🚀 已達單次任務批次上限，安全停止。")
            break
            
        print(f"⏳ 為了穩定性，等待 5 秒後執行下一批...")
        time.sleep(5) 

if __name__ == "__main__":
    fetch_malls_infinite_loop()