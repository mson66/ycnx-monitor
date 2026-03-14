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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FILE_PATH = os.path.join(BASE_DIR, "..", "data", "hkmallparkings.json")

# --- 2. 坐標轉換工具 (WGS-84 -> GCJ-02) ---
def wgs84_to_gcj02(lng, lat):
    """將 WGS-84 坐標轉換為 GCJ-02 坐標（火星坐標系）"""
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

def format_description(text):
    """格式化 description 字段，確保每條款後有換行符"""
    if not text:
        return text
    
    # 如果已經包含換行符，保持原樣
    if '\n' in text:
        return text
    
    # 在數字編號後添加換行符 (如 1. 2. 3.)
    import re
    formatted = re.sub(r'(\d+\.)\s*', r'\1\n', text)
    return formatted.strip()

def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    try:
        res = requests.get(url, timeout=20).json()
        return res.get('access_token')
    except Exception as e:
        print(f"❌ 獲取 Token 異常: {e}")
        return None

def fetch_malls_deep_search():
    print("--- 🧠 啟動 Gemini 3 Flash 深度採集 ---")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key={GEMINI_API_KEY}"
    
    prompt = """
    你是一名香港商業地產與跨境交通專家。請執行深度搜索，整理 2026 年最新香港商場泊車優惠。
    
    【搜索清單要求】
    1. 羅列所有香港帶停車場商場。
    2. 提取必須涵蓋全港至少 50 個商場，包括不限於以下商場：
       - 信和集團 (Sino Group): 奧海城、屯門市廣場、中港城、荃新天地。
       - 新鴻基 (SHKP): V city、YOHO MALL、apm、MOKO、新城市廣場、IFC、V Walk。
       - 恆隆: Fashion Walk、家樂坊、荷李活商業中心。
       - 其他: 圓方 Elements、海港城、時代廣場、東薈城、領展主要商場、太古城中心。
    3. 深挖泊車政策和推廣活動等內容，提取所有泊車有收費政策，包括免費停車，泊車禮遇，積分泊車優惠等。
    4. 重點提取「粵車南下」專屬禮遇（如FT車牌額外免停、專屬禮包）。
    
    【輸出格式】
    1. 重點提取所有香港商場的泊車優惠政策，消費優惠停車政策，以及「粵車南下」專屬優惠禮遇。
    2. 輸出格式必須是純 JSON 數組，嚴禁包含任何解釋性文字。
    3. 字段定義：
    - id: 唯一標識, 如海港城為harbourcity, 請確保同一個商場在不同次生成時使用相同的 id。
    - name: 商場中文全稱 （智能校對去重）
    - lat/lng: WGS-84 坐標系下的精確經緯度（原始坐標，系統會自動轉換）
    - isSouthbound: 若有針對「粵車南下」特有優惠禮遇則為 true，否則 false
    - parking: 這個描述非常重要。應描述無條件獲得免費泊車優惠，以及粵車南下專屬額外免費停車優惠。要求量化小時數，如果都沒有則描述最低消費的免費泊車時數（例1：免費停車1小時，例2:粵車南下額外2小時，例3:消費滿$100，免費停車1小時）
    - spending: 描述最低消費免費泊車門檻（例：消費滿$200，或積分兌換，優惠停车1小时）
    - presents: 消費獎賞與禮品回贈，需要描述具體內容等
    - description: 優先抄官網政策條款與細則，每條款用數字編號（如 1. 2. 3. ...）
    - link: 官方或可靠活動網址，具體精準指向泊車優惠頁面
    - update_time: （格式：yyyymmdd）根據官方條款中的優惠期起點日期
    - end_time: （格式：yyyymmdd）根據官方條款中的優惠期終止日期，沒有定義則留空不填寫。
    """

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.2,
            "maxOutputTokens": 65000
        }
    }
    
    try:
        res = requests.post(url, json=payload, timeout=90).json()
        
        if 'error' in res:
            print(f"❌ API 錯誤響應: {json.dumps(res.get('error'), ensure_ascii=False, indent=2)}")
            return []
        
        if 'candidates' not in res:
            print(f"❌ API 響應缺少 candidates 字段，完整響應: {json.dumps(res, ensure_ascii=False, indent=2)}")
            return []
        
        content = res['candidates'][0]['content']['parts'][0]['text'].strip()
        
        if not content.endswith(']'):
            last_bracket = content.rfind('}')
            if last_bracket != -1:
                content = content[:last_bracket+1] + ']'
        
        malls = json.loads(content)
        
        # 處理每個商場數據：坐標轉換 + 格式化 description
        for mall in malls:
            # 坐標轉換 (WGS-84 -> GCJ-02)
            try:
                lng = float(mall.get('lng', 0))
                lat = float(mall.get('lat', 0))
                if lng and lat:
                    gcj_lng, gcj_lat = wgs84_to_gcj02(lng, lat)
                    mall['lng'] = gcj_lng
                    mall['lat'] = gcj_lat
            except (ValueError, TypeError):
                pass
            
            # 格式化 description
            if 'description' in mall:
                mall['description'] = format_description(mall['description'])
        
        data_dir = os.path.dirname(JSON_FILE_PATH)
        os.makedirs(data_dir, exist_ok=True)
        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(malls, f, ensure_ascii=False, indent=2)
        print(f"💾 數據已本地備份至: {os.path.normpath(JSON_FILE_PATH)}")
        
        return malls
    except Exception as e:
        print(f"❌ AI 採集或 JSON 解析失敗: {e}")
        return []

def sync_batch_to_wechat(malls, batch_size=5, sleep_time=5):
    token = get_access_token()
    if not token: 
        print("❌ 無法獲取 Access Token，終止同步")
        return

    QUERY_API = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"
    ADD_API = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    UPDATE_API = f"https://api.weixin.qq.com/tcb/databaseupdate?access_token={token}"

    total = len(malls)
    print(f"🚀 總計 {total} 個商場，每批 {batch_size} 個，批次間隔 {sleep_time} 秒...")

    for i in range(0, total, batch_size):
        batch = malls[i : i + batch_size]
        print(f"\n📦 [批次 {i//batch_size + 1}] 同步中...")

        for item in batch:
            try:
                check_q = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).get()"
                res = requests.post(QUERY_API, json={"env": ENV_ID, "query": check_q}).json()
                
                if res.get('errcode') != 0:
                    print(f"   ❌ 查詢失敗: {res.get('errcode')} - {res.get('errmsg')}")
                    continue

                exists = len(res.get('data', [])) > 0
                data_str = json.dumps(item, ensure_ascii=False).replace('\\', '\\\\')
                
                if exists:
                    q = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).update({{ data: {data_str} }})"
                    target_api = UPDATE_API
                else:
                    q = f"db.collection('{COLLECTION_NAME}').add({{ data: {data_str} }})"
                    target_api = ADD_API

                resp = requests.post(target_api, json={"env": ENV_ID, "query": q}).json()

                if resp.get('errcode') == 0:
                    print(f"   ✅ {'更新' if exists else '新增'}: {item['name']}")
                else:
                    print(f"   ⚠️ 失敗 {item['name']}: {resp.get('errcode')} - {resp.get('errmsg')}")

            except Exception as e:
                print(f"   ❌ 處理 {item.get('name')} 異常: {e}")

        if i + batch_size < total:
            print(f"⏳ 等待 {sleep_time} 秒後執行下一批...")
            time.sleep(sleep_time)

if __name__ == "__main__":
    malls_data = fetch_malls_deep_search()
    if malls_data:
        sync_batch_to_wechat(malls_data, batch_size=5, sleep_time=5)
    else:
        print("⚠️ 無數據可同步。")
