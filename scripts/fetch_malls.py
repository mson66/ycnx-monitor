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
    if not text:
        return text
    
    if '\n' in text:
        return text
    
    import re
    formatted = re.sub(r'(?<!^)(\d+\.)', r'\n\1', text)
    return formatted.strip()

def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    try:
        res = requests.get(url, timeout=20).json()
        return res.get('access_token')
    except Exception as e:
        print(f"❌ 獲取 Token 異常: {e}")
        return None

def load_existing_malls():
    """從騰訊雲數據庫讀取現有商場數據"""
    token = get_access_token()
    if not token:
        print("⚠️ 無法獲取 Token，嘗試從本地文件讀取")
        return load_from_local_file()
    
    QUERY_API = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"
    
    try:
        query = f"db.collection('{COLLECTION_NAME}').limit(1000).get()"
        res = requests.post(QUERY_API, json={"env": ENV_ID, "query": query}, timeout=30).json()
        
        if res.get('errcode') != 0:
            print(f"⚠️ 雲數據庫查詢失敗: {res.get('errmsg')}，從本地文件讀取")
            return load_from_local_file()
        
        data = res.get('data', [])
        if not data:
            print("⚠️ 雲數據庫為空，從本地文件讀取")
            return load_from_local_file()
        
        existing_malls = []
        for item in data:
            if isinstance(item, str):
                parsed = json.loads(item)
            else:
                parsed = item
            
            existing_malls.append({
                'id': parsed.get('id'),
                'name': parsed.get('name')
            })
        
        print(f"📥 從雲數據庫加載 {len(existing_malls)} 個現有商場")
        return existing_malls
        
    except Exception as e:
        print(f"⚠️ 讀取雲數據庫異常: {e}，從本地文件讀取")
        return load_from_local_file()

def load_from_local_file():
    """從本地文件讀取現有商場數據"""
    if os.path.exists(JSON_FILE_PATH):
        try:
            with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            existing_malls = [{'id': m.get('id'), 'name': m.get('name')} for m in data if m.get('id')]
            print(f"📥 從本地文件加載 {len(existing_malls)} 個現有商場")
            return existing_malls
        except Exception as e:
            print(f"⚠️ 讀取本地文件異常: {e}")
    return []

def normalize_name(name):
    """標準化商場名稱：用於模糊匹配"""
    if not name:
        return ""
    import re
    n = name.lower().strip()
    n = re.sub(r'[^\w\u4e00-\u9fff]', '', n)
    return n

def fetch_malls_deep_search():
    print("--- 🧠 啟動 Gemini 3 Flash 深度採集 ---")
    
    existing_malls = load_existing_malls()
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key={GEMINI_API_KEY}"
    
    existing_list_text = json.dumps(existing_malls, ensure_ascii=False, indent=2) if existing_malls else "[]"
    
    prompt = f"""
你是一名香港商業地產與跨境交通專家。請執行深度搜索，整理 2026 年最新香港商場泊車優惠。

【現有商場列表】(請嚴格保持這些商場的 id 不變):
{existing_list_text}

【搜索任務要求】
1. 羅列所有香港帶停車場商場。
2. 提取必須涵蓋全港至少 50 個商場，包括不限於以下商場：
   - 信和集團 (Sino Group): 屯門市廣場、奧海城、荃新天地、中港城、黃金海岸商場、尖沙咀中心、藍灣廣場及朗壹廣場。
   - 新鴻基 (SHKP): V city、YOHO MALL、apm、MOKO、新城市廣場、IFC、V Walk、北角匯、大補超級城。
   - 恆隆: Fashion Walk，家樂坊、荷李活商業中心。
   - 其他: 圓方 Elements、海港城、時代廣場、東薈城、領展主要商場、太古城中心。
3. 深挖泊車政策和推廣活動等內容，提取所有泊車有收費政策，包括免費停車，泊車禮遇，積分泊車優惠等。
4. 重點提取「粵車南下」專屬禮遇（如FT車牌額外免停、專屬禮包）。

【重要規則 - 去重邏輯】
- 如果搜索到的商場在【現有商場列表】中存在，必須使用列表中相同的 id
- 判斷是否同一商場的標準：名稱相同、簡稱相同、或明確是同一商場的不同分期/分區
- 例如：「奧海城一期」「奧海城二期」都應該使用 id "olympiancity"
- 如果是列表中沒有的全新商場，請生成新的唯一 id（使用英文簡稱，如 harbourcity）

【輸出格式】
1. 輸出格式必須是純 JSON 數組，嚴禁包含任何解釋性文字。
2. 字段定義：
- id: 必須與現有列表中的 id 一致（如果判斷為同一商場），否則生成新 id
- name: 商場中文全稱
- lat/lng: WGS-84 坐標系下的精確經緯度
- isSouthbound: 若有針對「粵車南下」特有優惠禮遇則為 true，否則 false
- parking: 粵車南下專屬額外免費停車優惠。
- spending: 描述最低消費免費泊車門檻（例：消費滿$200，或積分兌換，優惠停车1小时）
- presents: 消費獎賞與禮品回贈，需要描述具體內容等
- description: 優先抄官網政策條款與細則核心部分，每條款用數字編號（如 1. 2. 3. ...）
- link: 官方或可靠活動網址，具體精準指向泊車優惠頁面
- update_time: （格式：yyyymmdd）優惠期起點日期
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
        
        # 建立現有商場索引（用於後續同步時判斷更新/新增）
        existing_id_index = {m['id']: True for m in existing_malls if m.get('id')}
        
        existing_name_index = {}
        existing_normalized_index = {}
        for m in existing_malls:
            if m.get('name') and m.get('id'):
                existing_name_index[m['name']] = m['id']
                existing_normalized_index[normalize_name(m['name'])] = m['id']
        
        # 標記每個商場是更新還是新增
        for mall in malls:
            mall_id = mall.get('id')
            mall_name = mall.get('name')
            mall_normalized = normalize_name(mall.get('name', ''))
            
            if mall_id in existing_id_index:
                mall['_action'] = 'update'
            elif mall.get('name') in existing_name_index:
                mall['id'] = existing_name_index[mall.get('name')]
                mall['_action'] = 'update'
            elif mall_normalized in existing_normalized_index:
                mall['id'] = existing_normalized_index[mall_normalized]
                mall['_action'] = 'update'
            else:
                mall['_action'] = 'add'
        
        # 處理每個商場數據
        today = time.strftime("%Y%m%d")
        for mall in malls:
            # 坐標轉換
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
            
            # 處理 update_time
            update_time = mall.get('update_time', '')
            if not update_time or len(str(update_time)) != 8:
                mall['update_time'] = today
        
        # 統計
        update_count = sum(1 for m in malls if m.get('_action') == 'update')
        add_count = sum(1 for m in malls if m.get('_action') == 'add')
        print(f"📊 AI 返回 {len(malls)} 個商場：{update_count} 個更新，{add_count} 個新增")
        
        # 保存本地
        data_dir = os.path.dirname(JSON_FILE_PATH)
        os.makedirs(data_dir, exist_ok=True)
        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(malls, f, ensure_ascii=False, indent=2)
        print(f"💾 數據已本地備份")
        
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
    print(f"🚀 開始同步 {total} 個商場到雲數據庫...")

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
                
                # 移除臨時字段
                item_copy = {k: v for k, v in item.items() if not k.startswith('_')}
                data_str = json.dumps(item_copy, ensure_ascii=False).replace('\\', '\\\\')
                
                if exists:
                    q = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).update({{ data: {data_str} }})"
                    target_api = UPDATE_API
                else:
                    q = f"db.collection('{COLLECTION_NAME}').add({{ data: {data_str} }})"
                    target_api = ADD_API

                resp = requests.post(target_api, json={"env": ENV_ID, "query": q}).json()

                if resp.get('errcode') == 0:
                    action = "更新" if exists else "新增"
                    print(f"   ✅ {action}: {item['name']}")
                else:
                    print(f"   ⚠️ 失敗 {item['name']}: {resp.get('errcode')} - {resp.get('errmsg')}")

            except Exception as e:
                print(f"   ❌ 處理 {item.get('name')} 異常: {e}")

        if i + batch_size < total:
            print(f"⏳ 等待 {sleep_time} 秒後執行下一批...")
            time.sleep(sleep_time)

    print("\n🎉 同步完成！")

if __name__ == "__main__":
    malls_data = fetch_malls_deep_search()
    if malls_data:
        sync_batch_to_wechat(malls_data, batch_size=5, sleep_time=5)
    else:
        print("⚠️ 無數據可同步。")
