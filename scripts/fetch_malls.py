import requests
import json
import os
import time

# --- 1. 配置與路徑初始化 ---
APPID = os.environ.get('WX_APPID', '').strip()
APPSECRET = os.environ.get('WX_APPSECRET', '').strip()
ENV_ID = os.environ.get('WX_ENV_ID', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
COLLECTION_NAME = "mall_offers"

# 當前數據的遠端 URL
DATA_URL = "https://raw.githubusercontent.com/mson66/ycnx-monitor/main/data/hkmallparkings.json"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_FILE_PATH = os.path.join(BASE_DIR, "..", "data", "hkmallparkings.json")

def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    try:
        res = requests.get(url, timeout=20).json()
        return res.get('access_token')
    except Exception as e:
        print(f"❌ 獲取 Token 異常: {e}")
        return None

def call_gemini_api(prompt):
    """調用 Gemini 2.5 Flash 接口"""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.2,
            "maxOutputTokens": 8192
        }
    }
    res = requests.post(url, json=payload, timeout=120).json()
    try:
        return json.loads(res['candidates'][0]['content']['parts'][0]['text'].strip())
    except (json.JSONDecodeError, KeyError) as e:
        print(f"⚠️ AI 返回數據解析失敗，嘗試修復截斷... {e}")
        return []

def fetch_malls_incremental():
    print("\n" + "="*50)
    print("🚀 啟動 Gemini 2.5 Flash 增量分批採集 (tx4 v6.8.4)")
    print("="*50)
    
    # 1. 讀取現有數據
    current_malls = []
    if os.path.exists(JSON_FILE_PATH):
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            current_malls = json.load(f)
        print(f"📁 已加載本地數據: {len(current_malls)} 個商場")
    else:
        try:
            resp = requests.get(DATA_URL, timeout=15)
            if resp.status_code == 200:
                current_malls = resp.json()
                print(f"🌐 已從 GitHub 加載數據: {len(current_malls)} 個商場")
        except:
            print("⚠️ 未發現現有數據，啟動全量模式。")

    # 2. 分批任務設定 (避免一次 50 個導致截斷)
    mall_targets = [
        "信和/新鴻基系商場", 
        "恆隆/領展/太古系商場", 
        "圓方/海港城/時代廣場及其他大型項目"
    ]
    
    mall_dict = {m['id']: m for m in current_malls}
    total_added = []
    total_updated = []

    for idx, target in enumerate(mall_targets):
        print(f"\n📦 [批次 {idx+1}/{len(mall_targets)}] 正在採集: {target}...")
        
        existing_summary = [f"{m.get('name')}({m.get('id')})" for m in list(mall_dict.values())]
        
        prompt = f"""
    你是一名香港商業地產與跨境交通專家。請執行深度搜索，整理 2026 年最新香港商場泊車優惠。
        
        【參考現有清單 (避免重複)】: {", ".join(existing_summary[:15])} ...
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

    【輸出格式】:
    1. 輸出格式必須是純 JSON 數組，嚴禁包含任何解釋性文字。
    2. 字段定義：
    - id: 唯一標識, 如海港城為harbourcity, 請確保同一個商場在不同次生成時使用相同的 id。
    - name: 商場中文全稱 （智能校對去重）
    - lat/lng: GCJ-02 坐標系下的精確經緯度
    - isSouthbound: 若有針對「粵車南下」特有優惠禮遇則為 true，否則 false
    - parking: 這個描述非常重要。應描述無條件獲得免費泊車優惠，以及粵車南下專屬額外免費停車優惠。要求量化小時數，如果都沒有則描述最低消費的免費泊車時數（例1：免費停車1小時，例2:粵車南下額外2小時，例3:消費滿$100，免費停車1小時）
    - spending: 描述最低消費免費泊車門檻（例：消費滿$200，或積分兌換，優惠停车1小时）
    - presents: 消費獎賞與禮品回贈，需要描述具體內容等
    - description: 優先抄官網政策條款與細則，一條不漏（長文本， 1. 2. 3. ... ）
    - link: 官方或可靠活動網址，具體精準指向泊車優惠頁面
    - update_time: （格式：yyyymmdd）根據官方條款中的優惠期起點日期
    - end_time: （格式：yyyymmdd）根據官方條款中的優惠期終止日期，沒有定義則留空不填寫。
        """

        new_batch = call_gemini_api(prompt)
        
        # 合併與查缺補漏
        for mall in new_batch:
            m_id, m_name = mall.get('id'), mall.get('name')
            if m_id in mall_dict:
                mall_dict[m_id] = mall
                total_updated.append(m_name)
            else:
                mall_dict[m_id] = mall
                total_added.append(m_name)
        
        print(f"✅ 批次 {idx+1} 完成，獲取到 {len(new_batch)} 條數據。")
        time.sleep(5) # 避免 API 頻率限制

    # 3. 成果匯報與保存
    final_list = list(mall_dict.values())
    print("\n" + "-"*30)
    print(f"📊 增量採集總結 (v6.8.4):")
    print(f"➕ 新增 ({len(total_added)}): {', '.join(total_added) if total_added else '無'}")
    print(f"🔄 更新 ({len(total_updated)}): {', '.join(total_updated) if total_updated else '無'}")
    print(f"📚 數據庫總計: {len(final_list)} 筆商場數據")
    print("-"*30)

    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)
    print(f"💾 完整數據已保存至: {JSON_FILE_PATH}")
    
    return final_list

def sync_batch_to_wechat(malls, batch_size=5, sleep_time=3):
    token = get_access_token()
    if not token: return
    print(f"\n🌐 正在同步至微信雲數據庫 (批次大小: {batch_size})...")
    
    QUERY_API = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"
    ADD_API = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    UPDATE_API = f"https://api.weixin.qq.com/tcb/databaseupdate?access_token={token}"

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
            except Exception as e:
                print(f"   ⚠️ 同步 {item.get('name')} 失敗: {e}")
        
        print(f"   已完成 {min(i + batch_size, len(malls))}/{len(malls)}")
        time.sleep(sleep_time)

if __name__ == "__main__":
    final_data = fetch_malls_incremental()
    if final_data:
        sync_batch_to_wechat(final_data)