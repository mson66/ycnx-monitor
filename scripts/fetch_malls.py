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

def fetch_malls_deep_search():
    print("\n" + "="*50)
    print("🚀 啟動 Gemini 2.5 Flash 深度採集與增量更新")
    print("="*50)
    
    # 1. 讀取並分析現有數據
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
            print("⚠️ 未發現現有數據，將啟動全量抓取。")

    existing_summary = [f"{m.get('name')}({m.get('id')})" for m in current_malls]
    print(f"🔍 現有清單摘要: {', '.join(existing_summary[:5])} ... 等 {len(existing_summary)} 個")

    # 2. 構造 Prompt (保留您的嚴格要求)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    prompt = f"""
    你是一名香港商業地產與跨境交通專家。請執行深度搜索，整理 2026 年最新香港商場泊車優惠。
    
    【當前已存在數據 (請勿重複生成完全相同的數據)】:
    {", ".join(existing_summary)}

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

    print("\n🧠 正在與 Gemini 2.5 Flash 通訊，請稍候...")
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
        content = res['candidates'][0]['content']['parts'][0]['text'].strip()
        new_malls_list = json.loads(content)
        
        print(f"✨ AI 返回了 {len(new_malls_list)} 個商場數據")

        # 3. 查缺補漏合併邏輯
        mall_dict = {m['id']: m for m in current_malls}
        add_names = []
        upd_names = []
        
        for mall in new_malls_list:
            m_id, m_name = mall.get('id'), mall.get('name')
            if m_id in mall_dict:
                mall_dict[m_id] = mall
                upd_names.append(m_name)
            else:
                mall_dict[m_id] = mall
                add_names.append(m_name)
        
        final_malls = list(mall_dict.values())

        # 4. 輸出成果統計
        print("\n" + "-"*30)
        print(f"📊 執行結果匯報:")
        print(f"➕ 新增商場 ({len(add_names)}): {', '.join(add_names) if add_names else '無'}")
        print(f"🔄 更新商場 ({len(upd_names)}): {', '.join(upd_names) if upd_names else '無'}")
        print(f"📚 數據庫現有總數: {len(final_malls)}")
        print("-"*30)

        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_malls, f, ensure_ascii=False, indent=2)
        print(f"💾 本地 JSON 已更新完成。\n")
            
        return final_malls
    except Exception as e:
        print(f"❌ 發生錯誤: {e}")
        return []

def sync_batch_to_wechat(malls, batch_size=5, sleep_time=5):
    token = get_access_token()
    if not token:
        print("❌ 獲取微信 Token 失敗，跳過同步")
        return

    print(f"🌐 啟動微信雲數據庫同步 (共 {len(malls)} 個)...")

    ADD_API = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    UPDATE_API = f"https://api.weixin.qq.com/tcb/databaseupdate?access_token={token}"
    QUERY_API = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"

    for i, mall in enumerate(malls):
        try:
            check_q = f"db.collection('{COLLECTION_NAME}').where({{id: '{mall['id']}'}}).get()"
            res = requests.post(QUERY_API, json={"env": ENV_ID, "query": check_q}).json()
            exists = len(res.get('data', [])) > 0
            data_str = json.dumps(mall, ensure_ascii=False).replace('\\', '\\\\')

            query = f"db.collection('{COLLECTION_NAME}').where({{id: '{mall['id']}'}}).update({{ data: {data_str} }})" if exists \
                    else f"db.collection('{COLLECTION_NAME}').add({{ data: {data_str} }})"

            requests.post(UPDATE_API if exists else ADD_API, json={"env": ENV_ID, "query": query})
        except Exception as e:
            print(f"   ⚠️ 同步失敗: {mall.get('name')} - {e}")

        if (i + 1) % batch_size == 0:
            print(f"   已同步 {i + 1}/{len(malls)} 個商場，休息 {sleep_time} 秒...")
            time.sleep(sleep_time)

    print("✅ 同步任務完成。")

if __name__ == "__main__":
    malls_data = fetch_malls_deep_search()
    if malls_data:
        sync_batch_to_wechat(malls_data)
