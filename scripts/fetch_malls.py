import requests
import json
import os
import time

# 1. 配置環境變量
APPID = os.environ.get('WX_APPID', '').strip()
APPSECRET = os.environ.get('WX_APPSECRET', '').strip()
ENV_ID = os.environ.get('WX_ENV_ID', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
COLLECTION_NAME = "mall_offers"

def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    try:
        res = requests.get(url, timeout=20).json()
        return res.get('access_token')
    except Exception as e:
        print(f"❌ 獲取 Token 異常: {e}")
        return None

def fetch_malls_deep_search():
    print("--- 🧠 啟動 Gemini 2.5 Flash 深度採集 ---")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    prompt = """
    你是一名香港商業地產與跨境交通專家。請執行深度搜索，整理 2026 年最新香港商場泊車優惠。
    
    【搜索清單要求】
    1. 必須涵蓋全港至少 20 個商場，重點包含：
       - 信和集團 (Sino Group): 奧海城、屯門市廣場、中港城、荃新天地。
       - 新鴻基 (SHKP): V city、YOHO MALL、apm、MOKO、新城市廣場、IFC、V Walk。
       - 恆隆: Fashion Walk、家樂坊、荷李活商業中心。
       - 其他: 圓方 Elements、海港城、時代廣場、東薈城、領展主要商場、太古城中心。
    2. 重點提取「粵車南下」專屬禮遇（如FT車牌額外免停、專屬禮包）。
    
    【輸出格式】
    1. 重點提取所有香港商場的泊車優惠政策，消費優惠停車政策，以及「粵車南下」專屬優惠禮遇。
    2. 輸出格式必須是純 JSON 數組，嚴禁包含任何解釋性文字。
    3. 字段定義：
    - id: 唯一標識, 如海港城為harbourcity, 請確保同一個商場在不同次生成時使用相同的 id。
    - name: 商場中文全稱 （智能校對去重）
    - lat/lng: GCJ-02 坐標系下的精確經緯度
    - isSouthbound: 若有針對「粵車南下」特有優惠禮遇則為 true，否則 false
    - parking: 簡述泊車優惠（例：粵車南下額外2小時）
    - spending: 簡述消費泊車抵扣（例：消費滿$200，或積分兌換，優惠停车1小时）
    - presents: 消費獎賞與禮品回贈等
    - description: 政策條款與細則（長文本， 1. 2. 3. ...）
    - link: 官方或可靠活動網址
    - update_time: （格式：yyyymmdd）官方發稿日期
    - end_time: （格式：yyyymmdd）官方條款，沒有定義則留空不填寫。
    """
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.2,
            "maxOutputTokens": 8192
        }
    }
    
    try:
        res = requests.post(url, json=payload, timeout=90).json()
        content = res['candidates'][0]['content']['parts'][0]['text'].strip()
        # 應急修復被截斷的 JSON
        if not content.endswith(']'):
            last_bracket = content.rfind('}')
            if last_bracket != -1:
                content = content[:last_bracket+1] + ']'
        return json.loads(content)
    except Exception as e:
        print(f"❌ AI 採集解析失敗: {e}")
        return []

def clean_data_for_wx(item):
    """將單個停車場數據轉化為符合微信 query 的 JSON 字符串"""
    s = json.dumps(item, ensure_ascii=False)
    # 針對微信 API 的特殊字符轉義處理
    return s.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r')

def sync_batch_to_wechat(malls, batch_size=5, sleep_time=5):
    """
    分批次執行 Upsert。
    batch_size: 每組處理的停車場數量
    sleep_time: 每組之間的間隔時間（秒）
    """
    token = get_access_token()
    if not token: return

    query_url = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"
    add_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    update_url = f"https://api.weixin.qq.com/tcb/databaseupdate?access_token={token}"

    total = len(malls)
    print(f"🚀 總計 {total} 個停車場，每批 {batch_size} 個，批次間隔 {sleep_time} 秒...")

    # 將數據列表切割為每 5 個一組
    for i in range(0, total, batch_size):
        batch = malls[i : i + batch_size]
        batch_num = i // batch_size + 1
        print(f"\n📦 [批次 {batch_num}] 正在同步中...")

        for item in batch:
            try:
                # 1. 檢查是否存在 (Upsert 邏輯)
                check_q = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).get()"
                res = requests.post(query_url, json={"env": ENV_ID, "query": check_q}).json()
                
                exists = len(res.get('data', [])) > 0
                cleaned_json = clean_data_for_wx(item)
                
                if exists:
                    # 2. 已存在則更新
                    q = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).update({{ data: {cleaned_json} }})"
                    resp = requests.post(update_url, json={"env": ENV_ID, "query": q}).json()
                    status = "✅ 更新"
                else:
                    # 3. 不存在則新增
                    q = f"db.collection('{COLLECTION_NAME}').add({{ data: {cleaned_json} }})"
                    resp = requests.post(add_url, json={"env": ENV_ID, "query": q}).json()
                    status = "🆕 新增"

                if resp.get('errcode') == 0:
                    print(f"   {status}: {item['name']}")
                else:
                    print(f"   ⚠️ 失敗 {item['name']}: {resp.get('errmsg')}")

            except Exception as e:
                print(f"   ❌ 處理 {item.get('name', '未知')} 時異常: {e}")

        # 批次結束後的等待時間
        if i + batch_size < total:
            print(f"⏳ 批次 {batch_num} 已完成，休眠 {sleep_time} 秒後繼續...")
            time.sleep(sleep_time)

    print(f"\n🎉 任務執行完畢！所有停車場已同步至雲端。")

if __name__ == "__main__":
    malls_data = fetch_malls_deep_search()
    if malls_data:
        # 每 5 個停車場為一批，每批間隔 5 秒
        sync_batch_to_wechat(malls_data, batch_size=5, sleep_time=5)
    else:
        print("⚠️ AI 未提供有效數據，流程終止。")