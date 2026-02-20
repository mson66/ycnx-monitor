import requests
import json
import os
import time
from datetime import datetime

# 1. 環境配置
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
    print("--- 🧠 啟動 Gemini 2.5 Flash 深度採集 (目標: 20+ 商場) ---")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    # 強化 Prompt：要求深度搜索與多樣性
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
            "temperature": 0.2, # 降低隨機性
            "maxOutputTokens": 8192 # 擴大 Token 限制以支持 20+ 數據
        }
    }
    
    try:
        res = requests.post(url, json=payload, timeout=60).json()
        content = res['candidates'][0]['content']['parts'][0]['text']
        malls = json.loads(content)
        # 確保返回的是數組
        return malls if isinstance(malls, list) else malls.get('malls', [])
    except Exception as e:
        print(f"❌ AI 採集失敗: {e}")
        return []

def clean_data_for_wx(item):
    """處理 JSON 字符串以符合微信 query 格式，防止特殊字符崩潰"""
    # 轉為 JSON 字符串並處理反斜槓
    return json.dumps(item, ensure_ascii=False).replace('\\', '\\\\')

def sync_to_wechat(malls):
    token = get_access_token()
    if not token: return

    query_url = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"
    add_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    update_url = f"https://api.weixin.qq.com/tcb/databaseupdate?access_token={token}"

    print(f"🚀 開始同步 {len(malls)} 條數據到雲端...")
    
    success_add = 0
    success_upd = 0

    for item in malls:
        # 1. 檢查是否存在 (根據 id)
        check_query = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).get()"
        try:
            res = requests.post(query_url, json={"env": ENV_ID, "query": check_query}).json()
            exists = len(res.get('data', [])) > 0
            
            # 準備數據
            cleaned_json = clean_data_for_wx(item)
            
            if exists:
                # 2. 執行更新
                upd_query = f"db.collection('{COLLECTION_NAME}').where({{id: '{item['id']}'}}).update({{ data: {cleaned_json} }})"
                resp = requests.post(update_url, json={"env": ENV_ID, "query": upd_query}).json()
                if resp.get('errcode') == 0: success_upd += 1
                print(f"   [更新] {item['name']}")
            else:
                # 3. 執行新增
                add_query = f"db.collection('{COLLECTION_NAME}').add({{ data: {cleaned_json} }})"
                resp = requests.post(add_url, json={"env": ENV_ID, "query": add_query}).json()
                if resp.get('errcode') == 0: success_add += 1
                print(f"   [新增] {item['name']}")
            
            # 根據參考代碼要求，每條處理完稍微休息，避免觸發頻率限制
            time.sleep(0.2) 
            
        except Exception as e:
            print(f"   ❌ 處理 {item.get('name')} 時出錯: {e}")

    print(f"🎉 同步完成！新增: {success_add}, 更新: {success_upd}")

if __name__ == "__main__":
    mall_data = fetch_malls_deep_search()
    if mall_data:
        sync_to_wechat(mall_data)
    else:
        print("⚠️ 未獲得 AI 數據，任務終止。")