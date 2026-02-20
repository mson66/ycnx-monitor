import requests
import json
import os
from datetime import datetime

# 1. 獲取環境變量 (GitHub Secrets)
APPID = os.environ.get('WX_APPID').strip()
APPSECRET = os.environ.get('WX_APPSECRET').strip()
ENV_ID = os.environ.get('WX_ENV_ID').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY').strip()

# 2. 獲取微信 Access Token
def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    res = requests.get(url).json()
    return res.get('access_token')

# 3. 調用 Gemini 2.5 Flash 獲取 AI 數據
def fetch_malls_from_ai():
    print("--- 正在調用 Gemini 2.5 Flash 獲取數據 ---")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    prompt = f"""
你是一名香港交通與商業數據專家。請搜索 2026 年最新香港商場泊車優惠。
- 每次生成前查閱已有的基礎數據：
- 商場數據源參考：
    - 信和集团
    - 新鸿基
    - 东荟城
    - 圆方 Elements
    - 領展
    - 其它大中型商場和媒體平台發布的信息。

要求：
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
        "generationConfig": { "responseMimeType": "application/json", "temperature": 0.1 }
    }
    
    res = requests.post(url, json=payload)
    return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])

# 4. 操作雲數據庫 (HTTP API 模式)
def db_query(token, query_string):
    url = f"https://api.weixin.qq.com/tcb/databasequery?access_token={token}"
    data = { "env": ENV_ID, "query": query_string }
    return requests.post(url, json=data).json()

def db_add(token, query_string):
    url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    data = { "env": ENV_ID, "query": query_string }
    return requests.post(url, json=data).json()

def db_update(token, query_string):
    url = f"https://api.weixin.qq.com/tcb/databaseupdate?access_token={token}"
    data = { "env": ENV_ID, "query": query_string }
    return requests.post(url, json=data).json()

def main():
    token = get_access_token()
    if not token:
        print("❌ 獲取微信 Token 失敗，請檢查 AppID 和 Secret")
        return

    malls = fetch_malls_from_ai()
    print(f"AI 成功提取 {len(malls)} 個商場")

    for mall in malls:
        # 查詢是否存在 (基於 id 字段)
        query = f"db.collection('mall_offers').where({{id: '{mall['id']}'}}).get()"
        res = db_query(token, query)
        
        # 微信 HTTP API 返回的 data 是一個 JSON 字符串列表
        if res.get('data'):
            # 執行更新
            update_query = f"db.collection('mall_offers').where({{id: '{mall['id']}'}}).update({{data: {json.dumps(mall)}}})"
            db_update(token, update_query)
            print(f"[更新] {mall['name']}")
        else:
            # 執行新增
            add_query = f"db.collection('mall_offers').add({{data: {json.dumps(mall)}}})"
            db_add(token, add_query)
            print(f"[新增] {mall['name']}")

    print("✅ 數據同步任務完成")

if __name__ == "__main__":
    main()