import requests
import json
import os
import time

# 配置讀取
APPID = os.environ.get("WECHAT_APP_ID")
APPSECRET = os.environ.get("WECHAT_APP_SECRET")
ENV_ID = os.environ.get("WECHAT_ENV_ID")
COLLECTION_NAME = "carpark_data"

def run_sync():
    # 1. 獲取 Token
    token_url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    token_resp = requests.get(token_url).json()
    token = token_resp.get("access_token")
    if not token:
        print(f"❌ Token 獲取失敗: {token_resp}")
        return

    # 2. 清空集合
    print(f"🧹 正在清空集合: {COLLECTION_NAME}...")
    del_url = f"https://api.weixin.qq.com/tcb/databasedelete?access_token={token}"
    del_query = f'db.collection("{COLLECTION_NAME}").where({{park_Id:_.exists(true)}}).remove()'
    requests.post(del_url, json={"env": ENV_ID, "query": del_query})

    # 3. 讀取數據
    file_path = "data/liberay_merged.json"
    with open(file_path, 'r', encoding='utf-8') as f:
        all_data = json.load(f)
    items = all_data.get("results", [])
    
    add_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    success_count = 0
    batch_size = 15  # 每 15 條一組，這是微信 API 最穩定的平衡點
    
    print(f"🚀 開始分組同步 {len(items)} 條數據 (Batch Size: {batch_size})...")
    
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        
        # 【核心修正 1】：對整組數據進行深層轉義，防止 SyntaxError
        # 先轉成 JSON 字符串，再翻倍反斜槓，處理微信解析器的特殊脾氣
        batch_json = json.dumps(batch, ensure_ascii=False)
        batch_json = batch_json.replace('\\', '\\\\')
        
        # 【核心修正 2】：批量語法 db.collection(...).add({ data: [ {...}, {...} ] })
        query = f"db.collection('{COLLECTION_NAME}').add({{ data: {batch_json} }})"
        
        payload = {
            "env": ENV_ID,
            "query": query
        }
        
        # 增加重試機制處理網路波動
        for retry in range(3):
            try:
                resp = requests.post(add_url, json=payload, timeout=20).json()
                if resp.get("errcode") == 0:
                    success_count += len(batch)
                    break
                else:
                    print(f"⚠️ 分組 {i//batch_size} 失敗: {resp.get('errmsg')}")
                    break 
            except Exception as e:
                print(f"⏳ 網路抖動，5秒後進行第 {retry+1} 次重試...")
                time.sleep(5)
        
        # 適度暫停，防止觸發微信頻率限制
        time.sleep(0.5)

    print(f"✅ [Tx4] 同步結束: 成功完成 {success_count}/{len(items)} 條紀錄。")

if __name__ == "__main__":
    run_sync()