import requests
import json
import os

# 1. 配置 (由 GitHub Secrets 注入)
    env_id = os.environ.get("WECHAT_ENV_ID")
    app_id = os.environ.get("WECHAT_APP_ID")
    app_secret = os.environ.get("WECHAT_APP_SECRET")
    collection_name = "carpark_data"  # 請確保與微信雲開發後台集合名稱一致

def run_sync():
    # --- 獲取 Token ---
    token_url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    token_resp = requests.get(token_url).json()
    token = token_resp.get("access_token")
    if not token:
        print(f"Token 獲取失敗: {token_resp}")
        return

    # --- 1. 清空集合 (依據你的要求) ---
    print(f"正在清空集合: {COLLECTION_NAME}...")
    del_url = f"https://api.weixin.qq.com/tcb/databasedelete?access_token={token}"
    # 刪除所有 park_Id 存在的數據
    del_query = f'db.collection("{COLLECTION_NAME}").where({{park_Id:_.exists(true)}}).remove()'
    requests.post(del_url, json={"env": ENV_ID, "query": del_query})

    # --- 2. 讀取並上傳數據 ---
    file_path = "data/liberay_merged.json"
    with open(file_path, 'r', encoding='utf-8') as f:
        all_data = json.load(f)
    
    items = all_data.get("results", [])
    add_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    
    success = 0
    for item in items:
        # 【核心修正】: 先將對象轉為 JSON 字符串，確保所有內部引號都被轉義
        # 再將其放入微信的 query 字符串中
        clean_json = json.dumps(item, ensure_ascii=False)
        
        # 使用 f-string 構建指令，確保 data 的內容被完整包裹
        query = f"db.collection('{COLLECTION_NAME}').add({{ data: {clean_json} }})"
        
        payload = {
            "env": ENV_ID,
            "query": query
        }
        
        resp = requests.post(add_url, json=payload).json()
        if resp.get("errcode") == 0:
            success += 1
        else:
            print(f"失敗 ID {item.get('park_Id')}: {resp.get('errmsg')}")

    print(f"同步結束: 成功 {success}/{len(items)}")

if __name__ == "__main__":
    run_sync()