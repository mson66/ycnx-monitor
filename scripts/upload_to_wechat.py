import requests
import json
import os

# 從 .yml 的 env 部分讀取配置
APPID = os.environ.get("WECHAT_APP_ID")
APPSECRET = os.environ.get("WECHAT_APP_SECRET")
ENV_ID = os.environ.get("WECHAT_ENV_ID")
COLLECTION_NAME = "carpark_data"

def run_sync():
    # --- 1. 獲取 Token ---
    token_url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    token_resp = requests.get(token_url).json()
    token = token_resp.get("access_token")
    if not token:
        print(f"❌ Token 獲取失敗: {token_resp}")
        return

    # --- 2. 清空舊數據 (滿足先清空要求) ---
    print(f"🧹 正在清空集合: {COLLECTION_NAME}...")
    del_url = f"https://api.weixin.qq.com/tcb/databasedelete?access_token={token}"
    # 只要 park_Id 存在就刪除，實現全選清空
    del_query = f'db.collection("{COLLECTION_NAME}").where({{park_Id:_.exists(true)}}).remove()'
    del_res = requests.post(del_url, json={"env": ENV_ID, "query": del_query}).json()
    print(f"🗑️ 已成功刪除 {del_res.get('deleted', 0)} 條舊紀錄")

    # --- 3. 讀取並上傳數據 ---
    file_path = "data/liberay_merged.json"
    if not os.path.exists(file_path):
        print(f"❌ 找不到文件: {file_path}")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        all_data = json.load(f)
    
    items = all_data.get("results", [])
    add_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    
    print(f"🚀 開始同步 {len(items)} 條新數據...")
    success = 0
    for item in items:
        # 【關鍵修復】：使用 json.dumps 並設置 ensure_ascii=False
        # 這會自動處理 item 內部所有的引號轉義和特殊字符，解決 SyntaxError
        clean_item_json = json.dumps(item, ensure_ascii=False)
        
        # 構建微信語法字串
        query = f"db.collection('{COLLECTION_NAME}').add({{ data: {clean_item_json} }})"
        
        payload = {
            "env": ENV_ID,
            "query": query
        }
        
        resp = requests.post(add_url, json=payload).json()
        if resp.get("errcode") == 0:
            success += 1
        else:
            print(f"⚠️ ID {item.get('park_Id')} 失敗: {resp.get('errmsg')}")

    print(f"✅ 同步結束: 成功 {success}/{len(items)}")

if __name__ == "__main__":
    run_sync()