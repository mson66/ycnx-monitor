import json
import os
import requests
import sys

def upload_to_wechat():
    # 1. 環境變量讀取
    env_id = os.environ.get("WECHAT_ENV_ID")
    app_id = os.environ.get("WECHAT_APP_ID")
    app_secret = os.environ.get("WECHAT_APP_SECRET")
    collection_name = "carpark_data"  # 請確保與微信雲開發後台集合名稱一致
    
    input_file = "data/liberay_merged.json"
    
    if not os.path.exists(input_file):
        print(f"❌ 找不到數據文件: {input_file}")
        return

    try:
        # 2. 獲取 Access Token
        token_url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={app_id}&secret={app_secret}"
        token_res = requests.get(token_url).json()
        access_token = token_res.get("access_token")
        
        if not access_token:
            print(f"❌ 獲取 Token 失敗: {token_res}")
            return

        # 3. 讀取並轉換數據格式 (JSON -> JSON Lines)
        with open(input_file, "r", encoding="utf-8") as f:
            full_data = json.load(f)
        
        results = full_data.get("results", [])
        if not results:
            print("⚠️ 沒有發現需要上傳的數據 (results 為空)")
            return

        # 將數據轉換為微信導入所需的 NDJSON 格式字符串
        # 每行一條 JSON，不加逗號
        ndjson_content = ""
        for item in results:
            ndjson_content += json.dumps(item, ensure_ascii=False) + "\n"

        # 4. 調用微信雲開發數據庫導入 API (使用 databaseImport)
        # 注意：這裡示範的是直接通過 HTTP API 插入數據的簡化邏輯
        # 為了穩定性，我們循環使用 databaseAdd 確保數據準確寫入
        import_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={access_token}"
        
        success_count = 0
        for item in results:
            # 微信 databaseAdd 每次支持單條或多條，這裡採用逐條或小批量確保語法正確
            query = f"db.collection('{collection_name}').add({{ data: {json.dumps(item, ensure_ascii=False)} }})"
            payload = {
                "env": env_id,
                "query": query
            }
            res = requests.post(import_url, json=payload).json()
            
            if res.get("errcode") == 0:
                success_count += 1
            else:
                print(f"⚠️ ID {item.get('park_Id')} 上傳失敗: {res}")

        print(f"✅ 同步完成！成功寫入 {success_count}/{len(results)} 條數據到集合 '{collection_name}'")

    except Exception as e:
        print(f"❌ 運行時出錯: {str(e)}")

if __name__ == "__main__":
    upload_to_wechat()