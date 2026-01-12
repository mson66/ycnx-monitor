import requests
import json
import os
from datetime import datetime

# 微信雲開發配置
APPID = os.environ.get("WECHAT_APPID")
APPSECRET = os.environ.get("WECHAT_APPSECRET")
ENV_ID = os.environ.get("WECHAT_ENV_ID")
COLLECTION_NAME = "carpark_data" # 你的集合名稱 告訴 Python 腳本：「請把這份 JSON 數據存進名為 carpark_data 的這個分類裡」。

def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    resp = requests.get(url)
    data = resp.json()
    if 'access_token' in data:
        return data['access_token']
    else:
        raise Exception(f"獲取 Access Token 失敗: {data}")

    # 微信數據庫單次寫入有限制，建議循環寫入或根據實際需求調整
    # 這裡演示上傳整個 JSON 作為一條記錄或更新某條記錄
    # 實際場景通常是遍歷 results 上傳
    
    # 這裡假設上傳 metadata 和 results 列表

def upload_json_to_db(file_path):
    token = get_access_token()
    upload_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 這裡保持你原有的 query 邏輯
    query = f"""
    db.collection("{COLLECTION_NAME}").add({{
        data: {{
            updated_at: db.serverDate(),
            content: {json.dumps(data, ensure_ascii=False)}
        }}
    }})
    """
    
    payload = {
        "env": ENV_ID,
        "query": query
    }
    
    resp = requests.post(upload_url, json=payload)
    result_text = resp.text
    print(f"上傳結果: {result_text}")

    # --- 新增日誌記錄 (追加模式 'a') ---
    log_path = os.path.join("data", "step_status.log")
    
    # 解析微信返回，判斷是否真的成功（通常 errcode 為 0 是成功）
    is_success = '"errcode":0' in result_text
    status_msg = "成功" if is_success else "失敗"
    
    with open(log_path, "a", encoding="utf-8") as log_f:
        log_f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 8: 雲端同步{status_msg}。返回: {result_text[:50]}...\n")
    # -----------------------------------

def main():
    file_path = os.path.join("data", "liberay_merged.json")
    if os.path.exists(file_path):
        print("開始上傳至微信雲開發...")
        try:
            upload_json_to_db(file_path)
            # 也可以在 main 這裡補一條流程結束的標記
            with open("data/step_status.log", "a", encoding="utf-8") as log_f:
                log_f.write("-" * 50 + "\n") # 畫一條分隔線表示本次月更結束
        except Exception as e:
            print(f"上傳失敗: {e}")
            with open("data/step_status.log", "a", encoding="utf-8") as log_f:
                log_f.write(f"[{datetime.now().strftime('%H:%M:%S')}] STEP 8 ERROR: {str(e)}\n")
    else:
        print("未找到合併後的文件")

if __name__ == "__main__":
    main()