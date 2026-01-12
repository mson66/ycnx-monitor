import requests
import json
import os
import time
from datetime import datetime

# 微信雲開發配置
APPID = os.environ.get("WECHAT_APP_ID")
APPSECRET = os.environ.get("WECHAT_APP_SECRET")
ENV_ID = os.environ.get("WECHAT_ENV_ID")
COLLECTION_NAME = "carpark_data"

def get_access_token():
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={APPID}&secret={APPSECRET}"
    resp = requests.get(url).json()
    if 'access_token' in resp:
        return resp['access_token']
    raise Exception(f"獲取 Token 失敗: {resp}")

def run_sync():
    log_path = os.path.join("data", "step_status.log")
    start_time = datetime.now()
    
    try:
        # 1. 獲取 Token
        token = get_access_token()

        # 2. 清空集合 (依據要求，先刪除原有數據)
        print(f"🧹 正在清空集合: {COLLECTION_NAME}...")
        del_url = f"https://api.weixin.qq.com/tcb/databasedelete?access_token={token}"
        del_query = f'db.collection("{COLLECTION_NAME}").where({{park_Id:_.exists(true)}}).remove()'
        del_res = requests.post(del_url, json={"env": ENV_ID, "query": del_query}).json()
        deleted_count = del_res.get("deleted", 0)
        print(f"🗑️ 已成功刪除 {deleted_count} 條舊紀錄")

        # 3. 讀取數據
        file_path = os.path.join("data", "liberay_merged.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
        items = all_data.get("results", [])
        total_items = len(items)
        
        # 4. 分組同步
        add_url = f"https://api.weixin.qq.com/tcb/databaseadd?access_token={token}"
        success_count = 0
        batch_size = 15
        
        print(f"🚀 開始分組同步 {total_items} 條數據...")
        
        for i in range(0, total_items, batch_size):
            batch = items[i : i + batch_size]
            
            # 格式處理：轉義反斜槓以解決微信解析器的 SyntaxError
            batch_json = json.dumps(batch, ensure_ascii=False).replace('\\', '\\\\')
            
            # 構建批量添加指令
            query = f"db.collection('{COLLECTION_NAME}').add({{ data: {batch_json} }})"
            payload = {"env": ENV_ID, "query": query}
            
            # 網路重試機制
            for retry in range(3):
                try:
                    resp = requests.post(add_url, json=payload, timeout=20).json()
                    if resp.get("errcode") == 0:
                        success_count += len(batch)
                        break
                    else:
                        print(f"⚠️ 分組 {i//batch_size + 1} 失敗: {resp.get('errmsg')}")
                        break
                except Exception as e:
                    if retry < 2:
                        time.sleep(3)
                        continue
                    raise e
            
            time.sleep(1) # 避免觸發頻率限制

        # 5. 寫入正式日誌報告
        report_msg = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 8: 雲端同步成功。成功完成 {success_count}/{total_items} 條紀錄 (已清理舊數據 {deleted_count} 條)。\n"
        print(report_msg)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(report_msg)
            f.write("-" * 50 + "\n")
        print(f"📝 報告已寫入日誌。")

    except Exception as e:
        error_msg = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] STEP 8 同步失敗: {str(e)}\n"
        print(error_msg)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(error_msg)

if __name__ == "__main__":
    run_sync()