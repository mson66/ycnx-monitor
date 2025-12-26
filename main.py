import os
import json
import time
from datetime import datetime
from db_manager import DBManager
from crawler_engine import CrawlerEngine

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

def main():
    db = DBManager()
    engine = CrawlerEngine(GROQ_API_KEY)
    now = datetime.now()
    
    # 策略：如果今天小于 23 号，且数据库已经有数据，可以根据需要跳过
    # 但为了兼容补抓历史数据，我们保留探测逻辑，重点在于“防重抓”
    
    current_id = 1
    new_data_found = False

    print(f"🚀 采集器启动时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        # 1. 检查数据库是否已存在该 ID
        if db.is_period_exists(current_id):
            current_id += 1
            continue
            
        # 2. 探测网络是否有新 PDF
        pdf_text, pdf_url = engine.download_and_convert(current_id)
        
        if pdf_text:
            # 3. 发现新 PDF，调用 AI 解析
            raw_data = engine.ai_extract(pdf_text, current_id)
            if raw_data:
                raw_data['period_id'] = current_id
                raw_data['pdf_url'] = pdf_url
                
                if db.insert_result(raw_data):
                    print(f"🎉 成功抓取第 {current_id} 期数据")
                    new_data_found = True
                    current_id += 1
                    time.sleep(5) # 短暂延迟
            else:
                print(f"⚠️ 第 {current_id} 期解析失败，可能文件尚未生成。")
                break
        else:
            # 4. 如果没找到 PDF，说明目前已经是最新的了
            print(f"🏁 探测结束，未发现新期数 (ID: {current_id})。")
            break

    # 5. 如果抓到了新数据，更新 JSON 文件供前端使用
    if new_data_found:
        history = db.get_all_history()
        # 写入历史全量数据
        with open("history_data.json", "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=4)
        # 写入最新一期数据
        with open("latest_data.json", "w", encoding="utf-8") as f:
            json.dump(history[0], f, ensure_ascii=False, indent=4)
        print("📁 结果已同步至 JSON 文件。")
    else:
        print("😴 本次运行未发现新数据，无需更新。")

if __name__ == "__main__":
    main()
