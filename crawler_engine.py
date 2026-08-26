import requests
import os
import json
import pdfplumber
import re
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from groq import Groq

class CrawlerEngine:
    def __init__(self, groq_api_key):
        self.client = Groq(api_key=groq_api_key)
        self.model_id = "openai/gpt-oss-20b"
        self.base_pdf_url = "https://ycnx.singlewindow.gd.cn/api/ycnx-approval/draw-lots-notarization/view-publicly-file/YCCQPCH"
        
        self.browser_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://ycnx.singlewindow.gd.cn/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=3, max=10),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout, ConnectionResetError)),
        reraise=True
    )
    def _make_request(self, url):
        """带重试机制的 HTTP 请求"""
        response = requests.get(
            url, 
            headers=self.browser_headers, 
            timeout=30,
            allow_redirects=True,
            verify=True
        )
        return response

    def download_and_convert(self, period_id):
        """下载 PDF 并仅提取第一页文本"""
        pdf_url = f"{self.base_pdf_url}{str(period_id).zfill(7)}"
        print(f"🌐 正在探测 URL: {pdf_url}")
        
        try:
            response = self._make_request(pdf_url)
            print(f"📡 响应状态: {response.status_code}, 内容类型: {response.headers.get('Content-Type', 'unknown')}")
            
            if response.status_code != 200:
                print(f"⚠️ 非200状态码: {response.status_code}")
                return None, pdf_url
            
            content_type = response.headers.get('Content-Type', '')
            if 'pdf' not in content_type.lower() and not response.content.startswith(b'%PDF'):
                print(f"⚠️ 非PDF内容: {content_type}")
                return None, pdf_url
            
            pdf_path = f"temp_{period_id}.pdf"
            with open(pdf_path, "wb") as f:
                f.write(response.content)
            
            full_text = ""
            with pdfplumber.open(pdf_path) as pdf:
                # 关键优化：只读第一页，提取核心汇总数据
                if pdf.pages:
                    full_text = pdf.pages[0].extract_text() or ""
            
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
            
            if not full_text:
                return None, pdf_url
                
            return full_text, pdf_url
            
        except Exception as e:
            print(f"❌ PDF处理失败: {e}")
            return None, pdf_url

    def ai_extract(self, pdf_content, period_id):
        """AI 结构化解析 + 业务逻辑校准"""
        print(f"🚀 正在调用 {self.model_id} 解析第 {period_id} 期数据...")
        
        prompt = (
            f"请从以下“粤车南下”抽签结果公证书文本中提取关键统计数据。\n"
            f"--- 文本内容 ---\n{pdf_content}\n--- 文本结束 ---\n"
            "请提取以下 JSON 格式：\n"
            "{\n"
            "  \"batch_no\": \"提取形如 YCCQPCHxxxxxxx 的完整批次号\",\n"
            "  \"draw_date\": \"抽签日期 YYYY-MM-DD\",\n"
            "  \"total_applied\": \"登记资料有效数\",\n"
            "  \"quota_total\": \"抽签名额总数\",\n"
            "  \"total_won\": \"中签数\",\n"
            "  \"total_lost\": \"未中签数\"\n"
            "}\n"
            "注意：只返回纯 JSON 内容。"
        )

        try:
            completion = self.client.chat.completions.create(
                model=self.model_id,
                messages=[
                    {"role": "system", "content": "你是一个只输出 JSON 的专业数据提取助手。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                response_format={"type": "json_object"}
            )
            
            raw = json.loads(completion.choices[0].message.content)
            
            # --- 数据清洗与强制类型转换 ---
            applied = self._to_int(raw.get("total_applied"))
            quota = self._to_int(raw.get("quota_total"))
            won = self._to_int(raw.get("total_won"))
            lost = self._to_int(raw.get("total_lost"))
            
            # --- 业务逻辑硬核修正 ---
            # 逻辑1：全员中签场景（如第2期）
            if applied > 0 and quota >= applied:
                won = applied
                lost = 0
            # 逻辑2：配额不足场景（如第1期）
            elif applied > quota and quota > 0:
                # 如果 AI 识别中签数错误（如变成1），则强制修正为配额数
                if won <= 1:
                    won = quota
                lost = applied - won

            # 逻辑3：计算准确中签率
            if applied > 0:
                rate_val = (won / applied) * 100
                win_rate = f"{rate_val:.2f}%" if rate_val < 100 else "100%"
            else:
                win_rate = "0%"

            return {
                "period_id": period_id,
                "batch_no": raw.get("batch_no") or f"YCCQPCH{str(period_id).zfill(7)}",
                "period_name": f"第{period_id}期",
                "draw_date": raw.get("draw_date"),
                "total_applied": applied,
                "quota_total": quota,
                "total_won": won,
                "total_lost": lost,
                "win_rate": win_rate
            }
            
        except Exception as e:
            print(f"❌ AI解析异常: {e}")
            return None

    def _to_int(self, value):
        """增强版数字清洗：处理逗号、单位、空格等"""
        if value is None: 
            return 0
        try:
            # 使用正则只保留数字部分
            clean_str = re.sub(r'[^\d]', '', str(value))
            return int(clean_str) if clean_str else 0
        except:
            return 0
