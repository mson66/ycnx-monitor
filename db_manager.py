import sqlite3

class DBManager:
    def __init__(self, db_name="ycnx_data.db"):
        # 建立数据库连接，check_same_thread=False 允许在多线程环境中使用
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_table()

    def create_table(self):
        """初始化数据库表结构，包含期数、批次号及全量统计字段"""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS lottery_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id INTEGER UNIQUE,         -- 内部期数序号 (1, 2, 3...)
                batch_no TEXT,                   -- 官方批次号 (YCCQPCH0000001)
                period_name TEXT,                -- 显示名称 (第1期)
                draw_date TEXT,                  -- 抽签日期
                total_applied INTEGER,           -- 登记资料有效数
                quota_total INTEGER,             -- 抽签名额总数
                total_won INTEGER,               -- 本轮中签数
                total_lost INTEGER,              -- 未中签数
                win_rate TEXT,                   -- 中签率 (字符串格式)
                pdf_url TEXT,                    -- 原始公告PDF地址
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()

    def is_period_exists(self, period_id):
        """检查特定期数是否已抓取"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM lottery_results WHERE period_id = ?", (period_id,))
        return cursor.fetchone() is not None

    def insert_result(self, data):
        """将 AI 提取并校验后的数据存入数据库"""
        cursor = self.conn.cursor()
        try:
            sql = '''
                INSERT INTO lottery_results 
                (period_id, batch_no, period_name, draw_date, total_applied, 
                 quota_total, total_won, total_lost, win_rate, pdf_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            params = (
                data['period_id'],
                data.get('batch_no'),
                data.get('period_name'),
                data.get('draw_date'),
                data.get('total_applied'),
                data.get('quota_total'),
                data.get('total_won'),
                data.get('total_lost'),
                data.get('win_rate'),
                data.get('pdf_url')
            )
            cursor.execute(sql, params)
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            print(f"⚠️ 第 {data['period_id']} 期已存在，跳过写入。")
            return False
        except Exception as e:
            print(f"❌ 数据库写入异常: {e}")
            return False

    def get_all_history(self):
        """获取所有历史记录，按期数倒序排列（最新在前）"""
        # 设置 row_factory 使得返回结果可以像字典一样访问
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM lottery_results ORDER BY period_id DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def close(self):
        """手动关闭数据库连接"""
        if self.conn:
            self.conn.close()
