const axios = require('axios');
const tcb = require('@cloudbase/node-sdk');

// 1. 初始化騰訊雲環境
const app = tcb.init({
  secretId: process.env.TENCENT_CLOUD_SECRET_ID,
  secretKey: process.env.TENCENT_CLOUD_SECRET_KEY,
  env: process.env.TCB_ENV_ID // 從 yml 傳入
});
const db = app.database();
const collection = db.collection('mall_offers');

// 2. 定義 Google AI Studio API 參數
// 修改後的 GEMINI 配置
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
// 建議使用 v1 穩定版或保持 v1beta 但更換模型 ID
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${GEMINI_API_KEY}`;

// 3. 嚴格定義數據 Schema 和 Prompt
const PROMPT = `
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
`;

async function fetchMallsFromGemini() {
  try {
    console.log('--- 正在調用 Gemini 2.5 Flash 獲取數據 ---');
    
    const response = await axios.post(GEMINI_URL, {
      contents: [{
        parts: [{ text: PROMPT }]
      }],
      generationConfig: {
        // Gemini 2.5 支持更嚴謹的輸出控制
        responseMimeType: "application/json",
        temperature: 0.1 // 降低隨機性，確保數據結構穩定
      }
    });

    // 提取 Gemini 返回的內容
    const content = response.data.candidates[0].content.parts[0].text;
    const newMalls = JSON.parse(content);
    
    // 如果返回的是對象外殼，則取出數組
    const mallArray = Array.isArray(newMalls) ? newMalls : (newMalls.malls || []);
    
    console.log(`Gemini 成功生成了 ${mallArray.length} 個商場數據。`);
    return mallArray;
  } catch (error) {
    console.error('Gemini API 調用出錯:', error.response?.data || error.message);
    return null;
  }
}

async function upsertToTencentCloud(malls) {
  if (!malls || malls.length === 0) return;

  let added = 0;
  let updated = 0;

  for (const mall of malls) {
    // 根據 ID 檢查是否存在
    const res = await collection.where({ id: mall.id }).get();

    const dataToSave = {
      ...mall,
      _lastUpdateSystemTime: new Date()
    };

    if (res.data.length > 0) {
      // 存在則更新
      const docId = res.data[0]._id;
      await collection.doc(docId).update(dataToSave);
      updated++;
    } else {
      // 不存在則新增
      await collection.add({
        ...dataToSave,
        _createTime: new Date()
      });
      added++;
    }
  }

  console.log(`數據同步完成：新增 ${added} 條，更新 ${updated} 條。`);
}

async function main() {
  const malls = await fetchMallsFromGemini();
  if (malls) {
    await upsertToTencentCloud(malls);
  } else {
    process.exit(1);
  }
}

main();