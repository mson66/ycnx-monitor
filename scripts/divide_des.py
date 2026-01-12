import json
import os
import math

def main():
    input_file = os.path.join("data", "description.json")
    output_s1 = os.path.join("data", "description-s1.json")
    output_s2 = os.path.join("data", "description-s2.json")

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        total = len(data)
        if total == 0:
            print("無數據可拆分")
            return

        mid_index = math.ceil(total / 2)
        
        part1 = data[:mid_index]
        part2 = data[mid_index:]
        
        # 尋找分割點的 park_Id 用於日誌
        split_id = part2[0]['park_Id'] if part2 else "N/A"

        print(f"總數據: {total} 條")
        print(f"分割點 Park ID: {split_id}")

        with open(output_s1, 'w', encoding='utf-8') as f:
            json.dump(part1, f, ensure_ascii=False, indent=2)
        print(f"已保存 Part 1 ({len(part1)} 條) 到 {output_s1}")

        with open(output_s2, 'w', encoding='utf-8') as f:
            json.dump(part2, f, ensure_ascii=False, indent=2)
        print(f"已保存 Part 2 ({len(part2)} 條) 到 {output_s2}")

    except Exception as e:
        print(f"拆分失敗: {e}")

if __name__ == "__main__":
    main()