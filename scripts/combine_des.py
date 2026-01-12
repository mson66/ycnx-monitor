import json
import os

def main():
    file1 = os.path.join("data", "rules-s1.json")
    file2 = os.path.join("data", "rules-s2.json")
    output = os.path.join("data", "description-rules.json")

    combined = []
    
    for f_path in [file1, file2]:
        if os.path.exists(f_path):
            with open(f_path, 'r', encoding='utf-8') as f:
                combined.extend(json.load(f))
        else:
            print(f"警告: 文件不存在 {f_path}")

    with open(output, 'w', encoding='utf-8') as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    
    print(f"已合併 {len(combined)} 條規則到 {output}")

if __name__ == "__main__":
    main()