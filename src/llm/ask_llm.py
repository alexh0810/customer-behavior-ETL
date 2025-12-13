# ask_llm.py
import os
import json
from groq import Client


def get_client():
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")
    return Client(api_key=api_key)


# Simple classification instructions
SYSTEM_PROMPT = """
Bạn là trợ lý phân loại từ khoá cho nền tảng xem phim/series.

Nhiệm vụ:
- Phân loại MỖI TỪ KHOÁ (nếu là tên phim/series hợp lệ) vào đúng một trong các thể loại:
  • C-DRAMA   → Phim/series Trung Quốc
  • K-DRAMA   → Phim/series Hàn Quốc
  • Action    → Phim hành động, tội phạm, siêu anh hùng, giật gân
  • Horror    → Phim kinh dị, ma, tâm linh, zombie

QUY TẮC NHẬN DIỆN:
1) Từ khoá chỉ gồm tiếng Việt hoặc tiếng Anh.  
   Bạn cần nhận diện tiêu đề phim KHÔNG phân biệt:
   - chữ hoa/chữ thường (case-insensitive)
   - cách viết có dấu, không dấu, hoặc mixed dấu (accent-insensitive)

   Ví dụ: “Hậu Duệ Mặt Trời”, “Hau Due Mat Troi”, “Hậu Due Mat Troi” → đều được coi là hợp lệ.

2) Nhận diện các tiêu đề phim/series tiếng Việt hoặc tiếng Anh ngay cả khi viết thiếu dấu, sai dấu, hoặc viết không chuẩn.

3) Giữ nguyên nguyên văn từ khoá (sau khi trim) khi đưa vào trường "keyword".

QUY TẮC PHÂN LOẠI:
4) Chỉ phân loại khi bạn THỰC SỰ CHẮC CHẮN về xuất xứ hoặc thể loại của phim.
   - Nếu không chắc → BỎ QUA (không đoán, không suy diễn, không hallucinate).

XỬ LÝ INPUT:
5) Input là danh sách chuỗi. Nếu phần tử không phải chuỗi → bỏ qua.
6) Nếu từ khoá trùng lặp → chỉ giữ lần đầu.
7) Kết quả phải theo đúng thứ tự xuất hiện trong input.

ĐỊNH DẠNG ĐẦU RA:
Luôn trả về DUY NHẤT MỘT JSON ARRAY.
Mỗi phần tử có dạng:
  {"keyword": "<string>", "genre": "C-DRAMA | K-DRAMA | Action | Horror"}

Nếu không có từ khoá hợp lệ → trả về [].
"""

# Prompt for batches
BATCH_TEMPLATE = """
Phân loại các từ khoá sau đây theo đúng quy tắc:

- Từ khoá chỉ gồm tiếng Việt hoặc tiếng Anh, có thể viết có dấu, không dấu hoặc mixed.
- Không phân biệt chữ hoa/chữ thường.
- Chỉ phân loại nếu từ khoá là tên phim/series và bạn CHẮC CHẮN về thể loại hoặc xuất xứ.
- Nếu không chắc → BỎ QUA (không đoán, không suy diễn).
- Chỉ trả về DUY NHẤT MỘT mảng JSON dạng:
  [
    {{ "keyword": "<string>", "genre": "C-DRAMA | K-DRAMA | Action | Horror" }},
    ...
  ]

Bỏ qua những từ khoá không rõ nghĩa hoặc không thể phân loại.

Keywords:
{keywords_json}

"""


def call_llm_batch(client, keywords):
    """Send ONE batch of keywords to Groq, return a list of dicts."""
    user_prompt = BATCH_TEMPLATE.format(
        keywords_json=json.dumps(keywords, ensure_ascii=False)
    )

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",  # You can change to your preferred model
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )

    text = response.choices[0].message.content.strip()
    # Try to parse response
    try:
        return json.loads(text)
    except:
        # Fallback: try to find the JSON array inside the text
        start = text.find("[")
        end = text.rfind("]")
        return json.loads(text[start : end + 1])


def classify_keywords_batchwise(keywords, batch_size):
    """
    Break keywords into batches, call the LLM, and combine results.
    """
    client = get_client()
    final_mapping = {}

    for i in range(0, len(keywords), batch_size):
        batch = keywords[i : i + batch_size]
        print(f"Processing batch {i//batch_size + 1} ...")

        results = call_llm_batch(client, batch)
        for item in results:
            k = item.get("keyword")
            g = item.get("genre")
            if k and g:
                final_mapping[k] = g

    return final_mapping
