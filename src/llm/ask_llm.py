# ask_llm.py
import os
import json
import re
from groq import Client


def get_client():
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")
    return Client(api_key=api_key)


# Simple classification instructions
SYSTEM_PROMPT = """
Bạn là trợ lý phân loại từ khoá cho nền tảng xem phim / series.

MỤC TIÊU:
Phân loại từ khoá tìm kiếm thành thể loại phim/series CHÍNH XÁC và NHẤT QUÁN.
Ưu tiên phân loại đúng hơn là phân loại nhiều.

CÁC THỂ LOẠI HỢP LỆ (CHỈ 1):
- C-DRAMA   → Phim / series Trung Quốc
- K-DRAMA   → Phim / series Hàn Quốc
- Action    → Phim hành động, tội phạm, siêu anh hùng, anime hành động
- Horror    → Phim kinh dị, ma, tâm linh, giết người, rùng rợn

⚠️ QUY TẮC ƯU TIÊN (RẤT QUAN TRỌNG):
1) Nếu từ khoá là phim/series Trung Quốc → LUÔN là C-DRAMA
2) Nếu từ khoá là phim/series Hàn Quốc → LUÔN là K-DRAMA
   (KỂ CẢ khi có yếu tố hành động, tâm linh, siêu nhiên)

3) CHỈ dùng Action hoặc Horror khi:
   - KHÔNG phải phim Trung / Hàn
   - Và thể loại đó là ĐẶC TRƯNG CHÍNH

❌ KHÔNG ĐƯỢC phân loại Horror chỉ vì:
- Có yếu tố huyền huyễn / fantasy
- Có ma mị nhẹ / tâm linh nhẹ
- Là phim cổ trang / ngôn tình

❌ KHÔNG ĐƯỢC phân loại nếu từ khoá là:
- Kênh truyền hình (vd: vtv, hbo, vtv6)
- Tên diễn viên / đạo diễn
- Thể loại chung chung (vd: phim hay, action movie)
- Từ khoá không rõ nghĩa

QUY TẮC NHẬN DIỆN:
- Không phân biệt hoa/thường
- Không phân biệt có dấu / không dấu
- Cho phép viết sai dấu / thiếu dấu
- Giữ nguyên keyword gốc trong output

MỨC ĐỘ CHẮC CHẮN:
- Nếu bạn KHÁ CHẮC CHẮN (≈70–80%) → CÓ THỂ phân loại
- Nếu KHÔNG CHẮC → BỎ QUA (KHÔNG đoán)

OUTPUT (BẮT BUỘC):
- CHỈ trả về DUY NHẤT MỘT JSON ARRAY
- KHÔNG giải thích
- KHÔNG markdown
- Mỗi keyword chỉ xuất hiện 1 lần

ĐỊNH DẠNG:
[
  { "keyword": "<string>", "genre": "C-DRAMA | K-DRAMA | Action | Horror" }
]

Nếu không có kết quả hợp lệ → [].

"""

# Prompt for batches
BATCH_TEMPLATE = """
Phân loại các từ khoá sau theo đúng quy tắc đã nêu.

Keywords:
{keywords_json}

"""


def extract_json_array(text):
    """
    Extract JSON array from text, handling incomplete arrays
    """
    # Try to find array start
    start = text.find("[")
    if start == -1:
        return None

    # Try to find array end
    end = text.rfind("]")

    if end == -1:
        # Array is incomplete - try to salvage what we have
        # Find last complete object
        last_complete = text.rfind("}")
        if last_complete > start:
            # Try to close the array
            text = text[: last_complete + 1] + "\n]"
            end = len(text) - 1
        else:
            return None

    return text[start : end + 1]


def safe_parse_json_array(text):
    """
    Safely parse JSON array with multiple fallback strategies
    """
    if not text or not text.strip():
        return []

    # Strategy 1: Try direct parse
    try:
        result = json.loads(text.strip())
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # Strategy 2: Extract JSON array substring
    json_str = extract_json_array(text)
    if json_str:
        try:
            result = json.loads(json_str)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    # Strategy 3: Try to fix common JSON issues
    if json_str:
        # Remove trailing commas before closing braces/brackets
        fixed = re.sub(r",(\s*[}\]])", r"\1", json_str)
        try:
            result = json.loads(fixed)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    # Strategy 4: Parse line by line for individual objects
    results = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("{") and line.endswith("},"):
            line = line.rstrip(",")
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and "keyword" in obj and "genre" in obj:
                    results.append(obj)
            except json.JSONDecodeError:
                continue

    if results:
        return results

    # Last resort: return empty list instead of raising error
    print(f"WARNING: Could not parse LLM response. Returning empty list.")
    print(f"Response preview: {text[:500]}...")
    return []


def call_llm_batch(client, keywords):
    """Send ONE batch of keywords to Groq, return a list of dicts."""
    user_prompt = BATCH_TEMPLATE.format(
        keywords_json=json.dumps(keywords, ensure_ascii=False)
    )

    try:
        response = client.chat.completions.create(
            model="qwen/qwen3-32b",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_tokens=8000,  # Increased to handle larger responses
        )

        text = response.choices[0].message.content.strip()

        # Use safe parsing
        results = safe_parse_json_array(text)

        # Remove duplicates while preserving order
        seen = set()
        unique_results = []
        for item in results:
            keyword = item.get("keyword")
            if keyword and keyword not in seen:
                seen.add(keyword)
                unique_results.append(item)

        return unique_results

    except Exception as e:
        print(f"ERROR calling LLM: {e}")
        return []


def classify_keywords_batchwise(keywords, batch_size):
    """
    Break keywords into batches, call the LLM, and combine results.
    """
    client = get_client()
    final_mapping = {}

    for i in range(0, len(keywords), batch_size):
        batch = keywords[i : i + batch_size]
        print(f"Processing batch {i//batch_size + 1} ({len(batch)} keywords)...")

        results = call_llm_batch(client, batch)
        print(f"  Received {len(results)} classifications")

        for item in results:
            k = item.get("keyword")
            g = item.get("genre")
            if k and g:
                final_mapping[k] = g

    return final_mapping
