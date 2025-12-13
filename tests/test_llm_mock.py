# tests/test_llm_mock.py
from src.llm.ask_llm import classify_keywords_batchwise


def test_classify_batchwise_monkeypatch(monkeypatch):
    # monkeypatch call_llm_batch to return a deterministic result
    monkeypatch.setattr(
        "src.llm.ask_llm.call_llm_batch",
        lambda keywords: [{"keyword": keywords[0], "genre": "Action"}],
    )
    res = classify_keywords_batchwise(["k1", "k2"], batch_size=2)
    assert res["k1"] == "Action"
