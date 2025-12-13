from src.llm.ask_llm import classify_keywords_batchwise


def test_classify_batchwise_monkeypatch(monkeypatch):
    # mock get_client so no real API key is required
    monkeypatch.setattr(
        "src.llm.ask_llm.get_client",
        lambda: "fake-client",
    )

    # mock call_llm_batch to return deterministic output
    monkeypatch.setattr(
        "src.llm.ask_llm.call_llm_batch",
        lambda client, keywords: [{"keyword": keywords[0], "genre": "Action"}],
    )

    res = classify_keywords_batchwise(["k1", "k2"], batch_size=2)

    assert res == {"k1": "Action"}
