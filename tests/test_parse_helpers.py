# tests/test_parse_helpers.py
from src.utils.parse_helpers import extract_first_json_array, parse_json_array_from_text


def test_extract_simple_array():
    text = 'Info\n [ {"keyword":"a","genre":"Action"} ] \n footer'
    arr = extract_first_json_array(text)
    assert arr is not None
    parsed = parse_json_array_from_text(text)
    assert isinstance(parsed, list)
    assert parsed[0]["keyword"] == "a"
