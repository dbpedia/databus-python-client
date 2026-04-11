import json
from databusclient.api.download import _matches_filters, _get_file_download_urls_from_artifact_jsonld

def test_matches_filters_none():
    node = {"@type": "Part", "file": "http://example.org/file.ttl.gz", "formatExtension": "ttl", "compression": "gz"}
    assert _matches_filters(node, None) is True
    assert _matches_filters(node, []) is True

def test_matches_filters_format():
    node = {"@type": "Part", "formatExtension": "ttl"}
    assert _matches_filters(node, [".ttl"]) is True
    assert _matches_filters(node, [".nt"]) is False

def test_matches_filters_compression():
    node = {"@type": "Part", "compression": "gz"}
    assert _matches_filters(node, ["..gz"]) is True
    assert _matches_filters(node, ["..bz2"]) is False

def test_matches_filters_cv_key_value():
    node = {"@type": "Part", "dcv:type": "gen", "dataid-cv:lang": "en"}
    assert _matches_filters(node, ["type=gen"]) is True
    assert _matches_filters(node, ["lang=en"]) is True
    assert _matches_filters(node, ["type=parsed"]) is False

def test_matches_filters_cv_value_only():
    node = {"@type": "Part", "dcv:type": "gen", "dataid-cv:lang": "en"}
    assert _matches_filters(node, ["gen"]) is True
    assert _matches_filters(node, ["en"]) is True
    assert _matches_filters(node, ["fr"]) is False

def test_matches_filters_multiple():
    node = {
        "@type": "Part", 
        "formatExtension": "ttl", 
        "compression": "gz", 
        "dcv:type": "gen"
    }
    assert _matches_filters(node, [".ttl", "..gz", "type=gen"]) is True
    assert _matches_filters(node, [".ttl", "..bz2"]) is False
    assert _matches_filters(node, [".nt", "..gz"]) is False

def test_get_urls_with_filters():
    json_data = {
        "@graph": [
            {"@type": "Part", "file": "url1", "formatExtension": "ttl", "dcv:type": "gen"},
            {"@type": "Part", "file": "url2", "formatExtension": "nt", "dcv:type": "gen"},
            {"@type": "Part", "file": "url3", "formatExtension": "ttl", "dcv:type": "parsed"},
        ]
    }
    json_str = json.dumps(json_data)
    
    assert _get_file_download_urls_from_artifact_jsonld(json_str, [".ttl"]) == ["url1", "url3"]
    assert _get_file_download_urls_from_artifact_jsonld(json_str, ["type=gen"]) == ["url1", "url2"]
    assert _get_file_download_urls_from_artifact_jsonld(json_str, [".ttl", "type=gen"]) == ["url1"]
