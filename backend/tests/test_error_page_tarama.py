from backend.services.error_page_tarama import classify_csv_anomalies, is_http_error_probe, probes_to_error_dicts


def _fail(url: str, status: int, kind: str = "http_error") -> dict:
    return {"url": url, "http_status": status, "kind": kind, "ok": False, "message": kind}


def test_stable_404s_do_not_mail():
    prev = [_fail(f"https://kur.doviz.com/a{i}", 404) for i in range(20)]
    cur = list(prev)
    out = classify_csv_anomalies(cur, prev, url_count=100, prev_url_count=100, prev_failure_count=20)
    assert out["should_mail"] is False
    assert out["items"] == []


def test_new_5xx_mails():
    prev = [_fail("https://kur.doviz.com/ok-now-404", 404)]
    cur = prev + [_fail("https://kur.doviz.com/down", 500)]
    out = classify_csv_anomalies(cur, prev, url_count=50, prev_url_count=50, prev_failure_count=1)
    assert out["should_mail"] is True
    assert out["new_5xx"] == 1
    assert any("5xx" in r for r in out["reasons"])


def test_new_404_burst_mails():
    prev = []
    cur = [_fail(f"https://kur.doviz.com/x{i}", 404) for i in range(8)]
    # ilk tarama + düşük oran → mail yok
    out = classify_csv_anomalies(cur, prev, url_count=200, prev_url_count=0, prev_failure_count=0)
    assert out["first_scan"] is True
    assert out["should_mail"] is False

    prev = [_fail("https://kur.doviz.com/old", 404)]
    cur = prev + [_fail(f"https://kur.doviz.com/n{i}", 404) for i in range(6)]
    out = classify_csv_anomalies(cur, prev, url_count=200, prev_url_count=200, prev_failure_count=1)
    assert out["new_404"] == 6
    assert out["should_mail"] is True


def test_prices_empty_needs_cluster():
    prev = []
    cur = [
        {"url": f"https://altin.doviz.com/b{i}", "http_status": 200, "kind": "prices_empty", "ok": False}
        for i in range(3)
    ]
    out = classify_csv_anomalies(cur, prev, url_count=80, prev_url_count=80, prev_failure_count=0)
    assert out["should_mail"] is False


def test_probes_to_error_dicts_skips_prices_empty():
    probes = [
        _fail("https://kur.doviz.com/a", 404),
        {"url": "https://altin.doviz.com/b", "http_status": 200, "kind": "prices_empty", "ok": False},
        _fail("https://m.doviz.com/c", 502),
        {"url": "https://kur.doviz.com/ok", "http_status": 200, "kind": "ok", "ok": True},
    ]
    rows = probes_to_error_dicts(probes)
    urls = {r["url"] for r in rows}
    assert "https://kur.doviz.com/a" in urls
    assert "https://m.doviz.com/c" in urls
    assert "https://altin.doviz.com/b" not in urls
    assert is_http_error_probe(probes[1]) is False
