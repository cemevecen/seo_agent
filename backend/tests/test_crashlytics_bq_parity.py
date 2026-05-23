"""Crash-free ve iOS/Android parity teşhis testleri."""

from unittest.mock import patch

from backend.services import crashlytics_bq as cbq


def test_query_crash_free_uses_sessions_when_available():
    sessions_result = {
        "total_users": 1000,
        "crashed_users": 5,
        "crash_free_pct": 99.5,
        "method": "firebase_sessions",
    }
    with patch.object(cbq, "_sessions_table_ref", return_value="`proj.firebase_sessions.app_IOS`"):
        with patch.object(cbq, "_batch_table_ref", return_value="`proj.firebase_crashlytics.app_IOS`"):
            with patch.object(cbq, "_query_crash_free_sessions", return_value=sessions_result) as mock_sess:
                out = cbq.query_crash_free("ios", "(SELECT 1)", 7, bundle="com.example.app")
    assert out == sessions_result
    mock_sess.assert_called_once()
    assert "firebase_crashlytics" in mock_sess.call_args[0][1]


def test_query_crash_free_hides_unreliable_legacy():
    legacy = {"crash_free_pct": 0.5, "method": "crashes_only_unreliable"}
    with patch.object(cbq, "_sessions_table_ref", return_value=None):
        with patch.object(cbq, "_query_crash_free_crashes_only", return_value=legacy):
            assert cbq.query_crash_free("ios", "`tbl`", 7, bundle="com.example.app") is None


def test_query_crash_free_prefers_batch_for_legacy():
    legacy = {"crash_free_pct": 98.0, "method": "crashes_only_unreliable"}
    with patch.object(cbq, "_sessions_table_ref", return_value=None):
        with patch.object(cbq, "_batch_table_ref", return_value="`proj.firebase_crashlytics.app_ANDROID`"):
            with patch.object(cbq, "_query_crash_free_crashes_only", return_value=legacy) as mock_legacy:
                out = cbq.query_crash_free("android", "(UNION ALL)", 7, bundle="com.Doviz")
    assert out == legacy
    assert mock_legacy.call_args[0][1] == "`proj.firebase_crashlytics.app_ANDROID`"


def test_analyze_platform_parity_findings_sessions_missing():
    android_health = {"event_count": 1000, "session_id_coverage_pct": 80, "active_days": 7, "affected_users": 500}
    ios_health = {"event_count": 90, "session_id_coverage_pct": 20, "active_days": 3, "affected_users": 40}

    def health_side_effect(plat, _table, _days):
        return ios_health if plat == "ios" else android_health

    with patch.object(cbq, "platform_ready", return_value=True):
        with patch.object(cbq, "_circuit_open", return_value=False):
            with patch.object(cbq, "_union_incompat", side_effect=lambda p: p == "ios"):
                with patch.object(cbq, "_discover_table_id", return_value="com_Doviz_ANDROID"):
                    with patch.object(cbq, "_sessions_dataset_exists", return_value=False):
                        with patch.object(cbq, "_discover_sessions_table_id", return_value=None):
                            with patch.object(cbq, "_table", return_value="`tbl`"):
                                with patch.object(cbq, "_batch_table_ref", return_value="`batch`"):
                                    with patch.object(cbq, "query_summary", return_value={"fatal": 1}):
                                        with patch.object(cbq, "query_table_health_stats", side_effect=health_side_effect):
                                            with patch.object(cbq, "query_crash_free", return_value=None):
                                                with patch.object(cbq, "_query_crash_free_crashes_only", return_value={"crash_free_pct": 0}):
                                                    report = cbq.analyze_platform_parity("doviz", days=7)

    assert report["comparison"]["ios_vs_android_pct"] == 9.0
    findings = " ".join(report["findings"])
    assert "firebase_sessions" in findings
    assert "realtime" in findings.lower() or "UNION" in findings


def test_list_crashlytics_products_excludes_unconfigured():
    with patch.object(cbq, "crashlytics_product_ready", side_effect=lambda p: p == "doviz"):
        assert cbq.list_crashlytics_products() == [{"id": "doviz", "label": "Döviz"}]


def test_crashlytics_product_ready_requires_discovered_table():
    def discover(plat, bundle):
        if bundle == "com.Doviz" and plat == "android":
            return "com_Doviz_ANDROID"
        return None

    with patch.object(cbq, "platform_ready", return_value=True):
        with patch.object(cbq, "_discover_table_id", side_effect=discover):
            assert cbq.crashlytics_product_ready("doviz") is True
            assert cbq.crashlytics_product_ready("sinemalar") is False


def test_crash_free_unavailable_hint_no_sessions_export():
    with patch.object(cbq, "_sessions_dataset_exists", return_value=False):
        msg = cbq.crash_free_unavailable_hint("android", "`tbl`", 7, bundle="com.Doviz")
    assert "Include sessions" in msg
    assert "ANDROID" in msg


def test_crash_free_unavailable_hint_dataset_exists_table_mismatch():
    with patch.object(cbq, "_sessions_dataset_exists", return_value=True):
        with patch.object(cbq, "_sessions_table_ref", return_value=None):
            with patch.object(cbq, "_list_sessions_tables", return_value=["com_other_ANDROID"]):
                with patch.object(cbq, "_get_bq_dataset_location", return_value="US"):
                    msg = cbq.crash_free_unavailable_hint("android", "`tbl`", 7, bundle="com.Doviz")
    assert "eşleşmedi" in msg
    assert "Include sessions" not in msg


def test_crash_free_unavailable_hint_short_period():
    with patch.object(cbq, "_sessions_dataset_exists", return_value=True):
        with patch.object(cbq, "_sessions_table_ref", return_value="`proj.firebase_sessions.app`"):
            with patch.object(cbq, "_discover_sessions_table_id", return_value="com_Doviz_ANDROID"):
                with patch.object(cbq, "_query_sessions_volume", return_value=(2, 1)):
                    msg = cbq.crash_free_unavailable_hint("android", "`tbl`", 1, bundle="com.Doviz")
    assert "1 günde" in msg or "1g" in msg
    assert "7g" in msg or "30g" in msg
    assert "Include sessions" not in msg


def test_min_sessions_for_crash_free_scales_with_days():
    assert cbq._min_sessions_for_crash_free(1) == 3
    assert cbq._min_sessions_for_crash_free(7) == 10
    assert cbq._min_sessions_for_crash_free(30) == 10


def test_analyze_platform_parity_not_configured_product():
    with patch.object(cbq, "crashlytics_product_ready", return_value=False):
        report = cbq.analyze_platform_parity("sinemalar", days=7)
    assert report["ok"] is False
    assert report["error"] == "crashlytics_not_configured"
    assert report["findings"]
