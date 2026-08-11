"""GA4 Realtime 404 spike yardımcı birim testleri."""

from backend.services.ga4_realtime import (
    _evaluate_404_spike_severity,
    _is_rt_404_page,
    _realtime_email_thread_key,
    alarm_worthy_for_email,
)


def test_is_rt_404_page_title_and_path():
    assert _is_rt_404_page("Sayfa Bulunamadı - Sinemalar.com") is True
    assert _is_rt_404_page("Güller ve Günahlar Oyuncuları") is False
    assert _is_rt_404_page("Some page", ["/mobileweb/movieCast/291941"]) is False
    assert _is_rt_404_page("Error", ["/404"]) is True


def test_evaluate_404_spike_steady_baseline_no_alert():
    # Sinemalar mweb: sürekli ~380 kul. 404 — spike değil
    assert _evaluate_404_spike_severity(384, 380, warn_threshold=10, crit_threshold=25) is None
    assert _evaluate_404_spike_severity(390, 384, warn_threshold=10, crit_threshold=25) is None


def test_realtime_email_thread_key_normalizes_domain():
    assert _realtime_email_thread_key("www.doviz.com", "mweb") == "doviz.com.mweb"
    assert _realtime_email_thread_key("www.sinemalar.com", "web") == "sinemalar.com.web"


def test_evaluate_404_spike_real_increase():
    # +14 / 370 ≈ %4 — spam değil
    assert _evaluate_404_spike_severity(384, 370, warn_threshold=10, crit_threshold=25) is None
    # Küçük hacim 50←10: uyarı olabilir, kritik değil (delta 40 < crit 50)
    assert _evaluate_404_spike_severity(50, 10, warn_threshold=10, crit_threshold=25) == "warning"
    assert _evaluate_404_spike_severity(5, 0, warn_threshold=10, crit_threshold=25) is None
    # Gerçek patlama: 80 → 200
    assert _evaluate_404_spike_severity(200, 80, warn_threshold=40, crit_threshold=80) == "critical"


def test_404_warning_not_emailed():
    assert alarm_worthy_for_email({"rule_id": "rt_404_warning", "severity": "warning"}) is False
    assert alarm_worthy_for_email({"rule_id": "rt_404_critical", "severity": "critical"}) is True
