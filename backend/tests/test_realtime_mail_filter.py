"""Realtime mail subject allow-list behavior."""

from backend.services import mailer


def test_compact_batch_chip_page_alarm():
    raw = "doviz — Fibabanka Altın Fiyatları -9 · Canlı Çeyrek -6"
    chip = mailer._compact_realtime_batch_chip(raw)
    assert "doviz" not in chip.lower()
    assert "Fibabanka" in chip


def test_compact_batch_chip_mweb_profile():
    raw = "doviz — Harem Euro Kuru -13 [mweb]"
    chip = mailer._compact_realtime_batch_chip(raw)
    assert "doviz" not in chip.lower()
    assert "Harem" in chip
    assert "[mweb]" in chip


def test_combined_subject_phone_preview():
    items = [
        ("doviz — Fibabanka Altın -21", "<p></p>"),
        ("doviz — Harem Euro -13 [mweb]", "<p></p>"),
        ("sinemalar — Yeni film ↑40 [mweb]", "<p></p>"),
    ]
    subj = mailer._combined_realtime_subject(items)
    assert subj.lower().startswith("seo realtime")
    assert "2 ·" in subj or "3 ·" in subj


def _ready_mailer(monkeypatch, sent_subjects: list[str]) -> None:
    mailer._last_realtime_batch_sent_at = None
    mailer._pending_realtime_batch_items = []
    monkeypatch.setattr(mailer.settings, "ga4_realtime_email_batch_interval_minutes", 0)
    monkeypatch.setattr(mailer.settings, "ga4_realtime_email_enabled", True)
    monkeypatch.setattr(mailer.settings, "mail_to", "ops@nokta.com")
    monkeypatch.setattr(mailer.settings, "mail_from", "seo@nokta.com")
    monkeypatch.setattr(mailer.settings, "outbound_gmail_api_enabled", True)
    monkeypatch.setattr(mailer, "_smtp_configured", lambda: True)
    monkeypatch.setattr(mailer, "_realtime_outbound_transport_ready", lambda: True)
    monkeypatch.setattr(mailer, "_gmail_oauth_outbound_ready", lambda: True)
    monkeypatch.setattr(mailer, "smtp_recipients_allowed", lambda _count: True)
    monkeypatch.setattr(mailer, "_smtp_dispatch_with_daily_quota", lambda _message: False)
    monkeypatch.setattr(mailer, "_realtime_digest_in_quiet_hours", lambda: False)

    def _fake_gmail_dispatch(message, db=None):
        sent_subjects.append(str(message["Subject"]))
        return True

    monkeypatch.setattr(mailer, "_gmail_api_dispatch", _fake_gmail_dispatch)

    class _FakeSession:
        def __enter__(self):
            return object()

        def __exit__(self, *args):
            return False

    monkeypatch.setattr("backend.database.SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(
        "backend.services.ga4_realtime.build_realtime_periodic_digest_html",
        lambda _db, queued_alarm_sections=0: f"<p>digest queued={queued_alarm_sections}</p>",
    )
    monkeypatch.setattr(
        "backend.services.ga4_realtime.realtime_periodic_digest_subject",
        lambda: "SEO Realtime · 1,5s özet · test",
    )


def test_realtime_batch_flush_sends_periodic_digest(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)

    mailer.realtime_email_batch_begin()
    assert mailer.send_realtime_email("doviz.com — +120 kul [web]", "<p>alarm</p>") is True
    assert mailer.realtime_email_batch_flush() is True

    assert len(sent) == 1
    assert sent[0].lower().startswith("seo realtime · 1,5s özet")


def test_realtime_batch_deferred_items_queued_not_dropped(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)
    mailer._last_realtime_batch_sent_at = mailer.time.time()
    monkeypatch.setattr(mailer.settings, "ga4_realtime_email_batch_interval_minutes", 90)

    mailer.realtime_email_batch_begin()
    mailer.send_realtime_email("doviz.com — +120 kul [web]", "<p>alarm</p>")
    assert mailer.realtime_email_batch_flush() is False
    assert sent == []
    assert mailer.realtime_email_batch_is_collecting()
    assert len(getattr(mailer._batch_ctx, "items", [])) == 1

    monkeypatch.setattr(mailer.settings, "ga4_realtime_email_batch_interval_minutes", 0)
    mailer._last_realtime_batch_sent_at = None
    assert mailer.realtime_email_batch_flush() is True
    assert len(sent) == 1
    assert mailer._pending_realtime_batch_items == []


def test_realtime_batch_not_sent_in_quiet_hours(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)
    monkeypatch.setattr(mailer, "_realtime_digest_in_quiet_hours", lambda: True)

    mailer.realtime_email_batch_begin()
    mailer.send_realtime_email("doviz.com — +120 kul [web]", "<p>alarm</p>")
    assert mailer.realtime_email_batch_flush() is False
    assert sent == []
