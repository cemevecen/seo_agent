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
    assert subj.startswith("3 ·")
    assert "alarm" not in subj.lower()
    assert "Fibabanka" in subj
    assert "doviz" not in subj.lower()
    assert "sinemalar" not in subj.lower()


def _ready_mailer(monkeypatch, sent_subjects: list[str]) -> None:
    mailer._last_realtime_batch_sent_at = None
    monkeypatch.setattr(mailer.settings, "ga4_realtime_email_batch_interval_minutes", 0)
    monkeypatch.setattr(mailer.settings, "ga4_realtime_email_enabled", True)
    monkeypatch.setattr(mailer.settings, "mail_to", "ops@example.com")
    monkeypatch.setattr(mailer.settings, "mail_from", "seo@example.com")
    monkeypatch.setattr(mailer, "_smtp_configured", lambda: True)
    monkeypatch.setattr(mailer, "smtp_recipients_allowed", lambda _count: True)
    monkeypatch.setattr(mailer, "_smtp_dispatch_with_daily_quota", lambda _message: False)

    def _fake_gmail_dispatch(message, db=None):
        sent_subjects.append(str(message["Subject"]))
        return True

    monkeypatch.setattr(mailer, "_gmail_api_dispatch", _fake_gmail_dispatch)


def test_realtime_mail_blocks_non_seo_realtime_subject(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)

    ok = mailer.send_realtime_email("doviz.com — +120 kul [web]", "<p>alarm</p>")

    assert ok is False
    assert sent == []


def test_realtime_news_mail_blocks_non_seo_realtime_subject(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)

    ok = mailer.send_realtime_news_email("sinemalar.com — haber +80 [mweb]", "<p>alarm</p>")

    assert ok is False
    assert sent == []


def test_realtime_batch_single_alarm_sends_seo_realtime_subject(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)

    mailer.realtime_email_batch_begin()
    assert mailer.send_realtime_email("doviz.com — +120 kul [web]", "<p>alarm</p>") is True
    assert mailer.realtime_email_batch_flush() is True

    assert len(sent) == 1
    assert "SEO Realtime:" not in sent[0]
    assert "alarm" not in sent[0].lower()


def test_realtime_batch_multiple_alarms_sends_single_seo_realtime_subject(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)

    mailer.realtime_email_batch_begin()
    mailer.send_realtime_email("doviz.com — +120 kul [web]", "<p>alarm 1</p>")
    mailer.send_realtime_news_email("sinemalar.com — haber +80 [mweb]", "<p>alarm 2</p>")
    assert mailer.realtime_email_batch_flush() is True

    assert len(sent) == 1
    assert sent[0].startswith("2 ·")
    assert "alarm" not in sent[0].lower()
