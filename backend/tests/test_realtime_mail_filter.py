"""Realtime mail subject allow-list behavior."""

from backend.services import mailer


def _ready_mailer(monkeypatch, sent_subjects: list[str]) -> None:
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
    assert sent[0].startswith("SEO Realtime: 1 alarm")


def test_realtime_batch_multiple_alarms_sends_single_seo_realtime_subject(monkeypatch):
    sent: list[str] = []
    _ready_mailer(monkeypatch, sent)

    mailer.realtime_email_batch_begin()
    mailer.send_realtime_email("doviz.com — +120 kul [web]", "<p>alarm 1</p>")
    mailer.send_realtime_news_email("sinemalar.com — haber +80 [mweb]", "<p>alarm 2</p>")
    assert mailer.realtime_email_batch_flush() is True

    assert len(sent) == 1
    assert sent[0].startswith("SEO Realtime: 2 alarm")
