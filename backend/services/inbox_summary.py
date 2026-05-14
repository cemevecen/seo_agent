import logging
from datetime import datetime
from sqlalchemy.orm import Session
from backend.models import SupportInboxThread, SupportInboxMessage
from backend.services import inbox_sync, mailer

logger = logging.getLogger(__name__)

def run_inbox_summary_job(db: Session):
    """
    1. Her saat başı inbox senkronize edilir.
    2. Okunmamış (gmail_unread=True) olan mesajlar toplanır.
    3. Özet rapor oluşturulur ve mail atılır.
    """
    logger.info("Starting hourly inbox summary job...")
    
    # 1. Sync inbox (DB'nin güncel olduğundan emin olalım)
    try:
        # Senkronizasyon yaparken max_threads'i biraz yüksek tutalım ki yeni gelenleri kaçırmasın
        inbox_sync.sync_inbox_threads(db, max_threads=60)
    except Exception as exc:
        logger.warning("Inbox summary sync failed (continuing with local data): %s", exc)

    # 2. Okunmamış mesajları sorgula
    # Hedef etiketler: info (doviz), sinemalar, feedback ve mixed (birden fazla hesaba gelenler)
    target_tags = ["info", "sinemalar", "feedback", "mixed"]
    unread_threads = (
        db.query(SupportInboxThread)
        .filter(SupportInboxThread.gmail_unread == True)
        .filter(SupportInboxThread.route_tag.in_(target_tags))
        .order_by(SupportInboxThread.last_internal_ms.desc())
        .all()
    )

    logger.info("Unread threads found: %d (tags: %s)", len(unread_threads), target_tags)

    if not unread_threads:
        logger.info("No unread threads found for summary. Skipping email.")
        return

    # 3. İstatistikleri ve Raporu Hazırla
    counts = {"info": 0, "sinemalar": 0, "feedback": 0, "mixed": 0}
    for t in unread_threads:
        if t.route_tag in counts:
            counts[t.route_tag] += 1

    lines = []
    lines.append("<div style='font-family: sans-serif; color: #333;'>")
    lines.append(f"<h2 style='color: #2563eb;'>📬 Okunmamış Mesaj Özeti</h2>")
    
    # Hesap bazlı özet
    lines.append("<div style='background: #f8fafc; padding: 15px; border-radius: 8px; margin-bottom: 20px;'>")
    lines.append(f"<b>info@doviz.com:</b> {counts['info']} mesaj<br/>")
    lines.append(f"<b>info@sinemalar.com:</b> {counts['sinemalar']} mesaj<br/>")
    lines.append(f"<b>feedback@doviz.com:</b> {counts['feedback']} mesaj<br/>")
    if counts["mixed"] > 0:
        lines.append(f"<b>Birden Fazla / Karma:</b> {counts['mixed']} mesaj<br/>")
    lines.append(f"<p><b>Toplam: {len(unread_threads)} okunmamış konuşma.</b></p>")
    lines.append("</div>")

    lines.append("<h3>Mesaj Listesi</h3>")
    lines.append("<ul style='list-style: none; padding: 0;'>")
    
    for t in unread_threads:
        # Gelen en son mesajı bul (giden mesajları atla)
        latest_inbound = (
            db.query(SupportInboxMessage)
            .filter(SupportInboxMessage.thread_id == t.id)
            .filter(SupportInboxMessage.is_outbound == False)
            .order_by(SupportInboxMessage.internal_ms.desc())
            .first()
        )
        
        sender = latest_inbound.from_addr if latest_inbound else "Bilinmiyor"
        date_str = ""
        if latest_inbound:
            try:
                dt = datetime.fromtimestamp(latest_inbound.internal_ms / 1000.0)
                date_str = dt.strftime("%d.%m %H:%M")
            except:
                date_str = "Bilinmiyor"
        
        tag_label = {
            "info": "info@doviz.com",
            "sinemalar": "info@sinemalar.com",
            "feedback": "feedback@doviz.com",
            "mixed": "Çoklu Hesap"
        }.get(t.route_tag, t.route_tag)
        
        lines.append(
            f"<li style='border-bottom: 1px solid #e2e8f0; padding: 10px 0;'>"
            f"<span style='background: #e0f2fe; color: #0369a1; padding: 2px 6px; border-radius: 4px; font-size: 12px; font-weight: bold;'>{tag_label}</span> "
            f"<b style='font-size: 15px;'>{t.subject}</b><br/>"
            f"<span style='color: #64748b; font-size: 13px;'>Kimden: {sender} | Tarih: {date_str}</span><br/>"
            f"<div style='color: #475569; font-size: 14px; margin-top: 5px; padding-left: 10px; border-left: 3px solid #cbd5e1;'>{t.snippet}</div>"
            f"</li>"
        )
    
    lines.append("</ul>")
    lines.append("<p style='margin-top: 30px; font-size: 12px; color: #94a3b8; border-top: 1px solid #e2e8f0; padding-top: 10px;'>"
                 "Bu e-posta SEO Agent tarafından saatlik olarak otomatik üretilmiştir.</p>")
    lines.append("</div>")
    
    html_body = "\n".join(lines)
    subject = f"Inbox Özeti: {len(unread_threads)} Yeni Mesaj"
    
    # 4. E-postayı gönder (Klasik sistemimiz: mailer.send_email -> Gmail API)
    ok = mailer.send_email(subject, html_body)
    if ok:
        logger.info("Inbox summary email sent successfully.")
    else:
        logger.error("Failed to send inbox summary email.")
