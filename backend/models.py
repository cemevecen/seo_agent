"""Aşama 2 için temel SQLAlchemy modelleri."""

from datetime import date, datetime

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, LargeBinary, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.database import Base


class Site(Base):
    """İzlenecek web sitesi bilgilerini tutar."""

    __tablename__ = "sites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    domain: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    credentials: Mapped[list["SiteCredential"]] = relationship(
        "SiteCredential", back_populates="site", cascade="all, delete-orphan"
    )
    metrics: Mapped[list["Metric"]] = relationship(
        "Metric", back_populates="site", cascade="all, delete-orphan"
    )
    alerts: Mapped[list["Alert"]] = relationship(
        "Alert", back_populates="site", cascade="all, delete-orphan"
    )
    api_usages: Mapped[list["ApiUsage"]] = relationship(
        "ApiUsage", back_populates="site", cascade="all, delete-orphan"
    )
    collector_runs: Mapped[list["CollectorRun"]] = relationship(
        "CollectorRun", back_populates="site", cascade="all, delete-orphan"
    )
    pagespeed_payload_snapshots: Mapped[list["PageSpeedPayloadSnapshot"]] = relationship(
        "PageSpeedPayloadSnapshot", back_populates="site", cascade="all, delete-orphan"
    )
    lighthouse_audit_records: Mapped[list["LighthouseAuditRecord"]] = relationship(
        "LighthouseAuditRecord", back_populates="site", cascade="all, delete-orphan"
    )
    search_console_query_snapshots: Mapped[list["SearchConsoleQuerySnapshot"]] = relationship(
        "SearchConsoleQuerySnapshot", back_populates="site", cascade="all, delete-orphan"
    )
    crux_history_snapshots: Mapped[list["CruxHistorySnapshot"]] = relationship(
        "CruxHistorySnapshot", back_populates="site", cascade="all, delete-orphan"
    )
    url_inspection_snapshots: Mapped[list["UrlInspectionSnapshot"]] = relationship(
        "UrlInspectionSnapshot", back_populates="site", cascade="all, delete-orphan"
    )
    ga4_report_snapshots: Mapped[list["Ga4ReportSnapshot"]] = relationship(
        "Ga4ReportSnapshot", back_populates="site", cascade="all, delete-orphan"
    )
    url_audit_records: Mapped[list["UrlAuditRecord"]] = relationship(
        "UrlAuditRecord", back_populates="site", cascade="all, delete-orphan"
    )
    external_profile: Mapped["ExternalSite | None"] = relationship(
        "ExternalSite", back_populates="site", cascade="all, delete-orphan", uselist=False
    )
    external_onboarding_jobs: Mapped[list["ExternalOnboardingJob"]] = relationship(
        "ExternalOnboardingJob", back_populates="site", cascade="all, delete-orphan"
    )


class SiteCredential(Base):
    """Siteye ait API credential verisini şifreli olarak saklar."""

    __tablename__ = "site_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    credential_type: Mapped[str] = mapped_column(String(100), nullable=False)
    encrypted_data: Mapped[str] = mapped_column(Text, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="credentials")


class ExternalSite(Base):
    """Search Console bagimsiz, external crawler profilini isaretler."""

    __tablename__ = "external_sites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(
        ForeignKey("sites.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="external_profile")


class ExternalOnboardingJob(Base):
    """External onboarding ilerleme durumunu processler arasi kalici saklar."""

    __tablename__ = "external_onboarding_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    domain: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", index=True)
    percent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    detail: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    site: Mapped["Site"] = relationship("Site", back_populates="external_onboarding_jobs")


class Metric(Base):
    """Toplanan SEO metrik değerlerini tutar."""

    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    # GA4 kanal anahtarları uzun olabiliyor; 100 char kesilince last/prev çakışıp %0 üretebiliyordu.
    metric_type: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="metrics")


class AppStoreRankSnapshot(Base):
    """App Intel kategori sıra trendi için mağaza anlık kayıtları."""

    __tablename__ = "app_store_rank_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    platform: Mapped[str] = mapped_column(String(10), nullable=False, index=True)  # android | ios
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    category_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    chart: Mapped[str | None] = mapped_column(String(100), nullable=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class PageSpeedAuditSnapshot(Base):
    """PageSpeed/Lighthouse audit detaylarını strategy bazında saklar."""

    __tablename__ = "pagespeed_audit_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    strategy: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    analysis_json: Mapped[str] = mapped_column(Text, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class CollectorRun(Base):
    """Her harici veri toplama akisi icin izleme ve hata kaydi tutar."""

    __tablename__ = "collector_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    strategy: Mapped[str] = mapped_column(String(20), nullable=False, default="all", index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="started", index=True)
    target_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    summary_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    requested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    site: Mapped["Site"] = relationship("Site", back_populates="collector_runs")


class PageSpeedPayloadSnapshot(Base):
    """Ham PageSpeed API payload'ini strategy bazinda saklar."""

    __tablename__ = "pagespeed_payload_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    collector_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("collector_runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    strategy: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site", back_populates="pagespeed_payload_snapshots")


class LighthouseAuditRecord(Base):
    """Her Lighthouse denetimini normalize satirlar halinde saklar."""

    __tablename__ = "lighthouse_audit_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    collector_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("collector_runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    strategy: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    section_key: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    section_title_en: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    section_title_tr: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    audit_id: Mapped[str] = mapped_column(String(150), nullable=False, index=True)
    audit_state: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    priority: Mapped[str] = mapped_column(String(30), nullable=False, default="MEDIUM")
    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    score_display_mode: Mapped[str] = mapped_column(String(50), nullable=False, default="")
    title_en: Mapped[str] = mapped_column(Text, nullable=False)
    title_tr: Mapped[str] = mapped_column(Text, nullable=False, default="")
    display_value: Mapped[str] = mapped_column(Text, nullable=False, default="")
    problem_en: Mapped[str] = mapped_column(Text, nullable=False, default="")
    problem_tr: Mapped[str] = mapped_column(Text, nullable=False, default="")
    impact_en: Mapped[str] = mapped_column(Text, nullable=False, default="")
    impact_tr: Mapped[str] = mapped_column(Text, nullable=False, default="")
    examples_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    solution_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    expected_result_en: Mapped[str] = mapped_column(Text, nullable=False, default="")
    expected_result_tr: Mapped[str] = mapped_column(Text, nullable=False, default="")
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site", back_populates="lighthouse_audit_records")


class SearchConsoleQuerySnapshot(Base):
    """Canli Search Console satirlarini buyuk hacimde saklar."""

    __tablename__ = "search_console_query_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    collector_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("collector_runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    property_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    data_scope: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    device: Mapped[str] = mapped_column(String(30), nullable=False, default="ALL", index=True)
    clicks: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    impressions: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    ctr: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    position: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    start_date: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    end_date: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site", back_populates="search_console_query_snapshots")


class Ga4ReportSnapshot(Base):
    """GA4 profil bazlı özet KPI, sayfa ve kaynak kırılımları (JSON)."""

    __tablename__ = "ga4_report_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    collector_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("collector_runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    profile: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    period_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30, index=True)
    last_start: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    last_end: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    prev_start: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    prev_end: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site", back_populates="ga4_report_snapshots")


class CruxHistorySnapshot(Base):
    """CrUX History API yanitlarini form factor bazinda saklar."""

    __tablename__ = "crux_history_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    collector_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("collector_runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    form_factor: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    target_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    summary_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site", back_populates="crux_history_snapshots")


class UrlInspectionSnapshot(Base):
    """Google index durumu, canonical ve crawl bilgisini saklar."""

    __tablename__ = "url_inspection_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    collector_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("collector_runs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    inspection_url: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    property_url: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    verdict: Mapped[str] = mapped_column(String(100), nullable=False, default="", index=True)
    coverage_state: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    indexing_state: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    page_fetch_state: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    robots_txt_state: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    google_canonical: Mapped[str] = mapped_column(Text, nullable=False, default="")
    user_canonical: Mapped[str] = mapped_column(Text, nullable=False, default="")
    last_crawl_time: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    summary_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site", back_populates="url_inspection_snapshots")


class Alert(Base):
    """Site bazlı alarm eşiklerini tutar."""

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    alert_type: Mapped[str] = mapped_column(String(100), nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="alerts")
    logs: Mapped[list["AlertLog"]] = relationship(
        "AlertLog", back_populates="alert", cascade="all, delete-orphan"
    )


class AlertLog(Base):
    """Tetiklenen alarm kayıtlarını tutar."""

    __tablename__ = "alert_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    alert_id: Mapped[int] = mapped_column(ForeignKey("alerts.id", ondelete="CASCADE"), nullable=False, index=True)
    domain: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    triggered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    sent_mail: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    alert: Mapped["Alert"] = relationship("Alert", back_populates="logs")


class UrlAuditRecord(Base):
    """Sitemap'ten çekilen her URL için SEO sinyali denetim kaydı."""

    __tablename__ = "url_audit_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    collector_run_id: Mapped[int | None] = mapped_column(ForeignKey("collector_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    final_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    status_code: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    content_type: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    sitemap_source: Mapped[str] = mapped_column(Text, nullable=False, default="")
    sitemap_lastmod: Mapped[str] = mapped_column(String(40), nullable=False, default="")

    # SEO sinyalleri
    has_title: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    title: Mapped[str] = mapped_column(Text, nullable=False, default="")
    title_length: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    has_meta_description: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    meta_description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    meta_description_length: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    has_h1: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    h1: Mapped[str] = mapped_column(Text, nullable=False, default="")
    h1_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    h2_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    has_canonical: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    canonical_matches_final: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_schema: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_noindex: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    meta_robots: Mapped[str] = mapped_column(Text, nullable=False, default="")
    has_og_title: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_og_description: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    search_clicks: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    search_impressions: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    search_ctr: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    search_console_seen: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    indexed_via: Mapped[str] = mapped_column(String(20), nullable=False, default="none")
    inspection_verdict: Mapped[str] = mapped_column(String(30), nullable=False, default="")
    issue_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    checks_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    # Genel skor: good / needs_improvement / poor
    seo_score: Mapped[str] = mapped_column(String(30), nullable=False, default="poor", index=True)

    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site", back_populates="url_audit_records")


class ApiUsage(Base):
    """Harici API kullanım sayaçlarını period bazında tutar."""

    __tablename__ = "api_usages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    period_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # day | month
    period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    call_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="api_usages")


class NotificationDeliveryLog(Base):
    """Operasyon e-posta bildirimlerinin tekrar kontrolünü tutar."""

    __tablename__ = "notification_delivery_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    notification_type: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    notification_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    subject: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    recipient: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    sent_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class SmtpDailySendLedger(Base):
    """SMTP ile başarılı gönderim sayısı — takvim günü (smtp_quota_calendar_timezone) başına tavan."""

    __tablename__ = "smtp_daily_send_ledgers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    day_key: Mapped[str] = mapped_column(String(12), nullable=False, unique=True, index=True)
    send_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AiDailyBriefReport(Base):
    """Günlük AI strateji özeti (sabah job); arayüz ve e-posta kaynağı."""

    __tablename__ = "ai_daily_brief_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    brief_date: Mapped[str] = mapped_column(String(10), nullable=False, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    ga4_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    pagespeed_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    search_console_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    alerts_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    turkish_qc_ok: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    qc_detail: Mapped[str] = mapped_column(Text, nullable=False, default="")
    email_sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    model_name: Mapped[str] = mapped_column(String(80), nullable=False, default="")


class AiBriefRunLog(Base):
    """Her başarılı AI özet üretiminde bir satır (gün ve model kırılımı için)."""

    __tablename__ = "ai_brief_run_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    day_key: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    model_name: Mapped[str] = mapped_column(String(80), nullable=False, default="")
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="scheduled")
    brief_date: Mapped[str] = mapped_column(String(10), nullable=False, default="")
    approx_try: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    llm_calls: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    run_detail: Mapped[str] = mapped_column(String(255), nullable=False, default="")


class LlmSpendMonth(Base):
    """Aylık tahmini LLM harcaması (TRY); token fiyatları .env ile kalibre edilir."""

    __tablename__ = "llm_spend_months"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    month_key: Mapped[str] = mapped_column(String(7), nullable=False, unique=True, index=True)
    total_try: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AdminAuthSetting(Base):
    """Allowlist dışı erişim için tek admin parola ayarı."""

    __tablename__ = "admin_auth_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    password_salt: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class RealtimeSnapshot(Base):
    """GA4 Realtime API kontrol sonuçları — trend grafiği ve geçmiş için."""

    __tablename__ = "realtime_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    profile: Mapped[str] = mapped_column(String(20), nullable=False, default="web", index=True)
    active_users_current: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    active_users_previous: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    pageviews_current: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    pageviews_previous: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    window_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    alarm_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site")


class RealtimeAlarmLog(Base):
    """GA4 Realtime alarm tetiklenme geçmişi."""

    __tablename__ = "realtime_alarm_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    rule_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    metric: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="warning")
    current_value: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    previous_value: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    change_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    triggered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    email_sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)

    site: Mapped["Site"] = relationship("Site")


class RealtimePageSnapshot(Base):
    """Sayfa bazlı Realtime trafik anlık görüntüsü — sayfa alarm karşılaştırması için."""

    __tablename__ = "realtime_page_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    profile: Mapped[str] = mapped_column(String(20), nullable=False, default="web")
    page_path: Mapped[str] = mapped_column(String(500), nullable=False)
    active_users: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    pageviews: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    rank: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site")


class RealtimeNewsSnapshot(Base):
    """Haberler (unifiedScreenName) Realtime anlık görüntüsü — haber trafik alarmı karşılaştırması."""

    __tablename__ = "realtime_news_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    profile: Mapped[str] = mapped_column(String(20), nullable=False, default="web")
    screen_title: Mapped[str] = mapped_column(String(500), nullable=False)
    active_users: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    pageviews: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    rank: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site")


class RealtimeAppEventSnapshot(Base):
    """Uygulama (android/ios) realtime event count snapshot — zirve karşılaştırması için."""

    __tablename__ = "realtime_app_event_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    profile: Mapped[str] = mapped_column(String(20), nullable=False, default="android")
    event_name: Mapped[str] = mapped_column(String(200), nullable=False)
    event_count: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    site: Mapped["Site"] = relationship("Site")


class AppIntelRawCache(Base):
    """App mağaza ham yorum payload'ı — Railway gibi ephemeral disk + çoklu dyno için Postgres önbellek."""

    __tablename__ = "app_intel_raw_cache"

    product_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)


class InboxGmailCredential(Base):
    """Gelen kutusu (Gmail API) için global OAuth — tek satır (id=1)."""

    __tablename__ = "inbox_gmail_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_email: Mapped[str] = mapped_column(String(320), nullable=False, default="")
    encrypted_data: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SupportInboxThread(Base):
    """info@ / feedback@ gibi adreslere gelen destek e-posta konuşmaları (Gmail thread)."""

    __tablename__ = "support_inbox_threads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gmail_thread_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    subject: Mapped[str] = mapped_column(String(998), nullable=False, default="")
    snippet: Mapped[str] = mapped_column(Text, nullable=False, default="")
    route_tag: Mapped[str] = mapped_column(String(32), nullable=False, default="mixed")
    gmail_unread: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    answered_flag: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_internal_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    ai_summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    ai_draft_reply: Mapped[str] = mapped_column(Text, nullable=False, default="")
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    messages: Mapped[list["SupportInboxMessage"]] = relationship(
        "SupportInboxMessage",
        back_populates="thread",
        cascade="all, delete-orphan",
    )


class NewsIntelligenceItem(Base):
    """Haber istihbaratı için toplanan veri öğeleri."""

    __tablename__ = "news_intelligence_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    url: Mapped[str] = mapped_column(String(2048), nullable=False, index=True)
    headline: Mapped[str] = mapped_column(String(500), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_url: Mapped[str] = mapped_column(String(512), nullable=True) # Gerçek kaynak URL (favicon için)
    image_url: Mapped[str] = mapped_column(String(1024), nullable=True) # Haber görseli
    category: Mapped[str] = mapped_column(String(100), nullable=True, index=True)  # İş, Dünya, Türkiye
    topic: Mapped[str] = mapped_column(String(100), nullable=True, index=True)     # Döviz, Finans, Ekonomi
    is_in_our_site: Mapped[bool] = mapped_column(Boolean, default=False)
    ai_note: Mapped[str] = mapped_column(Text, nullable=True)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class OmdbEnrichment(Base):
    """TMDB filmlerine ait OMDB/IMDb zenginleştirme verisi önbelleği."""

    __tablename__ = "omdb_enrichment"

    id:           Mapped[int]  = mapped_column(Integer, primary_key=True)
    tmdb_id:      Mapped[int]  = mapped_column(Integer, unique=True, nullable=False, index=True)
    imdb_id:      Mapped[str]  = mapped_column(String(20),  nullable=True)
    imdb_rating:  Mapped[str]  = mapped_column(String(10),  nullable=True)   # "7.6"
    imdb_votes:   Mapped[str]  = mapped_column(String(30),  nullable=True)   # "828,114"
    rt_score:     Mapped[str]  = mapped_column(String(10),  nullable=True)   # "85%"
    metacritic:   Mapped[str]  = mapped_column(String(10),  nullable=True)   # "67"
    age_rating:   Mapped[str]  = mapped_column(String(20),  nullable=True)   # "PG-13"
    box_office:   Mapped[str]  = mapped_column(String(40),  nullable=True)   # "$389,813,101"
    awards:       Mapped[str]  = mapped_column(String(255), nullable=True)
    fetched_at:   Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    found:        Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class SupportInboxMessage(Base):
    """Konuşma içindeki tek Gmail mesajı."""

    __tablename__ = "support_inbox_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    thread_id: Mapped[int] = mapped_column(
        ForeignKey("support_inbox_threads.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    gmail_message_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    from_addr: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    to_addr: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    subject: Mapped[str] = mapped_column(String(998), nullable=False, default="")
    body_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    internal_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    is_outbound: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    thread: Mapped["SupportInboxThread"] = relationship("SupportInboxThread", back_populates="messages")


class SiteErrorLog(Base):
    """Tespit edilen site hataları (GA4, SC, sunucu kaynaklı)."""
    __tablename__ = "site_error_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False, index=True)   # 404, 500, vs.
    source: Mapped[str] = mapped_column(String(30), nullable=False, index=True)     # "ga4", "sc", "server"
    error_type: Mapped[str] = mapped_column(String(50), nullable=False)             # "not_found", "server_error"
    hit_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)      # kaç kullanıcı etkilendi
    first_seen: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    extra_json: Mapped[str | None] = mapped_column(Text, nullable=True)             # ek veri (başlık, referrer, vs.)

    site: Mapped["Site"] = relationship("Site")



class MetaTagSnapshot(Base):
    """Günlük meta tag snapshot — UrlAuditRecord'dan alınan anlık görüntü, regresyon tespiti için."""
    __tablename__ = "meta_tag_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False, default="")
    title_length: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    meta_description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    meta_description_length: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False, default="")
    seo_score: Mapped[str] = mapped_column(String(30), nullable=False, default="poor")
    is_noindex: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_og_title: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    has_og_description: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_meta_tag_snapshots_site_date", "site_id", "snapshot_date"),
        Index("ix_meta_tag_snapshots_site_url_date", "site_id", "url", "snapshot_date"),
    )

    site: Mapped["Site"] = relationship("Site")


class AdPolicyViolation(Base):
    """Ad Manager Policy Center'dan CSV ile import edilen ihlaller."""
    __tablename__ = "ad_policy_violations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    issue_type: Mapped[str] = mapped_column(String(200), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    ad_requests_7d: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    enforcement: Mapped[str] = mapped_column(String(100), nullable=False, default="")
    first_reported: Mapped[date] = mapped_column(Date, nullable=True)
    last_reported: Mapped[date] = mapped_column(Date, nullable=True)
    page_title: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    page_title_fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    extra_json: Mapped[str] = mapped_column(Text, nullable=False, default="")
    our_status: Mapped[str] = mapped_column(String(30), nullable=False, default="new")
    our_notes: Mapped[str] = mapped_column(Text, nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=True)

    __table_args__ = (
        Index("ix_adpolicy_url", "url"),
        Index("ix_adpolicy_status", "our_status"),
        Index("ix_adpolicy_issue_type", "issue_type"),
        UniqueConstraint("url", "issue_type", name="uq_adpolicy_url_issue"),
    )


class PolicyCSVUpload(Base):
    """En son yüklenen Policy Center CSV'si — geriye dönük kontrol için."""
    __tablename__ = "policy_csv_uploads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    new_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    content: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
