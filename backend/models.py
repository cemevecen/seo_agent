"""Aşama 2 için temel SQLAlchemy modelleri."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
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
    metric_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="metrics")


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
