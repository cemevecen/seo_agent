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


class SiteCredential(Base):
    """Siteye ait API credential verisini şifreli olarak saklar."""

    __tablename__ = "site_credentials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    credential_type: Mapped[str] = mapped_column(String(100), nullable=False)
    encrypted_data: Mapped[str] = mapped_column(Text, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="credentials")


class Metric(Base):
    """Toplanan SEO metrik değerlerini tutar."""

    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id", ondelete="CASCADE"), nullable=False, index=True)
    metric_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    site: Mapped["Site"] = relationship("Site", back_populates="metrics")


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
    triggered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    sent_mail: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    alert: Mapped["Alert"] = relationship("Alert", back_populates="logs")
