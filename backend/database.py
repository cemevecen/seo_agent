"""SQLAlchemy veritabanı bağlantısı ve ortak Base tanımı."""

from sqlalchemy import create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker

from backend.config import settings

_IS_SQLITE = settings.database_url.startswith("sqlite")

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    connect_args={"check_same_thread": False, "timeout": 30} if _IS_SQLITE else {},
)

if _IS_SQLITE:

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        # Performans: 64 MB sayfa önbelleği (varsayılan ~8 MB)
        cursor.execute("PRAGMA cache_size=-65536")
        # Performans: geçici tablolar bellekte (disk yerine)
        cursor.execute("PRAGMA temp_store=2")
        # Performans: 256 MB memory-mapped I/O
        cursor.execute("PRAGMA mmap_size=268435456")
        cursor.close()


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """FastAPI dependency olarak transaction scope'lu oturum sağlar."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Alembic kullanmadan tüm tabloları create_all ile oluşturur."""
    from backend import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    ensure_indexes()


def ensure_indexes() -> None:
    """Sık kullanılan sorgular için eksik composite index'leri oluşturur.

    create_all mevcut tabloları değiştirmediğinden bu fonksiyon
    tek seferlik idempotent olarak çalışır (CREATE INDEX IF NOT EXISTS).
    """
    index_ddl = [
        # metrics: site_id + collected_at → get_latest_metrics GROUP BY
        "CREATE INDEX IF NOT EXISTS ix_metrics_site_collected ON metrics(site_id, collected_at)",
        # metrics: site_id + metric_type → tek tip sorguları
        "CREATE INDEX IF NOT EXISTS ix_metrics_site_type ON metrics(site_id, metric_type)",
        # alert_logs: triggered_at + id → get_recent_alerts ORDER BY
        "CREATE INDEX IF NOT EXISTS ix_alert_logs_triggered_id ON alert_logs(triggered_at DESC, id DESC)",
        # sc snapshots: site_id + data_scope + collected_at → SC batch sorgular
        "CREATE INDEX IF NOT EXISTS ix_sc_site_scope_collected ON search_console_query_snapshots(site_id, data_scope, collected_at)",
        # collector_runs: site_id + provider + strategy + requested_at → _latest_provider_run
        "CREATE INDEX IF NOT EXISTS ix_collector_runs_site_prov_strat ON collector_runs(site_id, provider, strategy, requested_at)",
    ]
    with engine.connect() as conn:
        for ddl in index_ddl:
            try:
                conn.execute(__import__("sqlalchemy").text(ddl))
            except Exception:  # noqa: BLE001
                pass  # Index zaten varsa ya da uyumsuz DB ise sessizce geç
        conn.commit()
