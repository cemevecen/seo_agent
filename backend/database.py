"""SQLAlchemy veritabanı bağlantısı ve ortak Base tanımı."""

import logging
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import declarative_base, sessionmaker

from backend.config import BASE_DIR, settings

LOGGER = logging.getLogger(__name__)


def _normalize_sqlite_url(url: str) -> str:
    """Göreli sqlite:///... yollarını proje köküne (seo-agent/) göre mutlak adrese çevirir."""
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        return url
    if url.startswith("sqlite:////"):
        return url
    rest = url[len(prefix) :]
    if not rest or rest.startswith(":memory:"):
        return url
    path = Path(rest)
    if path.is_absolute():
        return url
    abs_path = (BASE_DIR / rest).resolve()
    return f"sqlite:///{abs_path.as_posix()}"


# Railway ve bazı platformlar postgresql:// verir; psycopg3 için +psycopg gerekiyor
_db_url = _normalize_sqlite_url(settings.database_url)
_IS_SQLITE = _db_url.startswith("sqlite")
if _db_url.startswith("postgresql://"):
    _db_url = _db_url.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(
    _db_url,
    pool_pre_ping=True,
    # Railway gibi ortamlarda Postgres hazır olmadan container kalkabilir.
    # connect_timeout olmazsa startup aşamasında uzun süre "asılı" kalabiliyor.
    connect_args=(
        {"check_same_thread": False, "timeout": 30}
        if _IS_SQLITE
        else {"connect_timeout": 10}  # psycopg / libpq (seconds)
    ),
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
        # WAL dosyasının 49MB'a şişmesini önle: 1000 page (~4MB) sonra otomatik checkpoint
        cursor.execute("PRAGMA wal_autocheckpoint=1000")
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
        # url audits: latest snapshot ve skor kırılımları
        "CREATE INDEX IF NOT EXISTS ix_url_audit_site_run_score ON url_audit_records(site_id, collector_run_id, seo_score)",
        # app rank snapshots: ürün+platform trend sorguları
        "CREATE INDEX IF NOT EXISTS ix_app_rank_prod_platform_collected ON app_store_rank_snapshots(product_id, platform, collected_at)",
    ]
    with engine.connect() as conn:
        for ddl in index_ddl:
            try:
                conn.execute(__import__("sqlalchemy").text(ddl))
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug("Index oluşturma atlandı (%s): %s", ddl.split("ON")[0].strip(), exc)
        try:
            existing_cols = {
                row[1]
                for row in conn.execute(__import__("sqlalchemy").text("PRAGMA table_info(url_audit_records)")).fetchall()
            }
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("url_audit_records sütun listesi alınamadı: %s", exc)
            existing_cols = set()

        column_ddl = {
            "content_type": "VARCHAR(120) NOT NULL DEFAULT ''",
            "sitemap_source": "TEXT NOT NULL DEFAULT ''",
            "sitemap_lastmod": "VARCHAR(40) NOT NULL DEFAULT ''",
            "title_length": "INTEGER NOT NULL DEFAULT 0",
            "meta_description_length": "INTEGER NOT NULL DEFAULT 0",
            "h1_count": "INTEGER NOT NULL DEFAULT 0",
            "canonical_matches_final": "BOOLEAN NOT NULL DEFAULT 0",
            "meta_robots": "TEXT NOT NULL DEFAULT ''",
            "has_og_description": "BOOLEAN NOT NULL DEFAULT 0",
            "search_clicks": "FLOAT NOT NULL DEFAULT 0",
            "search_impressions": "FLOAT NOT NULL DEFAULT 0",
            "search_ctr": "FLOAT NOT NULL DEFAULT 0",
            "search_console_seen": "BOOLEAN NOT NULL DEFAULT 0",
            "indexed_via": "VARCHAR(20) NOT NULL DEFAULT 'none'",
            "inspection_verdict": "VARCHAR(30) NOT NULL DEFAULT ''",
            "issue_count": "INTEGER NOT NULL DEFAULT 0",
            "checks_json": "TEXT NOT NULL DEFAULT '{}'",
        }
        for column_name, ddl in column_ddl.items():
            if column_name in existing_cols:
                continue
            try:
                conn.execute(
                    __import__("sqlalchemy").text(
                        f"ALTER TABLE url_audit_records ADD COLUMN {column_name} {ddl}"
                    )
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug("ALTER TABLE url_audit_records ADD COLUMN %s atlandı: %s", column_name, exc)
        # ai_brief_run_logs: eski kurulumlara eksik sütunlar (create_all mevcut tabloları genişletmez)
        _txt = __import__("sqlalchemy").text

        def _ensure_ai_brief_run_col(name: str, sqlite_ddl: str, pg_ddl: str) -> None:
            try:
                if _IS_SQLITE:
                    rc = conn.execute(
                        _txt("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_brief_run_logs'")
                    )
                    if not rc.fetchone():
                        return
                    cols = {
                        row[1]
                        for row in conn.execute(_txt("PRAGMA table_info(ai_brief_run_logs)")).fetchall()
                    }
                    if name not in cols:
                        conn.execute(_txt(f"ALTER TABLE ai_brief_run_logs ADD COLUMN {name} {sqlite_ddl}"))
                else:
                    try:
                        conn.execute(
                            _txt(
                                f"ALTER TABLE ai_brief_run_logs ADD COLUMN IF NOT EXISTS {name} {pg_ddl}"
                            )
                        )
                    except Exception:  # noqa: BLE001
                        conn.execute(_txt(f"ALTER TABLE ai_brief_run_logs ADD COLUMN {name} {pg_ddl}"))
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug("ai_brief_run_logs ADD COLUMN IF NOT EXISTS atlandı: %s", exc)

        _ensure_ai_brief_run_col("approx_try", "FLOAT NOT NULL DEFAULT 0", "DOUBLE PRECISION NOT NULL DEFAULT 0")
        _ensure_ai_brief_run_col("llm_calls", "INTEGER NOT NULL DEFAULT 1", "INTEGER NOT NULL DEFAULT 1")
        _ensure_ai_brief_run_col("run_detail", "TEXT NOT NULL DEFAULT ''", "VARCHAR(255) NOT NULL DEFAULT ''")
        conn.commit()
