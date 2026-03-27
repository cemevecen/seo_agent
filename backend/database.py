"""SQLAlchemy veritabanı bağlantısı ve ortak Base tanımı."""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from backend.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
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
