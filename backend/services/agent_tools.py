"""ProjectControl AI Ajan — araç implementasyonları (Railway, GitHub, DB, iç API)."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import text

from backend.config import settings
from backend.database import SessionLocal

LOGGER = logging.getLogger(__name__)

_GH_BASE = "https://api.github.com"
_RAILWAY_BASE = "https://backboard.railway.app/graphql/v2"


# ── Yardımcı ─────────────────────────────────────────────────────────────────

def _gh_headers() -> dict[str, str]:
    token = settings.github_token or os.environ.get("GITHUB_TOKEN", "")
    if not token:
        raise RuntimeError("GITHUB_TOKEN tanımlı değil.")
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}


def _railway_headers() -> dict[str, str]:
    token = settings.railway_api_token or os.environ.get("RAILWAY_API_TOKEN", "")
    if not token:
        raise RuntimeError("RAILWAY_API_TOKEN tanımlı değil.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ── GitHub araçları ───────────────────────────────────────────────────────────

def github_list_issues(state: str = "open", limit: int = 10) -> dict[str, Any]:
    """GitHub repo'sundaki issue'ları listeler."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/issues",
            params={"state": state, "per_page": min(limit, 30)},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        items = [
            {
                "number": i["number"],
                "title": i["title"],
                "state": i["state"],
                "labels": [l["name"] for l in i.get("labels", [])],
                "created_at": i["created_at"],
                "url": i["html_url"],
            }
            for i in r.json()
            if "pull_request" not in i
        ]
        return {"issues": items, "count": len(items)}
    except Exception as e:
        return {"error": str(e)}


def github_list_prs(state: str = "open", limit: int = 10) -> dict[str, Any]:
    """Açık pull request'leri listeler."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/pulls",
            params={"state": state, "per_page": min(limit, 20)},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        items = [
            {
                "number": p["number"],
                "title": p["title"],
                "state": p["state"],
                "branch": p["head"]["ref"],
                "created_at": p["created_at"],
                "url": p["html_url"],
            }
            for p in r.json()
        ]
        return {"pull_requests": items, "count": len(items)}
    except Exception as e:
        return {"error": str(e)}


def github_recent_commits(limit: int = 10) -> dict[str, Any]:
    """Son commit'leri getirir."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/commits",
            params={"per_page": min(limit, 20)},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        items = [
            {
                "sha": c["sha"][:8],
                "message": c["commit"]["message"].split("\n")[0][:100],
                "author": c["commit"]["author"]["name"],
                "date": c["commit"]["author"]["date"],
                "url": c["html_url"],
            }
            for c in r.json()
        ]
        return {"commits": items, "count": len(items)}
    except Exception as e:
        return {"error": str(e)}


def github_create_issue(title: str, body: str, labels: list[str] | None = None) -> dict[str, Any]:
    """GitHub'da yeni issue oluşturur."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        payload: dict[str, Any] = {"title": title, "body": body}
        if labels:
            payload["labels"] = labels
        r = httpx.post(
            f"{_GH_BASE}/repos/{repo}/issues",
            json=payload,
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        return {"number": data["number"], "url": data["html_url"], "title": data["title"]}
    except Exception as e:
        return {"error": str(e)}


# ── Railway araçları ──────────────────────────────────────────────────────────

def railway_get_deployments(limit: int = 5) -> dict[str, Any]:
    """Railway'deki son deployment'ları getirir (GraphQL)."""
    project_id = settings.railway_project_id or os.environ.get("RAILWAY_PROJECT_ID", "")
    if not project_id:
        return {"error": "RAILWAY_PROJECT_ID tanımlı değil."}
    query = """
    query($projectId: String!, $first: Int!) {
      deployments(input: { projectId: $projectId }, first: $first) {
        edges {
          node {
            id
            status
            createdAt
            meta { commitMessage commitSha }
            service { name }
          }
        }
      }
    }
    """
    try:
        r = httpx.post(
            _RAILWAY_BASE,
            json={"query": query, "variables": {"projectId": project_id, "first": limit}},
            headers=_railway_headers(),
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        edges = data.get("data", {}).get("deployments", {}).get("edges", [])
        deploys = [
            {
                "id": e["node"]["id"][:12],
                "status": e["node"]["status"],
                "service": (e["node"].get("service") or {}).get("name", "?"),
                "created_at": e["node"]["createdAt"],
                "commit": (e["node"].get("meta") or {}).get("commitMessage", "")[:80],
                "sha": (e["node"].get("meta") or {}).get("commitSha", "")[:8],
            }
            for e in edges
        ]
        return {"deployments": deploys, "count": len(deploys)}
    except Exception as e:
        return {"error": str(e)}


def railway_get_logs(service_name: str = "", lines: int = 50) -> dict[str, Any]:
    """Railway servis loglarını getirir."""
    project_id = settings.railway_project_id or os.environ.get("RAILWAY_PROJECT_ID", "")
    if not project_id:
        return {"error": "RAILWAY_PROJECT_ID tanımlı değil."}
    # Railway'in log API'si REST değil, önce environment+service ID'yi almak gerekir
    query = """
    query($projectId: String!) {
      project(id: $projectId) {
        environments { edges { node { id name } } }
        services { edges { node { id name } } }
      }
    }
    """
    try:
        r = httpx.post(
            _RAILWAY_BASE,
            json={"query": query, "variables": {"projectId": project_id}},
            headers=_railway_headers(),
            timeout=20,
        )
        r.raise_for_status()
        data = r.json().get("data", {}).get("project", {})
        services = [e["node"] for e in data.get("services", {}).get("edges", [])]
        envs = [e["node"] for e in data.get("environments", {}).get("edges", [])]
        return {
            "services": [{"id": s["id"][:12], "name": s["name"]} for s in services],
            "environments": [{"id": e["id"][:12], "name": e["name"]} for e in envs],
            "note": "Log çekmek için servis ve ortam ID'si gerekir. Deployment listesi kullan.",
        }
    except Exception as e:
        return {"error": str(e)}


# ── Veritabanı araçları ───────────────────────────────────────────────────────

def db_table_stats() -> dict[str, Any]:
    """Veritabanı tablo istatistiklerini döner."""
    try:
        db = SessionLocal()
        try:
            result = db.execute(text("""
                SELECT
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    n_live_tup as row_count
                FROM pg_stat_user_tables
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                LIMIT 20
            """))
            rows = [
                {"table": r.tablename, "size": r.size, "rows": r.row_count}
                for r in result
            ]
            return {"tables": rows, "count": len(rows)}
        finally:
            db.close()
    except Exception as e:
        return {"error": str(e)}


def db_recent_errors(hours: int = 24) -> dict[str, Any]:
    """Son N saatteki hata kayıtlarını arar (varsa error log tablosundan)."""
    try:
        db = SessionLocal()
        try:
            # error_monitor tablosu varsa çek
            result = db.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('error_log', 'error_monitor', 'app_errors', 'alerts')
            """))
            tables = [r[0] for r in result]
            if not tables:
                return {"message": "Hata log tablosu bulunamadı.", "available_check": "db_table_stats kullan"}
            return {"found_tables": tables, "hint": f"Tablo bulundu: {tables}"}
        finally:
            db.close()
    except Exception as e:
        return {"error": str(e)}


def db_custom_query(sql: str) -> dict[str, Any]:
    """Güvenli SELECT sorgusu çalıştırır (sadece okuma)."""
    sql_stripped = sql.strip().upper()
    if not sql_stripped.startswith("SELECT"):
        return {"error": "Sadece SELECT sorguları izinlidir."}
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "CREATE"]
    for kw in forbidden:
        if kw in sql_stripped:
            return {"error": f"'{kw}' komutu yasak."}
    try:
        db = SessionLocal()
        try:
            result = db.execute(text(sql))
            cols = list(result.keys())
            rows = [dict(zip(cols, r)) for r in result.fetchmany(50)]
            return {"columns": cols, "rows": rows, "count": len(rows)}
        finally:
            db.close()
    except Exception as e:
        return {"error": str(e)}


# ── Sistem durumu araçları ────────────────────────────────────────────────────

def system_health_check() -> dict[str, Any]:
    """Uygulamanın genel sağlık durumunu kontrol eder."""
    checks: dict[str, Any] = {}

    # DB bağlantısı
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        checks["database"] = "ok"
    except Exception as e:
        checks["database"] = f"error: {e}"

    # Ortam değişkenleri
    checks["env"] = {
        "ANTHROPIC_API_KEY": "set" if settings.anthropic_api_key else "missing",
        "GITHUB_TOKEN": "set" if settings.github_token else "missing",
        "RAILWAY_API_TOKEN": "set" if settings.railway_api_token else "missing",
        "GP_REPORTS_BUCKET": "set" if os.environ.get("GP_REPORTS_BUCKET") else "missing",
        "GA4_SERVICE_ACCOUNT": "set" if (settings.ga4_service_account_json or settings.ga4_service_account_file) else "missing",
    }

    # Railway runtime bilgisi
    checks["runtime"] = {
        "platform": "railway" if os.environ.get("RAILWAY_ENVIRONMENT") else "local",
        "environment": os.environ.get("RAILWAY_ENVIRONMENT", "local"),
        "git_sha": os.environ.get("RAILWAY_GIT_COMMIT_SHA", "")[:8] or "unknown",
    }

    checks["timestamp"] = datetime.utcnow().isoformat() + "Z"
    return checks


def project_structure() -> dict[str, Any]:
    """Proje dosya yapısını özetler."""
    import os as _os
    base = "/home/user/seo_agent"
    if not _os.path.exists(base):
        base = _os.getcwd()

    structure: dict[str, Any] = {}
    for root, dirs, files in _os.walk(base):
        dirs[:] = [d for d in dirs if d not in ("__pycache__", ".git", "node_modules", ".venv", "venv")]
        rel = _os.path.relpath(root, base)
        if rel.count(_os.sep) > 2:
            continue
        py_files = [f for f in files if f.endswith(".py")]
        html_files = [f for f in files if f.endswith(".html")]
        if py_files or html_files:
            structure[rel] = {
                "py": py_files[:20],
                "html": html_files[:10],
            }
    return {"structure": structure, "base": base}


# ── Araç tanımları (Claude tool_use formatı) ──────────────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "github_list_issues",
        "description": "GitHub repo'sundaki issue'ları listeler. Bug raporları, görevler ve öneriler burada.",
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {"type": "string", "enum": ["open", "closed", "all"], "description": "Issue durumu", "default": "open"},
                "limit": {"type": "integer", "description": "Maksimum sonuç sayısı (max 30)", "default": 10},
            },
        },
    },
    {
        "name": "github_list_prs",
        "description": "GitHub pull request'lerini listeler.",
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {"type": "string", "enum": ["open", "closed", "all"], "default": "open"},
                "limit": {"type": "integer", "default": 10},
            },
        },
    },
    {
        "name": "github_recent_commits",
        "description": "Son commit'leri getirir. Ne değişti, kim değiştirdi.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Kaç commit (max 20)", "default": 10},
            },
        },
    },
    {
        "name": "github_create_issue",
        "description": "GitHub'da yeni bir issue oluşturur. Bug veya özellik talebi için.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Issue başlığı"},
                "body": {"type": "string", "description": "Issue detayı (Markdown)"},
                "labels": {"type": "array", "items": {"type": "string"}, "description": "Etiketler (ör. bug, enhancement)"},
            },
            "required": ["title", "body"],
        },
    },
    {
        "name": "railway_get_deployments",
        "description": "Railway'deki son deployment'ların listesi ve durumları.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Kaç deployment (max 10)", "default": 5},
            },
        },
    },
    {
        "name": "railway_get_logs",
        "description": "Railway servis yapısını ve log erişim bilgilerini getirir.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_name": {"type": "string", "description": "Servis adı (boş bırakılabilir)", "default": ""},
                "lines": {"type": "integer", "description": "Log satır sayısı", "default": 50},
            },
        },
    },
    {
        "name": "db_table_stats",
        "description": "Veritabanı tablolarının boyut ve satır sayısı istatistikleri.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "db_recent_errors",
        "description": "Son 24 saatteki hata loglarını kontrol eder.",
        "input_schema": {
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "description": "Kaç saat geriye bak", "default": 24},
            },
        },
    },
    {
        "name": "db_custom_query",
        "description": "Veritabanında güvenli bir SELECT sorgusu çalıştırır. Sadece okuma.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT sorgusu"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "system_health_check",
        "description": "Uygulamanın genel sağlık durumunu kontrol eder: DB, env değişkenleri, platform.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "project_structure",
        "description": "Projenin dosya yapısını özetler. Hangi modüller, hangi servisler var.",
        "input_schema": {"type": "object", "properties": {}},
    },
]


def execute_tool(name: str, inputs: dict[str, Any]) -> Any:
    """Araç adına göre ilgili fonksiyonu çalıştırır."""
    dispatch = {
        "github_list_issues": lambda: github_list_issues(**inputs),
        "github_list_prs": lambda: github_list_prs(**inputs),
        "github_recent_commits": lambda: github_recent_commits(**inputs),
        "github_create_issue": lambda: github_create_issue(**inputs),
        "railway_get_deployments": lambda: railway_get_deployments(**inputs),
        "railway_get_logs": lambda: railway_get_logs(**inputs),
        "db_table_stats": lambda: db_table_stats(),
        "db_recent_errors": lambda: db_recent_errors(**inputs),
        "db_custom_query": lambda: db_custom_query(**inputs),
        "system_health_check": lambda: system_health_check(),
        "project_structure": lambda: project_structure(),
    }
    fn = dispatch.get(name)
    if not fn:
        return {"error": f"Bilinmeyen araç: {name}"}
    try:
        return fn()
    except Exception as e:
        LOGGER.exception("Araç hatası: %s", name)
        return {"error": str(e)}
