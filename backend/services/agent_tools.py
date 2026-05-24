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


def github_list_branches() -> dict[str, Any]:
    """Repo'daki tüm branch'leri listeler."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/branches",
            params={"per_page": 50},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        branches = [
            {
                "name": b["name"],
                "sha": b["commit"]["sha"][:8],
                "protected": b.get("protected", False),
            }
            for b in r.json()
        ]
        # main/master branch'ini öne al
        branches.sort(key=lambda b: (0 if b["name"] in ("main", "master") else 1, b["name"]))
        return {"branches": branches, "count": len(branches)}
    except Exception as e:
        return {"error": str(e)}


def github_commit_stats() -> dict[str, Any]:
    """Projedeki toplam commit sayısını ve branch başına commit sayılarını döner."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        # Toplam commit sayısı: per_page=1 ile son sayfa numarasını Link header'dan oku
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/commits",
            params={"per_page": 1},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        total_main = 0
        link = r.headers.get("Link", "")
        if 'rel="last"' in link:
            import re
            m = re.search(r'page=(\d+)>; rel="last"', link)
            if m:
                total_main = int(m.group(1))
        else:
            # tek sayfa — listeden say
            total_main = len(r.json()) if r.status_code == 200 else 0

        # Tüm branch'ler ve her birinin son commit SHA'sı
        rb = httpx.get(
            f"{_GH_BASE}/repos/{repo}/branches",
            params={"per_page": 50},
            headers=_gh_headers(),
            timeout=15,
        )
        rb.raise_for_status()
        branches = rb.json()

        branch_stats = []
        for b in branches:
            bname = b["name"]
            br = httpx.get(
                f"{_GH_BASE}/repos/{repo}/commits",
                params={"per_page": 1, "sha": bname},
                headers=_gh_headers(),
                timeout=15,
            )
            count = 0
            blink = br.headers.get("Link", "")
            if 'rel="last"' in blink:
                import re
                m = re.search(r'page=(\d+)>; rel="last"', blink)
                if m:
                    count = int(m.group(1))
            else:
                count = len(br.json()) if br.status_code == 200 else 0
            branch_stats.append({"branch": bname, "commit_count": count})

        branch_stats.sort(key=lambda x: -x["commit_count"])
        return {
            "total_commits_main": total_main,
            "branches": branch_stats,
            "note": "commit sayıları branch'e özgü (shared commit'ler birden fazla branch'te sayılır)",
        }
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


def github_get_branch_diff(base: str = "main", head: str = "") -> dict[str, Any]:
    """İki branch arasındaki farkı gösterir — kaç commit geride/ileride."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    if not head:
        return {"error": "head branch adı gerekli."}
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/compare/{base}...{head}",
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "status": data.get("status"),
            "ahead_by": data.get("ahead_by", 0),
            "behind_by": data.get("behind_by", 0),
            "commits": [
                {"sha": c["sha"][:8], "message": c["commit"]["message"].split("\n")[0][:80]}
                for c in data.get("commits", [])[:10]
            ],
        }
    except Exception as e:
        return {"error": str(e)}


def github_contributor_stats() -> dict[str, Any]:
    """Kim en çok commit attı, toplam katkı sayıları. 'En aktif geliştirici kim?' gibi sorular için."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/contributors",
            params={"per_page": 20, "anon": "true"},
            headers=_gh_headers(),
            timeout=20,
        )
        r.raise_for_status()
        contributors = [
            {
                "login": c.get("login") or c.get("name", "anonim"),
                "contributions": c["contributions"],
                "type": c.get("type", "User"),
            }
            for c in r.json()
        ]
        total = sum(c["contributions"] for c in contributors)
        return {"contributors": contributors, "total_contributions": total, "count": len(contributors)}
    except Exception as e:
        return {"error": str(e)}


def github_file_history(path: str, limit: int = 5) -> dict[str, Any]:
    """Belirli bir dosyanın commit geçmişi — kim ne zaman değiştirdi."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/commits",
            params={"path": path, "per_page": min(limit, 10)},
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
            }
            for c in r.json()
        ]
        return {"path": path, "commits": items, "count": len(items)}
    except Exception as e:
        return {"error": str(e)}


def github_repo_languages() -> dict[str, Any]:
    """Repo'daki programlama dilleri ve byte dağılımı."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(f"{_GH_BASE}/repos/{repo}/languages", headers=_gh_headers(), timeout=15)
        r.raise_for_status()
        langs = r.json()
        total = sum(langs.values()) or 1
        result = [
            {"language": lang, "bytes": bytes_, "percent": round(bytes_ / total * 100, 1)}
            for lang, bytes_ in sorted(langs.items(), key=lambda x: -x[1])
        ]
        return {"languages": result, "total_bytes": total}
    except Exception as e:
        return {"error": str(e)}


def github_search_code(query: str) -> dict[str, Any]:
    """Repo içinde kod arar. 'X nerede tanımlanmış', 'Y fonksiyonu hangi dosyada' gibi sorular için."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/search/code",
            params={"q": f"{query} repo:{repo}", "per_page": 10},
            headers={**_gh_headers(), "Accept": "application/vnd.github.text-match+json"},
            timeout=20,
        )
        r.raise_for_status()
        items = [
            {
                "path": i["path"],
                "url": i["html_url"],
                "matches": [m.get("fragment", "")[:150] for m in i.get("text_matches", [])[:2]],
            }
            for i in r.json().get("items", [])
        ]
        return {"results": items, "count": len(items), "query": query}
    except Exception as e:
        return {"error": str(e)}


def github_get_releases() -> dict[str, Any]:
    """Repo'daki release'leri listeler."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/releases",
            params={"per_page": 10},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        releases = [
            {
                "tag": rel["tag_name"],
                "name": rel["name"],
                "published_at": rel["published_at"],
                "prerelease": rel["prerelease"],
                "url": rel["html_url"],
            }
            for rel in r.json()
        ]
        return {"releases": releases, "count": len(releases)}
    except Exception as e:
        return {"error": str(e)}


def github_get_repo_info() -> dict[str, Any]:
    """Repo genel bilgisi: yıldız, fork, açık issue sayısı, default branch, son push."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(f"{_GH_BASE}/repos/{repo}", headers=_gh_headers(), timeout=15)
        r.raise_for_status()
        d = r.json()
        return {
            "name": d["full_name"],
            "default_branch": d["default_branch"],
            "open_issues": d["open_issues_count"],
            "stars": d["stargazers_count"],
            "forks": d["forks_count"],
            "pushed_at": d["pushed_at"],
            "visibility": d["visibility"],
        }
    except Exception as e:
        return {"error": str(e)}


def github_list_workflows() -> dict[str, Any]:
    """GitHub Actions workflow çalıştırmalarını listeler (CI/CD durumu)."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/actions/runs",
            params={"per_page": 10},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        runs = [
            {
                "name": run["name"],
                "status": run["status"],
                "conclusion": run["conclusion"],
                "branch": run["head_branch"],
                "created_at": run["created_at"],
                "url": run["html_url"],
            }
            for run in r.json().get("workflow_runs", [])
        ]
        return {"workflow_runs": runs, "count": len(runs)}
    except Exception as e:
        return {"error": str(e)}


# ── Railway araçları ──────────────────────────────────────────────────────────

def _railway_query(query: str, variables: dict) -> dict:
    """Railway GraphQL isteği atar, data döner."""
    project_id = settings.railway_project_id or os.environ.get("RAILWAY_PROJECT_ID", "")
    if not project_id:
        raise RuntimeError("RAILWAY_PROJECT_ID tanımlı değil.")
    r = httpx.post(
        _RAILWAY_BASE,
        json={"query": query, "variables": {**variables, "projectId": project_id}},
        headers=_railway_headers(),
        timeout=25,
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL hata: {data['errors'][0].get('message', str(data['errors']))}")
    return data.get("data", {})


def railway_get_deployments(limit: int = 5) -> dict[str, Any]:
    """Son deployment'ların listesi ve durumları."""
    try:
        data = _railway_query("""
        query($projectId: String!, $first: Int!) {
          deployments(input: { projectId: $projectId }, first: $first) {
            edges {
              node {
                id status createdAt updatedAt
                meta { commitMessage commitSha branch }
                service { name }
              }
            }
          }
        }
        """, {"first": limit})
        edges = data.get("deployments", {}).get("edges", [])
        deploys = [
            {
                "id": e["node"]["id"][:12],
                "status": e["node"]["status"],
                "service": (e["node"].get("service") or {}).get("name", "?"),
                "created_at": e["node"]["createdAt"],
                "updated_at": e["node"].get("updatedAt", ""),
                "branch": (e["node"].get("meta") or {}).get("branch", ""),
                "commit": (e["node"].get("meta") or {}).get("commitMessage", "")[:80],
                "sha": (e["node"].get("meta") or {}).get("commitSha", "")[:8],
            }
            for e in edges
        ]
        return {"deployments": deploys, "count": len(deploys)}
    except Exception as e:
        return {"error": str(e)}


def railway_get_project_info() -> dict[str, Any]:
    """Proje genel bilgisi: servisler, ortamlar, volume'lar, proje adı."""
    try:
        data = _railway_query("""
        query($projectId: String!) {
          project(id: $projectId) {
            id name description createdAt updatedAt
            environments { edges { node { id name createdAt } } }
            services { edges { node { id name createdAt updatedAt } } }
          }
        }
        """, {})
        proj = data.get("project", {})
        return {
            "name": proj.get("name"),
            "description": proj.get("description"),
            "created_at": proj.get("createdAt"),
            "updated_at": proj.get("updatedAt"),
            "environments": [e["node"]["name"] for e in proj.get("environments", {}).get("edges", [])],
            "services": [
                {"name": s["node"]["name"], "id": s["node"]["id"][:12]}
                for s in proj.get("services", {}).get("edges", [])
            ],
        }
    except Exception as e:
        return {"error": str(e)}


def railway_get_service_status() -> dict[str, Any]:
    """Her servisin son deployment durumu — çalışıyor mu, ne zaman deploy edildi."""
    try:
        data = _railway_query("""
        query($projectId: String!) {
          project(id: $projectId) {
            services {
              edges {
                node {
                  id name
                  deployments(first: 1) {
                    edges {
                      node {
                        status createdAt updatedAt
                        meta { commitMessage commitSha }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """, {})
        services = data.get("project", {}).get("services", {}).get("edges", [])
        result = []
        for s in services:
            node = s["node"]
            deploys = node.get("deployments", {}).get("edges", [])
            last = deploys[0]["node"] if deploys else {}
            result.append({
                "service": node["name"],
                "status": last.get("status", "NO_DEPLOY"),
                "last_deploy": last.get("createdAt", ""),
                "last_commit": (last.get("meta") or {}).get("commitMessage", "")[:60],
                "sha": (last.get("meta") or {}).get("commitSha", "")[:8],
            })
        return {"services": result}
    except Exception as e:
        return {"error": str(e)}


def railway_get_logs(service_name: str = "", lines: int = 50) -> dict[str, Any]:
    """Railway servis ve ortam yapısını listeler (gerçek log streaming Railway dashboard'dan yapılır)."""
    try:
        data = _railway_query("""
        query($projectId: String!) {
          project(id: $projectId) {
            environments { edges { node { id name } } }
            services { edges { node { id name } } }
          }
        }
        """, {})
        proj = data.get("project", {})
        services = [e["node"] for e in proj.get("services", {}).get("edges", [])]
        envs = [e["node"] for e in proj.get("environments", {}).get("edges", [])]
        return {
            "services": [{"id": s["id"][:12], "name": s["name"]} for s in services],
            "environments": [{"id": e["id"][:12], "name": e["name"]} for e in envs],
            "note": "canlı log için Railway dashboard → Deployments → View Logs kullan.",
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
        "GEMINI_API_KEY": "set" if settings.gemini_api_key else "missing",
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


# ── GitHub kod yazma + PR araçları ───────────────────────────────────────────

import base64 as _base64


def github_get_file(path: str, branch: str = "main") -> dict[str, Any]:
    """GitHub'dan bir dosyanın içeriğini okur."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/contents/{path}",
            params={"ref": branch},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        content = _base64.b64decode(data["content"]).decode("utf-8", errors="replace")
        return {"path": data["path"], "sha": data["sha"], "content": content[:8000], "size": data["size"]}
    except Exception as e:
        return {"error": str(e)}


def github_create_or_update_file(path: str, content: str, message: str, branch: str = "main") -> dict[str, Any]:
    """GitHub'da yeni dosya oluşturur veya mevcut dosyayı günceller."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        # Mevcut SHA'yı al (update için gerekli)
        existing_sha: str | None = None
        r = httpx.get(
            f"{_GH_BASE}/repos/{repo}/contents/{path}",
            params={"ref": branch},
            headers=_gh_headers(),
            timeout=10,
        )
        if r.status_code == 200:
            existing_sha = r.json().get("sha")

        payload: dict[str, Any] = {
            "message": message,
            "content": _base64.b64encode(content.encode()).decode(),
            "branch": branch,
        }
        if existing_sha:
            payload["sha"] = existing_sha

        r = httpx.put(
            f"{_GH_BASE}/repos/{repo}/contents/{path}",
            json=payload,
            headers=_gh_headers(),
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "path": path,
            "branch": branch,
            "sha": data.get("content", {}).get("sha", "")[:8],
            "url": data.get("content", {}).get("html_url", ""),
            "action": "updated" if existing_sha else "created",
        }
    except Exception as e:
        return {"error": str(e)}


def github_create_branch_from_main(branch_name: str) -> dict[str, Any]:
    """main'den yeni bir branch oluşturur."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        # main'in SHA'sını al
        r = httpx.get(f"{_GH_BASE}/repos/{repo}/git/ref/heads/main", headers=_gh_headers(), timeout=10)
        r.raise_for_status()
        sha = r.json()["object"]["sha"]
        # Branch oluştur
        r = httpx.post(
            f"{_GH_BASE}/repos/{repo}/git/refs",
            json={"ref": f"refs/heads/{branch_name}", "sha": sha},
            headers=_gh_headers(),
            timeout=10,
        )
        r.raise_for_status()
        return {"branch": branch_name, "sha": sha[:8], "created": True}
    except Exception as e:
        return {"error": str(e)}


def github_create_pr(title: str, body: str, branch: str, base: str = "main") -> dict[str, Any]:
    """GitHub'da pull request açar."""
    repo = settings.github_repo or "cemevecen/seo_agent"
    try:
        r = httpx.post(
            f"{_GH_BASE}/repos/{repo}/pulls",
            json={"title": title, "body": body, "head": branch, "base": base},
            headers=_gh_headers(),
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        return {"number": data["number"], "url": data["html_url"], "title": data["title"], "state": data["state"]}
    except Exception as e:
        return {"error": str(e)}


# ── Veritabanı şema + doğal dil sorgu araçları ────────────────────────────────

def db_get_schema() -> dict[str, Any]:
    """Veritabanı şemasını döner: tablo adları, sütunlar, satır sayıları."""
    try:
        db = SessionLocal()
        try:
            result = db.execute(text("""
                SELECT
                    t.tablename,
                    s.n_live_tup AS row_count,
                    pg_size_pretty(pg_total_relation_size('public.'||t.tablename)) AS size
                FROM pg_tables t
                LEFT JOIN pg_stat_user_tables s ON s.relname = t.tablename
                WHERE t.schemaname = 'public'
                ORDER BY s.n_live_tup DESC NULLS LAST
                LIMIT 40
            """))
            tables = []
            for row in result:
                # Sütunları al
                cols_result = db.execute(text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :tname
                    ORDER BY ordinal_position
                    LIMIT 20
                """), {"tname": row.tablename})
                cols = [{"name": c.column_name, "type": c.data_type, "nullable": c.is_nullable == "YES"}
                        for c in cols_result]
                tables.append({"table": row.tablename, "rows": row.row_count or 0, "size": row.size, "columns": cols})
            return {"tables": tables, "count": len(tables)}
        finally:
            db.close()
    except Exception as e:
        return {"error": str(e)}


# ── Sohbet geçmişi araçları ───────────────────────────────────────────────────

def ai_talk_save_messages(session_id: str, messages: list[dict]) -> dict[str, Any]:
    """Sohbet geçmişini veritabanına kaydeder (maks 30 mesaj)."""
    try:
        from backend.models import AiTalkHistory
        db = SessionLocal()
        try:
            record = db.query(AiTalkHistory).filter_by(session_id=session_id).first()
            kept = messages[-30:]
            payload = json.dumps(kept, ensure_ascii=False)
            if record:
                record.messages = payload
                record.message_count = len(kept)
                record.last_message_at = datetime.utcnow()
            else:
                record = AiTalkHistory(
                    session_id=session_id,
                    messages=payload,
                    message_count=len(kept),
                    last_message_at=datetime.utcnow(),
                )
                db.add(record)
            db.commit()
            return {"ok": True, "count": len(kept)}
        finally:
            db.close()
    except Exception as e:
        return {"error": str(e)}


def ai_talk_get_messages(session_id: str) -> list[dict]:
    """Session'a ait sohbet geçmişini döner."""
    try:
        from backend.models import AiTalkHistory
        db = SessionLocal()
        try:
            record = db.query(AiTalkHistory).filter_by(session_id=session_id).first()
            if not record:
                return []
            return json.loads(record.messages or "[]")
        finally:
            db.close()
    except Exception:
        return []


# ── Proaktif izleme ───────────────────────────────────────────────────────────

def create_alert(alert_type: str, severity: str, title: str, summary: str, detail: dict) -> None:
    """Yeni alert kaydı oluşturur (duplicate check: son 2 saatte aynı tip varsa oluşturma)."""
    try:
        from backend.models import AiTalkAlert
        db = SessionLocal()
        try:
            cutoff = datetime.utcnow() - timedelta(hours=2)
            existing = db.query(AiTalkAlert).filter(
                AiTalkAlert.alert_type == alert_type,
                AiTalkAlert.created_at >= cutoff,
            ).first()
            if existing:
                return
            alert = AiTalkAlert(
                alert_type=alert_type,
                severity=severity,
                title=title,
                summary=summary,
                detail=json.dumps(detail, ensure_ascii=False, default=str),
            )
            db.add(alert)
            db.commit()
        finally:
            db.close()
    except Exception as e:
        LOGGER.warning("Alert kaydı oluşturulamadı: %s", e)


def get_unread_alerts(limit: int = 10) -> list[dict]:
    """Okunmamış uyarıları döner."""
    try:
        from backend.models import AiTalkAlert
        db = SessionLocal()
        try:
            rows = db.query(AiTalkAlert).filter(
                AiTalkAlert.read_at.is_(None)
            ).order_by(AiTalkAlert.created_at.desc()).limit(limit).all()
            return [
                {
                    "id": r.id, "type": r.alert_type, "severity": r.severity,
                    "title": r.title, "summary": r.summary,
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]
        finally:
            db.close()
    except Exception as e:
        LOGGER.warning("Alert listesi alınamadı: %s", e)
        return []


def mark_alert_read(alert_id: int) -> dict[str, Any]:
    """Uyarıyı okundu olarak işaretler."""
    try:
        from backend.models import AiTalkAlert
        db = SessionLocal()
        try:
            alert = db.query(AiTalkAlert).filter_by(id=alert_id).first()
            if alert:
                alert.read_at = datetime.utcnow()
                db.commit()
            return {"ok": True}
        finally:
            db.close()
    except Exception as e:
        return {"error": str(e)}


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
        "name": "github_list_branches",
        "description": "GitHub repo'sundaki tüm branch'leri listeler. Hangi branch'ler var, hangisi main, eski branch var mı diye kontrol eder.",
        "input_schema": {"type": "object", "properties": {}},
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
        "name": "github_commit_stats",
        "description": "Projedeki toplam commit sayısını ve her branch'teki commit sayısını döner. 'toplam kaç commit var', 'en çok commit hangi branch' gibi sorular için kullan.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "github_contributor_stats",
        "description": "Kim kaç commit atmış, en aktif geliştirici kim, toplam katkı sayısı kaç. 'katkıda bulunanlar', 'en çok commit atan' gibi sorular için.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "github_file_history",
        "description": "Belirli bir dosyanın commit geçmişi: kim ne zaman değiştirdi. 'bu dosya en son ne zaman değişti', 'X dosyasını kim yazdı' gibi sorular için.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Dosya yolu, ör: backend/main.py"},
                "limit": {"type": "integer", "description": "Kaç commit (max 10)", "default": 5},
            },
            "required": ["path"],
        },
    },
    {
        "name": "github_repo_languages",
        "description": "Repo'da hangi programlama dilleri kullanılıyor, yüzde dağılımı nedir.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "github_search_code",
        "description": "Repo içinde kod/metin arar. 'X fonksiyonu nerede', 'Y değişkeni hangi dosyada tanımlı', 'Z endpoint nerede' gibi sorular için.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Arama terimi, ör: 'stream_agent_response' veya 'def login'"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "github_get_releases",
        "description": "Repo'daki release/sürüm geçmişini listeler.",
        "input_schema": {"type": "object", "properties": {}},
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
        "name": "github_get_repo_info",
        "description": "GitHub repo genel bilgisi: default branch, açık issue sayısı, son push zamanı, görünürlük.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "github_get_branch_diff",
        "description": "İki branch arasındaki farkı karşılaştırır. Bir branch main'den ne kadar geride/ileride, hangi commitler var.",
        "input_schema": {
            "type": "object",
            "properties": {
                "base": {"type": "string", "description": "Temel branch (varsayılan: main)", "default": "main"},
                "head": {"type": "string", "description": "Karşılaştırılacak branch"},
            },
            "required": ["head"],
        },
    },
    {
        "name": "github_list_workflows",
        "description": "GitHub Actions CI/CD workflow çalıştırmalarını listeler. Build başarısız mı, test geçti mi kontrol eder.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "github_get_file",
        "description": "GitHub'da bir dosyanın içeriğini okur. Mevcut kodu görmek veya düzenlemeden önce okumak için kullan.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Dosya yolu, ör: backend/main.py"},
                "branch": {"type": "string", "description": "Branch adı", "default": "main"},
            },
            "required": ["path"],
        },
    },
    {
        "name": "github_create_or_update_file",
        "description": "GitHub'da yeni dosya oluştur veya mevcut dosyayı güncelle. Kod yazma işlemleri için.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Dosya yolu"},
                "content": {"type": "string", "description": "Dosyanın tam içeriği"},
                "message": {"type": "string", "description": "Commit mesajı"},
                "branch": {"type": "string", "description": "Hangi branch'e yazılacak", "default": "main"},
            },
            "required": ["path", "content", "message"],
        },
    },
    {
        "name": "github_create_branch_from_main",
        "description": "main'den yeni bir feature branch oluşturur. PR açmadan önce kullan.",
        "input_schema": {
            "type": "object",
            "properties": {
                "branch_name": {"type": "string", "description": "Yeni branch adı, ör: fix/ga4-quota-error"},
            },
            "required": ["branch_name"],
        },
    },
    {
        "name": "github_create_pr",
        "description": "Bir branch'ten main'e pull request açar.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "PR başlığı"},
                "body": {"type": "string", "description": "PR açıklaması (Markdown)"},
                "branch": {"type": "string", "description": "Kaynak branch"},
                "base": {"type": "string", "description": "Hedef branch", "default": "main"},
            },
            "required": ["title", "body", "branch"],
        },
    },
    {
        "name": "db_get_schema",
        "description": "Veritabanı şemasını döner: tüm tablolar, sütunlar, satır sayıları. Doğal dil sorularını SQL'e çevirmeden önce kullan.",
        "input_schema": {"type": "object", "properties": {}},
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
        "description": "Son deployment'ların listesi ve durumları (SUCCESS/FAILED/CRASHED). 'son deploy ne zaman', 'deploy başarılı mı', 'hangi commit deploy edildi' gibi sorular için.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Kaç deployment (max 10)", "default": 5},
            },
        },
    },
    {
        "name": "railway_get_project_info",
        "description": "Railway proje genel bilgisi: servisler, ortamlar, proje adı, oluşturulma tarihi. 'kaç servis var', 'ortamlar neler' gibi sorular için.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "railway_get_service_status",
        "description": "Her servisin anlık durumu: çalışıyor mu, son deploy ne zaman, hangi commit. 'servisler ayakta mı', 'production sağlıklı mı' gibi sorular için.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "railway_get_logs",
        "description": "Railway servis ve ortam listesi. Canlı log için dashboard yönlendirmesi yapar.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_name": {"type": "string", "default": ""},
                "lines": {"type": "integer", "default": 50},
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
    {
        "name": "page_fetch_crashlytics_summary",
        "description": "Firebase/Crashlytics özeti: fatal/anr/non_fatal, crash-free, top issue'lar.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "default": "doviz"},
                "platform": {"type": "string", "default": "all"},
                "days": {"type": "integer", "default": 7},
                "limit_issues": {"type": "integer", "default": 8},
            },
        },
    },
    {
        "name": "page_fetch_inbox_threads",
        "description": "Inbox thread listesi. route: all|doviz|sinemalar|reklam|nstat|firebase",
        "input_schema": {
            "type": "object",
            "properties": {
                "route": {"type": "string", "default": "all"},
                "limit": {"type": "integer", "default": 15},
            },
        },
    },
    {
        "name": "page_fetch_inbox_thread",
        "description": "Tek inbox thread detayı, mesajlar ve AI özet/taslak.",
        "input_schema": {
            "type": "object",
            "properties": {"thread_id": {"type": "integer"}},
            "required": ["thread_id"],
        },
    },
    {
        "name": "page_fetch_news_intelligence",
        "description": "NEWS/intelligence son haber başlıkları.",
        "input_schema": {
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "default": 12},
                "source": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
            },
        },
    },
    {
        "name": "page_fetch_app_intel",
        "description": "App Store / Play intel KPI özeti.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product": {"type": "string", "default": "doviz"},
                "period_days": {"type": "integer", "default": 30},
            },
        },
    },
    {
        "name": "page_fetch_errors_summary",
        "description": "404/5xx hata özeti (site_id gerekli).",
        "input_schema": {
            "type": "object",
            "properties": {
                "site_id": {"type": "integer"},
                "days": {"type": "integer", "default": 7},
            },
            "required": ["site_id"],
        },
    },
    {
        "name": "page_fetch_ga4_realtime",
        "description": "GA4 realtime tek site özeti.",
        "input_schema": {
            "type": "object",
            "properties": {
                "site_id": {"type": "integer"},
                "window": {"type": "integer", "default": 10},
            },
            "required": ["site_id"],
        },
    },
    {
        "name": "page_fetch_home_dashboard",
        "description": "Ana sayfa (Günün Özeti): doviz/sinemalar realtime, GA4 session, Search Console, pozisyon düşüşleri. «bu ekranı özetle» / home sayfası sorularında ZORUNLU.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "page_list_sites",
        "description": "Site id ↔ domain listesi.",
        "input_schema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "default": 30}},
        },
    },
]


def execute_tool(name: str, inputs: dict[str, Any]) -> Any:
    """Araç adına göre ilgili fonksiyonu çalıştırır."""
    from backend.services import page_context_tools as pct

    dispatch = {
        "github_commit_stats": lambda: github_commit_stats(),
        "github_contributor_stats": lambda: github_contributor_stats(),
        "github_file_history": lambda: github_file_history(**inputs),
        "github_repo_languages": lambda: github_repo_languages(),
        "github_search_code": lambda: github_search_code(**inputs),
        "github_get_releases": lambda: github_get_releases(),
        "github_list_branches": lambda: github_list_branches(),
        "github_get_repo_info": lambda: github_get_repo_info(),
        "github_get_branch_diff": lambda: github_get_branch_diff(**inputs),
        "github_list_workflows": lambda: github_list_workflows(),
        "github_get_file": lambda: github_get_file(**inputs),
        "github_create_or_update_file": lambda: github_create_or_update_file(**inputs),
        "github_create_branch_from_main": lambda: github_create_branch_from_main(**inputs),
        "github_create_pr": lambda: github_create_pr(**inputs),
        "github_list_issues": lambda: github_list_issues(**inputs),
        "github_list_prs": lambda: github_list_prs(**inputs),
        "github_recent_commits": lambda: github_recent_commits(**inputs),
        "github_create_issue": lambda: github_create_issue(**inputs),
        "railway_get_deployments": lambda: railway_get_deployments(**inputs),
        "railway_get_project_info": lambda: railway_get_project_info(),
        "railway_get_service_status": lambda: railway_get_service_status(),
        "railway_get_logs": lambda: railway_get_logs(**inputs),
        "db_table_stats": lambda: db_table_stats(),
        "db_get_schema": lambda: db_get_schema(),
        "db_recent_errors": lambda: db_recent_errors(**inputs),
        "db_custom_query": lambda: db_custom_query(**inputs),
        "system_health_check": lambda: system_health_check(),
        "project_structure": lambda: project_structure(),
        "page_fetch_crashlytics_summary": lambda: pct.page_fetch_crashlytics_summary(**inputs),
        "page_fetch_inbox_threads": lambda: pct.page_fetch_inbox_threads(**inputs),
        "page_fetch_inbox_thread": lambda: pct.page_fetch_inbox_thread(**inputs),
        "page_fetch_news_intelligence": lambda: pct.page_fetch_news_intelligence(**inputs),
        "page_fetch_app_intel": lambda: pct.page_fetch_app_intel(**inputs),
        "page_fetch_errors_summary": lambda: pct.page_fetch_errors_summary(**inputs),
        "page_fetch_ga4_realtime": lambda: pct.page_fetch_ga4_realtime(**inputs),
        "page_fetch_home_dashboard": lambda: pct.page_fetch_home_dashboard(),
        "page_list_sites": lambda: pct.page_list_sites(**inputs),
    }
    fn = dispatch.get(name)
    if not fn:
        return {"error": f"Bilinmeyen araç: {name}"}
    try:
        return fn()
    except Exception as e:
        LOGGER.exception("Araç hatası: %s", name)
        return {"error": str(e)}
