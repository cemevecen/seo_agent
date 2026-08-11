"""Sayfa tarama kuyruğu — canlı panel 127.0.0.1’e ulaşamaz; Railway kuyruğunu Mac daemon çeker."""

from __future__ import annotations

import json
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

JOBS: dict[str, dict[str, Any]] = {
    "play": {"id": "play", "label": "Play Console", "kind": "bridge", "path": "/sync-play"},
    "asc": {"id": "asc", "label": "App Store Connect", "kind": "bridge", "path": "/sync-asc"},
    "firebase": {"id": "firebase", "label": "Firebase Console", "kind": "bridge", "path": "/sync-firebase"},
    "cwv": {"id": "cwv", "label": "Web Vitals (GSC)", "kind": "bridge", "path": "/sync-gsc-cwv"},
    "notification": {"id": "notification", "label": "Notification", "kind": "bridge", "path": "/sync"},
    "news": {"id": "news", "label": "Haberler", "kind": "bridge", "path": "/sync-news?days=7"},
    "virgul": {"id": "virgul", "label": "Virgül", "kind": "bridge", "path": "/sync-virgul"},
    "market": {"id": "market", "label": "Piyasa", "kind": "bridge", "path": "/sync-market"},
    "links": {"id": "links", "label": "Backlinks (GSC)", "kind": "bridge", "path": "/sync-gsc-links"},
    "policy": {"id": "policy", "label": "Ad Manager Policy", "kind": "bridge", "path": "/sync-policy"},
    "noads": {"id": "noads", "label": "Sinemalar noAds", "kind": "bridge", "path": "/sync-noads"},
    "seo": {"id": "seo", "label": "SEO denetim", "kind": "bridge", "path": "/sync-seo-audit"},
    "errors": {
        "id": "errors",
        "label": "Hata / CSV tarama",
        "kind": "poll",
        "startUrl": "/api/errors/refresh-all/start",
        "progressUrl": "/api/errors/refresh-all/progress",
    },
    "alerts": {
        "id": "alerts",
        "label": "Uyarılar (Search Console)",
        "kind": "api",
        "url": "/alerts/refresh",
        "waitAfterMs": 45000,
    },
}

PAGES: dict[str, list[str]] = {
    "home": ["play", "asc", "firebase", "cwv", "notification", "virgul", "market"],
    "android": ["play", "firebase", "market"],
    "ios": ["asc", "firebase"],
    "news": ["news"],
    "virgul": ["virgul"],
    "notification": ["notification"],
    "firebase": ["firebase"],
    "app": ["play", "asc", "firebase"],
    "vitals": ["cwv"],
    "alerts": ["alerts"],
    "seo": ["seo"],
    "s-firebase": ["firebase"],
    "backlinks": ["links"],
    "policy": ["policy", "noads"],
    "errors": ["errors"],
}

BRIDGE_STALE_SEC = 90.0
CLAIM_STALE_SEC = 2 * 60 * 60
RUN_TTL_SEC = 3 * 60 * 60

_lock = threading.Lock()
_runs: dict[str, dict[str, Any]] = {}
_bridge_seen_at: float | None = None
_memory_only = False


def reset_for_tests() -> None:
    global _bridge_seen_at, _memory_only
    _memory_only = True
    with _lock:
        _runs.clear()
        _bridge_seen_at = None


def jobs_for(page: str) -> list[dict[str, Any]]:
    ids = PAGES.get((page or "").strip()) or []
    out = []
    for jid in ids:
        spec = JOBS.get(jid)
        if spec:
            out.append(dict(spec))
    return out


@contextmanager
def _state() -> Iterator[None]:
    """Kuyruk durumunu Postgres’te kilitle; tablo yoksa süreç-içi belleğe düş."""
    global _runs, _bridge_seen_at
    with _lock:
        if _memory_only:
            yield
            return
        db = None
        row = None
        try:
            from backend.database import SessionLocal
            from backend.models import PageTaramaState

            db = SessionLocal()
            row = (
                db.query(PageTaramaState)
                .filter(PageTaramaState.id == 1)
                .with_for_update()
                .first()
            )
            if row is None:
                row = PageTaramaState(id=1, runs_json="{}", bridge_seen_at=None)
                db.add(row)
                db.flush()
            loaded = json.loads(row.runs_json or "{}")
            if not isinstance(loaded, dict):
                loaded = {}
            _runs = loaded
            _bridge_seen_at = row.bridge_seen_at
        except Exception:
            if db is not None:
                try:
                    db.rollback()
                except Exception:
                    pass
                try:
                    db.close()
                except Exception:
                    pass
            yield
            return
        try:
            yield
            row.runs_json = json.dumps(_runs)
            row.bridge_seen_at = _bridge_seen_at
            row.updated_at = datetime.utcnow()
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            raise
        finally:
            db.close()


def _prune_locked(now: float) -> None:
    dead = [rid for rid, run in _runs.items() if now - float(run.get("started_at") or 0) > RUN_TTL_SEC]
    for rid in dead:
        _runs.pop(rid, None)


def touch_bridge() -> None:
    global _bridge_seen_at
    with _state():
        _bridge_seen_at = time.time()


def bridge_age_sec(now: float | None = None) -> float | None:
    ts = _bridge_seen_at
    if not ts:
        with _state():
            ts = _bridge_seen_at
    if not ts:
        return None
    return max(0.0, (now or time.time()) - ts)


def start_run(page: str) -> dict[str, Any]:
    specs = [s for s in jobs_for(page) if s.get("kind") == "bridge"]
    if not specs:
        raise ValueError("no_bridge_jobs")
    now = time.time()
    run_id = uuid.uuid4().hex[:16]
    jobs = []
    for spec in specs:
        jobs.append(
            {
                "id": spec["id"],
                "label": spec["label"],
                "kind": spec["kind"],
                "path": spec.get("path") or "",
                "status": "queued" if spec["kind"] == "bridge" else "wait",
                "detail": "",
                "claimed_at": None,
            }
        )
    run = {
        "id": run_id,
        "page": page,
        "started_at": now,
        "jobs": jobs,
        "local_kicked": False,
    }
    with _state():
        _prune_locked(now)
        _runs[run_id] = run
        return _public_run_locked(run, now)


def get_run(run_id: str) -> dict[str, Any] | None:
    now = time.time()
    with _state():
        run = _runs.get(run_id)
        if not run:
            return None
        _expire_locked(run, now)
        return _public_run_locked(run, now)


def claim_next() -> dict[str, Any] | None:
    """Mac daemon bir sonraki köprü işini alır. Aynı anda tek claimed/running."""
    global _bridge_seen_at
    now = time.time()
    with _state():
        _bridge_seen_at = now
        _prune_locked(now)
        for run in _runs.values():
            _expire_locked(run, now)
            for job in run["jobs"]:
                if job.get("kind") != "bridge":
                    continue
                if job.get("status") in ("claimed", "running"):
                    age = now - float(job.get("claimed_at") or now)
                    if age < CLAIM_STALE_SEC:
                        return None
                    job["status"] = "fail"
                    job["detail"] = "Mac tarama zaman aşımı"
        for run in sorted(_runs.values(), key=lambda r: float(r.get("started_at") or 0)):
            _expire_locked(run, now)
            for job in run["jobs"]:
                if job.get("kind") == "bridge" and job.get("status") == "queued":
                    job["status"] = "claimed"
                    job["claimed_at"] = now
                    job["detail"] = "Mac tarama başladı"
                    return {
                        "run_id": run["id"],
                        "job_id": job["id"],
                        "path": job.get("path") or "",
                        "label": job.get("label") or job["id"],
                    }
        return None


def record_result(run_id: str, job_id: str, *, ok: bool, message: str = "") -> bool:
    now = time.time()
    with _state():
        run = _runs.get(run_id)
        if not run:
            return False
        for job in run["jobs"]:
            if job.get("id") != job_id:
                continue
            job["status"] = "ok" if ok else "fail"
            job["detail"] = (message or ("Tamam" if ok else "Hata"))[:180]
            job["finished_at"] = now
            return True
        return False


def mark_running(run_id: str, job_id: str, detail: str = "") -> None:
    with _state():
        run = _runs.get(run_id)
        if not run:
            return
        for job in run["jobs"]:
            if job.get("id") == job_id:
                job["status"] = "running"
                if detail:
                    job["detail"] = detail[:180]
                return


def _any_in_flight_locked() -> bool:
    for run in _runs.values():
        for job in run["jobs"]:
            if job.get("status") in ("claimed", "running"):
                return True
    return False


def _expire_locked(run: dict[str, Any], now: float) -> None:
    if _any_in_flight_locked():
        return
    age = None if _bridge_seen_at is None else now - _bridge_seen_at
    started_age = now - float(run.get("started_at") or 0)
    stale = (age is None and started_age >= BRIDGE_STALE_SEC) or (
        age is not None and age >= BRIDGE_STALE_SEC
    )
    if not stale:
        return
    for job in run["jobs"]:
        if job.get("kind") == "bridge" and job.get("status") == "queued":
            job["status"] = "fail"
            job["detail"] = "Mac köprü yok — daemon çalışmıyor"


def _public_run_locked(run: dict[str, Any], now: float) -> dict[str, Any]:
    jobs = [dict(j) for j in run["jobs"]]
    bridge_jobs = [j for j in jobs if j.get("kind") == "bridge"]
    active = [
        j
        for j in jobs
        if j.get("status") in ("queued", "claimed", "running", "wait")
    ]
    done = sum(1 for j in jobs if j.get("status") in ("ok", "fail"))
    total = max(1, len(jobs))
    pct = int(round(100 * done / total)) if not active else int(round(100 * (done + 0.35) / total))
    age = None if _bridge_seen_at is None else max(0.0, now - _bridge_seen_at)
    failed = sum(1 for j in jobs if j.get("status") == "fail")
    running = bool(active)
    msg = ""
    current = next((j for j in jobs if j.get("status") in ("running", "claimed")), None)
    if current:
        msg = (current.get("label") or "") + " çalışıyor…"
    elif running and age is None:
        msg = "Mac köprü bekleniyor…"
    elif running and age is not None:
        msg = "Mac kuyrukta · köprü bağlı"
    elif failed:
        msg = f"{failed} tarama hata verdi."
    else:
        msg = "Tüm taramalar bitti"
        pct = 100
    return {
        "id": run["id"],
        "page": run["page"],
        "running": running,
        "pct": min(99, pct) if running else (100 if done else 0),
        "jobs": jobs,
        "bridge_seen_at": _bridge_seen_at,
        "bridge_age_sec": age,
        "bridge_jobs": len(bridge_jobs),
        "message": msg,
        "failed": failed,
        "done": done,
        "total": len(jobs),
    }
