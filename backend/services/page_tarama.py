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
    "play_vitals": {
        "id": "play_vitals",
        "label": "Android Vitals",
        "kind": "bridge",
        "path": "/sync-play-vitals",
    },
    "asc": {"id": "asc", "label": "App Store Connect", "kind": "bridge", "path": "/sync-asc"},
    "firebase": {"id": "firebase", "label": "Firebase Console", "kind": "bridge", "path": "/sync-firebase"},
    "cwv": {"id": "cwv", "label": "Web Vitals (GSC)", "kind": "bridge", "path": "/sync-gsc-cwv"},
    "notification": {"id": "notification", "label": "Notification", "kind": "bridge", "path": "/sync"},
    "news": {"id": "news", "label": "News", "kind": "bridge", "path": "/sync-news?days=7"},
    "virgul": {"id": "virgul", "label": "Virgül", "kind": "bridge", "path": "/sync-virgul"},
    "revenue_targets": {
        "id": "revenue_targets",
        "label": "Virgül Targets",
        "kind": "bridge",
        "path": "/sync-revenue-targets",
    },
    "market": {"id": "market", "label": "Market", "kind": "bridge", "path": "/sync-market"},
    "links": {"id": "links", "label": "Backlinks (GSC)", "kind": "bridge", "path": "/sync-gsc-links"},
    "policy": {"id": "policy", "label": "Ad Manager Policy", "kind": "bridge", "path": "/sync-policy"},
    "noads": {"id": "noads", "label": "Sinemalar noAds", "kind": "bridge", "path": "/sync-noads"},
    "moderation": {
        "id": "moderation",
        "label": "Sinemalar Moderation",
        "kind": "bridge",
        "path": "/sync-sinemalar-moderation?which=both",
    },
    "empower_intel": {
        "id": "empower_intel",
        "label": "Empower Intel (Döviz)",
        "kind": "bridge",
        "path": "/sync-empower-intel?mode=yesterday",
    },
    "empower_intel_sinemalar": {
        "id": "empower_intel_sinemalar",
        "label": "Empower Intel (Sinemalar)",
        "kind": "bridge",
        "path": "/sync-empower-intel-sinemalar?mode=yesterday",
    },
    "seo": {"id": "seo", "label": "SEO Audit", "kind": "bridge", "path": "/sync-seo-audit"},
    "errors": {
        "id": "errors",
        "label": "Errors / CSV scan",
        "kind": "poll",
        "startUrl": "/api/errors/refresh-all/start",
        "progressUrl": "/api/errors/refresh-all/progress",
    },
    "alerts": {
        "id": "alerts",
        "label": "Alerts (Search Console)",
        "kind": "poll",
        "startUrl": "/alerts/refresh",
        "progressUrl": "/alerts/refresh/status",
    },
}

PAGES: dict[str, list[str]] = {
    "home": ["play", "asc", "firebase", "cwv", "notification", "virgul", "market"],
    "android": ["play_vitals", "play", "firebase", "market"],
    "ios": ["asc", "firebase"],
    "news": ["news"],
    "virgul": ["virgul", "revenue_targets"],
    "notification": ["notification"],
    "firebase": ["firebase"],
    "app": ["play", "asc", "firebase"],
    "vitals": ["cwv"],
    "alerts": ["alerts"],
    "seo": ["seo"],
    "backlinks": ["links"],
    "policy": ["policy", "noads"],
    "moderation": ["moderation"],
    "empower-sinemalar": ["empower_intel_sinemalar"],
    "x-data": ["empower_intel"],
    "errors": ["errors"],
}

BRIDGE_STALE_SEC = 90.0
# Claimed/running iş progress göndermezse (daemon çöktü / claim loop kilitli) kuyruk açılsın.
# Firebase/ASC uzun; Railway yavaşken progress post timeout olabilir — 3 dk çok kısa.
PROGRESS_STALE_SEC = 900.0
# SEO + Virgül gibi farklı işler birbirini bloklamasın (Mac kilitleri ayrıca korur)
MAX_INFLIGHT_JOBS = 3
# Play/Firebase/ASC/GSC/Policy aynı Firefox profili — biri bitmeden diğeri claim edilmesin
BROWSER_JOB_IDS = frozenset(
    {
        "play",
        "play_vitals",
        "asc",
        "firebase",
        "cwv",
        "links",
        "policy",
        "noads",
        "moderation",
        "empower_intel",
        "empower_intel_sinemalar",
    }
)
CLAIM_STALE_SEC = 2 * 60 * 60
RUN_TTL_SEC = 3 * 60 * 60
MANUAL_LIMIT = 3
MANUAL_WINDOW_SEC = 60 * 60

_lock = threading.Lock()
_runs: dict[str, dict[str, Any]] = {}
_manual_starts: list[float] = []
_bridge_seen_at: float | None = None
_memory_only = False


class ManualLimitExceeded(Exception):
    def __init__(self, quota: dict[str, Any]):
        self.quota = quota
        super().__init__(str(quota.get("message") or "manual_limit"))


def reset_for_tests() -> None:
    global _bridge_seen_at, _memory_only
    _memory_only = True
    with _lock:
        _runs.clear()
        _manual_starts.clear()
        _bridge_seen_at = None


def jobs_for(page: str) -> list[dict[str, Any]]:
    ids = PAGES.get((page or "").strip()) or []
    out = []
    for jid in ids:
        spec = JOBS.get(jid)
        if spec:
            out.append(dict(spec))
    return out


def _unpack_state(loaded: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], list[float]]:
    if isinstance(loaded.get("runs"), dict) and "manual_starts" in loaded:
        starts = []
        for raw in loaded.get("manual_starts") or []:
            try:
                starts.append(float(raw))
            except (TypeError, ValueError):
                continue
        runs = loaded.get("runs") or {}
        if not isinstance(runs, dict):
            runs = {}
        return runs, starts
    return loaded, []


def _pack_state() -> dict[str, Any]:
    return {"runs": _runs, "manual_starts": list(_manual_starts)}


def _prune_manual_locked(now: float) -> None:
    cutoff = now - MANUAL_WINDOW_SEC
    _manual_starts[:] = [t for t in _manual_starts if float(t) > cutoff]


def _fmt_retry(sec: int) -> str:
    if sec <= 60:
        return "1 min"
    mins = int(round(sec / 60.0))
    if mins < 60:
        return f"{mins} min"
    hours = max(1, int(round(mins / 60.0)))
    return f"{hours} h"


def is_manual_limit_exempt(email: str | None = None, *, unlimited: bool = False) -> bool:
    """Owner/admin hesaplar (cemevecen@gmail.com, cemevecen@nokta.com) saatte 3 sınırına tabi değil."""
    if unlimited:
        return True
    em = (email or "").strip()
    if not em:
        return False
    try:
        from backend.services.panel_visitor_alerts import is_owner_email

        return bool(is_owner_email(em))
    except Exception:  # noqa: BLE001
        from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS

        return em.lower() in {e.lower() for e in ADMIN_MEMBER_EMAILS}


def _quota_locked(now: float, *, unlimited: bool = False) -> dict[str, Any]:
    _prune_manual_locked(now)
    if unlimited:
        return {
            "limit": 0,
            "used": 0,
            "remaining": 999,
            "window_sec": int(MANUAL_WINDOW_SEC),
            "retry_after_sec": 0,
            "unlimited": True,
            "message": "Unlimited Update page (admin)",
        }
    used = len(_manual_starts)
    remaining = max(0, MANUAL_LIMIT - used)
    retry_after = 0
    if remaining <= 0 and _manual_starts:
        oldest = min(float(t) for t in _manual_starts)
        retry_after = int(max(1, round(oldest + MANUAL_WINDOW_SEC - now)))
    if remaining <= 0:
        message = (
            f"At most {MANUAL_LIMIT} page updates per hour. "
            f"Try again in {_fmt_retry(retry_after)}."
        )
    else:
        message = f"{MANUAL_LIMIT} per hour · {remaining} left"
    return {
        "limit": MANUAL_LIMIT,
        "used": used,
        "remaining": remaining,
        "window_sec": int(MANUAL_WINDOW_SEC),
        "retry_after_sec": retry_after,
        "unlimited": False,
        "message": message,
    }


def quota_status(*, email: str | None = None, unlimited: bool = False) -> dict[str, Any]:
    exempt = is_manual_limit_exempt(email, unlimited=unlimited)
    now = time.time()
    with _state():
        return _quota_locked(now, unlimited=exempt)


def begin_manual(
    page: str,
    *,
    email: str | None = None,
    unlimited: bool = False,
) -> dict[str, Any]:
    """Kotadan 1 hak düş; köprü işi varsa kuyruğa yaz. Admin e-postaları kotadan muaf."""
    page = (page or "").strip()
    if page not in PAGES:
        raise ValueError("unknown_page")
    specs = [s for s in jobs_for(page) if s.get("kind") == "bridge"]
    exempt = is_manual_limit_exempt(email, unlimited=unlimited)
    now = time.time()
    with _state():
        quota = _quota_locked(now, unlimited=exempt)
        if not exempt:
            if quota["remaining"] <= 0:
                raise ManualLimitExceeded(quota)
            _manual_starts.append(now)
            quota = _quota_locked(now, unlimited=False)
        run = None
        if specs:
            run_id = uuid.uuid4().hex[:16]
            jobs = []
            for spec in specs:
                jobs.append(
                    {
                        "id": spec["id"],
                        "label": spec["label"],
                        "kind": spec["kind"],
                        "path": spec.get("path") or "",
                        "status": "queued",
                        "detail": "",
                        "claimed_at": None,
                    }
                )
            rec = {
                "id": run_id,
                "page": page,
                "started_at": now,
                "jobs": jobs,
                "local_kicked": False,
            }
            _prune_locked(now)
            _runs[run_id] = rec
            run = _public_run_locked(rec, now)
        return {"quota": quota, "run": run}


@contextmanager
def _state() -> Iterator[None]:
    """Kuyruk durumunu Postgres’te kilitle; tablo yoksa süreç-içi belleğe düş."""
    global _runs, _bridge_seen_at, _manual_starts
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
            _runs, _manual_starts = _unpack_state(loaded)
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
            row.runs_json = json.dumps(_pack_state())
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


def touch_bridge(*, refresh_inflight: bool = False) -> None:
    """Mac keepalive. refresh_inflight yalnızca gerçek progress POST ile kullanılmalı.

    bridge-ping ile progress_at yenilenmez — aksi halde Mac restart sonrası
    zombie claimed/running işler sonsuza kadar %94'te kalır (lost-progress reaper çalışmaz).
    """
    global _bridge_seen_at
    now = time.time()
    with _state():
        _bridge_seen_at = now
        if not refresh_inflight:
            return
        for run in _runs.values():
            for job in run["jobs"]:
                if job.get("kind") != "bridge":
                    continue
                if job.get("status") in ("claimed", "running"):
                    job["progress_at"] = now


def fail_inflight_jobs(*, reason: str = "Scan interrupted — try Update page again") -> int:
    """Claimed/running köprü işlerini fail et (Mac bridge restart / orphan temizliği)."""
    now = time.time()
    n = 0
    msg = (reason or "Scan interrupted")[:180]
    with _state():
        for run in _runs.values():
            for job in run["jobs"]:
                if job.get("kind") != "bridge":
                    continue
                if job.get("status") not in ("claimed", "running"):
                    continue
                job["status"] = "fail"
                job["detail"] = msg
                job["finished_at"] = now
                n += 1
    return n


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
    """Mac daemon bir sonraki köprü işini alır.

    Aynı anda en fazla MAX_INFLIGHT_JOBS; aynı job_id ikinci kez claim edilmez.
    BROWSER_JOB_IDS (Play/Firebase/ASC…) tek sıra — waiting_lock / çift RUNNING yok.
    Market/Virgül/SEO tarayıcı dışı işler paralel kalabilir.
    """
    global _bridge_seen_at
    now = time.time()
    with _state():
        _bridge_seen_at = now
        _prune_locked(now)
        _reap_stale_inflight_locked(now)
        inflight_ids: set[str] = set()
        inflight_n = 0
        browser_inflight = False
        for run in _runs.values():
            _expire_locked(run, now)
            for job in run["jobs"]:
                if job.get("kind") != "bridge":
                    continue
                if job.get("status") in ("claimed", "running"):
                    inflight_n += 1
                    jid = str(job.get("id") or "")
                    if jid:
                        inflight_ids.add(jid)
                    if jid in BROWSER_JOB_IDS:
                        browser_inflight = True
        if inflight_n >= MAX_INFLIGHT_JOBS:
            return None
        for run in sorted(_runs.values(), key=lambda r: float(r.get("started_at") or 0)):
            _expire_locked(run, now)
            for job in run["jobs"]:
                if job.get("kind") != "bridge" or job.get("status") != "queued":
                    continue
                jid = str(job.get("id") or "")
                if jid and jid in inflight_ids:
                    continue
                if jid in BROWSER_JOB_IDS and browser_inflight:
                    continue
                job["status"] = "claimed"
                job["claimed_at"] = now
                job["progress_at"] = now
                job["detail"] = "Queued on Mac · starting"
                return {
                    "run_id": run["id"],
                    "job_id": job["id"],
                    "path": job.get("path") or "",
                    "label": job.get("label") or job["id"],
                    "page": str(run.get("page") or ""),
                }
        return None


def requeue_claim(run_id: str, job_id: str, *, detail: str = "") -> bool:
    """Mac kilit meşgulse claim'i geri al — UI 'waiting' kalsın, fail olmasın."""
    now = time.time()
    with _state():
        run = _runs.get((run_id or "").strip())
        if not run:
            return False
        for job in run["jobs"]:
            if job.get("id") != job_id:
                continue
            if job.get("status") not in ("claimed", "running"):
                return False
            job["status"] = "queued"
            job["claimed_at"] = None
            job["progress_at"] = now
            job["detail"] = (detail or "Waiting for previous scan · back in queue")[:180]
            for key in ("phase", "step", "total_steps", "platform", "sub_label"):
                job.pop(key, None)
            return True
        return False


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
            job["detail"] = (message or ("Done" if ok else "Error"))[:180]
            job["finished_at"] = now
            return True
        return False


def mark_running(
    run_id: str,
    job_id: str,
    detail: str = "",
    *,
    phase: str = "",
    step: int | None = None,
    total_steps: int | None = None,
    platform: str = "",
    sub_label: str = "",
) -> None:
    with _state():
        run = _runs.get(run_id)
        if not run:
            return
        for job in run["jobs"]:
            if job.get("id") == job_id:
                job["status"] = "running"
                if detail:
                    job["detail"] = detail[:400]
                if phase:
                    job["phase"] = str(phase)[:80]
                if platform:
                    job["platform"] = str(platform)[:40]
                if sub_label:
                    job["sub_label"] = str(sub_label)[:160]
                if step is not None:
                    try:
                        job["step"] = max(0, int(step))
                    except (TypeError, ValueError):
                        pass
                if total_steps is not None:
                    try:
                        job["total_steps"] = max(0, int(total_steps))
                    except (TypeError, ValueError):
                        pass
                job["progress_at"] = time.time()
                return


def _reap_stale_inflight_locked(now: float) -> None:
    """İlerleme kalp atışı kesilen claimed/running işleri fail et — kuyruk kilitlenmesin."""
    for run in _runs.values():
        for job in run["jobs"]:
            if job.get("kind") != "bridge":
                continue
            if job.get("status") not in ("claimed", "running"):
                continue
            claimed_at = float(job.get("claimed_at") or now)
            progress_at = float(job.get("progress_at") or claimed_at)
            age = now - claimed_at
            quiet = now - progress_at
            if age >= CLAIM_STALE_SEC:
                job["status"] = "fail"
                job["detail"] = "Scan timed out"
                job["finished_at"] = now
            elif quiet >= PROGRESS_STALE_SEC:
                job["status"] = "fail"
                job["detail"] = "Scan lost progress — try again"
                job["finished_at"] = now


def _any_in_flight_locked() -> bool:
    for run in _runs.values():
        for job in run["jobs"]:
            if job.get("status") in ("claimed", "running"):
                return True
    return False


def _expire_locked(run: dict[str, Any], now: float) -> None:
    """Bridge yoksa / sessizse bekleyen işleri fail et. Önce stale in-flight temizlenir."""
    _reap_stale_inflight_locked(now)
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
            job["detail"] = "Automatic scan unavailable"
            job["finished_at"] = now


def _fmt_elapsed(sec: float) -> str:
    sec = max(0, int(sec or 0))
    if sec < 60:
        return f"{sec}s"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _public_run_locked(run: dict[str, Any], now: float) -> dict[str, Any]:
    jobs = [dict(j) for j in run["jobs"]]
    bridge_jobs = [j for j in jobs if j.get("kind") == "bridge"]
    active = [
        j
        for j in jobs
        if j.get("status") in ("queued", "claimed", "running", "wait")
    ]
    waiting = [j for j in jobs if j.get("status") in ("queued", "wait")]
    done_ok = sum(1 for j in jobs if j.get("status") == "ok")
    done_fail = sum(1 for j in jobs if j.get("status") == "fail")
    done = done_ok + done_fail
    total = max(1, len(jobs))
    age = None if _bridge_seen_at is None else max(0.0, now - _bridge_seen_at)
    failed = done_fail
    running = bool(active)
    current = next((j for j in jobs if j.get("status") in ("running", "claimed")), None)
    elapsed = max(0.0, now - float(run.get("started_at") or now))

    # Ağırlıklı yüzde: biten işler + mevcut işin alt adımları
    frac = float(done)
    if current:
        st = current.get("step")
        ts = current.get("total_steps")
        try:
            st_i = int(st) if st is not None else 0
            ts_i = int(ts) if ts is not None else 0
        except (TypeError, ValueError):
            st_i, ts_i = 0, 0
        if ts_i > 0:
            frac += min(0.99, max(0.0, st_i / ts_i))
        else:
            frac += 0.15  # çalışıyor ama alt adım yok
    pct = int(round(100 * frac / total)) if total else 0
    if running:
        pct = min(99, max(1, pct)) if done < total else min(99, pct)
    else:
        pct = 100 if not failed else int(round(100 * done / total))

    waiting_labels = [str(j.get("label") or j.get("id") or "") for j in waiting]
    current_label = str((current or {}).get("label") or "") if current else ""
    current_detail = str((current or {}).get("detail") or "") if current else ""
    current_phase = str((current or {}).get("phase") or "") if current else ""
    current_sub = str((current or {}).get("sub_label") or "") if current else ""
    current_plat = str((current or {}).get("platform") or "") if current else ""
    cur_step = (current or {}).get("step")
    cur_total_steps = (current or {}).get("total_steps")

    if current:
        bits = [current_label or "Scan"]
        if current_plat:
            bits.append(current_plat)
        if current_phase:
            bits.append(current_phase)
        if current_sub:
            bits.append(current_sub)
        elif current_detail:
            bits.append(current_detail[:120])
        if cur_step is not None and cur_total_steps:
            bits.append(f"step {cur_step}/{cur_total_steps}")
        msg = " · ".join(bits)
    elif running and age is None:
        msg = (
            f"Waiting for scan… {len(waiting)} job(s) queued"
            + (f" ({', '.join(waiting_labels[:4])})" if waiting_labels else "")
        )
    elif running and age is not None:
        msg = (
            f"Queued · last activity {_fmt_elapsed(age)} ago · "
            f"{len(waiting)} waiting"
            + (f": {', '.join(waiting_labels[:4])}" if waiting_labels else "")
        )
    elif failed:
        msg = f"Finished with {failed} failure(s) · {done_ok} ok · {_fmt_elapsed(elapsed)}"
    else:
        msg = f"All {total} scan(s) finished · {_fmt_elapsed(elapsed)}"
        pct = 100

    return {
        "id": run["id"],
        "page": run["page"],
        "running": running,
        "pct": pct,
        "jobs": jobs,
        "bridge_seen_at": _bridge_seen_at,
        "bridge_age_sec": age,
        "bridge_jobs": len(bridge_jobs),
        "message": msg,
        "failed": failed,
        "done": done,
        "done_ok": done_ok,
        "done_fail": done_fail,
        "total": len(jobs),
        "waiting": len(waiting),
        "waiting_labels": waiting_labels,
        "current_job_id": (current or {}).get("id"),
        "current_label": current_label,
        "current_detail": current_detail,
        "current_phase": current_phase,
        "current_sub_label": current_sub,
        "current_platform": current_plat,
        "current_step": cur_step,
        "current_total_steps": cur_total_steps,
        "elapsed_sec": int(elapsed),
        "elapsed_label": _fmt_elapsed(elapsed),
        "started_at": run.get("started_at"),
    }
