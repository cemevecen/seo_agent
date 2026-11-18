"""Ana sayfa — izlenen Döviz URL trafiği (ana sayfa / alt linklerle).

İki agregasyon:
- exact: yalnız kategori ana path (ör. kur.doviz.com/)
- tree: ana + tüm alt path’ler (ör. /amerikan-dolari, /euro, …)
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any
from urllib.parse import urlparse

from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    OrderBy,
    RunReportRequest,
)
from sqlalchemy.orm import Session

from backend.collectors.ga4 import _calendar_windows, _client
from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties

LOGGER = logging.getLogger(__name__)

# Ana kategoriler (hub)
WATCH_CATEGORIES: list[dict[str, str]] = [
    {"url": "https://kur.doviz.com/", "label": "Kur", "group": "Kur"},
    {"url": "https://altin.doviz.com/", "label": "Altın", "group": "Altın"},
    {"url": "https://www.doviz.com/emtia", "label": "Emtia", "group": "Emtia"},
    {"url": "https://www.doviz.com/pariteler", "label": "Pariteler", "group": "Pariteler"},
    {"url": "https://www.doviz.com/kripto-paralar", "label": "Kripto", "group": "Kripto"},
    {"url": "https://borsa.doviz.com/", "label": "Borsa", "group": "Borsa"},
    {"url": "https://borsa.doviz.com/halka-arz", "label": "Halka arz", "group": "Halka arz"},
    {"url": "https://www.doviz.com/yakit-sarj", "label": "Yakıt / şarj", "group": "Yakıt"},
    {"url": "https://haber.doviz.com/", "label": "Haber", "group": "Haber"},
    {"url": "https://www.doviz.com/kredi-karsilastirma", "label": "Kredi karşılaştırma", "group": "Kredi"},
]

# Sabit önemli alt sayfalar / section hub’ları (kullanıcı örnekleri + aynı tip).
# scope=exact → yalnız o path; scope=tree → path + alt path’ler.
WATCH_DETAILS: list[dict[str, str]] = [
    # Kur
    {"url": "https://kur.doviz.com/serbest-piyasa/amerikan-dolari", "label": "USD · serbest", "group": "Kur", "scope": "exact"},
    {"url": "https://kur.doviz.com/serbest-piyasa/euro", "label": "EUR · serbest", "group": "Kur", "scope": "exact"},
    {"url": "https://kur.doviz.com/serbest-piyasa/sterlin", "label": "GBP · serbest", "group": "Kur", "scope": "exact"},
    {"url": "https://kur.doviz.com/harem/amerikan-dolari", "label": "USD · harem", "group": "Kur", "scope": "exact"},
    {"url": "https://kur.doviz.com/harem/euro", "label": "EUR · harem", "group": "Kur", "scope": "exact"},
    # Altın
    {"url": "https://altin.doviz.com/gram-altin", "label": "Gram altın", "group": "Altın", "scope": "exact"},
    {"url": "https://altin.doviz.com/ceyrek-altin", "label": "Çeyrek altın", "group": "Altın", "scope": "exact"},
    {"url": "https://altin.doviz.com/ata-altin", "label": "Ata altın", "group": "Altın", "scope": "exact"},
    {"url": "https://altin.doviz.com/gumus", "label": "Gümüş", "group": "Altın", "scope": "exact"},
    {"url": "https://altin.doviz.com/harem", "label": "Harem · altın", "group": "Altın", "scope": "tree"},
    {"url": "https://altin.doviz.com/harem/gram-altin", "label": "Gram · harem", "group": "Altın", "scope": "exact"},
    {"url": "https://altin.doviz.com/akbank/gram-altin", "label": "Gram · akbank", "group": "Altın", "scope": "exact"},
    # Emtia / parite / kripto
    {"url": "https://www.doviz.com/emtia/brent-petrol", "label": "Brent petrol", "group": "Emtia", "scope": "exact"},
    {"url": "https://www.doviz.com/emtia/ons-altin", "label": "Ons altın", "group": "Emtia", "scope": "exact"},
    {"url": "https://www.doviz.com/pariteler/eur-usd", "label": "EUR/USD", "group": "Pariteler", "scope": "exact"},
    {"url": "https://www.doviz.com/pariteler/gbp-usd", "label": "GBP/USD", "group": "Pariteler", "scope": "exact"},
    {"url": "https://www.doviz.com/kripto-paralar/bitcoin", "label": "Bitcoin", "group": "Kripto", "scope": "exact"},
    {"url": "https://www.doviz.com/kripto-paralar/ethereum", "label": "Ethereum", "group": "Kripto", "scope": "exact"},
    # Borsa / halka arz
    {"url": "https://borsa.doviz.com/hisseler", "label": "Hisseler", "group": "Borsa", "scope": "tree"},
    {"url": "https://borsa.doviz.com/hisseler/ktlev-katilimevim-tas-fin", "label": "KTLEV", "group": "Borsa", "scope": "exact"},
    {"url": "https://borsa.doviz.com/endeksler", "label": "Endeksler", "group": "Borsa", "scope": "tree"},
    {"url": "https://borsa.doviz.com/halka-arz/taslak-halka-arzlar", "label": "Taslak halka arz", "group": "Halka arz", "scope": "exact"},
    # Yakıt / şarj (yakit-sarj hub’ından ayrı path’ler)
    {"url": "https://www.doviz.com/akaryakit-fiyatlari", "label": "Akaryakıt fiyatları", "group": "Yakıt", "scope": "tree"},
    {"url": "https://www.doviz.com/ev-sarj-fiyatlari", "label": "Ev şarj fiyatları", "group": "Yakıt", "scope": "tree"},
    # Haber
    {"url": "https://haber.doviz.com/yerel-ve-sektorel-haberleri", "label": "Yerel & sektörel", "group": "Haber", "scope": "tree"},
]

# Geriye dönük alias
WATCH_URLS = WATCH_CATEGORIES

_PERIODS = (1, 7, 30)
_CACHE_TTL_SEC = 600.0
_CACHE_VERSION = 5
_TOP_CHILDREN = 12
_ROW_LIMIT = 8000
_PLATFORMS = ("all", "web", "mweb")
# Desktop alt alan → m.doviz.com path öneki (realtime canonical ile aynı)
_MWEB_PREFIX_BY_HOST = {
    "kur.doviz.com": "/kur",
    "altin.doviz.com": "/altin",
    "borsa.doviz.com": "/borsa",
    "haber.doviz.com": "/haber",
}
_MWEB_HOST = "m.doviz.com"
_cache: dict[str, Any] = {"at": 0.0, "version": 0, "payload": None}


def _parse_url(url: str) -> tuple[str, str]:
    raw = (url or "").strip()
    if not raw:
        return "", "/"
    if "://" not in raw:
        raw = "https://" + raw
    p = urlparse(raw)
    host = (p.hostname or "").strip().lower()
    path = (p.path or "/").strip() or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return host, path


def _path_variants(path: str) -> list[str]:
    p = (path or "/").strip() or "/"
    if p != "/" and p.endswith("/"):
        p = p.rstrip("/")
    if p == "/":
        return ["/"]
    return [p, p + "/"]


def _hosts_for(host: str) -> list[str]:
    h = (host or "").strip().lower()
    if not h:
        return []
    if h.startswith("www."):
        bare = h[4:]
        return [h, f"m.{bare}"]
    return [h]


def _norm_path(path: str) -> str:
    p = (path or "/").strip() or "/"
    if p != "/" and p.endswith("/"):
        p = p.rstrip("/")
    return p


def _fmt_range(start: str, end: str) -> str:
    def _d(iso: str) -> str:
        try:
            y, m, d = iso[:10].split("-")
            return f"{d}.{m}"
        except Exception:
            return iso[:10]

    if start[:10] == end[:10]:
        try:
            y, m, d = start[:10].split("-")
            return f"{d}.{m}.{y}"
        except Exception:
            return start[:10]
    return f"{_d(start)}–{_d(end)}"


def _pct_delta(cur: float, prev: float) -> tuple[str, str, float]:
    try:
        c = float(cur)
        p = float(prev)
    except (TypeError, ValueError):
        return ("—", "flat", 0.0)
    if p <= 0:
        if c > 0:
            return ("+∞", "up-strong", 999.0)
        return ("0%", "flat", 0.0)
    pct = (c - p) / p * 100.0
    ap = abs(pct)
    if ap < 0.05:
        tone = "flat"
    elif pct > 0:
        tone = "up-strong" if ap >= 20 else ("up" if ap >= 8 else "up-mild")
    else:
        tone = "down-strong" if ap >= 20 else ("down" if ap >= 8 else "down-mild")
    sign = "+" if pct > 0 else ""
    return (f"{sign}{pct:.1f}%", tone, round(pct, 2))


def _fmt_int(n: float) -> str:
    try:
        v = int(round(float(n)))
    except (TypeError, ValueError):
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M".replace(".0M", "M")
    if v >= 1_000:
        return f"{v / 1_000:.1f}K".replace(".0K", "K")
    return str(v)


def _child_label(path: str) -> str:
    p = _norm_path(path)
    if p == "/":
        return "ana sayfa"
    return p.rsplit("/", 1)[-1] or p


def _mweb_alias_paths(host: str, path: str) -> list[str]:
    """Desktop host+path → m.doviz.com üzerinde eşdeğer path’ler."""
    prefix = _MWEB_PREFIX_BY_HOST.get((host or "").strip().lower())
    if not prefix:
        return []
    p = _norm_path(path)
    if p == "/":
        return [prefix]
    # /kur + /serbest-piyasa/... → /kur/serbest-piyasa/...
    return [prefix + p]


def _target_from_item(item: dict[str, str], *, kind: str) -> dict[str, Any]:
    host, path = _parse_url(item["url"])
    scope = str(item.get("scope") or ("tree" if kind == "category" else "exact")).strip().lower()
    if scope not in ("exact", "tree"):
        scope = "exact"
    mweb_paths = _mweb_alias_paths(host, path)
    hosts = _hosts_for(host)
    # Subdomain kategoriler: m.doviz.com satırlarını da host setine ekle (eşleme için)
    if mweb_paths and _MWEB_HOST not in hosts:
        hosts = list(hosts) + [_MWEB_HOST]
    return {
        "url": item["url"],
        "label": item["label"],
        "group": item.get("group") or item.get("label") or "",
        "kind": kind,
        "scope": scope,
        "host": host,
        "path": path,
        "hosts": hosts,
        "paths": _path_variants(path),
        "is_root": path == "/",
        "mweb_paths": mweb_paths,
        "mweb_prefix": _MWEB_PREFIX_BY_HOST.get(host) or "",
    }


def _watch_targets(*, include_details: bool = True) -> list[dict[str, Any]]:
    out = [_target_from_item(item, kind="category") for item in WATCH_CATEGORIES]
    if include_details:
        seen = {t["url"] for t in out}
        for item in WATCH_DETAILS:
            if item["url"] in seen:
                continue
            out.append(_target_from_item(item, kind="detail"))
            seen.add(item["url"])
    return out


def _host_group(hosts: list[str]) -> FilterExpression:
    exprs = [
        FilterExpression(
            filter=Filter(
                field_name="hostName",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value=h,
                    case_sensitive=False,
                ),
            )
        )
        for h in hosts
    ]
    if len(exprs) == 1:
        return exprs[0]
    return FilterExpression(or_group=FilterExpressionList(expressions=exprs))


def _path_filter_exprs(paths_exact: list[str], *, prefixes: list[str] | None = None) -> list[FilterExpression]:
    exprs: list[FilterExpression] = []
    seen: set[str] = set()
    for p in paths_exact:
        variants = _path_variants(p) if p != "/" else ["/"]
        # root-like aliases (/kur): both /kur and /kur/
        if p != "/" and not p.endswith("/"):
            variants = list(dict.fromkeys(variants + [p, p + "/"]))
        for v in variants:
            key = f"exact:{v}"
            if key in seen:
                continue
            seen.add(key)
            exprs.append(
                FilterExpression(
                    filter=Filter(
                        field_name="pagePath",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value=v,
                            case_sensitive=False,
                        ),
                    )
                )
            )
    for pref in prefixes or []:
        if not pref or pref == "/":
            continue
        key = f"prefix:{pref}/"
        if key in seen:
            continue
        seen.add(key)
        exprs.append(
            FilterExpression(
                filter=Filter(
                    field_name="pagePath",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
                        value=pref + "/",
                        case_sensitive=False,
                    ),
                )
            )
        )
    return exprs


def _build_tree_filter(targets: list[dict[str, Any]]) -> FilterExpression:
    """Ana + alt path’leri kapsayan geniş filtre (tek rapordan exact/tree ayrıştırılır)."""
    exprs: list[FilterExpression] = []
    root_hosts_done: set[str] = set()
    mweb_prefixes_done: set[str] = set()
    www_hosts = ["www.doviz.com", "m.doviz.com"]

    for t in targets:
        desktop_hosts = [h for h in t["hosts"] if h != _MWEB_HOST]

        # kur/altın/borsa/haber desktop host — tüm path’ler
        if t["is_root"] and desktop_hosts:
            key = ",".join(desktop_hosts)
            if key not in root_hosts_done:
                root_hosts_done.add(key)
                exprs.append(_host_group(desktop_hosts))

        # m.doviz.com /kur|/altin|/borsa|/haber (prefix bir kez)
        mweb_prefix = str(t.get("mweb_prefix") or "")
        if mweb_prefix and mweb_prefix not in mweb_prefixes_done:
            mweb_prefixes_done.add(mweb_prefix)
            path_exprs = _path_filter_exprs([mweb_prefix], prefixes=[mweb_prefix])
            path_g = (
                path_exprs[0]
                if len(path_exprs) == 1
                else FilterExpression(or_group=FilterExpressionList(expressions=path_exprs))
            )
            exprs.append(
                FilterExpression(
                    and_group=FilterExpressionList(
                        expressions=[_host_group([_MWEB_HOST]), path_g]
                    )
                )
            )

        if t["is_root"]:
            continue

        # www / section path’leri (emtia, pariteler, akaryakit, …)
        # subdomain detail’ler zaten root host + mweb prefix ile kapsanır
        if t["host"] in _MWEB_PREFIX_BY_HOST:
            continue

        hosts = desktop_hosts or list(www_hosts)
        # www hedeflerinde m. host da olsun
        if t["host"].startswith("www.") and _MWEB_HOST not in hosts:
            hosts = list(hosts) + [_MWEB_HOST]
        host_g = _host_group(hosts)
        want_tree = not (t.get("kind") == "detail" and t.get("scope") == "exact")
        prefixes = [t["path"]] if want_tree and t["path"] != "/" else []
        path_exprs = _path_filter_exprs(t["paths"], prefixes=prefixes)
        if not path_exprs:
            continue
        path_g = (
            path_exprs[0]
            if len(path_exprs) == 1
            else FilterExpression(or_group=FilterExpressionList(expressions=path_exprs))
        )
        exprs.append(
            FilterExpression(and_group=FilterExpressionList(expressions=[host_g, path_g]))
        )
    if len(exprs) == 1:
        return exprs[0]
    return FilterExpression(or_group=FilterExpressionList(expressions=exprs))


def _path_is_exact(target: dict[str, Any], host: str, path: str) -> bool:
    h = (host or "").strip().lower()
    p = _norm_path(path)
    desktop_hosts = {x for x in (target.get("hosts") or []) if x != _MWEB_HOST}
    if h in desktop_hosts and p == target["path"]:
        return True
    # m.doviz.com /kur/... ↔ kur.doviz.com/...
    if h == _MWEB_HOST:
        for mp in target.get("mweb_paths") or []:
            if p == _norm_path(mp):
                return True
    return False


def _path_belongs_tree(target: dict[str, Any], host: str, path: str) -> bool:
    h = (host or "").strip().lower()
    p = _norm_path(path)
    # Leaf detail: tree == exact
    if target.get("scope") == "exact" and target.get("kind") == "detail":
        return _path_is_exact(target, host, path)

    desktop_hosts = {x for x in (target.get("hosts") or []) if x != _MWEB_HOST}
    if h in desktop_hosts:
        if target["is_root"]:
            return True
        return p == target["path"] or p.startswith(target["path"] + "/")

    # mweb alias tree
    if h == _MWEB_HOST:
        prefix = str(target.get("mweb_prefix") or "")
        if target["is_root"] and prefix:
            return p == _norm_path(prefix) or p.startswith(prefix + "/")
        for mp in target.get("mweb_paths") or []:
            mp_n = _norm_path(mp)
            if p == mp_n or p.startswith(mp_n + "/"):
                return True
    return False


def _match_exact_key(host: str, path: str, targets: list[dict[str, Any]]) -> str | None:
    for t in targets:
        if _path_is_exact(t, host, path):
            return t["url"]
    return None


def _match_tree_keys(host: str, path: str, targets: list[dict[str, Any]]) -> list[str]:
    return [t["url"] for t in targets if _path_belongs_tree(t, host, path)]


def _empty_mode_maps(targets: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    return {t["url"]: {"last": 0.0, "prev": 0.0} for t in targets}


def _run_range_rows(
    client,
    property_id: str,
    *,
    start: str,
    end: str,
    dim_filter: FilterExpression,
) -> list[tuple[str, str, float]]:
    req = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="hostName"),
            Dimension(name="pagePath"),
        ],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimension_filter=dim_filter,
        order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
        limit=_ROW_LIMIT,
    )
    try:
        resp = client.run_report(req, timeout=120.0)
    except Exception:
        LOGGER.warning(
            "home watch-urls GA4 range %s..%s failed", start, end, exc_info=True
        )
        return []
    out: list[tuple[str, str, float]] = []
    for row in resp.rows or []:
        if len(row.dimension_values) < 2:
            continue
        host = str(row.dimension_values[0].value or "").strip()
        path = str(row.dimension_values[1].value or "").strip()
        val = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        if not host:
            continue
        out.append((host, path, val))
    return out


def _accumulate_rows(
    rows: list[tuple[str, str, float]],
    *,
    targets: list[dict[str, Any]],
    slot: str,
    exact_map: dict[str, dict[str, float]],
    tree_map: dict[str, dict[str, float]],
    children: dict[str, dict[str, dict[str, float]]],
) -> None:
    for host, path, val in rows:
        exact_key = _match_exact_key(host, path, targets)
        if exact_key:
            exact_map[exact_key][slot] = exact_map[exact_key].get(slot, 0.0) + val
        tree_keys = _match_tree_keys(host, path, targets)
        pnorm = _norm_path(path)
        for key in tree_keys:
            tree_map[key][slot] = tree_map[key].get(slot, 0.0) + val
            ch = children.setdefault(key, {})
            bucket = ch.setdefault(pnorm, {"last": 0.0, "prev": 0.0})
            bucket[slot] = bucket.get(slot, 0.0) + val


def _normalize_property_id(raw: str) -> str:
    prop = str(raw or "").strip()
    if prop.startswith("properties/"):
        prop = prop.split("/", 1)[1]
    return prop


def _resolve_profile_properties(db: Session, site_id: int = 1) -> list[tuple[str, str]]:
    """[(profile, property_id), ...] — web / mweb sırası."""
    record = get_ga4_credentials_record(db, site_id)
    props = load_ga4_properties(record) or {}
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for key in ("web", "mweb"):
        pid = _normalize_property_id(str(props.get(key) or ""))
        if pid and pid not in seen:
            out.append((key, pid))
            seen.add(pid)
    if not out:
        for v in props.values():
            pid = _normalize_property_id(str(v or ""))
            if pid and pid not in seen:
                out.append(("web", pid))
                break
    return out


def _resolve_web_mweb_properties(db: Session, site_id: int = 1) -> list[str]:
    """Geriye dönük: yalnız property id listesi."""
    return [pid for _, pid in _resolve_profile_properties(db, site_id)]


def _period_cell(last_v: float, prev_v: float, days: int, meta: dict[str, str]) -> dict[str, Any]:
    delta_fmt, tone, delta_pct = _pct_delta(last_v, prev_v)
    return {
        "days": days,
        "label": f"{days}g" if days > 1 else "1g",
        "title": (
            "Son 1 gün"
            if days == 1
            else ("Son 1 hafta" if days == 7 else "Son 1 ay")
        ),
        "last": last_v,
        "prev": prev_v,
        "last_fmt": _fmt_int(last_v),
        "prev_fmt": _fmt_int(prev_v),
        "delta_fmt": delta_fmt,
        "delta_tone": tone,
        "delta_pct": delta_pct,
        "current_label": meta["current_label"],
        "previous_label": meta["previous_label"],
        "has_data": (last_v > 0 or prev_v > 0),
    }


def _format_children(
    target: dict[str, Any],
    child_paths: dict[str, dict[int, dict[str, float]]],
    period_meta: dict[int, dict[str, str]],
) -> list[dict[str, Any]]:
    """Alt path listesi — 7g last’e göre top N (ana path hariç)."""
    scored: list[tuple[float, str]] = []
    for path, by_days in (child_paths or {}).items():
        if path == target["path"]:
            continue
        last7 = float(((by_days or {}).get(7) or {}).get("last") or 0.0)
        scored.append((last7, path))
    scored.sort(key=lambda x: (-x[0], x[1]))
    out: list[dict[str, Any]] = []
    for _, path in scored[:_TOP_CHILDREN]:
        by_days = child_paths.get(path) or {}
        periods = []
        for days in _PERIODS:
            bucket = by_days.get(days) or {}
            periods.append(
                _period_cell(
                    float(bucket.get("last") or 0.0),
                    float(bucket.get("prev") or 0.0),
                    days,
                    period_meta[days],
                )
            )
        href_path = "/" if path == "/" else path
        out.append(
            {
                "path": path,
                "label": _child_label(path),
                "url": f"https://{target['host']}{href_path}",
                "periods": periods,
            }
        )
    return out


def build_home_watch_urls_context(db: Session, *, force: bool = False) -> dict[str, Any]:
    now = time.time()
    if (
        not force
        and _cache.get("payload") is not None
        and int(_cache.get("version") or 0) == _CACHE_VERSION
        and (now - float(_cache.get("at") or 0.0)) < _CACHE_TTL_SEC
    ):
        return dict(_cache["payload"])

    targets = _watch_targets()
    profiles = _resolve_profile_properties(db, 1)
    period_meta: dict[int, dict[str, str]] = {}
    for days in _PERIODS:
        (ls, le), (ps, pe) = _calendar_windows(days)
        period_meta[days] = {
            "current_label": _fmt_range(ls, le),
            "previous_label": _fmt_range(ps, pe),
        }

    # platform -> days -> url -> {last, prev}
    exact_maps: dict[str, dict[int, dict[str, dict[str, float]]]] = {
        p: {d: _empty_mode_maps(targets) for d in _PERIODS} for p in _PLATFORMS
    }
    tree_maps: dict[str, dict[int, dict[str, dict[str, float]]]] = {
        p: {d: _empty_mode_maps(targets) for d in _PERIODS} for p in _PLATFORMS
    }
    # platform -> url -> path -> days -> {last, prev}
    children_maps: dict[str, dict[str, dict[str, dict[int, dict[str, float]]]]] = {
        p: {t["url"]: {} for t in targets} for p in _PLATFORMS
    }

    error = None
    if not profiles:
        error = "GA4 web property bulunamadı"
    else:
        try:
            client = _client()
            dim_filter = _build_tree_filter(targets)
            for days in _PERIODS:
                (last_start, last_end), (prev_start, prev_end) = _calendar_windows(days)
                for profile, pid in profiles:
                    last_rows = _run_range_rows(
                        client, pid, start=last_start, end=last_end, dim_filter=dim_filter
                    )
                    prev_rows = _run_range_rows(
                        client, pid, start=prev_start, end=prev_end, dim_filter=dim_filter
                    )
                    for plat in (profile, "all"):
                        child_slot: dict[str, dict[str, dict[str, float]]] = {
                            t["url"]: {} for t in targets
                        }
                        _accumulate_rows(
                            last_rows,
                            targets=targets,
                            slot="last",
                            exact_map=exact_maps[plat][days],
                            tree_map=tree_maps[plat][days],
                            children=child_slot,
                        )
                        _accumulate_rows(
                            prev_rows,
                            targets=targets,
                            slot="prev",
                            exact_map=exact_maps[plat][days],
                            tree_map=tree_maps[plat][days],
                            children=child_slot,
                        )
                        dest_root = children_maps[plat]
                        for url, path_map in child_slot.items():
                            dest = dest_root.setdefault(url, {})
                            for path, bucket in path_map.items():
                                by_days = dest.setdefault(path, {})
                                cur = by_days.setdefault(days, {"last": 0.0, "prev": 0.0})
                                cur["last"] += float(bucket.get("last") or 0.0)
                                cur["prev"] += float(bucket.get("prev") or 0.0)
        except Exception as exc:
            LOGGER.exception("home watch-urls fetch failed")
            error = str(exc)

    def _periods_for(maps: dict[str, dict[int, dict[str, dict[str, float]]]], plat: str, url: str) -> list[dict[str, Any]]:
        out = []
        for days in _PERIODS:
            bucket = ((maps.get(plat) or {}).get(days) or {}).get(url) or {}
            out.append(
                _period_cell(
                    float(bucket.get("last") or 0.0),
                    float(bucket.get("prev") or 0.0),
                    days,
                    period_meta[days],
                )
            )
        return out

    rows: list[dict[str, Any]] = []
    for t in targets:
        url = t["url"]
        want_children = t.get("kind") == "category" or t.get("scope") == "tree"
        by_platform: dict[str, Any] = {}
        for plat in _PLATFORMS:
            exact_periods = _periods_for(exact_maps, plat, url)
            tree_periods = _periods_for(tree_maps, plat, url)
            child_rows = (
                _format_children(t, (children_maps.get(plat) or {}).get(url) or {}, period_meta)
                if want_children
                else []
            )
            by_platform[plat] = {
                "periods_exact": exact_periods,
                "periods_tree": tree_periods,
                "children": child_rows,
                "children_count": len(child_rows),
            }
        # Varsayılan görünüm: all
        all_block = by_platform["all"]
        rows.append(
            {
                "url": url,
                "label": t["label"],
                "group": t.get("group") or "",
                "kind": t.get("kind") or "category",
                "scope": t.get("scope") or "tree",
                "host": t["host"],
                "path": t["path"],
                "is_root": t["is_root"],
                "by_platform": by_platform,
                "periods_exact": all_block["periods_exact"],
                "periods_tree": all_block["periods_tree"],
                "periods": all_block["periods_exact"],
                "children": all_block["children"],
                "children_count": all_block["children_count"],
            }
        )

    category_rows = [r for r in rows if r["kind"] == "category"]
    detail_rows = [r for r in rows if r["kind"] == "detail"]

    payload = {
        "rows": category_rows,
        "category_rows": category_rows,
        "detail_rows": detail_rows,
        "error": error,
        "metric_label": "sessions",
        "synced_at": date.today().isoformat(),
        "period_meta": {str(d): period_meta[d] for d in _PERIODS},
        "modes": [
            {"id": "exact", "label": "Sadece ana sayfa"},
            {"id": "tree", "label": "Alt linklerle birlikte"},
        ],
        "platforms": [
            {"id": "all", "label": "Tümü"},
            {"id": "web", "label": "Web"},
            {"id": "mweb", "label": "MWeb"},
        ],
        "default_mode": "exact",
        "default_platform": "all",
        "available_platforms": [p for p, _ in profiles] or ["web"],
    }
    _cache["at"] = now
    _cache["version"] = _CACHE_VERSION
    _cache["payload"] = payload
    return dict(payload)
