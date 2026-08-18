"""d-lab — GA4 Data API'nin panelde kullanılmayan alanları.

Property'de mevcut olan ama hiç sorgulanmayan boyut/metrikleri container'lara
böler. **Yalnızca GA4 Data API** kullanılır; başka servis, export veya kazıma
yoktur.

Yapı:
  · `BREAKDOWNS` — bildirimsel boyut kırılımları (özel + standart). Hepsi aynı
    şekli paylaşır: container başına `per_profile`, böylece her container kendi
    platform filtresini kurabilir.
  · Bespoke bloklar — kullanıcı (DAU/WAU/MAU), içerik derinliği, saatlik ritim,
    kitle.

Her istek bağımsızdır: biri hata alırsa diğerleri etkilenmez ve eksiklik sayfada
görünür. Sessiz boş container bırakılmaz.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

LOGGER = logging.getLogger(__name__)

_DEFAULT_SITE_ID = 1
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 300.0
_MAX_WORKERS = 8  # property başına eşzamanlı istek sınırı 10

# ── İlerleme ────────────────────────────────────────────────────────────────
# Rapor tek bir HTTP isteği ama içeride onlarca GA4 çağrısı var; istemci
# bekleme süresince ne olduğunu göremiyordu. Havuzdaki her iş bitince sayaç
# artar, istemci ayrı bir uçtan okur. Uydurma animasyon değil, gerçek sayım.
_PROGRESS: dict[str, dict[str, Any]] = {}
_PROGRESS_TTL_SEC = 300.0
_PROGRESS_MAX = 64  # aynı anda izlenebilecek istek sayısı (üst sınır)
_PROGRESS_LOCK = threading.Lock()


def _progress_prune(now: float) -> None:
    """Süresi geçmiş kayıtları at — kilit tutulurken çağrılır."""
    stale = [
        key for key, rec in _PROGRESS.items()
        if (now - float(rec.get("updated_at") or 0)) > _PROGRESS_TTL_SEC
    ]
    for key in stale:
        _PROGRESS.pop(key, None)
    # Aşırı birikmeye karşı: en eskiden başlayarak kırp
    if len(_PROGRESS) > _PROGRESS_MAX:
        for key in sorted(_PROGRESS, key=lambda k: _PROGRESS[k].get("updated_at") or 0)[
            : len(_PROGRESS) - _PROGRESS_MAX
        ]:
            _PROGRESS.pop(key, None)


def progress_start(token: str, *, total: int, phase: str = "fetch") -> None:
    if not token:
        return
    now = time.time()
    with _PROGRESS_LOCK:
        _PROGRESS[token] = {
            "total": max(0, int(total)),
            "done": 0,
            "phase": phase,
            "label": "",
            "started_at": now,
            "updated_at": now,
            "finished": False,
            "ok": None,
        }
        # Budama eklemeden SONRA: önce budayınca yeni kayıt sınırı bir aşıyordu
        _progress_prune(now)


def progress_tick(token: str, label: str = "") -> None:
    if not token:
        return
    with _PROGRESS_LOCK:
        rec = _PROGRESS.get(token)
        if not rec:
            return
        rec["done"] = int(rec.get("done") or 0) + 1
        rec["label"] = str(label or "")[:60]
        rec["updated_at"] = time.time()


def progress_finish(token: str, *, ok: bool = True, phase: str = "done") -> None:
    if not token:
        return
    with _PROGRESS_LOCK:
        rec = _PROGRESS.get(token)
        if not rec:
            return
        rec["finished"] = True
        rec["ok"] = bool(ok)
        rec["phase"] = phase
        rec["done"] = int(rec.get("total") or rec.get("done") or 0)
        rec["updated_at"] = time.time()


def progress_snapshot(token: str) -> dict[str, Any]:
    """İstemcinin okuduğu durum. Bilinmeyen token sessizce 'bekliyor' döner."""
    with _PROGRESS_LOCK:
        rec = _PROGRESS.get(str(token or ""))
        if not rec:
            return {"known": False, "total": 0, "done": 0, "percent": 0,
                    "phase": "waiting", "label": "", "finished": False}
        total = int(rec.get("total") or 0)
        done = min(int(rec.get("done") or 0), total or int(rec.get("done") or 0))
        percent = 100 if rec.get("finished") else (
            int(round(100.0 * done / total)) if total else 0
        )
        return {
            "known": True,
            "total": total,
            "done": done,
            "percent": percent,
            "phase": str(rec.get("phase") or ""),
            "label": str(rec.get("label") or ""),
            "finished": bool(rec.get("finished")),
            "ok": rec.get("ok"),
            "elapsed_sec": round(time.time() - float(rec.get("started_at") or 0), 1),
        }

# GA4 «değersiz» boyut değerleri — filtrelenmezse (not set) satırı listeyi yutuyor
_EMPTY_VALUES = ("(not set)", "", "(none)")

PROFILES: tuple[str, ...] = ("web", "mweb", "android", "ios")
APP_PROFILES: tuple[str, ...] = ("android", "ios")
SITE_PROFILES: tuple[str, ...] = ("web", "mweb")


# Container grupları — sayfada bu sırayla, bu başlıklarla toplanır.
# 34 container tek düz akışta arama yapılamaz hale gelmişti; ilgili olanlar
# (ör. Kanal ile Kaynak/aracı, Cihaz modeli ile Cihaz kategorisi) araya onlarca
# kart girdiği için birbirinden kopuyordu.
GROUPS: tuple[dict[str, str], ...] = (
    {"key": "engagement", "label": "Kullanıcı & etkileşim"},
    {"key": "acquisition", "label": "Edinim"},
    {"key": "behavior", "label": "Davranış"},
    {"key": "audience", "label": "Kitle & cihaz"},
    {"key": "app", "label": "Uygulama"},
)
_DEFAULT_GROUP = "behavior"

# Standart boyut kırılımları — bildirimsel, çünkü hepsi aynı şekli paylaşıyor.
# `profiles` o kırılımın anlamlı olduğu yüzeyleri sınırlar (ör. appVersion yalnız
# uygulamalarda). Ölçülen maliyet istek başına 1–3 token.
# Kaldırılanlar (kullanıcı isteğiyle, geri getirilebilir):
#   search_text — «Uygulama içi arama», customEvent:search_text, yalnız iOS
#   campaign    — «Kampanya», sessionCampaignName, dört yüzey
# İkisi de çalışıyordu; şimdilik sayfadan çıkarıldı.
BREAKDOWNS: tuple[dict[str, Any], ...] = (
    # Özel boyutlar (property başına tanımlı; olmayan yüzey «tanımlı değil» der)
    {"key": "asset_key", "group": "behavior", "label": "Varlık ilgisi", "dimension": "customEvent:asset_key",
     "metric": "eventCount", "profiles": PROFILES,
     "hint": "Hangi varlığa bakılıyor"},
    {"key": "nav_from", "group": "behavior", "label": "Habere nereden gelindi", "dimension": "customEvent:from",
     "metric": "eventCount", "profiles": ("ios",),
     "hint": "iOS: from parametresi · Android: giriş olayı",
     # Android'de `customEvent:from` tanımlı değil (GA4 400 veriyor), ama aynı
     # bilgi ayrı olay adlarıyla geliyor. Yalnızca «bir yüzeyden habere girildi»
     # anlamına gelen olaylar sayılır; gösterim (impression), bildirim teslimi
     # ve makale içi eylemler (yorum, tepki, paylaşım, pull-to-refresh) giriş
     # yüzeyi değildir ve dışarıda bırakılır — yoksa 2,9M'lik impression tek
     # başına listeyi yutar.
     "per_profile": {
         "android": {
             "dimension": "eventName",
             "values": {
                 "asset_detail_news_analyzes_opened": "asset_detail",
                 "notification_news_clicked": "notification",
                 "home_news_clicked": "home",
                 "bottom_navigation_news": "navigation_manager",
                 "first_tab_news": "first_tab",
                 "news_item_clicked": "news_list",
                 "home_header_news_clicked": "home_header",
                 "asset_detail_home_news_clicked": "asset_detail_home",
                 "asset_detail_news_analyzes_item_clicked": "asset_detail_item",
             },
         }
     }},
    {"key": "sections_enabled", "group": "app", "label": "Açılan bölümler", "dimension": "customEvent:sections_enabled",
     "metric": "eventCount", "profiles": ("ios",), "hint": ""},
    {"key": "sections_disabled", "group": "app", "label": "Kapatılan bölümler", "dimension": "customEvent:sections_disabled",
     "metric": "eventCount", "profiles": ("ios",), "hint": ""},
    {"key": "menu_item", "group": "behavior", "label": "Menü kullanımı", "dimension": "customEvent:menu_item",
     "metric": "eventCount", "profiles": SITE_PROFILES, "hint": ""},
    {"key": "card_name", "group": "behavior", "label": "Ana sayfa kartları", "dimension": "customEvent:card_name",
     "metric": "eventCount", "profiles": ("mweb",), "hint": ""},
    # Standart boyutlar
    {"key": "events", "group": "behavior", "label": "Olaylar", "dimension": "eventName",
     "metric": "eventCount", "profiles": PROFILES,
     "hint": "En çok tetiklenen olaylar"},
    {"key": "app_version", "group": "app", "label": "Uygulama sürümü", "dimension": "appVersion",
     "metric": "activeUsers", "profiles": APP_PROFILES,
     "hint": "Sürüm benimsenmesi — eski sürümde kalan kullanıcı"},
    {"key": "new_returning", "group": "engagement", "label": "Yeni / dönen", "dimension": "newVsReturning",
     "metric": "activeUsers", "profiles": PROFILES,
     "hint": "Sadık kitle mi, yeni kullanıcı mı"},
    {"key": "channel", "group": "acquisition", "label": "Kanal", "dimension": "sessionDefaultChannelGroup",
     "metric": "sessions", "profiles": PROFILES,
     "hint": "Oturum nereden geldi"},
    {"key": "country", "group": "audience", "label": "Ülke", "dimension": "country",
     "metric": "activeUsers", "profiles": PROFILES, "hint": ""},
    {"key": "language", "group": "audience", "label": "Dil", "dimension": "language",
     "metric": "activeUsers", "profiles": PROFILES, "hint": ""},
    {"key": "os_version", "group": "audience", "label": "İşletim sistemi sürümü", "dimension": "operatingSystemVersion",
     "metric": "activeUsers", "profiles": PROFILES, "hint": ""},
    {"key": "device", "group": "audience", "label": "Cihaz modeli", "dimension": "deviceModel",
     "metric": "activeUsers", "profiles": APP_PROFILES, "hint": ""},
    {"key": "landing", "group": "acquisition", "label": "Giriş sayfaları", "dimension": "landingPagePlusQueryString",
     "metric": "sessions", "profiles": SITE_PROFILES,
     "hint": "Siteye ilk girilen sayfa"},
    # ── Edinim ──────────────────────────────────────────────────────────────
    {"key": "source_medium", "group": "acquisition", "label": "Kaynak / aracı", "dimension": "sessionSourceMedium",
     "metric": "sessions", "profiles": PROFILES,
     "hint": "Kanal grubundan bir kademe derin — hangi site, hangi yolla"},
    {"key": "first_channel", "group": "acquisition", "label": "İlk edinim kanalı", "dimension": "firstUserDefaultChannelGroup",
     "metric": "newUsers", "profiles": PROFILES,
     "hint": "Kullanıcıyı ilk kez getiren kanal — oturum kanalından farklı"},
    {"key": "referrer", "group": "acquisition", "label": "Yönlendiren sayfa", "dimension": "pageReferrer",
     "metric": "sessions", "profiles": SITE_PROFILES,
     "hint": "Ziyaretin geldiği tam adres"},
    # ── Kitle / cihaz ───────────────────────────────────────────────────────
    {"key": "city", "group": "audience", "label": "Şehir", "dimension": "city",
     "metric": "activeUsers", "profiles": PROFILES,
     "hint": "Ülke kırılımının altı — yerel içerik kararı için"},
    {"key": "device_category", "group": "audience", "label": "Cihaz kategorisi", "dimension": "deviceCategory",
     "metric": "activeUsers", "profiles": PROFILES, "hint": "masaüstü / mobil / tablet"},
    {"key": "device_brand", "group": "audience", "label": "Cihaz markası", "dimension": "mobileDeviceBranding",
     "metric": "activeUsers", "profiles": ("web", "mweb", "android"),
     "hint": "iOS'ta tek marka olduğu için sorulmaz"},
    {"key": "browser", "group": "audience", "label": "Tarayıcı", "dimension": "browser",
     "metric": "activeUsers", "profiles": SITE_PROFILES, "hint": ""},
    {"key": "os", "group": "audience", "label": "İşletim sistemi", "dimension": "operatingSystem",
     "metric": "activeUsers", "profiles": SITE_PROFILES,
     "hint": "Uygulamalarda tek değer olduğu için yalnız web/mWeb"},
    {"key": "screen_resolution", "group": "audience", "label": "Ekran çözünürlüğü", "dimension": "screenResolution",
     "metric": "activeUsers", "profiles": SITE_PROFILES,
     "hint": "Tasarım kırılım noktaları hangi genişliğe göre seçilmeli"},
    # ── Davranış ────────────────────────────────────────────────────────────
    {"key": "screen_name", "group": "behavior", "label": "Ekran / sayfa adı", "dimension": "unifiedScreenName",
     "metric": "screenPageViews", "profiles": PROFILES,
     "hint": "Uygulamada ekran, sitede sayfa başlığı — tek isimlendirmede"},
    {"key": "signed_in", "group": "engagement", "label": "Üyelik durumu", "dimension": "signedInWithUserId",
     "metric": "activeUsers", "profiles": SITE_PROFILES,
     "hint": "Giriş yapmış kullanıcı payı"},
    {"key": "weekday", "group": "behavior", "label": "Haftanın günü", "dimension": "dayOfWeek",
     "metric": "activeUsers", "profiles": PROFILES,
     "hint": "0 = Pazar"},
)

_WEEKDAY_TR = ("Pazar", "Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi")


def resolve_profiles(properties: dict[str, str], selected: str | None) -> list[str]:
    """Filtre → çalışılacak profil listesi. «hepsi» tanımlı olanların tümü."""
    available = [p for p in PROFILES if str(properties.get(p) or "").strip()]
    key = (selected or "hepsi").strip().lower()
    if key in ("", "hepsi", "all"):
        return available
    return [p for p in available if p == key]


def _types() -> Any:
    from google.analytics.data_v1beta import types

    return types


def _exclude_empty(dimension: str) -> Any:
    t = _types()
    return t.FilterExpression(
        not_expression=t.FilterExpression(
            filter=t.Filter(
                field_name=dimension,
                in_list_filter=t.Filter.InListFilter(values=list(_EMPTY_VALUES)),
            )
        )
    )


def _only_values(dimension: str, values: list[str]) -> Any:
    """Yalnızca sayılan değerler — sunucu tarafında daraltma.

    İstemcide ayıklamak yerine GA4'ten baştan sadece bunları istemek hem daha
    ucuz hem de kesin: limit yüzünden bir yüzeyin listeden düşmesi imkânsız.
    """
    t = _types()
    return t.FilterExpression(
        filter=t.Filter(
            field_name=dimension,
            in_list_filter=t.Filter.InListFilter(values=list(values)),
        )
    )


def _run(
    client: Any,
    property_id: str,
    *,
    dimensions: list[str],
    metrics: list[str],
    start: str,
    end: str,
    limit: int = 25,
    dimension_filter: Any = None,
    order_metric: str | None = None,
) -> list[dict[str, Any]]:
    """Tek RunReport → sözlük listesi. Boyut adları anahtar olur."""
    t = _types()
    kwargs: dict[str, Any] = {
        "property": f"properties/{property_id}",
        "dimensions": [t.Dimension(name=d) for d in dimensions],
        "metrics": [t.Metric(name=m) for m in metrics],
        "date_ranges": [t.DateRange(start_date=start, end_date=end)],
        "limit": max(1, min(int(limit), 250)),
    }
    if dimension_filter is not None:
        kwargs["dimension_filter"] = dimension_filter
    if order_metric:
        kwargs["order_bys"] = [
            t.OrderBy(metric=t.OrderBy.MetricOrderBy(metric_name=order_metric), desc=True)
        ]
    resp = client.run_report(t.RunReportRequest(**kwargs))
    out: list[dict[str, Any]] = []
    for row in resp.rows or []:
        item: dict[str, Any] = {}
        for i, dim in enumerate(dimensions):
            item[dim] = row.dimension_values[i].value if i < len(row.dimension_values) else ""
        for i, met in enumerate(metrics):
            raw = row.metric_values[i].value if i < len(row.metric_values) else "0"
            try:
                item[met] = float(raw or 0)
            except (TypeError, ValueError):
                item[met] = 0.0
        out.append(item)
    return out


def _block(name: str, fn: Any) -> dict[str, Any]:
    """Blok gövdesini hata yutmadan ama sayfayı düşürmeden çalıştır."""
    try:
        return {"ok": True, "error": None, **fn()}
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("x-ga4 blok başarısız [%s]: %s", name, exc)
        return {"ok": False, "error": str(exc)[:220]}


# ── 1. Kullanıcı & kararlılık ───────────────────────────────────────────────

USER_METRICS = ("active1DayUsers", "active7DayUsers", "active28DayUsers")


def _user_stability(client: Any, properties: dict[str, str], profiles: list[str]) -> dict[str, Any]:
    """DAU / WAU / MAU — Firebase konsolu yerine doğrudan GA4.

    Crash-free burada gösterilmiyor: /firebase sayfasında zaten var, ikinci kez
    çekmek hem gereksiz istek hem çift kaynak olurdu.
    """
    rows = []
    for pf in profiles:
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        entry: dict[str, Any] = {"profile": pf}
        try:
            data = _run(
                client, pid,
                dimensions=[], metrics=list(USER_METRICS),
                start="yesterday", end="yesterday", limit=1,
            )
            entry.update(data[0] if data else {})
        except Exception as exc:  # noqa: BLE001
            entry["users_error"] = str(exc)[:140]
        rows.append(entry)
    return {"rows": rows}


# ── Yardımcı: boyut tanımlı mı ───────────────────────────────────────────────

def _dimension_missing(exc: Exception) -> bool:
    """GA4 «bu property'de böyle bir boyut yok» hatası mı?

    Bu bir arıza değil, ölçüm kurulumu eksikliği — kullanıcıya ham 400 metni
    yerine anlaşılır bir not göstermek için ayrılır.
    """
    text = str(exc).lower()
    return "is not a valid dimension" in text or "did you mean" in text


# ── 4. İçerik derinliği ─────────────────────────────────────────────────────

DEPTH_METRICS = ("screenPageViews", "userEngagementDuration", "newUsers")


def _content_depth(
    client: Any, properties: dict[str, str], profiles: list[str],
    start: str, end: str, limit: int,
) -> dict[str, Any]:
    """Sayfa başına gerçek okuma süresi ve yeni kullanıcı payı.

    `scrolledUsers` bu property'de veri döndürmüyor (scroll ölçümü boş), bu
    yüzden okuma derinliği `userEngagementDuration` üzerinden hesaplanır.
    """
    rows_out: list[dict[str, Any]] = []
    for pf in [p for p in SITE_PROFILES if p in profiles]:
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        rows = _run(
            client, pid,
            dimensions=["pagePath"], metrics=list(DEPTH_METRICS),
            start=start, end=end, limit=limit, order_metric="screenPageViews",
        )
        for r in rows:
            views = float(r.get("screenPageViews") or 0)
            engagement = float(r.get("userEngagementDuration") or 0)
            rows_out.append(
                {
                    "profile": pf,
                    "page": r.get("pagePath") or "",
                    "views": views,
                    "engagement_seconds": engagement,
                    "seconds_per_view": round(engagement / views, 1) if views else 0.0,
                    "new_users": float(r.get("newUsers") or 0),
                }
            )
    rows_out.sort(key=lambda r: -r["views"])
    return {"rows": rows_out[: limit * 2]}


# ── 5. Saatlik ritim ────────────────────────────────────────────────────────

def _hourly(
    client: Any, properties: dict[str, str], profiles: list[str], start: str, end: str
) -> dict[str, Any]:
    """Saat × aktif kullanıcı — yayın saati kararı için.

    GA4 yüksek kardinalitede `(other)` kovası döndürebiliyor; gizlenmez, ayrı
    satır olarak raporlanır.
    """
    series: dict[str, Any] = {}
    for pf in profiles:
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        try:
            rows = _run(
                client, pid,
                dimensions=["hour"], metrics=["activeUsers", "sessions"],
                start=start, end=end, limit=30,
            )
        except Exception as exc:  # noqa: BLE001
            series[pf] = {"error": str(exc)[:140]}
            continue
        hours = []
        other = 0.0
        for r in rows:
            label = str(r.get("hour") or "")
            if label.isdigit():
                hours.append({"hour": int(label), "users": r["activeUsers"], "sessions": r["sessions"]})
            else:
                other += float(r.get("activeUsers") or 0)
        hours.sort(key=lambda h: h["hour"])
        series[pf] = {"hours": hours, "other_users": other}
    return {"series": series}


# ── 6. Kitle ────────────────────────────────────────────────────────────────

def _engagement(
    client: Any, properties: dict[str, str], profiles: list[str], start: str, end: str,
) -> dict[str, Any]:
    """Etkileşim kalitesi — yüzeyler yan yana.

    Kırılım değil, tek satırlık oran/ortalama metrikleri. Bunlar boyut
    listesiyle gelmiyor; ayrı bir blok olarak toplanır ki yüzeyler doğrudan
    kıyaslanabilsin (ör. iOS oturum başına 16 ekran, mWeb 1.8).
    """
    mets = [
        "sessions", "engagementRate", "bounceRate",
        "averageSessionDuration", "screenPageViewsPerSession", "eventsPerSession",
    ]
    rows: list[dict[str, Any]] = []
    for pf in profiles:
        pid = str(properties.get(pf) or "").strip()
        if not pid:
            continue
        try:
            got = _run(client, pid, dimensions=[], metrics=mets, start=start, end=end, limit=1)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("x-ga4 etkileşim [%s]: %s", pf, exc)
            continue
        if not got:
            continue
        r = got[0]
        rows.append({
            "profile": pf,
            "sessions": r.get("sessions"),
            "engagement_rate": r.get("engagementRate"),
            "bounce_rate": r.get("bounceRate"),
            "avg_session_sec": r.get("averageSessionDuration"),
            "views_per_session": r.get("screenPageViewsPerSession"),
            "events_per_session": r.get("eventsPerSession"),
        })
    return {"rows": rows}


def _audience(
    client: Any, properties: dict[str, str], profiles: list[str],
    start: str, end: str, limit: int,
) -> dict[str, Any]:
    # Demografi/ilgi verisi yüzey bağımsız; seçili profil yoksa web'e düşer
    target = profiles[0] if profiles else "web"
    pid = str(properties.get(target) or properties.get("web") or "").strip()
    if not pid:
        return {"interests": [], "demographics": [], "audiences": []}

    def _safe(dims: list[str], key: str) -> list[dict[str, Any]]:
        try:
            rows = _run(
                client, pid, dimensions=dims, metrics=["activeUsers"],
                start=start, end=end, limit=limit, order_metric="activeUsers",
            )
            return [
                {"label": " · ".join(str(r.get(d) or "") for d in dims), "users": r["activeUsers"]}
                for r in rows
            ]
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("x-ga4 kitle bloğu [%s]: %s", key, exc)
            return []

    return {
        "interests": _safe(["brandingInterest"], "brandingInterest"),
        "demographics": _safe(["userAgeBracket", "userGender"], "demographics"),
        "audiences": _safe(["audienceName"], "audienceName"),
    }


# ── Standart kırılımlar (bildirimsel) ───────────────────────────────────────

def _label_value(dimension: str, raw: str) -> str:
    """Ham GA4 değerini okunur hale getir (şimdilik yalnızca gün numarası)."""
    if dimension == "dayOfWeek" and str(raw).isdigit():
        idx = int(raw)
        if 0 <= idx < len(_WEEKDAY_TR):
            return _WEEKDAY_TR[idx]
    return raw


def _breakdown_task(
    client: Any, spec: dict[str, Any], profile: str, property_id: str,
    start: str, end: str, limit: int,
) -> dict[str, Any]:
    # Bir yüzeyde boyut yoksa aynı bilgiyi taşıyan başka bir boyut kullanılabilir
    # (ör. Android'de `customEvent:from` tanımlı değil ama giriş yüzeyi ayrı ayrı
    # olay adlarıyla geliyor). Böylece platformlar tek container'da karşılaştırılır.
    override = (spec.get("per_profile") or {}).get(profile) or {}
    dim = override.get("dimension") or spec["dimension"]
    value_map: dict[str, str] = override.get("values") or {}
    metric = spec["metric"]
    out: dict[str, Any] = {"key": spec["key"], "profile": profile}
    try:
        rows = _run(
            client, property_id,
            dimensions=[dim], metrics=[metric],
            start=start, end=end, limit=limit,
            dimension_filter=(
                _only_values(dim, list(value_map)) if value_map else _exclude_empty(dim)
            ),
            order_metric=metric,
        )
        # deviceModel üretici kodu veriyor (SM-S938B / iPhone18,2); panelde
        # okunabilir ada çevrilir. Bilinmeyen kod olduğu gibi kalır.
        if dim == "deviceModel":
            from backend.services.device_names import pretty_device_model

            def _label(value: Any) -> str:
                return pretty_device_model(value, platform=profile)
        else:
            def _label(value: Any) -> str:
                return value_map.get(value) or _label_value(dim, value)

        out["rows"] = [
            {"value": _label(r[dim]), "raw": r[dim], "metric": r[metric]}
            for r in rows
        ]
        if value_map:
            out["mapped_from"] = dim
    except Exception as exc:  # noqa: BLE001
        out["rows"] = []
        if _dimension_missing(exc):
            out["undefined"] = True
        else:
            out["error"] = str(exc)[:140]
    return out


def _plan_breakdowns(
    properties: dict[str, str], profiles: list[str]
) -> list[tuple[dict[str, Any], str, str]]:
    """(spec, profil, property) üçlüleri — tek düz paralel havuz için."""
    plan = []
    for spec in BREAKDOWNS:
        allowed = set(spec["profiles"]) | set((spec.get("per_profile") or {}).keys())
        for pf in profiles:
            if pf not in allowed:
                continue
            pid = str(properties.get(pf) or "").strip()
            if pid:
                plan.append((spec, pf, pid))
    return plan


# ── Toplayıcı ───────────────────────────────────────────────────────────────

def build_x_ga4_report(
    db: Any,
    *,
    site_id: int = _DEFAULT_SITE_ID,
    days: int = 7,
    limit: int = 15,
    profile: str | None = None,
    force: bool = False,
    progress_token: str | None = None,
) -> dict[str, Any]:
    """Tüm blokları tek düz paralel havuzda çeker.

    İstekler blok içinde değil, blok×profil düzeyinde düzleştirilir — aksi halde
    iç içe havuzlar birbirini bekletiyor ve «hepsi» seçiliyken sayfa yavaşlıyor.
    Ölçülen maliyet istek başına 1–3 token; tam sayfa ~50 istekte bile günlük
    200.000 token bütçesinin binde biri.
    """
    from backend.services.ga4_auth import get_ga4_connection_status

    safe_days = max(1, min(int(days or 7), 90))
    safe_limit = max(5, min(int(limit or 15), 50))
    profile_key = (profile or "hepsi").strip().lower()
    cache_key = f"{site_id}|{safe_days}|{safe_limit}|{profile_key}"
    token = str(progress_token or "").strip()[:64]
    progress_start(token, total=1, phase="starting")

    def _bail(payload: dict[str, Any]) -> dict[str, Any]:
        """Erken çıkış — çubuk asılı kalmasın."""
        progress_finish(token, ok=bool(payload.get("ok")), phase="done")
        return payload

    if not force:
        hit = _CACHE.get(cache_key)
        if hit and (time.time() - hit[0]) < _CACHE_TTL_SEC:
            return _bail({**hit[1], "cached": True})

    status = get_ga4_connection_status(db, site_id)
    if not status.get("connected"):
        return _bail({
            "ok": False,
            "error": str(status.get("label") or "GA4 bağlı değil"),
            "blocks": {},
        })
    properties = (status.get("properties") or {}) if isinstance(status, dict) else {}
    if not properties:
        return _bail({"ok": False, "error": "GA4 property tanımlı değil", "blocks": {}})

    from backend.collectors.ga4 import _client

    client = _client()
    start = f"{safe_days}daysAgo" if safe_days > 1 else "yesterday"
    end = "yesterday"
    profiles = resolve_profiles(properties, profile_key)
    if not profiles:
        return _bail(
            {"ok": False, "error": f"«{profile_key}» için GA4 property yok", "blocks": {}}
        )

    jobs: dict[str, Any] = {
        "user_stability": lambda: _block("user_stability", lambda: _user_stability(client, properties, profiles)),
        "content_depth": lambda: _block("content_depth", lambda: _content_depth(client, properties, profiles, start, end, safe_limit)),
        "hourly": lambda: _block("hourly", lambda: _hourly(client, properties, profiles, start, end)),
        "audience": lambda: _block("audience", lambda: _audience(client, properties, profiles, start, end, safe_limit)),
        "engagement": lambda: _block("engagement", lambda: _engagement(client, properties, profiles, start, end)),
    }
    names = list(jobs.keys())
    plan = _plan_breakdowns(properties, profiles)

    # Blok işleri ve kırılım istekleri aynı havuzda — hiçbiri diğerini bekletmez
    tasks: list[Any] = [jobs[n] for n in names]
    tasks += [
        (lambda spec=spec, pf=pf, pid=pid: _breakdown_task(
            client, spec, pf, pid, start, end, safe_limit))
        for spec, pf, pid in plan
    ]
    # İlerleme etiketleri görev sırasıyla birebir aynı — pool.map sırayı korur
    labels = list(names) + [f"{spec['key']} · {pf}" for spec, pf, _pid in plan]
    progress_start(token, total=len(tasks), phase="fetch")

    def _run(indexed: tuple[int, Any]) -> Any:
        idx, fn = indexed
        try:
            return fn()
        finally:
            progress_tick(token, labels[idx] if idx < len(labels) else "")

    with ThreadPoolExecutor(max_workers=min(_MAX_WORKERS, max(1, len(tasks)))) as pool:
        results = list(pool.map(_run, enumerate(tasks)))

    blocks = dict(zip(names, results[: len(names)]))

    # Kırılımlar: spec sırasını koru, profilleri altında topla
    grouped: dict[str, dict[str, Any]] = {}
    for item in results[len(names):]:
        slot = grouped.setdefault(item["key"], {"per_profile": {}})
        slot["per_profile"][item["profile"]] = {
            k: v for k, v in item.items() if k not in ("key", "profile")
        }
    breakdowns = [
        {
            "key": spec["key"], "label": spec["label"], "hint": spec.get("hint") or "",
            "dimension": spec["dimension"], "metric": spec["metric"],
            "group": spec.get("group") or _DEFAULT_GROUP,
            "per_profile": grouped.get(spec["key"], {}).get("per_profile", {}),
        }
        for spec in BREAKDOWNS
        if spec["key"] in grouped
    ]

    out = {
        "ok": True,
        "error": None,
        "cached": False,
        "window": {"start": start, "end": end, "days": safe_days},
        "profile": profile_key,
        "profiles": profiles,
        "available_profiles": [p for p in PROFILES if str(properties.get(p) or "").strip()],
        "blocks": blocks,
        "breakdowns": breakdowns,
        "groups": [dict(g) for g in GROUPS],
        "requests": len(tasks),
        "note": "Tüm veriler GA4 Data API'den gelir; başka kaynak kullanılmaz.",
    }
    _CACHE[cache_key] = (time.time(), out)
    progress_finish(token, ok=True, phase="done")
    return out
