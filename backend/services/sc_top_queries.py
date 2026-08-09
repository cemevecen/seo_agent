"""Search Console Top queries — /search-console tablosu ile aynı agregasyon."""

from __future__ import annotations


def sc_position_delta(current: float, previous: float) -> float:
    """Search Console ort. pozisyon: önceki − güncel (sıra birimi, yüzde değil).
    Pozitif = sıra sayısı düştü (iyileşme), negatif = yükseldi (kötüleşme)."""
    try:
        c = float(current or 0.0)
        p = float(previous or 0.0)
    except (TypeError, ValueError):
        return 0.0
    d = p - c
    # UI `{:+.2f}` ile gösterim; iki ondalıkta 0.00 olan farkları tam sıfır yap
    if round(d, 2) == 0.0:
        return 0.0
    return d


def aggregate_search_console_queries(rows: list[dict]) -> dict[str, dict]:
    aggregated: dict[str, dict] = {}
    for row in rows:
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        item = aggregated.setdefault(
            query,
            {
                "query": query,
                "clicks": 0.0,
                "impressions": 0.0,
                "position_weighted_total": 0.0,
                "position_weighted_impressions": 0.0,
                "fallback_position_total": 0.0,
                "fallback_position_count": 0,
            },
        )
        clicks = float(row.get("clicks", 0.0) or 0.0)
        impressions = float(row.get("impressions", 0.0) or 0.0)
        position = float(row.get("position", 0.0) or 0.0)
        item["clicks"] += clicks
        item["impressions"] += impressions
        if impressions > 0:
            item["position_weighted_total"] += position * impressions
            item["position_weighted_impressions"] += impressions
        elif position > 0:
            item["fallback_position_total"] += position
            item["fallback_position_count"] += 1

    normalized: dict[str, dict] = {}
    for query, item in aggregated.items():
        impressions = float(item["impressions"])
        if item["position_weighted_impressions"] > 0:
            position = item["position_weighted_total"] / item["position_weighted_impressions"]
        elif item["fallback_position_count"] > 0:
            position = item["fallback_position_total"] / item["fallback_position_count"]
        else:
            position = 0.0
        normalized[query] = {
            "query": query,
            "clicks": float(item["clicks"]),
            "impressions": impressions,
            "ctr": (float(item["clicks"]) / impressions * 100.0) if impressions > 0 else 0.0,
            "position": position,
        }
    return normalized


def build_search_console_top_queries(
    current_rows: list[dict],
    previous_rows: list[dict],
    *,
    limit: int = 50,
) -> list[dict]:
    """En çok tık alan sorgular + 7g vs önceki 7g (Search Console Top queries tablosu)."""
    current_map = aggregate_search_console_queries(current_rows)
    previous_map = aggregate_search_console_queries(previous_rows)
    items: list[dict] = []

    if current_map:
        for query, current in sorted(current_map.items(), key=lambda item: item[1]["clicks"], reverse=True)[:limit]:
            previous = previous_map.get(query, {})
            previous_position = float(previous.get("position", current["position"]))
            current_position = float(current["position"])
            items.append(
                {
                    "query": query,
                    "clicks_current": float(current.get("clicks", 0.0)),
                    "clicks_previous": float(previous.get("clicks", 0.0)),
                    "clicks_diff": float(current.get("clicks", 0.0)) - float(previous.get("clicks", 0.0)),
                    "impressions_current": float(current.get("impressions", 0.0)),
                    "impressions_previous": float(previous.get("impressions", 0.0)),
                    "position_current": current_position,
                    "position_previous": previous_position,
                    "position_diff": sc_position_delta(current_position, previous_position),
                }
            )
    elif previous_map:
        for query, prev in sorted(previous_map.items(), key=lambda item: item[1]["clicks"], reverse=True)[:limit]:
            prev_position = float(prev.get("position", 0.0))
            items.append(
                {
                    "query": query,
                    "clicks_current": 0.0,
                    "clicks_previous": float(prev.get("clicks", 0.0)),
                    "clicks_diff": -float(prev.get("clicks", 0.0)),
                    "impressions_current": 0.0,
                    "impressions_previous": float(prev.get("impressions", 0.0)),
                    "position_current": 0.0,
                    "position_previous": prev_position,
                    "position_diff": -prev_position,
                }
            )
    return items
