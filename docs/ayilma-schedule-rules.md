# Ayılma çizelgesi — nöbet arası boşluk tercihi

## 24 nöbet arası boş gün

| Sabit | Değer | Anlam |
|-------|-------|--------|
| `IDLE_24_GAP_MAX` | 3 | Sert tavan: iki 24 arasında en fazla 3 gün (izin hariç sayım) |
| `IDLE_24_GAP_SOFT` | 2 | Tercih edilen hedef |

## Ruh hali kuralı (yumuşak)

**Kaçınılacak kalıp:** `3 gün boş + 24 + 3 gün boş`  
Uzun boşluk → nöbet → yine uzun boşluk; çalışanlar için yorucu hissettiriyor.

**Tercih edilen kalıp:** `2 gün boş + 24 + 2 gün boş`  
Daha dengeli dinlenme–nöbet ritmi.

Motor (`backend/services/ayilma_schedule.py`):

1. Boşluk 2 güne ulaşınca (mesai kotası elveriyorsa) 24 yazmayı erken dener — 3'e kadar bekletmez.
2. `_shorten_triple_gap_sandwiches` post-pass: kalan `3+24+3` kalıplarında 24'ü bir gün öne çeker veya sonraki boşlukta erken 24 atar.
3. Yeni 24 atarken `gap_ideal = |gap − 2|` ile 2 günlük aralık tercih edilir.

**Not:** Kota, gece doluluğu (günde 2×24), dinlenme ve mesai bandı buna baskın — bazen 3+24+3 kaçınılmaz; uyarı metninde sayılır.

## İlgili sabitler

- Gün aşırı 24 zinciri: `GUN_ASIRI_STREAK_SOFT` / `MAX` / `ABSOLUTE`
- Fazla mesai bandı: `HOURS_BALANCE_TOLERANCE` (16s)
