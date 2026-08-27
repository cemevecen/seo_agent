# Ayılma çizelgesi — hesaplama kuralları

Kaynak: `backend/services/ayilma_schedule.py`  
Geri dönüş: tag `ayilma-restore-*` (kurallar tazelendikten sonra) · öncesi `ayilma-before-rules-refresh-*`

## Hiyerarşi (yukarı baskın)

1. İzin / çalışmasın / day_only  
2. Dinlenme (16/24 ertesi ≥1 gün boş; ardışık gece yok)  
3. Günde 2 gece nöbeti  
4. İST / Yİ / RP dönüşü → sonraki ilk takvim günü 24 (hafta sonu dahil)  
5. Hafta içi 1×8 + 2×24 · hafta sonu yalnız 2×24  
6. **24 arası boş hücre ≤3 (KATİ; 4+ yasak)** · hedef 2+24+2  
7. Mesai: kalan günlerle aylık ideal doldur · bant ≤16s · üst sınır 400s  
8. Gün aşırı zincir ≤4 (çok sıkışıkta 5, asla 5 üstü)  
9. Kişi başı 8 / üst üste 8 / ikili çeşitlilik  

## Sabitler

| Sabit | Değer | Anlam |
|-------|-------|--------|
| `IDLE_24_GAP_MAX` | 3 | **Kati:** iki 24 arasında en fazla 3 boş hücre (4/5/6 yasak) |
| `IDLE_24_GAP_SOFT` | 2 | Tercih: 2+24+2 |
| `GUN_ASIRI_STREAK_SOFT` | 3 | Gün aşırı yumuşak hedef |
| `GUN_ASIRI_STREAK_MAX` | 4 | Normal tavan (aşılmaz tercihen) |
| `GUN_ASIRI_STREAK_ABSOLUTE` | 5 | Çok sıkışık son çare; **asla 5 üstü yok** |
| `HOURS_BALANCE_TOLERANCE` | 16 | Personel arası saat bandı |
| `MAX_MONTHLY_HOURS` | 400 | Aylık mesai üst sınırı |
| `YI_DAY_HOURS` / `RP_DAY_HOURS` | 8 | İzin/rapor gün kredisi |
| `EIGHT_PER_PERSON_*` | 2 / 3 / 4 | Kişi başı düz 8 min / hedef / max |
| `NIGHT_SHIFTS_PER_DAY` | 2 | Her gün 2 gece |

## Boşluk (ruh hali + kati)

- **Kati:** bir hemşirede 24’ler arası **4+ boş hücre olmaz**.  
- **Hedef:** `2 boş + 24 + 2 boş`. Kaçın: `3+24+3`.  
- Motor: soft’ta erken 24, `_shorten_triple_gap_sandwiches`, gap pass (İST/gece sonrası), variant retry.

## Mesai / izin

- Aylık ideal = hafta içi gün × 8.  
- Zorunlu nöbet = ideal − Yİ − RP; **kalan günlerle bu taban doldurulur**; üstüne denge kurallarıyla ek mesai gelebilir.  
- İST kotadan düşülmez; fiili mesai ile denge.  
- Yİ/RP/İST bitişinin **ertesi takvim günü** (Cmt/Paz dahil) → **24**.  

## Gün aşırı

- Zincir `24+boş+24+…`: yumuşak ≤3, normal ≤4, uç ≤5 (yalnız gece doldurma).  
