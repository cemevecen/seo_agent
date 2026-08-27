# Ayılma çizelgesi — hesaplama kuralları

Kaynak: `backend/services/ayilma_schedule.py`  
**Hesap tabanı:** `aa3ee655` (2026-08-26 21:40 GMT+3).  
Geri dönüş (önceki uzun motor): tag `ayilma-calc-before-rollback-20260827`.

## Hiyerarşi (özet)

0. **Gülten Çelik** panelde boş satır; hesaba dahil değil.  
1. İzin / day_only  
2. Dinlenme (16/24 sonrası tercih; `prefer_48h_after_24`)  
3. Günde 2 gece nöbeti  
4. Yİ/RP dönüşü → ertesi gün 24 tercih  
5. Hafta içi mümkünse 1×8 + 2×24 · hafta sonu yalnız 2×24  
6. Mesai: Yİ 8s kredi · personel arası ~16s bant · üst 300s  
7. Gün aşırı zincir **≤3 kati** (4. yasak; izin yoğun haftada da)  
8. Kişi başı 8 · hedef 3 (2–4) · art arda 8 en fazla 2  

## Yeniden oluştur (`variant`)

- `variant=0` sabit öneri; `variant>0` eşit adaylarda kadro/gün sırası ve denge takasları karışır.  
- Panel «Yeniden oluştur» her tıklamada `variant++`; aynı çizelge gelirse birkaç kez daha dener.  

## Sabitler

| Sabit | Değer |
|-------|--------|
| `MAX_MONTHLY_HOURS` | 300 |
| `HOURS_BALANCE_TOLERANCE` | 16 |
| `YI_DAY_HOURS` | 8 |
| `EIGHT_PER_PERSON_*` | 2 / 3 / 4 |
| `GUN_ASIRI_STREAK_MAX` | 3 | Kati tavan; 4+ yok |
| `CONSECUTIVE_8_STREAK_MAX` | 2 |

## Özel koşul

- **Çalışsın** — seçilen günlerde o kişiye nöbet/8 tercihi  
- **Çalışmasın** — seçilen günlerde atanmaz (sert)  
- **Sabit vardiya** — takvimde güne basarak `8` / `16` / `24` pinlenir; çizelge buna uymak zorunda  
- Haftalık tekrar: seçilen tarihin hafta günü ay boyunca tekrarlanır  
