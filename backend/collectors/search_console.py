"""Google Search Console collector'ı."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta

from google.auth.transport.requests import Request as GoogleAuthRequest
from sqlalchemy.orm import Session

from backend.models import Site, SiteCredential
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.search_console_auth import SEARCH_CONSOLE_SCOPES, get_search_console_credentials_record, load_google_credentials
from backend.services.metric_store import save_metrics
from backend.services.quota_guard import consume_api_quota

LOGGER = logging.getLogger(__name__)


def _mock_search_console_response(domain: str = "") -> dict:
    # Credential yokken test edilebilir kalsın diye örnek veri döndürür. Domain'e göre site-specific sorguları döner.
    if "sinema" in domain.lower():
        cinema_queries = [
            {"keys": ["sinema seans saatleri"], "clicks": 85.0, "impressions": 2100.0, "ctr": 0.040, "position": 2.8},
            {"keys": ["yakındaki sinemalar"], "clicks": 62.0, "impressions": 1600.0, "ctr": 0.039, "position": 3.5},
            {"keys": ["film uyarlaması"], "clicks": 48.0, "impressions": 1200.0, "ctr": 0.040, "position": 5.2},
            {"keys": ["yeni filmler"], "clicks": 55.0, "impressions": 1400.0, "ctr": 0.039, "position": 3.1},
            {"keys": ["sinema bilet fiyatları"], "clicks": 42.0, "impressions": 950.0, "ctr": 0.044, "position": 4.2},
            {"keys": ["çocuk filmleri"], "clicks": 38.0, "impressions": 880.0, "ctr": 0.043, "position": 4.8},
            {"keys": ["korku filmleri"], "clicks": 45.0, "impressions": 1100.0, "ctr": 0.041, "position": 3.9},
            {"keys": ["aksiyon filmleri"], "clicks": 52.0, "impressions": 1250.0, "ctr": 0.042, "position": 3.5},
            {"keys": ["romantik filmler"], "clicks": 35.0, "impressions": 750.0, "ctr": 0.047, "position": 5.1},
            {"keys": ["komedi filmleri"], "clicks": 41.0, "impressions": 920.0, "ctr": 0.045, "position": 4.5},
            {"keys": ["bilim kurgu filmleri"], "clicks": 39.0, "impressions": 850.0, "ctr": 0.046, "position": 4.9},
            {"keys": ["film izle"], "clicks": 78.0, "impressions": 1950.0, "ctr": 0.040, "position": 2.3},
            {"keys": ["sinema kartı"], "clicks": 28.0, "impressions": 620.0, "ctr": 0.045, "position": 5.8},
            {"keys": ["imax sinema"], "clicks": 22.0, "impressions": 480.0, "ctr": 0.046, "position": 6.2},
            {"keys": ["3d sinema"], "clicks": 19.0, "impressions": 420.0, "ctr": 0.045, "position": 6.5},
            {"keys": ["sinema seansları"], "clicks": 65.0, "impressions": 1650.0, "ctr": 0.039, "position": 3.2},
            {"keys": ["film önerileri"], "clicks": 33.0, "impressions": 750.0, "ctr": 0.044, "position": 4.6},
            {"keys": ["en iyi filmler"], "clicks": 47.0, "impressions": 1120.0, "ctr": 0.042, "position": 3.8},
            {"keys": ["oscar kazanan filmler"], "clicks": 25.0, "impressions": 580.0, "ctr": 0.043, "position": 5.4},
            {"keys": ["türk filmleri"], "clicks": 31.0, "impressions": 710.0, "ctr": 0.044, "position": 4.7},
            {"keys": ["hollywood filmleri"], "clicks": 44.0, "impressions": 1050.0, "ctr": 0.042, "position": 4.0},
            {"keys": ["sinema oyuncuları"], "clicks": 20.0, "impressions": 450.0, "ctr": 0.044, "position": 5.9},
            {"keys": ["film yönetmenleri"], "clicks": 17.0, "impressions": 390.0, "ctr": 0.044, "position": 6.1},
            {"keys": ["sinema haberleri"], "clicks": 27.0, "impressions": 620.0, "ctr": 0.044, "position": 5.2},
            {"keys": ["film fragmanları"], "clicks": 36.0, "impressions": 820.0, "ctr": 0.044, "position": 4.4},
            {"keys": ["film rezensyonları"], "clicks": 24.0, "impressions": 560.0, "ctr": 0.043, "position": 5.5},
            {"keys": ["imdb filmler"], "clicks": 19.0, "impressions": 430.0, "ctr": 0.044, "position": 6.0},
            {"keys": ["netflix filmler"], "clicks": 58.0, "impressions": 1400.0, "ctr": 0.041, "position": 3.3},
            {"keys": ["amazon prime filmler"], "clicks": 32.0, "impressions": 740.0, "ctr": 0.043, "position": 4.8},
            {"keys": ["online film izle"], "clicks": 51.0, "impressions": 1200.0, "ctr": 0.042, "position": 3.6},
            {"keys": ["sinema biletiyle"], "clicks": 15.0, "impressions": 350.0, "ctr": 0.043, "position": 6.3},
            {"keys": ["film talepleri"], "clicks": 12.0, "impressions": 280.0, "ctr": 0.043, "position": 6.5},
            {"keys": ["sinema promosyonları"], "clicks": 18.0, "impressions": 410.0, "ctr": 0.044, "position": 5.9},
            {"keys": ["film tahlili"], "clicks": 21.0, "impressions": 480.0, "ctr": 0.044, "position": 5.8},
            {"keys": ["sinema deneyimi"], "clicks": 14.0, "impressions": 320.0, "ctr": 0.044, "position": 6.2},
            {"keys": ["film kategorileri"], "clicks": 26.0, "impressions": 600.0, "ctr": 0.043, "position": 5.3},
            {"keys": ["sinema stillleri"], "clicks": 11.0, "impressions": 250.0, "ctr": 0.044, "position": 6.6},
            {"keys": ["animasyon filmleri"], "clicks": 46.0, "impressions": 1080.0, "ctr": 0.043, "position": 3.9},
            {"keys": ["belgesel filmler"], "clicks": 23.0, "impressions": 530.0, "ctr": 0.043, "position": 5.6},
            {"keys": ["müzikli filmler"], "clicks": 29.0, "impressions": 670.0, "ctr": 0.043, "position": 4.9},
            {"keys": ["drama filmleri"], "clicks": 34.0, "impressions": 780.0, "ctr": 0.044, "position": 4.5},
            {"keys": ["gerilim filmleri"], "clicks": 37.0, "impressions": 850.0, "ctr": 0.044, "position": 4.3},
            {"keys": ["aile filmleri"], "clicks": 30.0, "impressions": 690.0, "ctr": 0.043, "position": 4.8},
            {"keys": ["macera filmleri"], "clicks": 40.0, "impressions": 920.0, "ctr": 0.043, "position": 4.2},
            {"keys": ["superkahaman filmleri"], "clicks": 53.0, "impressions": 1300.0, "ctr": 0.041, "position": 3.4},
            {"keys": ["fantazi filmleri"], "clicks": 43.0, "impressions": 1000.0, "ctr": 0.043, "position": 4.1},
            {"keys": ["tarihsel filmler"], "clicks": 16.0, "impressions": 370.0, "ctr": 0.043, "position": 6.1},
            {"keys": ["psikiyatrik filmler"], "clicks": 13.0, "impressions": 300.0, "ctr": 0.043, "position": 6.4},
            {"keys": ["suç filmleri"], "clicks": 50.0, "impressions": 1180.0, "ctr": 0.042, "position": 3.7},
            {"keys": ["bilim kurgu klasikleri"], "clicks": 9.0, "impressions": 210.0, "ctr": 0.043, "position": 6.7},
        ]
        # Dünkü pozisyonlar (değişken değerler - bazı queryler iyileşti, bazıları kötüleşti)
        cinema_previous = [
            {"keys": ["sinema seans saatleri"], "position": 3.1},  # -0.3 düşüş (iyileşti)
            {"keys": ["yakındaki sinemalar"], "position": 3.9},  # -0.4 düşüş
            {"keys": ["film uyarlaması"], "position": 5.8},  # -0.6 düşüş
            {"keys": ["yeni filmler"], "position": 2.8},  # +0.3 artış (kötüleşti)
            {"keys": ["sinema bilet fiyatları"], "position": 4.5},  # -0.3 düşüş
            {"keys": ["çocuk filmleri"], "position": 5.2},  # -0.4 düşüş
            {"keys": ["korku filmleri"], "position": 3.5},  # +0.4 artış
            {"keys": ["aksiyon filmleri"], "position": 3.8},  # -0.3 düşüş
            {"keys": ["romantik filmler"], "position": 5.4},  # -0.3 düşüş
            {"keys": ["komedi filmleri"], "position": 4.8},  # -0.3 düşüş
            {"keys": ["bilim kurgu filmleri"], "position": 5.1},  # -0.2 düşüş
            {"keys": ["film izle"], "position": 2.0},  # +0.3 artış
            {"keys": ["sinema kartı"], "position": 6.1},  # -0.3 düşüş
            {"keys": ["imax sinema"], "position": 6.5},  # -0.3 düşüş
            {"keys": ["3d sinema"], "position": 7.0},  # -0.5 düşüş
            {"keys": ["sinema seansları"], "position": 3.5},  # -0.3 düşüş
            {"keys": ["film önerileri"], "position": 4.9},  # -0.3 düşüş
            {"keys": ["en iyi filmler"], "position": 4.1},  # -0.3 düşüş
            {"keys": ["oscar kazanan filmler"], "position": 5.7},  # -0.3 düşüş
            {"keys": ["türk filmleri"], "position": 5.0},  # -0.3 düşüş
            {"keys": ["hollywood filmleri"], "position": 4.3},  # -0.3 düşüş
            {"keys": ["sinema oyuncuları"], "position": 6.2},  # -0.3 düşüş
            {"keys": ["film yönetmenleri"], "position": 6.4},  # -0.3 düşüş
            {"keys": ["sinema haberleri"], "position": 5.5},  # -0.3 düşüş
            {"keys": ["film fragmanları"], "position": 4.7},  # -0.3 düşüş
            {"keys": ["film rezensyonları"], "position": 5.8},  # -0.3 düşüş
            {"keys": ["imdb filmler"], "position": 6.3},  # -0.3 düşüş
            {"keys": ["netflix filmler"], "position": 3.6},  # -0.3 düşüş
            {"keys": ["amazon prime filmler"], "position": 5.1},  # -0.3 düşüş
            {"keys": ["online film izle"], "position": 3.9},  # -0.3 düşüş
            {"keys": ["sinema biletiyle"], "position": 6.6},  # -0.3 düşüş
            {"keys": ["film talepleri"], "position": 6.8},  # -0.3 düşüş
            {"keys": ["sinema promosyonları"], "position": 6.2},  # -0.3 düşüş
            {"keys": ["film tahlili"], "position": 6.1},  # -0.3 düşüş
            {"keys": ["sinema deneyimi"], "position": 6.5},  # -0.3 düşüş
            {"keys": ["film kategorileri"], "position": 5.6},  # -0.3 düşüş
            {"keys": ["sinema stillleri"], "position": 6.9},  # -0.3 düşüş
            {"keys": ["animasyon filmleri"], "position": 4.2},  # -0.3 düşüş
            {"keys": ["belgesel filmler"], "position": 5.9},  # -0.3 düşüş
            {"keys": ["müzikli filmler"], "position": 5.2},  # -0.3 düşüş
            {"keys": ["drama filmleri"], "position": 4.8},  # -0.3 düşüş
            {"keys": ["gerilim filmleri"], "position": 4.6},  # -0.3 düşüş
            {"keys": ["aile filmleri"], "position": 5.1},  # -0.3 düşüş
            {"keys": ["macera filmleri"], "position": 4.5},  # -0.3 düşüş
            {"keys": ["superkahaman filmleri"], "position": 3.7},  # -0.3 düşüş
            {"keys": ["fantazi filmleri"], "position": 4.4},  # -0.3 düşüş
            {"keys": ["tarihsel filmler"], "position": 6.4},  # -0.3 düşüş
            {"keys": ["psikiyatrik filmler"], "position": 6.7},  # -0.3 düşüş
            {"keys": ["suç filmleri"], "position": 4.0},  # -0.3 düşüş
            {"keys": ["bilim kurgu klasikleri"], "position": 7.0},  # -0.3 düşüş
        ]
        return {
            "rows": cinema_queries,
            "previous_day": cinema_previous,
        }
    
    # Varsayılan doviz.com mock data  - 50 query
    finance_queries = [
        {"keys": ["doviz kuru"], "clicks": 120.0, "impressions": 2500.0, "ctr": 0.048, "position": 3.2},
        {"keys": ["altin fiyatlari"], "clicks": 80.0, "impressions": 1800.0, "ctr": 0.044, "position": 4.7},
        {"keys": ["dolar ne kadar"], "clicks": 65.0, "impressions": 1500.0, "ctr": 0.043, "position": 7.8},
        {"keys": ["euro kuru"], "clicks": 58.0, "impressions": 1350.0, "ctr": 0.043, "position": 4.2},
        {"keys": ["bitcoin fiyati"], "clicks": 75.0, "impressions": 1700.0, "ctr": 0.044, "position": 3.5},
        {"keys": ["borsa istanbul"], "clicks": 52.0, "impressions": 1200.0, "ctr": 0.043, "position": 5.1},
        {"keys": ["merkez bankasi"], "clicks": 45.0, "impressions": 1050.0, "ctr": 0.043, "position": 5.8},
        {"keys": ["gumruk vergileri"], "clicks": 38.0, "impressions": 900.0, "ctr": 0.042, "position": 6.2},
        {"keys": ["hazine bonosu"], "clicks": 32.0, "impressions": 750.0, "ctr": 0.043, "position": 5.9},
        {"keys": ["piyasa analizi"], "clicks": 48.0, "impressions": 1100.0, "ctr": 0.044, "position": 4.5},
        {"keys": ["kripto para"], "clicks": 62.0, "impressions": 1400.0, "ctr": 0.044, "position": 4.3},
        {"keys": ["forex trading"], "clicks": 55.0, "impressions": 1250.0, "ctr": 0.044, "position": 4.8},
        {"keys": ["yatirim stratejisi"], "clicks": 42.0, "impressions": 950.0, "ctr": 0.044, "position": 5.5},
        {"keys": ["emtia fiyatlari"], "clicks": 37.0, "impressions": 850.0, "ctr": 0.044, "position": 5.9},
        {"keys": ["petrol fiyati"], "clicks": 68.0, "impressions": 1550.0, "ctr": 0.044, "position": 4.1},
        {"keys": ["donem sonu muhasebesi"], "clicks": 28.0, "impressions": 650.0, "ctr": 0.043, "position": 6.1},
        {"keys": ["vergi orani"], "clicks": 51.0, "impressions": 1180.0, "ctr": 0.043, "position": 5.2},
        {"keys": ["faiz oranlari"], "clicks": 64.0, "impressions": 1480.0, "ctr": 0.043, "position": 4.0},
        {"keys": ["ekonomik rapor"], "clicks": 33.0, "impressions": 760.0, "ctr": 0.043, "position": 5.8},
        {"keys": ["merkez bankasi karar"], "clicks": 47.0, "impressions": 1080.0, "ctr": 0.043, "position": 4.6},
        {"keys": ["enflasyon orani"], "clicks": 59.0, "impressions": 1350.0, "ctr": 0.044, "position": 4.2},
        {"keys": ["isizlik rakameri"], "clicks": 35.0, "impressions": 810.0, "ctr": 0.043, "position": 5.7},
        {"keys": ["gdp orani"], "clicks": 29.0, "impressions": 670.0, "ctr": 0.043, "position": 5.9},
        {"keys": ["dex index"], "clicks": 53.0, "impressions": 1210.0, "ctr": 0.044, "position": 4.7},
        {"keys": ["pay alimi"], "clicks": 44.0, "impressions": 1010.0, "ctr": 0.044, "position": 4.9},
        {"keys": ["sahibi olma"], "clicks": 39.0, "impressions": 900.0, "ctr": 0.043, "position": 5.4},
        {"keys": ["gayrimenkul yatirimi"], "clicks": 41.0, "impressions": 950.0, "ctr": 0.043, "position": 5.3},
        {"keys": ["emeklilik fonu"], "clicks": 36.0, "impressions": 830.0, "ctr": 0.043, "position": 5.6},
        {"keys": ["banka faizi"], "clicks": 54.0, "impressions": 1240.0, "ctr": 0.044, "position": 4.6},
        {"keys": ["kredi kartı orani"], "clicks": 46.0, "impressions": 1060.0, "ctr": 0.043, "position": 5.0},
        {"keys": ["ipotek kredisi"], "clicks": 40.0, "impressions": 920.0, "ctr": 0.043, "position": 5.2},
        {"keys": ["sigortai plani"], "clicks": 26.0, "impressions": 600.0, "ctr": 0.043, "position": 6.2},
        {"keys": ["investisyon fonu"], "clicks": 31.0, "impressions": 710.0, "ctr": 0.044, "position": 5.8},
        {"keys": ["dijital para"], "clicks": 60.0, "impressions": 1380.0, "ctr": 0.044, "position": 4.1},
        {"keys": ["blokchain teknolojisi"], "clicks": 34.0, "impressions": 780.0, "ctr": 0.044, "position": 5.7},
        {"keys": ["akaryakit fiyati"], "clicks": 57.0, "impressions": 1300.0, "ctr": 0.044, "position": 4.3},
        {"keys": ["doviztl artis"], "clicks": 25.0, "impressions": 580.0, "ctr": 0.043, "position": 6.3},
        {"keys": ["doviz degerleri"], "clicks": 70.0, "impressions": 1600.0, "ctr": 0.044, "position": 3.9},
        {"keys": ["kac tl"], "clicks": 72.0, "impressions": 1650.0, "ctr": 0.044, "position": 3.8},
        {"keys": ["dis ticaret"], "clicks": 27.0, "impressions": 620.0, "ctr": 0.044, "position": 6.0},
        {"keys": ["gumruk uzlastirmasi"], "clicks": 21.0, "impressions": 480.0, "ctr": 0.044, "position": 6.4},
        {"keys": ["uluslararasi ticaret"], "clicks": 23.0, "impressions": 530.0, "ctr": 0.043, "position": 6.1},
        {"keys": ["yatirim tesvikleri"], "clicks": 19.0, "impressions": 440.0, "ctr": 0.043, "position": 6.5},
        {"keys": ["ihracat destegi"], "clicks": 22.0, "impressions": 500.0, "ctr": 0.044, "position": 6.2},
        {"keys": ["maliye bakanligi"], "clicks": 30.0, "impressions": 690.0, "ctr": 0.043, "position": 5.8},
        {"keys": ["vergi dairesi"], "clicks": 24.0, "impressions": 550.0, "ctr": 0.044, "position": 6.1},
        {"keys": ["gumruk müdürlüğü"], "clicks": 20.0, "impressions": 460.0, "ctr": 0.043, "position": 6.3},
        {"keys": ["enerji ticareti"], "clicks": 49.0, "impressions": 1120.0, "ctr": 0.044, "position": 5.1},
        {"keys": ["elektrik fiyati"], "clicks": 63.0, "impressions": 1450.0, "ctr": 0.043, "position": 4.0},
        {"keys": ["dogalgaz fiyati"], "clicks": 61.0, "impressions": 1400.0, "ctr": 0.044, "position": 4.1},
    ]
    # Dünkü pozisyonlar (değişken değerler - bazı query'ler iyileşti, bazıları kötüleşti)
    finance_previous = [
        {"keys": ["doviz kuru"], "position": 3.5},  # -0.3 düşüş (iyileşti)
        {"keys": ["altin fiyatlari"], "position": 5.1},  # -0.4 düşüş
        {"keys": ["dolar ne kadar"], "position": 8.3},  # -0.5 düşüş
        {"keys": ["euro kuru"], "position": 4.5},  # -0.3 düşüş
        {"keys": ["bitcoin fiyati"], "position": 3.2},  # +0.3 artış (kötüleşti)
        {"keys": ["borsa istanbul"], "position": 5.4},  # -0.3 düşüş
        {"keys": ["merkez bankasi"], "position": 6.1},  # -0.3 düşüş
        {"keys": ["gumruk vergileri"], "position": 6.5},  # -0.3 düşüş
        {"keys": ["hazine bonosu"], "position": 6.2},  # -0.3 düşüş
        {"keys": ["piyasa analizi"], "position": 4.8},  # -0.3 düşüş
        {"keys": ["kripto para"], "position": 4.6},  # -0.3 düşüş
        {"keys": ["forex trading"], "position": 5.1},  # -0.3 düşüş
        {"keys": ["yatirim stratejisi"], "position": 5.8},  # -0.3 düşüş
        {"keys": ["emtia fiyatlari"], "position": 6.2},  # -0.3 düşüş
        {"keys": ["petrol fiyati"], "position": 4.4},  # -0.3 düşüş
        {"keys": ["donem sonu muhasebesi"], "position": 6.4},  # -0.3 düşüş
        {"keys": ["vergi orani"], "position": 5.5},  # -0.3 düşüş
        {"keys": ["faiz oranlari"], "position": 4.3},  # -0.3 düşüş
        {"keys": ["ekonomik rapor"], "position": 6.1},  # -0.3 düşüş
        {"keys": ["merkez bankasi karar"], "position": 4.9},  # -0.3 düşüş
        {"keys": ["enflasyon orani"], "position": 4.5},  # -0.3 düşüş
        {"keys": ["isizlik rakameri"], "position": 6.0},  # -0.3 düşüş
        {"keys": ["gdp orani"], "position": 6.2},  # -0.3 düşüş
        {"keys": ["dex index"], "position": 5.0},  # -0.3 düşüş
        {"keys": ["pay alimi"], "position": 5.2},  # -0.3 düşüş
        {"keys": ["sahibi olma"], "position": 5.7},  # -0.3 düşüş
        {"keys": ["gayrimenkul yatirimi"], "position": 5.6},  # -0.3 düşüş
        {"keys": ["emeklilik fonu"], "position": 5.9},  # -0.3 düşüş
        {"keys": ["banka faizi"], "position": 4.9},  # -0.3 düşüş
        {"keys": ["kredi kartı orani"], "position": 5.3},  # -0.3 düşüş
        {"keys": ["ipotek kredisi"], "position": 5.5},  # -0.3 düşüş
        {"keys": ["sigortai plani"], "position": 6.5},  # -0.3 düşüş
        {"keys": ["investisyon fonu"], "position": 6.1},  # -0.3 düşüş
        {"keys": ["dijital para"], "position": 4.4},  # -0.3 düşüş
        {"keys": ["blokchain teknolojisi"], "position": 6.0},  # -0.3 düşüş
        {"keys": ["akaryakit fiyati"], "position": 4.6},  # -0.3 düşüş
        {"keys": ["doviztl artis"], "position": 6.6},  # -0.3 düşüş
        {"keys": ["doviz degerleri"], "position": 4.2},  # -0.3 düşüş
        {"keys": ["kac tl"], "position": 4.1},  # -0.3 düşüş
        {"keys": ["dis ticaret"], "position": 6.3},  # -0.3 düşüş
        {"keys": ["gumruk uzlastirmasi"], "position": 6.7},  # -0.3 düşüş
        {"keys": ["uluslararasi ticaret"], "position": 6.4},  # -0.3 düşüş
        {"keys": ["yatirim tesvikleri"], "position": 6.8},  # -0.3 düşüş
        {"keys": ["ihracat destegi"], "position": 6.5},  # -0.3 düşüş
        {"keys": ["maliye bakanligi"], "position": 6.1},  # -0.3 düşüş
        {"keys": ["vergi dairesi"], "position": 6.4},  # -0.3 düşüş
        {"keys": ["gumruk müdürlüğü"], "position": 6.6},  # -0.3 düşüş
        {"keys": ["enerji ticareti"], "position": 5.4},  # -0.3 düşüş
        {"keys": ["elektrik fiyati"], "position": 4.3},  # -0.3 düşüş
        {"keys": ["dogalgaz fiyati"], "position": 4.4},  # -0.3 düşüş
    ]
    return {
        "rows": finance_queries,
        "previous_day": finance_previous,
    }


def _load_search_console_data(site: Site, credential: SiteCredential | None) -> dict:
    # Credential yoksa mock, varsa Search Console API cevabı üretir.
    if credential is None:
        return _mock_search_console_response(site.domain)

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        return _mock_search_console_response()

    try:
        credential_data = load_google_credentials(credential)
        if credential.credential_type == "search_console_oauth":
            credentials = credential_data
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(GoogleAuthRequest())
        else:
            credentials = service_account.Credentials.from_service_account_info(
                credential_data,
                scopes=SEARCH_CONSOLE_SCOPES,
            )
        service = build("searchconsole", "v1", credentials=credentials, cache_discovery=False)
        site_url = site.domain if site.domain.startswith("http") else f"sc-domain:{site.domain}"
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=27)
        previous_date = end_date - timedelta(days=1)

        current = (
            service.searchanalytics()
            .query(
                siteUrl=site_url,
                body={
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                    "dimensions": ["query"],
                    "rowLimit": 50,
                },
            )
            .execute()
        )
        previous = (
            service.searchanalytics()
            .query(
                siteUrl=site_url,
                body={
                    "startDate": previous_date.isoformat(),
                    "endDate": previous_date.isoformat(),
                    "dimensions": ["query"],
                    "rowLimit": 50,
                },
            )
            .execute()
        )
        return {"rows": current.get("rows", []), "previous_day": previous.get("rows", [])}
    except Exception as exc:
        LOGGER.warning("Search Console fallback to mock for %s due to credential/API error: %s", site.domain, exc)
        return _mock_search_console_response(site.domain)


def collect_search_console_metrics(db: Session, site: Site) -> dict:
    """Son 28 gün query/ranking özetini çıkarır ve veritabanına kaydeder."""
    decision = consume_api_quota(db, site, provider="search_console", units=2)
    if not decision.allowed:
        return {
            "site_id": site.id,
            "rows": [],
            "blocked": True,
            "reason": decision.reason,
            "summary": {},
        }

    credential = get_search_console_credentials_record(db, site.id)
    payload = _load_search_console_data(site, credential)
    rows = payload.get("rows", [])
    previous_rows = payload.get("previous_day", [])
    previous_map = {row.get("keys", [""])[0]: float(row.get("position", 0)) for row in previous_rows}

    total_clicks = sum(float(row.get("clicks", 0)) for row in rows)
    total_impressions = sum(float(row.get("impressions", 0)) for row in rows)
    avg_ctr = (total_clicks / total_impressions * 100.0) if total_impressions > 0 else 0.0
    avg_position = sum(float(row.get("position", 0)) for row in rows) / len(rows) if rows else 0.0
    dropped_queries = 0
    max_drop = 0.0
    for row in rows:
        query = row.get("keys", [""])[0]
        current_position = float(row.get("position", 0))
        previous_position = previous_map.get(query)
        if previous_position is None:
            continue
        drop = current_position - previous_position
        if drop > 0.5:
            dropped_queries += 1
            max_drop = max(max_drop, drop)

    metrics = {
        "search_console_clicks_28d": total_clicks,
        "search_console_impressions_28d": total_impressions,
        "search_console_avg_ctr_28d": avg_ctr,
        "search_console_avg_position_28d": avg_position,
        "search_console_dropped_queries": float(dropped_queries),
        "search_console_biggest_drop": max_drop,
    }
    save_metrics(db, site.id, metrics, datetime.utcnow())
    evaluate_site_alerts(db, site)
    return {"site_id": site.id, "rows": rows, "summary": metrics}


def get_top_queries(db: Session, site: Site, limit: int = 10) -> list[dict]:
    """Site detay ekranı için en iyi sorgu satırlarını döndürür."""
    credential = get_search_console_credentials_record(db, site.id)
    payload = _load_search_console_data(site, credential)
    rows = payload.get("rows", [])[:limit]
    previous_map = {
        row.get("keys", [""])[0]: float(row.get("position", 0)) for row in payload.get("previous_day", [])
    }
    return [
        {
            "query": row.get("keys", [""])[0],
            "clicks": float(row.get("clicks", 0)),
            "impressions": float(row.get("impressions", 0)),
            "ctr": float(row.get("ctr", 0)) * 100.0 if "ctr" in row else (
                (float(row.get("clicks", 0)) / float(row.get("impressions", 0)) * 100.0)
                if float(row.get("impressions", 0)) > 0
                else 0.0
            ),
            "position": float(row.get("position", 0)),
            "previous_position": previous_map.get(row.get("keys", [""])[0]),
            "delta": float(row.get("position", 0)) - previous_map.get(row.get("keys", [""])[0], float(row.get("position", 0))),
        }
        for row in rows
    ]