#!/usr/bin/env python3
"""Start the FastAPI server with proper module path.

Örnekler:
  python run_server.py
  python run_server.py --reload
  python run_server.py -p 8013 --isolate

--isolate: Bu porta özel sqlite (backend/seo_agent_<port>.db), OAuth callback ve live refresh URL'lerini ayarlar.
Google Cloud OAuth istemcisinde her port için redirect URI eklemen gerekir:
  http://127.0.0.1:<port>/api/search-console/oauth/callback

8012 (veya seçilen port) loopback’te zaten dinleniyorsa (ör. docker compose) başlamaz;
yanlış sürece düşmeyi önlemek için. Gerekirse: --skip-port-check
"""
from __future__ import annotations

import argparse
import os
import socket
import sys
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SEO Agent yerel sunucu")
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=int(os.environ.get("APP_PORT") or os.environ.get("PORT") or 8012),
        help="Dinlenecek port (varsayılan: 8012 veya APP_PORT)",
    )
    parser.add_argument(
        "--isolate",
        action="store_true",
        help=(
            "Ayrı yerel kopya: backend/seo_agent_<port>.db ve bu porta göre "
            "GOOGLE_OAUTH_REDIRECT_URI + LIVE_REFRESH_URLS (ortam değişkeni olarak set edilir, .env'i ezer)"
        ),
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Geliştirme: dosya değişince sunucuyu yeniden başlat (watchfiles CPU kullanır). Varsayılan: kapalı.",
    )
    parser.add_argument(
        "--skip-port-check",
        action="store_true",
        help="Loopback port doluluk kontrolünü atla (özel durumlar; genelde gerekmez).",
    )
    return parser.parse_args()


def _loopback_port_in_use(port: int) -> bool:
    """True if something already accepts TCP on this port on loopback (IPv4 and/or IPv6).

    İki süreç (ör. Docker yayını + yerel uvicorn) aynı host portuna bind edince tarayıcı
    yanlış olana düşebiliyor; GA4 gibi .env farkları görünür hale geliyor.
    """
    targets: list[tuple[str, int]] = [("127.0.0.1", socket.AF_INET)]
    if getattr(socket, "has_ipv6", False):
        targets.append(("::1", socket.AF_INET6))
    for host, family in targets:
        try:
            with socket.socket(family, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.25)
                if sock.connect_ex((host, port)) == 0:
                    return True
        except OSError:
            continue
    return False


def _apply_isolate_env(port: int) -> None:
    os.environ["APP_PORT"] = str(port)
    os.environ["DATABASE_URL"] = f"sqlite:///backend/seo_agent_{port}.db"
    base = f"http://127.0.0.1:{port}"
    os.environ["GOOGLE_OAUTH_REDIRECT_URI"] = f"{base}/api/search-console/oauth/callback"
    os.environ["LIVE_REFRESH_URLS"] = f"{base}/health,{base}/"


if __name__ == "__main__":
    args = _parse_args()
    if args.isolate:
        _apply_isolate_env(args.port)

    if not args.skip_port_check and _loopback_port_in_use(args.port):
        print(
            f"Port {args.port} loopback üzerinde zaten kullanılıyor (çoğunlukla `docker compose` "
            f"ile yayınlanan uygulama).\n"
            f"Aynı anda `python run_server.py` çalıştırmak tarayıcıyı yanlış sürece yönlendirebilir "
            f"(ör. GA4 service account uyarısı).\n\n"
            f"Seçenekler:\n"
            f"  • Yerel Python kullanacaksan: önce `docker compose stop app` (veya tüm stack: "
            f"`docker compose down`).\n"
            f"  • Docker kullanmaya devam edeceksen: bu komutu çalıştırma; tarayıcıdan "
            f"http://127.0.0.1:{args.port} adresine git.\n"
            f"  • İkisini birden istiyorsan: `python run_server.py -p 8013` (OAuth redirect URI’lerini "
            f"o porta göre güncelle).\n"
            f"  • Kontrolü atlamak (önerilmez): `--skip-port-check`\n",
            file=sys.stderr,
        )
        sys.exit(1)

    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))

    from backend.main import app  # noqa: F401  # import side effects: routes, scheduler

    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host="127.0.0.1",
        port=args.port,
        reload=args.reload,
    )
