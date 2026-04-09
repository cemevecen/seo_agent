#!/usr/bin/env python3
"""Start the FastAPI server with proper module path.

Örnekler:
  python run_server.py
  python run_server.py --reload
  python run_server.py -p 8013 --isolate

--isolate: Bu porta özel sqlite (backend/seo_agent_<port>.db), OAuth callback ve live refresh URL'lerini ayarlar.
Google Cloud OAuth istemcisinde her port için redirect URI eklemen gerekir:
  http://127.0.0.1:<port>/api/search-console/oauth/callback
"""
from __future__ import annotations

import argparse
import os
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
    return parser.parse_args()


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
