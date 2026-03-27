"""API endpoint'leri için merkezi rate limiting bileşeni."""

from slowapi import Limiter
from slowapi.util import get_remote_address


# Aynı IP için dakikada en fazla 60 istek sınırı uygulanır.
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])
