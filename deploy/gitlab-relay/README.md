# GitLab API relay (tüm tarayıcılar — sunucu üzerinden boards)

Railway’deki SEO Agent, şirket içi `git.nokta.com` adresine doğrudan TCP ile ulaşamaz. Bu relay’i **VPN/ofis ağına erişen bir makinede** çalıştırıp dışarıya HTTPS ile açarsanız, `/boards` **her tarayıcıda** aynı şekilde çalışır (kullanıcı başına token gerekmez).

## 1. Relay’i ayağa kaldırın

```bash
cd deploy/gitlab-relay
cp .env.example .env   # token ve secret doldurun
docker compose up -d --build
curl -s http://127.0.0.1:8090/health
```

## 2. İnternete açın (örnekler)

- Cloudflare Tunnel → `https://gitlab-relay.sirket.com`
- nginx + Let’s Encrypt
- Tailscale Funnel / public node

Dış URL’nin `https://.../api/v4/version` yoluna **sadece SEO Agent Railway IP’lerinden** veya relay secret ile erişim verin.

## 3. Railway (SEO Agent) ortam değişkenleri

| Değişken | Örnek |
|----------|--------|
| `GITLAB_API_BASE_URL` | `https://gitlab-relay.sirket.com/api/v4` |
| `GITLAB_PRIVATE_TOKEN` | GitLab PAT (relay ile aynı veya relay kendi token’ını kullanır) |
| `GITLAB_RELAY_SECRET` | Relay ile aynı gizli anahtar |

`GITLAB_BOARDS_BROWSER_FALLBACK=0` (varsayılan) — tarayıcı başına token kapalı.

Deploy sonrası `/boards` → üstte **GitLab erişilebilir**, kanban herkes için yüklenir.
