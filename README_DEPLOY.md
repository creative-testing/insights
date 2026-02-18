# README_DEPLOY — Insights SaaS (Vultr VPS + Cloudflare R2)

## Objectif
Déployer l'API FastAPI SaaS en HTTPS sur Vultr VPS, avec OAuth Facebook et stockage R2.

---

## 0) Prérequis rapides
- **VPS Vultr** (Ubuntu 22.04, Docker installé).
- Compte **Cloudflare R2** (ou S3 équivalent).
- App **Facebook** (mode Dév ok pour tests).

---

## 1) Architecture actuelle

**URL de production** : `https://insights.theaipipe.com`

**VPS Vultr** :
- SSH: `ssh root@66.135.5.31`
- Chemin: `/root/insights-backend/`
- Docker containers: `insights-api`, `insights-cron`
- Reverse proxy: nginx sur ports 80/443

**CI/CD** :
- Push sur `master` → GitHub Actions déploie automatiquement sur le VPS
- Workflow: `.github/workflows/deploy-vps.yml`

**Env Vars (dans docker-compose ou .env sur le VPS)**
```
ENVIRONMENT=production
DEBUG=false
API_VERSION=v1

# Secrets appli
SECRET_KEY=<32+ chars>
SESSION_SECRET=<32+ chars>
TOKEN_ENCRYPTION_KEY=<Fernet key base64>
JWT_ISSUER=insights-api

# DB (PostgreSQL sur le VPS)
DATABASE_URL=postgresql://...

# OAuth Facebook
META_APP_ID=<facebook-app-id>
META_APP_SECRET=<facebook-app-secret>
META_API_VERSION=v23.0
META_REDIRECT_URI=https://insights.theaipipe.com/auth/facebook/callback

# CORS & Cookies
ALLOWED_ORIGINS=https://insights.theaipipe.com,http://localhost:8080
COOKIE_SAMESITE=none
COOKIE_DOMAIN=

# Dashboard URL (post-OAuth redirect)
DASHBOARD_URL=https://insights.theaipipe.com/oauth-callback.html

# Storage R2 (bucket name kept as-is for backward compat)
STORAGE_MODE=r2
STORAGE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
STORAGE_ACCESS_KEY=<...>
STORAGE_SECRET_KEY=<...>
STORAGE_BUCKET=creative-testing-data
STORAGE_REGION=auto

# Divers
RATE_LIMIT_PER_MINUTE=60
SENTRY_DSN=
```

Vérifiez l'API:
```
curl -s https://insights.theaipipe.com/health  # 200 + JSON
```

---

## 2) Facebook App

Dans **Facebook Login → Settings**:
- **Valid OAuth Redirect URIs**:
  `https://insights.theaipipe.com/auth/facebook/callback`
- (Optionnel pour Live) Privacy Policy URL + Terms of Service.

> **Switch d'app plus tard** (ex: "Ads‑Alchemy opt"): changez **uniquement** `META_APP_ID` et `META_APP_SECRET` dans les env vars du VPS. Pas de migration nécessaire.

---

## 3) Cloudflare R2 (après validation OAuth)

1. Bucket existant: `creative-testing-data` (nom legacy, données existantes — ne pas renommer).
2. Générez des **API tokens** (Read/Write).
3. Renseignez dans les env vars du VPS:
```
STORAGE_MODE=r2
STORAGE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
STORAGE_ACCESS_KEY=<...>
STORAGE_SECRET_KEY=<...>
STORAGE_BUCKET=creative-testing-data
STORAGE_REGION=auto
```
4. Redéployez.

**Vérification (facultative)**
```
# si vous utilisez awscli:
aws s3 ls --endpoint-url=$STORAGE_ENDPOINT s3://creative-testing-data/
```

---

## 4) Tests E2E (prod‑like)

1. **Login OAuth**
   - Ouvrir `https://insights.theaipipe.com/auth/facebook/login`
   - Consent
   - Redirection vers : `DASHBOARD_URL?token=&tenant_id=...`

2. **Refresh Data**
   - Cron automatique toutes les 2h
   - Logs: `docker logs insights-cron`

3. **Data API**
   - `GET /api/data/files/{act_id}/data/optimized/manifest.json` → 200
   - `{meta,agg,summary}.json` présents

4. **FE error surfacing (mode dev‑login)**
   - en local (DEBUG=true): 400 clair `"No OAuth token found..."`.

---

## 5) Rollback

- Problème R2 → `STORAGE_MODE=local` puis redeploy.
- Redirection → changer `DASHBOARD_URL`.

---

## 6) Sécurité

- Tokens OAuth chiffrés (Fernet).
- JWT `iss/aud` vérifiés.
- CORS réduit à theaipipe.com + localhost.
- Clé R2 avec droits minimes (bucket scope).
- Pas de secrets commités (variables d'environnement uniquement).

---

## 7) Notes d'archi

- Chemins de stockage:
```
tenants/{tenant_id}/accounts/{act_id}/data/optimized/
├─ meta_v1.json
├─ agg_v1.json
├─ summary_v1.json
└─ manifest.json
```
- Endpoint data (auth requis):
```
GET /api/data/files/{act_id}/{filename}
```
- Fenêtre de fetch: **30 jours (J‑30 → J‑1)** en daily (`time_increment=1`).

---

## 8) Checklist Go‑Live (5 minutes)

- [ ] Homepage API 200
- [ ] OAuth OK (redirection vers dashboard SaaS)
- [ ] Refresh OK (agg/meta/summary/manifest)
- [ ] R2 activé et listé

---

## 9) Switch futur vers l'app Facebook "Ads‑Alchemy opt"

Quand tes patrons veulent passer sur **leur** app:

1. Dans les env vars du VPS, remplace **uniquement**:
```
META_APP_ID=1496103148207058
META_APP_SECRET=<secret de Ads-Alchemy opt>
```

2. Vérifie que `Valid OAuth Redirect URIs` contient bien:
```
https://insights.theaipipe.com/auth/facebook/callback
```

> Aucune migration à faire. Les tenants restent isolés par `tenant_id`, le user est identifié par Meta, et le pipeline reste identique.
