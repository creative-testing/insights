"""
Router d'authentification Facebook OAuth avec state sécurisé
"""
from datetime import datetime, timedelta
import secrets
import time
from urllib.parse import urlencode
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from cryptography.fernet import Fernet

from jose import jwt, JWTError
from pydantic import BaseModel

from ..database import get_db
from ..config import settings
from ..services.meta_client import meta_client, MetaAPIError
from ..utils.jwt import create_access_token
from .. import models

router = APIRouter(prefix="/facebook", tags=["auth"])


# === Supabase Auth Integration ===

class SyncFacebookRequest(BaseModel):
    """Request body for /auth/sync-facebook endpoint"""
    provider_token: str  # Facebook short-lived token from Supabase OAuth


def verify_supabase_token(token: str) -> dict:
    """
    Verify and decode a Supabase JWT token

    Returns:
        Decoded payload with 'sub' (user_id), 'email', etc.

    Raises:
        HTTPException if token is invalid
    """
    if not settings.SUPABASE_JWT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Supabase JWT secret not configured"
        )

    try:
        payload = jwt.decode(
            token,
            settings.SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated"
        )
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=401,
            detail=f"Invalid Supabase token: {str(e)}"
        )

# Fernet pour chiffrement des tokens
fernet = Fernet(settings.TOKEN_ENCRYPTION_KEY.encode())


@router.get("/login")
async def facebook_login(request: Request, lang: Optional[str] = None):
    """
    Initie le flux OAuth Facebook
    Génère un state sécurisé et redirige vers Facebook
    Supporte ?lang=en pour forcer la popup OAuth en anglais
    """
    # Générer state sécurisé pour CSRF protection avec TTL
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = {
        "value": state,
        "timestamp": int(time.time()),
        "lang": lang  # Store lang to preserve it through OAuth flow
    }

    # Paramètres OAuth
    params = {
        "client_id": settings.META_APP_ID,
        "redirect_uri": settings.META_REDIRECT_URI,
        "response_type": "code",
        "state": state,
        "scope": "email,ads_read,public_profile",
    }

    # Force English locale for Facebook OAuth popup if lang=en
    if lang == "en":
        params["locale"] = "en_US"

    # URL d'autorisation Facebook
    auth_url = f"https://www.facebook.com/{settings.META_API_VERSION}/dialog/oauth?{urlencode(params)}"

    return RedirectResponse(url=auth_url)


@router.get("/callback")
async def facebook_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Callback OAuth Facebook
    Échange le code contre un token, récupère les ad accounts, et crée le tenant/user
    """
    # Gestion des erreurs OAuth
    if error:
        raise HTTPException(status_code=400, detail=f"OAuth error: {error}")

    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state parameter")

    # Vérification state (CSRF protection + TTL)
    state_data = request.session.get("oauth_state")
    if not state_data:
        raise HTTPException(status_code=403, detail="Invalid OAuth state (CSRF detected)")

    # Vérifier le state value
    expected_state = state_data.get("value") if isinstance(state_data, dict) else state_data
    if state != expected_state:
        request.session.pop("oauth_state", None)
        raise HTTPException(status_code=403, detail="Invalid OAuth state (CSRF detected)")

    # Vérifier TTL (10 minutes max)
    if isinstance(state_data, dict):
        timestamp = state_data.get("timestamp", 0)
        if int(time.time()) - timestamp > 600:  # 10 minutes
            request.session.pop("oauth_state", None)
            raise HTTPException(status_code=403, detail="Expired OAuth state (session timeout)")

    # Nettoyer le state de la session
    request.session.pop("oauth_state", None)

    try:
        # 1. Échanger code contre access token (long-lived, 60 jours)
        token_data = await meta_client.exchange_code_for_token(
            code=code,
            redirect_uri=settings.META_REDIRECT_URI
        )
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in") or 5184000  # ~60 jours par défaut

        # 2. Récupérer métadonnées du token (user_id, scopes)
        token_info = await meta_client.debug_token(access_token)
        meta_user_id = token_info["user_id"]
        scopes = token_info.get("scopes", [])

        # Vérifier que le scope ads_read est présent
        if "ads_read" not in scopes:
            raise HTTPException(
                status_code=403,
                detail="Missing required scope: ads_read. Please re-authorize the application."
            )

        # 3. Récupérer infos utilisateur
        user_info = await meta_client.get_user_info(access_token, fields="id,name,email")
        user_name = user_info.get("name", "Unknown")
        user_email = user_info.get("email")

        # Normaliser email en minuscules + trim (pour l'index case-insensitive + CHECK constraint)
        if user_email:
            user_email = user_email.strip().lower()
        else:
            # Fallback: certains comptes Facebook n'ont pas d'email attaché
            # On génère un email fictif basé sur le meta_user_id pour satisfaire la contrainte NOT NULL
            user_email = f"{meta_user_id}@noemail.facebook"

        # 4. Récupérer ad accounts
        ad_accounts = await meta_client.get_ad_accounts(
            access_token,
            fields="id,name,currency,timezone_name,account_status"
        )

        # Chiffrer le token avant l'upsert
        token_encrypted = fernet.encrypt(access_token.encode()).decode()
        expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

        # 5. Transaction atomique : tenant → user → token → ad accounts
        with db.begin_nested():  # SAVEPOINT pour rollback partiel si erreur
            # 5a. Upsert tenant (ON CONFLICT DO UPDATE on meta_user_id)
            stmt = insert(models.Tenant).values(
                meta_user_id=meta_user_id,
                name=f"{user_name}'s Workspace",
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["meta_user_id"],
                set_={"name": stmt.excluded.name, "updated_at": func.now()},
            )
            db.execute(stmt)
            db.flush()

            # Récupérer le tenant créé/mis à jour
            tenant = db.execute(
                select(models.Tenant).where(models.Tenant.meta_user_id == meta_user_id)
            ).scalar_one()

            # 5b. Upsert user (ON CONFLICT DO UPDATE on tenant_id + meta_user_id)
            stmt = insert(models.User).values(
                tenant_id=tenant.id,
                meta_user_id=meta_user_id,
                email=user_email,
                name=user_name,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["tenant_id", "meta_user_id"],
                set_={
                    "email": stmt.excluded.email,
                    "name": stmt.excluded.name,
                    "updated_at": func.now(),
                },
            )
            db.execute(stmt)
            db.flush()

            # Récupérer le user créé/mis à jour
            user = db.execute(
                select(models.User).where(
                    models.User.tenant_id == tenant.id,
                    models.User.meta_user_id == meta_user_id
                )
            ).scalar_one()

            # 5c. Upsert OAuth token (ON CONFLICT DO UPDATE on user_id + provider)
            stmt = insert(models.OAuthToken).values(
                tenant_id=tenant.id,
                user_id=user.id,
                provider="meta",
                fb_user_id=meta_user_id,
                access_token=token_encrypted.encode(),
                expires_at=expires_at,
                scopes=scopes if isinstance(scopes, list) else [scopes],
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["user_id", "provider"],
                set_={
                    "access_token": stmt.excluded.access_token,
                    "expires_at": stmt.excluded.expires_at,
                    "scopes": stmt.excluded.scopes,
                    "fb_user_id": stmt.excluded.fb_user_id,
                },
            )
            db.execute(stmt)

            # 5d. Upsert ad accounts (ON CONFLICT DO UPDATE on tenant_id + fb_account_id)
            for account in ad_accounts:
                fb_account_id = account["id"]
                account_name = account.get("name", "Unknown")
                account_currency = account.get("currency")  # USD, MXN, EUR, etc.

                stmt = insert(models.AdAccount).values(
                    tenant_id=tenant.id,
                    fb_account_id=fb_account_id,
                    name=account_name,
                    currency=account_currency,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["tenant_id", "fb_account_id"],
                    set_={"name": stmt.excluded.name, "currency": stmt.excluded.currency},
                )
                db.execute(stmt)

            # 5e. Ensure tenant has a subscription (create FREE plan if none exists)
            existing_subscription = db.execute(
                select(models.Subscription).where(models.Subscription.tenant_id == tenant.id)
            ).scalar_one_or_none()

            if not existing_subscription:
                # Create default FREE plan subscription
                subscription = models.Subscription(
                    tenant_id=tenant.id,
                    plan="free",
                    status="active",
                    quota_accounts=3,  # Free tier: 3 accounts max
                    quota_refresh_per_day=1,  # Free tier: 1 refresh/day
                )
                db.add(subscription)

        # 6. Commit la transaction principale
        db.commit()

        # 7. Générer JWT access token pour l'API
        access_token = create_access_token(
            user_id=user.id,
            tenant_id=tenant.id
        )

        # 8. Redirect direct vers dashboard avec token (pas de page intermédiaire)
        redirect_url = f"{settings.DASHBOARD_URL}?token={access_token}&tenant_id={tenant.id}"

        # Preserve lang parameter if it was set during login
        lang_from_session = state_data.get("lang") if isinstance(state_data, dict) else None
        if lang_from_session:
            redirect_url += f"&lang={lang_from_session}"

        response = RedirectResponse(url=redirect_url, status_code=302)

        # Poser le JWT dans un cookie HttpOnly sécurisé
        response.set_cookie(
            key="access_token",
            value=access_token,
            httponly=True,
            secure=not settings.DEBUG,  # HTTPS seulement en production
            samesite=settings.COOKIE_SAMESITE,  # "lax" for same eTLD+1, "none" for cross-site
            domain=settings.COOKIE_DOMAIN or None,  # None = current domain only
            max_age=30 * 60,  # 30 minutes
            path="/"
        )

        return response

    except MetaAPIError as e:
        raise HTTPException(status_code=502, detail=f"Meta API error: {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.post("/dev-login")
def dev_login(db: Session = Depends(get_db)):
    """
    DEBUG ONLY: Dev login endpoint to bypass OAuth for testing
    Creates/reuses a dev tenant and returns JWT token + cookie
    """
    if not settings.DEBUG:
        raise HTTPException(status_code=404, detail="Not found")

    # Get or create dev tenant
    tenant = db.execute(
        select(models.Tenant).where(models.Tenant.meta_user_id == "dev_tenant")
    ).scalar_one_or_none()

    if not tenant:
        tenant = models.Tenant(name="Dev Tenant", meta_user_id="dev_tenant")
        db.add(tenant)
        db.flush()

        # Create dev user
        user = models.User(
            tenant_id=tenant.id,
            meta_user_id="dev_user",
            email="dev@example.com",
            name="Dev User"
        )
        db.add(user)
        db.flush()

        # Create FREE subscription
        subscription = models.Subscription(
            tenant_id=tenant.id,
            plan="free",
            status="active",
            quota_accounts=3,
            quota_refresh_per_day=1
        )
        db.add(subscription)

        # Create dev ad account
        ad_account = models.AdAccount(
            tenant_id=tenant.id,
            fb_account_id="act_123456789",
            name="Dev Ad Account"
        )
        db.add(ad_account)

        db.commit()

    # Get user
    user = db.execute(
        select(models.User).where(
            models.User.tenant_id == tenant.id,
            models.User.meta_user_id == "dev_user"
        )
    ).scalar_one()

    # Create JWT token
    token = create_access_token(user.id, tenant.id)

    # Return JSON with token + set cookie
    resp = JSONResponse({
        "access_token": token,
        "tenant_id": str(tenant.id),
        "user_id": str(user.id),
        "message": "Dev login successful (DEBUG mode only)"
    })

    resp.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        secure=not settings.DEBUG,
        samesite=settings.COOKIE_SAMESITE,
        domain=settings.COOKIE_DOMAIN or None,
        max_age=30 * 60,  # 30 minutes
        path="/"
    )

    return resp


@router.post("/logout")
async def logout(request: Request):
    """Déconnexion (clear session)"""
    request.session.clear()
    return {"message": "Logged out successfully"}


@router.post("/test-token")
def test_token(tenant_id: str, db: Session = Depends(get_db)):
    """
    TEMPORARY: Generate JWT token for testing
    TODO: DELETE THIS ENDPOINT AFTER TESTING
    """
    from uuid import UUID

    # Validate tenant exists
    tenant = db.execute(
        select(models.Tenant).where(models.Tenant.id == UUID(tenant_id))
    ).scalar_one_or_none()

    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    # Get user for this tenant
    user = db.execute(
        select(models.User).where(models.User.tenant_id == UUID(tenant_id))
    ).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found for tenant")

    user = user[0]

    # Generate token (7 days expiry)
    token = create_access_token(user.id, tenant.id, expires_delta=timedelta(days=7))

    return {
        "access_token": token,
        "tenant_id": str(tenant.id),
        "user_id": str(user.id),
        "message": "TEMPORARY test token - DELETE this endpoint after testing!"
    }


# === Supabase Auth Sync Endpoint ===

@router.post("/login-via-supabase")
async def login_via_supabase(
    request: Request,
    db: Session = Depends(get_db)
):
    """
    SSO endpoint: authenticate an Insights user from a shared Supabase session.

    Used by the cross-app cookie SSO flow when a user navigates from
    Imagen/Scriptwriter to Insights via the AppSwitcher.

    1. Validates the Supabase JWT from the Authorization header
    2. Looks up the user by supabase_user_id
    3. Checks they have a valid (non-expired) Meta OAuth token
    4. Returns an Insights JWT if everything checks out

    Returns 403 if user not found or Facebook not linked.
    """
    # 1. Extract and validate Supabase JWT
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    supabase_token = auth_header.replace("Bearer ", "")
    supabase_payload = verify_supabase_token(supabase_token)

    supabase_user_id = supabase_payload.get("sub")
    if not supabase_user_id:
        raise HTTPException(status_code=401, detail="Invalid Supabase token: missing user ID")

    # 2. Look up user by supabase_user_id
    user = db.execute(
        select(models.User).where(models.User.supabase_user_id == supabase_user_id)
    ).scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=403,
            detail="User not found in Insights. Please connect your Facebook account first."
        )

    # 3. Check for a valid (non-expired) Meta OAuth token
    oauth_token = db.execute(
        select(models.OAuthToken).where(
            models.OAuthToken.user_id == user.id,
            models.OAuthToken.provider == "meta"
        )
    ).scalar_one_or_none()

    if not oauth_token:
        raise HTTPException(
            status_code=403,
            detail="Facebook not linked. Please connect your Facebook Ads account."
        )

    if oauth_token.expires_at and oauth_token.expires_at < datetime.utcnow():
        raise HTTPException(
            status_code=403,
            detail="Facebook token expired. Please reconnect your Facebook Ads account."
        )

    # 4. Generate Insights JWT
    insights_token = create_access_token(
        user_id=user.id,
        tenant_id=user.tenant_id
    )

    return JSONResponse({
        "success": True,
        "access_token": insights_token,
        "tenant_id": str(user.tenant_id),
        "user_id": str(user.id),
        "supabase_user_id": supabase_user_id,
    })


@router.post("/sync-facebook")
async def sync_facebook_token(
    request: Request,
    body: SyncFacebookRequest,
    db: Session = Depends(get_db)
):
    """
    Sync Facebook OAuth token from Supabase Auth to Insights backend.

    This endpoint bridges Supabase Auth (frontend) with Insights (backend):
    1. Validates the Supabase JWT to get supabase_user_id
    2. Exchanges the short-lived Facebook token for a long-lived one (60 days)
    3. Creates/updates user in Insights DB with supabase_user_id link
    4. Returns Insights JWT for subsequent API calls

    Headers:
        Authorization: Bearer <supabase_jwt>

    Body:
        provider_token: Facebook access token from Supabase session.provider_token
    """
    # 1. Extract and validate Supabase JWT
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    supabase_token = auth_header.replace("Bearer ", "")
    supabase_payload = verify_supabase_token(supabase_token)

    supabase_user_id = supabase_payload.get("sub")
    user_email = supabase_payload.get("email")

    if not supabase_user_id:
        raise HTTPException(status_code=401, detail="Invalid Supabase token: missing user ID")

    try:
        # 2. Exchange short-lived Facebook token for long-lived (60 days)
        token_data = await meta_client.exchange_short_to_long_token(body.provider_token)
        access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in") or 5184000  # ~60 days

        # 3. Debug token to get meta_user_id and verify scopes
        token_info = await meta_client.debug_token(access_token)
        meta_user_id = token_info["user_id"]
        scopes = token_info.get("scopes", [])

        # Verify required scope
        if "ads_read" not in scopes:
            raise HTTPException(
                status_code=403,
                detail="Missing required scope: ads_read. Please re-authorize with Facebook."
            )

        # 4. Get user info from Meta
        user_info = await meta_client.get_user_info(access_token, fields="id,name,email")
        user_name = user_info.get("name", "Unknown")
        meta_email = user_info.get("email")

        # Use Supabase email if Meta doesn't provide one
        final_email = meta_email or user_email or f"{meta_user_id}@noemail.facebook"
        if final_email:
            final_email = final_email.strip().lower()

        # 5. Get ad accounts
        ad_accounts = await meta_client.get_ad_accounts(
            access_token,
            fields="id,name,currency,timezone_name,account_status"
        )

        # Encrypt token
        token_encrypted = fernet.encrypt(access_token.encode()).decode()
        expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

        # 6. Transaction: create/update tenant, user, token, ad accounts
        with db.begin_nested():
            # 6a. Upsert tenant (keyed by meta_user_id)
            stmt = insert(models.Tenant).values(
                meta_user_id=meta_user_id,
                name=f"{user_name}'s Workspace",
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["meta_user_id"],
                set_={"name": stmt.excluded.name, "updated_at": func.now()},
            )
            db.execute(stmt)
            db.flush()

            tenant = db.execute(
                select(models.Tenant).where(models.Tenant.meta_user_id == meta_user_id)
            ).scalar_one()

            # 6b. Upsert user WITH supabase_user_id link
            stmt = insert(models.User).values(
                tenant_id=tenant.id,
                meta_user_id=meta_user_id,
                supabase_user_id=supabase_user_id,  # <-- The key link!
                email=final_email,
                name=user_name,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["tenant_id", "meta_user_id"],
                set_={
                    "email": stmt.excluded.email,
                    "name": stmt.excluded.name,
                    "supabase_user_id": stmt.excluded.supabase_user_id,
                    "updated_at": func.now(),
                },
            )
            db.execute(stmt)
            db.flush()

            user = db.execute(
                select(models.User).where(
                    models.User.tenant_id == tenant.id,
                    models.User.meta_user_id == meta_user_id
                )
            ).scalar_one()

            # 6c. Upsert OAuth token
            stmt = insert(models.OAuthToken).values(
                tenant_id=tenant.id,
                user_id=user.id,
                provider="meta",
                fb_user_id=meta_user_id,
                access_token=token_encrypted.encode(),
                expires_at=expires_at,
                scopes=scopes if isinstance(scopes, list) else [scopes],
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["user_id", "provider"],
                set_={
                    "access_token": stmt.excluded.access_token,
                    "expires_at": stmt.excluded.expires_at,
                    "scopes": stmt.excluded.scopes,
                    "fb_user_id": stmt.excluded.fb_user_id,
                },
            )
            db.execute(stmt)

            # 6d. Upsert ad accounts
            for account in ad_accounts:
                fb_account_id = account["id"]
                account_name = account.get("name", "Unknown")
                account_currency = account.get("currency")

                stmt = insert(models.AdAccount).values(
                    tenant_id=tenant.id,
                    fb_account_id=fb_account_id,
                    name=account_name,
                    currency=account_currency,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["tenant_id", "fb_account_id"],
                    set_={"name": stmt.excluded.name, "currency": stmt.excluded.currency},
                )
                db.execute(stmt)

            # 6e. Ensure subscription exists
            existing_subscription = db.execute(
                select(models.Subscription).where(models.Subscription.tenant_id == tenant.id)
            ).scalar_one_or_none()

            if not existing_subscription:
                subscription = models.Subscription(
                    tenant_id=tenant.id,
                    plan="free",
                    status="active",
                    quota_accounts=3,
                    quota_refresh_per_day=1,
                )
                db.add(subscription)

        db.commit()

        # 7. Generate Insights JWT
        insights_token = create_access_token(
            user_id=user.id,
            tenant_id=tenant.id
        )

        return JSONResponse({
            "success": True,
            "access_token": insights_token,
            "tenant_id": str(tenant.id),
            "user_id": str(user.id),
            "supabase_user_id": supabase_user_id,
            "meta_user_id": meta_user_id,
            "ad_accounts_count": len(ad_accounts),
            "message": "Facebook token synced successfully"
        })

    except MetaAPIError as e:
        raise HTTPException(status_code=502, detail=f"Meta API error: {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
