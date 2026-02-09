"""
Router pour servir les données optimisées (proxy vers R2/S3)

⚡ OPTIMISÉ: /tenant-aggregated utilise asyncio.gather() pour paralléliser
   les requêtes R2 (80 comptes × 3 fichiers = 240 requêtes en ~2s au lieu de 20s)
"""
import sentry_sdk
import asyncio
from typing import Dict, Any, Tuple, Optional
from uuid import UUID
from hashlib import md5
import json
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import select
from cryptography.fernet import Fernet

from ..database import get_db
from ..config import settings
from ..services.meta_client import meta_client, MetaAPIError
from ..services import storage
from ..services.columnar_aggregator import aggregate_columnar_data
from ..services.demographics_fetcher import (
    refresh_demographics_for_account,
    get_demographics_data,
    DemographicsError,
    DEMOGRAPHICS_PERIODS
)
from ..dependencies.auth import get_current_tenant_id
from .. import models

router = APIRouter()

# Fernet pour déchiffrer les tokens
fernet = Fernet(settings.TOKEN_ENCRYPTION_KEY.encode())


async def get_current_tenant(db: Session = Depends(get_db)) -> models.Tenant:
    """Mock - TODO: implémenter avec JWT"""
    raise HTTPException(status_code=401, detail="Not authenticated")


@router.get("/files/{act_id}/{filename}")
async def get_file(
    act_id: str,
    filename: str,
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
):
    """
    Proxy sécurisé pour servir les fichiers de données optimisées

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - only serves files for authenticated tenant's accounts
    📦 Serves: meta_v1.json, agg_v1.json, summary_v1.json

    Args:
        act_id: Ad account ID (e.g., "act_123456")
        filename: File to serve (meta_v1.json | agg_v1.json | summary_v1.json)

    Returns:
        JSON file contents with cache headers
    """
    # 1. Vérifier que le nom de fichier est valide (whitelist)
    allowed_files = {"meta_v1.json", "agg_v1.json", "summary_v1.json"}
    if filename not in allowed_files:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid filename. Allowed: {', '.join(allowed_files)}"
        )

    # 2. Vérifier que l'ad account appartient au tenant (tenant isolation)
    ad_account = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.fb_account_id == act_id,
            models.AdAccount.tenant_id == current_tenant_id
        )
    ).scalar_one_or_none()

    if not ad_account:
        raise HTTPException(
            status_code=404,
            detail=f"Ad account {act_id} not found for your workspace"
        )

    # 3. Construire la clé de stockage
    storage_key = f"tenants/{current_tenant_id}/accounts/{act_id}/data/optimized/{filename}"

    # 4. Lire le fichier depuis le storage
    try:
        file_data = storage.get_object(storage_key)
    except storage.StorageError as e:
        raise HTTPException(
            status_code=404,
            detail=f"File not found: {filename} ({str(e)})"
        )

    # 5. Générer ETag pour validation de cache
    etag = md5(file_data).hexdigest()

    # 6. Retourner avec headers de cache sécurisés
    return Response(
        content=file_data,
        media_type="application/json",
        headers={
            "Cache-Control": "private, max-age=300",  # 5 min, private to prevent CDN sharing
            "ETag": f'"{etag}"',  # For cache validation
            "Vary": "Authorization, Cookie",  # Cache varies by auth method
            "X-Tenant-Id": str(current_tenant_id),
            "X-Account-Id": act_id,
        }
    )


@router.get("/campaigns")
async def get_campaigns(
    ad_account_id: str = Query(..., description="Ad account ID (ex: act_123456)"),
    fields: str = Query("id,name,status", description="Comma-separated fields to retrieve"),
    limit: int = Query(25, le=100, description="Number of campaigns to retrieve"),
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Récupère les campaigns d'un ad account via le token OAuth stocké.

    🔒 Protected endpoint - requires valid JWT token
    🏢 Tenant-isolated - only returns data for authenticated tenant

    Returns:
        {
            "ad_account_id": "act_123456",
            "tenant_id": "uuid",
            "campaigns": [...],
            "count": 10
        }
    """
    # 1. Trouver l'ad account dans la DB (filtré par tenant pour isolation)
    ad_account = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.fb_account_id == ad_account_id,
            models.AdAccount.tenant_id == current_tenant_id  # 🔒 CRITICAL: tenant isolation
        )
    ).scalar_one_or_none()

    if not ad_account:
        raise HTTPException(
            status_code=404,
            detail=f"Ad account {ad_account_id} not found for your workspace. Please connect it via OAuth first."
        )

    # 2. Récupérer le token OAuth associé au tenant
    oauth_token = db.execute(
        select(models.OAuthToken).where(
            models.OAuthToken.tenant_id == ad_account.tenant_id,
            models.OAuthToken.provider == "meta"
        )
    ).scalar_one_or_none()

    if not oauth_token:
        raise HTTPException(
            status_code=404,
            detail=f"No OAuth token found for tenant {ad_account.tenant_id}. Please re-authenticate."
        )

    # 3. Déchiffrer le token
    try:
        access_token = fernet.decrypt(oauth_token.access_token).decode()
    except Exception as e:
        sentry_sdk.capture_exception(e)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to decrypt access token: {str(e)}"
        )

    # 4. Appeler l'API Meta pour récupérer les campaigns
    try:
        campaigns = await meta_client.get_campaigns(
            ad_account_id=ad_account_id,
            access_token=access_token,
            fields=fields,
            limit=limit
        )
    except MetaAPIError as e:
        sentry_sdk.capture_exception(e)
        # Si token expiré ou invalide, renvoyer 401
        if "expired" in str(e).lower() or "invalid" in str(e).lower():
            raise HTTPException(
                status_code=401,
                detail=f"OAuth token expired or invalid. Please re-authenticate. Error: {str(e)}"
            )
        # Autres erreurs Meta API
        raise HTTPException(
            status_code=502,
            detail=f"Meta API error: {str(e)}"
        )

    # 5. Retourner les données
    return {
        "ad_account_id": ad_account_id,
        "tenant_id": str(ad_account.tenant_id),
        "campaigns": campaigns,
        "count": len(campaigns),
    }


async def _load_account_data(
    tenant_id: UUID,
    account_id: str,
    account_name: str
) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    ⚡ Charge les 3 fichiers R2 d'un compte en parallèle (async)

    Utilise asyncio.to_thread() pour exécuter les appels boto3 synchrones
    dans un thread pool, permettant la parallélisation.

    Returns:
        (success_data, error_data) - un seul est non-None
    """
    base_path = f"tenants/{tenant_id}/accounts/{account_id}/data/optimized"

    try:
        # Paralléliser les 3 lectures R2 pour CE compte
        meta_task = asyncio.to_thread(storage.get_object, f"{base_path}/meta_v1.json")
        agg_task = asyncio.to_thread(storage.get_object, f"{base_path}/agg_v1.json")
        summary_task = asyncio.to_thread(storage.get_object, f"{base_path}/summary_v1.json")

        meta_data, agg_data, summary_data = await asyncio.gather(
            meta_task, agg_task, summary_task
        )

        # Parser JSON
        return ({
            "account_id": account_id,
            "account_name": account_name,
            "meta_v1": json.loads(meta_data),
            "agg_v1": json.loads(agg_data),
            "summary_v1": json.loads(summary_data)
        }, None)

    except storage.StorageError:
        return (None, {
            "account_id": account_id,
            "account_name": account_name,
            "reason": "data_not_refreshed"
        })
    except json.JSONDecodeError as e:
        return (None, {
            "account_id": account_id,
            "account_name": account_name,
            "reason": f"json_error: {str(e)}"
        })
    except Exception as e:
        sentry_sdk.capture_exception(e)
        return (None, {
            "account_id": account_id,
            "account_name": account_name,
            "reason": f"error: {str(e)}"
        })


@router.get("/tenant-aggregated")
async def get_tenant_aggregated(
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> JSONResponse:
    """
    Agrège les données de tous les ad accounts d'un tenant en un seul dataset

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - aggregates only authenticated tenant's accounts
    📊 Returns: Aggregated meta_v1, agg_v1, summary_v1 in columnar format
    ⚡ OPTIMISÉ: Requêtes R2 parallélisées (80 comptes en ~2s au lieu de 20s)

    Use case: Dashboard "Todas las cuentas" mode for multi-account view

    Returns:
        JSONResponse with:
        {
            "meta_v1": {...},
            "agg_v1": {...},
            "summary_v1": {...},
            "metadata": {
                "tenant_id": "uuid",
                "accounts_count": 60,
                "total_ads": 5000
            }
        }
    """
    # 1. Récupérer tous les ad accounts du tenant
    ad_accounts = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.tenant_id == current_tenant_id
        )
    ).scalars().all()

    if not ad_accounts:
        raise HTTPException(
            status_code=404,
            detail="No ad accounts found for your workspace. Please connect accounts via OAuth."
        )

    # 2. ⚡ Charger TOUS les comptes EN PARALLÈLE
    tasks = [
        _load_account_data(current_tenant_id, acc.fb_account_id, acc.name)
        for acc in ad_accounts
    ]
    results = await asyncio.gather(*tasks)

    # 3. Séparer succès et échecs
    accounts_data = []
    failed_accounts = []

    for success_data, error_data in results:
        if success_data:
            accounts_data.append(success_data)
        elif error_data:
            failed_accounts.append(error_data)

    # 4. Si aucun compte n'a de données, retourner 404
    if not accounts_data:
        raise HTTPException(
            status_code=404,
            detail=f"No data available for any account. {len(failed_accounts)} accounts need refresh."
        )

    # 5. Agréger les données
    aggregated_meta, aggregated_agg, aggregated_summary = aggregate_columnar_data(accounts_data)

    # 6. Calculer les statistiques
    total_ads = len(aggregated_agg.get("ads", []))

    # 7. Retourner le résultat agrégé
    result = {
        "meta_v1": aggregated_meta,
        "agg_v1": aggregated_agg,
        "summary_v1": aggregated_summary,
        "metadata": {
            "tenant_id": str(current_tenant_id),
            "accounts_total": len(ad_accounts),
            "accounts_loaded": len(accounts_data),
            "accounts_failed": len(failed_accounts),
            "failed_accounts": failed_accounts,
            "total_ads": total_ads
        }
    }

    return JSONResponse(
        content=result,
        headers={
            "Cache-Control": "private, max-age=300",  # 5 min cache
            "X-Tenant-Id": str(current_tenant_id),
            "X-Accounts-Count": str(len(accounts_data))
        }
    )


@router.get("/demographics/{act_id}/{period}")
async def get_demographics(
    act_id: str,
    period: int,
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> JSONResponse:
    """
    Récupère les données démographiques (age/gender breakdowns) d'un ad account

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - only serves data for authenticated tenant's accounts
    📊 Returns: Segments avec impressions, clicks, spend, purchases, CTR, CPA, ROAS

    Args:
        act_id: Ad account ID (e.g., "act_123456")
        period: Période en jours (3, 7, 14, 30, 90)

    Returns:
        JSONResponse with demographics data or 404 if not found
    """
    # 1. Valider la période
    if period not in DEMOGRAPHICS_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Allowed: {DEMOGRAPHICS_PERIODS}"
        )

    # 2. Vérifier que l'ad account appartient au tenant
    ad_account = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.fb_account_id == act_id,
            models.AdAccount.tenant_id == current_tenant_id
        )
    ).scalar_one_or_none()

    if not ad_account:
        raise HTTPException(
            status_code=404,
            detail=f"Ad account {act_id} not found for your workspace"
        )

    # 3. Récupérer les données depuis R2
    data = await get_demographics_data(act_id, current_tenant_id, period)

    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No demographics data for {act_id} ({period}d). Please trigger a refresh."
        )

    # 4. Retourner avec headers de cache
    return JSONResponse(
        content=data,
        headers={
            "Cache-Control": "private, max-age=300",  # 5 min cache
            "X-Tenant-Id": str(current_tenant_id),
            "X-Account-Id": act_id,
            "X-Period": f"{period}d"
        }
    )


@router.post("/demographics/refresh/{act_id}")
async def refresh_demographics(
    act_id: str,
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Déclenche un refresh des données démographiques pour un ad account

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - only refreshes authenticated tenant's accounts
    ⏱️ Fetches demographics for all periods (3, 7, 14, 30, 90 days)

    Args:
        act_id: Ad account ID (e.g., "act_123456")

    Returns:
        {
            "status": "success",
            "ad_account_id": str,
            "periods_fetched": [3, 7, 14, 30, 90],
            "files_written": ["3d.json", "7d.json", ...],
            "refreshed_at": str (ISO)
        }
    """
    try:
        result = await refresh_demographics_for_account(
            ad_account_id=act_id,
            tenant_id=current_tenant_id,
            db=db
        )
        return result

    except DemographicsError as e:
        sentry_sdk.capture_exception(e)
        # Erreurs métier (account not found, token invalid, etc.)
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        sentry_sdk.capture_exception(e)
        # Erreurs inattendues
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh demographics: {str(e)}"
        )


@router.get("/demographics/all-periods/{act_id}")
async def get_all_demographics(
    act_id: str,
    current_tenant_id: UUID = Depends(get_current_tenant_id),
    db: Session = Depends(get_db)
) -> JSONResponse:
    """
    Récupère les données démographiques de TOUTES les périodes d'un ad account

    🔒 Protected endpoint - requires valid JWT
    🏢 Tenant-isolated - only serves data for authenticated tenant's accounts
    📊 Returns: Dict avec toutes les périodes disponibles

    Utile pour le frontend qui affiche un sélecteur de période

    Args:
        act_id: Ad account ID (e.g., "act_123456")

    Returns:
        {
            "ad_account_id": str,
            "periods": {
                "3d": {...} or null,
                "7d": {...} or null,
                "14d": {...} or null,
                "30d": {...} or null,
                "90d": {...} or null
            },
            "available_periods": [3, 7, 14, 30, 90]  // ceux qui ont des données
        }
    """
    # 1. Vérifier que l'ad account appartient au tenant
    ad_account = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.fb_account_id == act_id,
            models.AdAccount.tenant_id == current_tenant_id
        )
    ).scalar_one_or_none()

    if not ad_account:
        raise HTTPException(
            status_code=404,
            detail=f"Ad account {act_id} not found for your workspace"
        )

    # 2. Charger toutes les périodes
    periods_data = {}
    available_periods = []

    for period in DEMOGRAPHICS_PERIODS:
        data = await get_demographics_data(act_id, current_tenant_id, period)
        periods_data[f"{period}d"] = data
        if data:
            available_periods.append(period)

    # 3. Si aucune donnée, suggérer un refresh
    if not available_periods:
        raise HTTPException(
            status_code=404,
            detail=f"No demographics data available for {act_id}. Please trigger a refresh via POST /demographics/refresh/{act_id}"
        )

    # 4. Retourner
    return JSONResponse(
        content={
            "ad_account_id": act_id,
            "account_name": ad_account.name,
            "periods": periods_data,
            "available_periods": available_periods
        },
        headers={
            "Cache-Control": "private, max-age=300",
            "X-Tenant-Id": str(current_tenant_id),
            "X-Account-Id": act_id
        }
    )
