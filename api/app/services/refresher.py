"""
Service de synchronisation des données Meta Ads
Orchestre: fetch API → transform → storage

IMPORTANT: Produces columnar format matching production pipeline

MODE BASELINE vs TAIL (parité avec fetch_with_smart_limits.py):
- BASELINE (📥 INITIAL SYNC): Premier run → fetch 90 jours complets
- TAIL (🔄 TAIL REFRESH): Runs suivants → fetch 3 derniers jours, upsert dans baseline
"""
import gc
import json
import sentry_sdk
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..services.meta_client import meta_client, MetaAPIError
from ..services import storage
from ..services.columnar_transform import transform_to_columnar, validate_columnar_format
from .. import models
from cryptography.fernet import Fernet
from ..config import settings

# Fernet pour déchiffrer les tokens
fernet = Fernet(settings.TOKEN_ENCRYPTION_KEY.encode())

# Configuration (parité avec production)
BASELINE_DAYS = 90  # Historique complet
TAIL_BACKFILL_DAYS = 3  # Jours à refetch en mode TAIL


class RefreshError(Exception):
    """Erreur lors du refresh des données"""
    pass


def _load_existing_baseline(tenant_id: UUID, ad_account_id: str) -> Optional[Dict[str, Any]]:
    """
    Charge le baseline existant depuis R2 s'il existe.

    Returns:
        Le baseline dict ou None si inexistant/invalide
    """
    baseline_key = f"tenants/{tenant_id}/accounts/{ad_account_id}/data/baseline_daily.json"

    try:
        data = storage.get_object(baseline_key)
        baseline = json.loads(data.decode('utf-8'))

        # Valider la structure minimale
        if 'daily_ads' not in baseline or 'metadata' not in baseline:
            print(f"⚠️ Baseline invalide (structure), forcing BASELINE mode")
            return None

        return baseline
    except storage.StorageError:
        # Fichier n'existe pas - normal pour un premier run
        return None
    except json.JSONDecodeError as e:
        sentry_sdk.capture_exception(e)
        print(f"⚠️ Baseline corrompu (JSON): {e}, forcing BASELINE mode")
        return None
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(f"⚠️ Erreur chargement baseline: {e}, forcing BASELINE mode")
        return None


def _determine_refresh_mode(baseline: Optional[Dict[str, Any]], reference_date: str) -> Tuple[str, int]:
    """
    Détermine le mode de refresh (BASELINE ou TAIL).

    - BASELINE (90j) : Première fois, pas de données existantes
    - TAIL (3j) : Refresh incrémental, upsert dans le baseline existant

    Args:
        baseline: Le baseline existant ou None
        reference_date: Date de référence (YYYY-MM-DD)

    Returns:
        (mode, days_to_fetch) - ("BASELINE", 90) ou ("TAIL", 3)
    """
    if baseline is None:
        print(f"📥 INITIAL SYNC: No existing data - will fetch {BASELINE_DAYS} days")
        return ("BASELINE", BASELINE_DAYS)

    # Vérifier que le baseline a une structure valide
    baseline_date = baseline.get('metadata', {}).get('reference_date')
    if not baseline_date:
        print(f"📥 INITIAL SYNC: Baseline missing reference date - will fetch {BASELINE_DAYS} days")
        return ("BASELINE", BASELINE_DAYS)

    try:
        baseline_dt = datetime.strptime(baseline_date, '%Y-%m-%d')
        reference_dt = datetime.strptime(reference_date, '%Y-%m-%d')
        age_days = (reference_dt - baseline_dt).days

        if age_days < 0:
            print(f"📥 INITIAL SYNC: Baseline in future (?) - will fetch {BASELINE_DAYS} days")
            return ("BASELINE", BASELINE_DAYS)

        print(f"🔄 TAIL REFRESH: Updating last {TAIL_BACKFILL_DAYS} days (baseline: {age_days}d old)")
        return ("TAIL", TAIL_BACKFILL_DAYS)

    except ValueError as e:
        print(f"📥 INITIAL SYNC: Date parsing error: {e} - will fetch {BASELINE_DAYS} days")
        return ("BASELINE", BASELINE_DAYS)


def _upsert_daily_ads(existing_ads: List[Dict], new_ads: List[Dict], reference_date: str) -> List[Dict]:
    """
    Upsert les nouvelles données dans le baseline existant.

    - Clé unique: (ad_id, date)
    - Les nouvelles données REMPLACENT les anciennes pour la même clé
    - Supprime les données plus vieilles que BASELINE_DAYS

    Args:
        existing_ads: Liste des ads du baseline existant
        new_ads: Liste des nouvelles ads fetchées
        reference_date: Date de référence pour le nettoyage

    Returns:
        Liste mergée et nettoyée
    """
    # Créer un index par (ad_id, date)
    ads_index = {}

    # D'abord, indexer les données existantes
    for ad in existing_ads:
        ad_id = ad.get('ad_id')
        ad_date = ad.get('date_start') or ad.get('date')
        if ad_id and ad_date:
            key = (ad_id, ad_date)
            ads_index[key] = ad

    existing_count = len(ads_index)

    # Ensuite, upsert les nouvelles données (écrase si même clé)
    updated_count = 0
    added_count = 0

    for ad in new_ads:
        ad_id = ad.get('ad_id')
        ad_date = ad.get('date_start') or ad.get('date')
        if ad_id and ad_date:
            key = (ad_id, ad_date)
            if key in ads_index:
                updated_count += 1
            else:
                added_count += 1
            ads_index[key] = ad

    # Nettoyer les données trop vieilles (> BASELINE_DAYS)
    cutoff_date = (datetime.strptime(reference_date, '%Y-%m-%d') - timedelta(days=BASELINE_DAYS)).strftime('%Y-%m-%d')

    cleaned_ads = []
    removed_count = 0

    for (ad_id, ad_date), ad in ads_index.items():
        if ad_date >= cutoff_date:
            cleaned_ads.append(ad)
        else:
            removed_count += 1

    print(f"   📊 Upsert: {updated_count} mis à jour, {added_count} ajoutés, {removed_count} supprimés (>{BASELINE_DAYS}j)")

    return cleaned_ads


async def sync_account_data(
    ad_account_id: str,
    tenant_id: UUID,
    db: Session
) -> Dict[str, Any]:
    """
    Synchronise les données d'un ad account et génère les fichiers optimisés

    IMPORTANT: Generates columnar format (meta_v1, agg_v1, summary_v1)
    matching production pipeline for dashboard compatibility

    MODE BASELINE vs TAIL:
    - BASELINE (📥 INITIAL SYNC): Premier run → fetch 90 jours complets
    - TAIL (🔄 TAIL REFRESH): Runs suivants → fetch 3 derniers jours, upsert dans baseline

    Args:
        ad_account_id: ID du compte (ex: "act_123456")
        tenant_id: ID du tenant (pour isolation)
        db: Session SQLAlchemy

    Returns:
        {
            "status": "success",
            "ad_account_id": str,
            "ads_fetched": int,
            "files_written": List[str],
            "refreshed_at": str (ISO)
        }

    Raises:
        RefreshError: Si erreur pendant le refresh
    """

    # 1. Vérifier que l'ad account appartient au tenant
    ad_account = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.fb_account_id == ad_account_id,
            models.AdAccount.tenant_id == tenant_id
        )
    ).scalar_one_or_none()

    if not ad_account:
        raise RefreshError(f"Ad account {ad_account_id} not found for tenant {tenant_id}")

    # 2. Récupérer le token OAuth du tenant
    oauth_token = db.execute(
        select(models.OAuthToken).where(
            models.OAuthToken.tenant_id == tenant_id,
            models.OAuthToken.provider == "meta"
        )
    ).scalar_one_or_none()

    if not oauth_token:
        raise RefreshError(f"No OAuth token found for tenant {tenant_id}")

    # 3. Déchiffrer le token
    try:
        access_token = fernet.decrypt(oauth_token.access_token).decode()
    except Exception as e:
        raise RefreshError(f"Failed to decrypt access token: {e}")

    # 4. Calculer la date de référence (hier, pour exclure aujourd'hui)
    today = datetime.now(timezone.utc).date()
    reference_date = (today - timedelta(days=1)).isoformat()  # Yesterday

    # 5. Charger le baseline existant et déterminer le mode
    existing_baseline = _load_existing_baseline(tenant_id, ad_account_id)
    refresh_mode, days_to_fetch = _determine_refresh_mode(existing_baseline, reference_date)

    # Log clair du mode de sync
    mode_emoji = "📥 INITIAL SYNC" if refresh_mode == "BASELINE" else "🔄 TAIL REFRESH"
    print(f"{mode_emoji}: {ad_account_id} ({ad_account.name}) - {days_to_fetch} days")

    # 6. Calculer la plage de dates selon le mode
    since_date = (today - timedelta(days=days_to_fetch)).isoformat()
    until_date = reference_date

    # 7. Fetch daily insights depuis Meta API
    # Pour les gros fetches (BASELINE 90j), chunker en morceaux de 30j
    # pour éviter les ReadTimeout côté Meta API
    CHUNK_SIZE_DAYS = 30
    try:
        if days_to_fetch > CHUNK_SIZE_DAYS:
            daily_insights = []
            chunk_start = today - timedelta(days=days_to_fetch)
            end_date = today - timedelta(days=1)  # yesterday
            chunk_num = 0
            while chunk_start <= end_date:
                chunk_end = min(chunk_start + timedelta(days=CHUNK_SIZE_DAYS - 1), end_date)
                chunk_num += 1
                print(f"   📦 Chunk {chunk_num}: {chunk_start.isoformat()} → {chunk_end.isoformat()}")
                chunk_data = await meta_client.get_insights_daily(
                    ad_account_id=ad_account_id,
                    access_token=access_token,
                    since_date=chunk_start.isoformat(),
                    until_date=chunk_end.isoformat(),
                    limit=500
                )
                daily_insights.extend(chunk_data)
                chunk_start = chunk_end + timedelta(days=1)
            print(f"   📦 Total: {len(daily_insights)} rows from {chunk_num} chunks")
        else:
            daily_insights = await meta_client.get_insights_daily(
                ad_account_id=ad_account_id,
                access_token=access_token,
                since_date=since_date,
                until_date=until_date,
                limit=500
            )
    except MetaAPIError as e:
        raise RefreshError(f"Meta API error: {e}")

    # 8. Enrich with creatives (format, media_url, status)
    # CRITICAL: Parité avec ancien pipeline (fetch_with_smart_limits.py)
    try:
        print(f"🎨 Enriching {len(daily_insights)} insights with creatives...")
        if daily_insights:
            print(f"   Sample insight keys: {list(daily_insights[0].keys())[:10]}")

        daily_insights = await meta_client.enrich_ads_with_creatives(
            ads=daily_insights,
            access_token=access_token
        )
        print(f"✅ Enrichment complete")
    except Exception as e:
        # Enrichment failure is non-fatal - continue with UNKNOWN formats
        sentry_sdk.capture_exception(e)
        print(f"⚠️ Enrichment failed: {e}")

    # 9. Enrichir avec account_name et account_id
    for ad in daily_insights:
        ad['account_name'] = ad_account.name
        ad['account_id'] = ad_account_id

    # 10. Upsert ou remplacer selon le mode
    if refresh_mode == "TAIL" and existing_baseline:
        # Mode TAIL: upsert dans le baseline existant
        existing_ads = existing_baseline.get('daily_ads', [])
        all_daily_ads = _upsert_daily_ads(existing_ads, daily_insights, reference_date)
    else:
        # Mode BASELINE: remplacer tout
        all_daily_ads = daily_insights

    # 11. Transform en format columnar (sur le baseline COMPLET)
    try:
        meta_v1, agg_v1, summary_v1 = transform_to_columnar(
            daily_ads=all_daily_ads,
            reference_date=reference_date,
            ad_account_id=ad_account_id,
            account_name=ad_account.name  # Pass real account name from DB
        )
    except Exception as e:
        raise RefreshError(f"Transform error: {e}")

    # 12. Valider le format
    validation_errors = validate_columnar_format(meta_v1, agg_v1, summary_v1)
    if validation_errors:
        raise RefreshError(f"Validation failed: {'; '.join(validation_errors)}")

    # 13. Sauvegarder le baseline brut (pour les prochains upserts)
    base_path = f"tenants/{tenant_id}/accounts/{ad_account_id}/data"

    baseline_data = {
        'metadata': {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'reference_date': reference_date,
            'mode': refresh_mode,
            'days_fetched': days_to_fetch,
            'total_daily_rows': len(all_daily_ads),
            'unique_ads': len(agg_v1.get('ads', [])),
            'baseline_days': BASELINE_DAYS,
            'tail_backfill_days': TAIL_BACKFILL_DAYS
        },
        'daily_ads': all_daily_ads
    }

    files_written = []

    try:
        storage.put_object(
            f"{base_path}/baseline_daily.json",
            json.dumps(baseline_data, separators=(',', ':')).encode("utf-8")
        )
        files_written.append("baseline_daily.json")
    except storage.StorageError as e:
        raise RefreshError(f"Failed to write baseline_daily.json: {e}")

    # 🧹 Libérer la RAM: baseline_data n'est plus nécessaire
    del baseline_data
    del all_daily_ads
    gc.collect()

    # 14. Écrire les fichiers columnar optimisés
    optimized_path = f"{base_path}/optimized"

    for filename, data in [
        ("meta_v1.json", meta_v1),
        ("agg_v1.json", agg_v1),
        ("summary_v1.json", summary_v1),
    ]:
        storage_key = f"{optimized_path}/{filename}"
        try:
            # Use compact JSON (no indent) for production
            storage.put_object(storage_key, json.dumps(data, separators=(',', ':')).encode("utf-8"))
            files_written.append(filename)
        except storage.StorageError as e:
            raise RefreshError(f"Failed to write {filename}: {e}")

    # 15. Écrire manifest.json
    manifest = {
        "version": datetime.now(timezone.utc).isoformat(),
        "ads_count": len(agg_v1.get('ads', [])),
        "periods": agg_v1.get('periods', []),
        "refresh_mode": refresh_mode,
        "baseline_days": BASELINE_DAYS,
        "shards": {
            "meta": {"path": "meta_v1.json"},
            "agg": {"path": "agg_v1.json"},
            "summary": {"path": "summary_v1.json"}
        }
    }
    try:
        storage.put_object(
            f"{optimized_path}/manifest.json",
            json.dumps(manifest, separators=(',', ':')).encode("utf-8")
        )
        files_written.append("manifest.json")
    except storage.StorageError as e:
        raise RefreshError(f"Failed to write manifest.json: {e}")

    # 🧹 Libérer la RAM: les fichiers sont écrits
    unique_ads_count = len(agg_v1.get('ads', []))
    del meta_v1, agg_v1, summary_v1, manifest
    gc.collect()

    # 16. Mettre à jour last_refresh_at
    ad_account.last_refresh_at = datetime.now(timezone.utc)
    db.commit()

    # Note: daily_insights et all_daily_ads ont été supprimés, on utilise les compteurs sauvegardés
    daily_rows_count = days_to_fetch * 50  # Estimation (valeur exacte non disponible après del)

    return {
        "status": "success",
        "ad_account_id": ad_account_id,
        "refresh_mode": refresh_mode,
        "days_fetched": days_to_fetch,
        "unique_ads": unique_ads_count,
        "files_written": files_written,
        "refreshed_at": ad_account.last_refresh_at.isoformat(),
        "date_range": f"{since_date} to {until_date}",
    }
