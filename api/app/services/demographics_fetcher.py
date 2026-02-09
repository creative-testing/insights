"""
Service de fetch des données démographiques (age/gender breakdowns)
Adapté de scripts/production/fetch_demographics.py pour le SaaS

Fetch les insights avec breakdowns age/gender, agrège par segment,
calcule les métriques (CTR, CPA, ROAS) et stocke dans R2.
"""
import json
import sentry_sdk
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy import select
from cryptography.fernet import Fernet

from ..services.meta_client import meta_client, MetaAPIError
from ..services import storage
from .. import models
from ..config import settings

# Fernet pour déchiffrer les tokens
fernet = Fernet(settings.TOKEN_ENCRYPTION_KEY.encode())

# Périodes à fetcher (correspondent aux boutons de l'UI)
DEMOGRAPHICS_PERIODS = [3, 7, 14, 30, 90]


class DemographicsError(Exception):
    """Erreur lors du fetch des demographics"""
    pass


def _extract_purchase_data(actions: List[Dict], action_values: List[Dict]) -> Tuple[float, float]:
    """
    Extrait purchases et purchase_value depuis actions/action_values.
    Suit la même logique que le pipeline principal.
    """
    purchases = 0.0
    purchase_value = 0.0

    # Ordre de priorité pour les conversions
    purchase_keys = [
        'omni_purchase',
        'purchase',
        'onsite_conversion.purchase',
        'offsite_conversion.fb_pixel_purchase',
        'catalog_sale'
    ]

    # Extraire purchases (nombre)
    if actions:
        for key in purchase_keys:
            for action in actions:
                if action.get('action_type') == key:
                    try:
                        purchases = float(action.get('value', 0))
                        break
                    except (TypeError, ValueError):
                        continue
            if purchases > 0:
                break

    # Extraire purchase_value (montant)
    if action_values:
        for key in purchase_keys:
            for action in action_values:
                if action.get('action_type') == key:
                    try:
                        purchase_value = float(action.get('value', 0))
                        break
                    except (TypeError, ValueError):
                        continue
            if purchase_value > 0:
                break

    return purchases, purchase_value


def _aggregate_segments(raw_results: List[Dict]) -> List[Dict]:
    """
    Agrège les résultats par segment (age, gender).
    Retourne une liste de segments avec métriques calculées.
    """
    segments_map = defaultdict(lambda: {
        'impressions': 0,
        'clicks': 0,
        'spend': 0.0,
        'purchases': 0.0,
        'purchase_value': 0.0
    })

    for row in raw_results:
        age = row.get('age', 'unknown')
        gender = row.get('gender', 'unknown')
        key = (age, gender)

        # Agréger les métriques de base
        segments_map[key]['impressions'] += int(row.get('impressions', 0))
        segments_map[key]['clicks'] += int(row.get('clicks', 0))
        segments_map[key]['spend'] += float(row.get('spend', 0))

        # Extraire purchases et purchase_value
        actions = row.get('actions', [])
        action_values = row.get('action_values', [])
        purchases, purchase_value = _extract_purchase_data(actions, action_values)

        segments_map[key]['purchases'] += purchases
        segments_map[key]['purchase_value'] += purchase_value

    # Convertir en liste et calculer les métriques dérivées
    segments = []
    for (age, gender), metrics in segments_map.items():
        segment = {
            'age': age,
            'gender': gender,
            'impressions': metrics['impressions'],
            'clicks': metrics['clicks'],
            'spend': round(metrics['spend'], 2),
            'purchases': int(metrics['purchases']),
            'purchase_value': round(metrics['purchase_value'], 2),
            # Métriques calculées
            'ctr': round((metrics['clicks'] / metrics['impressions'] * 100) if metrics['impressions'] > 0 else 0, 2),
            'cpa': round((metrics['spend'] / metrics['purchases']) if metrics['purchases'] > 0 else 0, 2),
            'roas': round((metrics['purchase_value'] / metrics['spend']) if metrics['spend'] > 0 else 0, 2)
        }
        segments.append(segment)

    # Trier par spend décroissant
    segments.sort(key=lambda x: x['spend'], reverse=True)

    return segments


def _calculate_totals(segments: List[Dict]) -> Dict:
    """Calcule les totaux à partir des segments"""
    totals = {
        'impressions': sum(s['impressions'] for s in segments),
        'clicks': sum(s['clicks'] for s in segments),
        'spend': round(sum(s['spend'] for s in segments), 2),
        'purchases': sum(s['purchases'] for s in segments),
        'purchase_value': round(sum(s['purchase_value'] for s in segments), 2)
    }

    # Calculer ROAS global
    totals['roas'] = round(
        (totals['purchase_value'] / totals['spend']) if totals['spend'] > 0 else 0,
        2
    )

    return totals


async def refresh_demographics_for_account(
    ad_account_id: str,
    tenant_id: UUID,
    db: Session
) -> Dict[str, Any]:
    """
    Refresh les données démographiques d'un ad account pour toutes les périodes.

    Args:
        ad_account_id: ID du compte (ex: "act_123456")
        tenant_id: ID du tenant (pour isolation)
        db: Session SQLAlchemy

    Returns:
        {
            "status": "success",
            "ad_account_id": str,
            "periods_fetched": List[int],
            "files_written": List[str],
            "refreshed_at": str (ISO)
        }

    Raises:
        DemographicsError: Si erreur pendant le fetch
    """
    # 1. Vérifier que l'ad account appartient au tenant
    ad_account = db.execute(
        select(models.AdAccount).where(
            models.AdAccount.fb_account_id == ad_account_id,
            models.AdAccount.tenant_id == tenant_id
        )
    ).scalar_one_or_none()

    if not ad_account:
        raise DemographicsError(f"Ad account {ad_account_id} not found for tenant {tenant_id}")

    # 2. Récupérer le token OAuth du tenant
    oauth_token = db.execute(
        select(models.OAuthToken).where(
            models.OAuthToken.tenant_id == tenant_id,
            models.OAuthToken.provider == "meta"
        )
    ).scalar_one_or_none()

    if not oauth_token:
        raise DemographicsError(f"No OAuth token found for tenant {tenant_id}")

    # 3. Déchiffrer le token
    try:
        access_token = fernet.decrypt(oauth_token.access_token).decode()
    except Exception as e:
        raise DemographicsError(f"Failed to decrypt access token: {e}")

    # 4. Date de référence (hier)
    today = datetime.now(timezone.utc).date()
    reference_date = today - timedelta(days=1)

    # 5. Fetch pour chaque période
    files_written = []
    periods_fetched = []
    base_path = f"tenants/{tenant_id}/accounts/{ad_account_id}/demographics"

    for period_days in DEMOGRAPHICS_PERIODS:
        since_date = (reference_date - timedelta(days=period_days - 1)).isoformat()
        until_date = reference_date.isoformat()

        try:
            print(f"  📊 Fetching demographics for {ad_account.name} ({period_days}d)...")

            # Fetch les données brutes
            raw_results = await meta_client.get_demographics(
                ad_account_id=ad_account_id,
                access_token=access_token,
                since_date=since_date,
                until_date=until_date
            )

            if not raw_results:
                print(f"    ⚠️ No data for {period_days}d")
                continue

            # Agréger par segment
            segments = _aggregate_segments(raw_results)
            totals = _calculate_totals(segments)

            # Construire le résultat
            result = {
                "metadata": {
                    "account_id": ad_account_id,
                    "account_name": ad_account.name,
                    "period": f"{period_days}d",
                    "date_range": f"{since_date}..{until_date}",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "source": "insights(level=account, breakdowns=[age,gender])"
                },
                "segments": segments,
                "totals": totals
            }

            # Sauvegarder dans R2
            storage_key = f"{base_path}/{period_days}d.json"
            storage.put_object(
                storage_key,
                json.dumps(result, separators=(',', ':')).encode("utf-8")
            )

            files_written.append(f"{period_days}d.json")
            periods_fetched.append(period_days)
            print(f"    ✅ {period_days}d: {len(segments)} segments, ${totals['spend']:,.2f} spend")

        except MetaAPIError as e:
            sentry_sdk.capture_exception(e)
            print(f"    ❌ {period_days}d: API error - {e}")
            # Continue avec les autres périodes
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print(f"    ❌ {period_days}d: Error - {e}")
            # Continue avec les autres périodes

    return {
        "status": "success",
        "ad_account_id": ad_account_id,
        "periods_fetched": periods_fetched,
        "files_written": files_written,
        "refreshed_at": datetime.now(timezone.utc).isoformat()
    }


async def get_demographics_data(
    ad_account_id: str,
    tenant_id: UUID,
    period: int
) -> Optional[Dict[str, Any]]:
    """
    Récupère les données démographiques depuis R2.

    Args:
        ad_account_id: ID du compte
        tenant_id: ID du tenant
        period: Période en jours (3, 7, 14, 30, 90)

    Returns:
        Les données JSON ou None si inexistantes
    """
    storage_key = f"tenants/{tenant_id}/accounts/{ad_account_id}/demographics/{period}d.json"

    try:
        data = storage.get_object(storage_key)
        return json.loads(data.decode('utf-8'))
    except storage.StorageError:
        return None
    except json.JSONDecodeError:
        return None
